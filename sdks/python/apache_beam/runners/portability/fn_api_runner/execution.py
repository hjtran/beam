#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Set of utilities for execution of a pipeline by the FnApiRunner."""

# mypy: disallow-untyped-defs

import collections
import copy
import itertools
import logging
import struct
import typing
import uuid
import weakref
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import DefaultDict
from typing import Dict
from typing import Generic
from typing import Iterable
from typing import Iterator
from typing import List
from typing import MutableMapping
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import TypeVar
from typing import Union

from typing_extensions import Protocol

from apache_beam import coders
from apache_beam.coders.coder_impl import CoderImpl
from apache_beam.coders.coder_impl import WindowedValueCoderImpl
from apache_beam.coders.coder_impl import create_InputStream
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.coders.coders import WindowedValueCoder
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners import pipeline_context
from apache_beam.runners.common import ENCODED_IMPULSE_VALUE
from apache_beam.runners.direct.clock import RealClock
from apache_beam.runners.direct.clock import TestClock
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner.translations import DataInput
from apache_beam.runners.portability.fn_api_runner.translations import DataOutput
from apache_beam.runners.portability.fn_api_runner.translations import OutputTimers
from apache_beam.runners.portability.fn_api_runner.translations import Stage
from apache_beam.runners.portability.fn_api_runner.translations import create_buffer_id
from apache_beam.runners.portability.fn_api_runner.translations import only_element
from apache_beam.runners.portability.fn_api_runner.translations import split_buffer_id
from apache_beam.runners.portability.fn_api_runner.translations import unique_name
from apache_beam.runners.portability.fn_api_runner.watermark_manager import WatermarkManager
from apache_beam.runners.worker import bundle_processor
from apache_beam.transforms import core
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.window import BoundedWindow
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils import proto_utils
from apache_beam.utils import windowed_value
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

if TYPE_CHECKING:
  from apache_beam.runners.portability.fn_api_runner import worker_handlers
  from apache_beam.runners.portability.fn_api_runner.translations import DataSideInput

_LOGGER = logging.getLogger(__name__)

SAFE_WINDOW_FNS = set(
    window.WindowFn._known_urns.keys()) - {python_urns.PICKLED_WINDOWFN}


class Buffer(Protocol):
  def __iter__(self) -> Iterator[bytes]:
    pass

  def append(self, item: bytes) -> None:
    pass

  def extend(self, other: 'Buffer') -> None:
    pass


class PartitionableBuffer(Buffer, Protocol):
  def copy(self) -> 'PartitionableBuffer':
    pass

  def partition(self, n: int) -> List[List[bytes]]:
    pass

  @property
  def cleared(self) -> bool:
    pass

  def clear(self) -> None:
    pass

  def reset(self) -> None:
    pass


class ListBuffer:
  """Used to support parititioning of a list."""
  def __init__(self, coder_impl: Optional[CoderImpl]) -> None:
    self._coder_impl = coder_impl or CoderImpl()
    self._inputs: List[bytes] = []
    self._grouped_output: Optional[List[List[bytes]]] = None
    self.cleared = False

  def copy(self) -> 'ListBuffer':
    new = ListBuffer(self._coder_impl)
    new._inputs = [v for v in self._inputs]
    return new

  def extend(self, extra: 'Buffer') -> None:
    if self.cleared:
      raise RuntimeError('Trying to append to a cleared ListBuffer.')
    if self._grouped_output:
      raise RuntimeError('ListBuffer append after read.')
    assert isinstance(extra, ListBuffer)
    self._inputs.extend(extra._inputs)

  def append(self, element: bytes) -> None:
    if self.cleared:
      raise RuntimeError('Trying to append to a cleared ListBuffer.')
    if self._grouped_output:
      raise RuntimeError('ListBuffer append after read.')
    self._inputs.append(element)

  def partition(self, n: int) -> List[List[bytes]]:
    if self.cleared:
      raise RuntimeError('Trying to partition a cleared ListBuffer.')
    if len(self._inputs) >= n or len(self._inputs) == 0:
      return [self._inputs[k::n] for k in range(n)]
    else:
      if not self._grouped_output:
        output_stream_list = [create_OutputStream() for _ in range(n)]
        idx = 0
        for input in self._inputs:
          input_stream = create_InputStream(input)
          while input_stream.size() > 0:
            decoded_value = self._coder_impl.decode_from_stream(
                input_stream, True)
            self._coder_impl.encode_to_stream(
                decoded_value, output_stream_list[idx], True)
            idx = (idx + 1) % n
        self._grouped_output = [[output_stream.get()]
                                for output_stream in output_stream_list]
      return self._grouped_output

  def __iter__(self) -> Iterator[bytes]:
    if self.cleared:
      raise RuntimeError('Trying to iterate through a cleared ListBuffer.')
    return iter(self._inputs)

  def clear(self) -> None:
    self.cleared = True
    self._inputs = []
    self._grouped_output = None

  def reset(self) -> None:
    """Resets a cleared buffer for reuse."""
    if not self.cleared:
      raise RuntimeError('Trying to reset a non-cleared ListBuffer.')
    self.cleared = False


class GroupingBuffer(object):
  """Used to accumulate groupded (shuffled) results."""
  def __init__(
      self,
      pre_grouped_coder: coders.Coder,
      post_grouped_coder: coders.Coder,
      windowing: core.Windowing) -> None:
    self._key_coder = pre_grouped_coder.key_coder()
    self._pre_grouped_coder = pre_grouped_coder
    self._post_grouped_coder = post_grouped_coder
    self._table: DefaultDict[bytes, List[Any]] = collections.defaultdict(list)
    self._windowing = windowing
    self._grouped_output: Optional[List[List[bytes]]] = None

  def copy(self) -> 'GroupingBuffer':
    # This is a silly temporary optimization. This class must be removed once
    # full support for streaming is added (i.e. once we use trigger_manager for
    # data grouping instead of GroupingBuffer).
    return self

  def append(self, elements_data: bytes) -> None:
    if self._grouped_output:
      raise RuntimeError('Grouping table append after read.')
    input_stream = create_InputStream(elements_data)
    coder_impl = self._pre_grouped_coder.get_impl()
    key_coder_impl = self._key_coder.get_impl()
    # TODO(robertwb): We could optimize this even more by using a
    # window-dropping coder for the data plane.
    is_trivial_windowing = self._windowing.is_default()
    while input_stream.size() > 0:
      windowed_key_value = coder_impl.decode_from_stream(input_stream, True)
      key, value = windowed_key_value.value
      self._table[key_coder_impl.encode(key)].append(
          value if is_trivial_windowing else windowed_key_value.
          with_value(value))

  def extend(self, input_buffer: Buffer) -> None:
    if isinstance(input_buffer, ListBuffer):
      # TODO(pabloem): GroupingBuffer will be removed once shuffling is done
      #  via state. Remove this workaround along with that.
      return
    assert isinstance(input_buffer, GroupingBuffer), \
      'Input was not GroupingBuffer: %s' % input_buffer
    for key, values in input_buffer._table.items():
      self._table[key].extend(values)

  def partition(self, n: int) -> List[List[bytes]]:
    """ It is used to partition _GroupingBuffer to N parts. Once it is
    partitioned, it would not be re-partitioned with diff N. Re-partition
    is not supported now.
    """
    if not self._grouped_output:
      if self._windowing.is_default():
        globally_window = GlobalWindows.windowed_value(
            None,
            timestamp=GlobalWindow().max_timestamp(),
            pane_info=windowed_value.PaneInfo(
                is_first=True,
                is_last=True,
                timing=windowed_value.PaneInfoTiming.ON_TIME,
                index=0,
                nonspeculative_index=0)).with_value
        windowed_key_values = lambda key, values: [
            globally_window((key, values))
        ]
      else:
        # TODO(pabloem, BEAM-7514): Trigger driver needs access to the clock
        #   note that this only comes through if windowing is default - but what
        #   about having multiple firings on the global window.
        #   May need to revise.
        trigger_driver = trigger.create_trigger_driver(self._windowing, True)
        windowed_key_values = trigger_driver.process_entire_key
      coder_impl = self._post_grouped_coder.get_impl()
      key_coder_impl = self._key_coder.get_impl()
      self._grouped_output = [[] for _ in range(n)]
      output_stream_list = [create_OutputStream() for _ in range(n)]
      for idx, (encoded_key, windowed_values) in enumerate(self._table.items()):
        key = key_coder_impl.decode(encoded_key)
        for wkvs in windowed_key_values(key, windowed_values):
          coder_impl.encode_to_stream(wkvs, output_stream_list[idx % n], True)
      for ix, output_stream in enumerate(output_stream_list):
        self._grouped_output[ix] = [output_stream.get()]
      self._table.clear()
    return self._grouped_output

  def __iter__(self) -> Iterator[bytes]:
    """ Since partition() returns a list of lists, add this __iter__ to return
    a list to simplify code when we need to iterate through ALL elements of
    _GroupingBuffer.
    """
    return itertools.chain(*self.partition(1))

  # these should never be accessed, but they allow this class to meet the
  # PartionableBuffer protocol
  cleared = False

  def clear(self) -> None:
    pass

  def reset(self) -> None:
    pass


class WindowGroupingBuffer(object):
  """Used to partition windowed side inputs."""
  def __init__(
      self,
      access_pattern: beam_runner_api_pb2.FunctionSpec,
      coder: WindowedValueCoder) -> None:
    # Here's where we would use a different type of partitioning
    # (e.g. also by key) for a different access pattern.
    if access_pattern.urn == common_urns.side_inputs.ITERABLE.urn:
      self._kv_extractor = lambda value: ('', value)
      self._key_coder: coders.Coder = coders.SingletonCoder('')
      self._value_coder = coder.wrapped_value_coder
    elif access_pattern.urn == common_urns.side_inputs.MULTIMAP.urn:
      self._kv_extractor = lambda value: value
      self._key_coder = coder.wrapped_value_coder.key_coder()
      self._value_coder = (coder.wrapped_value_coder.value_coder())
    else:
      raise ValueError("Unknown access pattern: '%s'" % access_pattern.urn)
    self._windowed_value_coder = coder
    self._window_coder = coder.window_coder
    self._values_by_window: DefaultDict[Tuple[str, BoundedWindow],
                                        List[Any]] = collections.defaultdict(
                                            list)

  def append(self, elements_data: bytes) -> None:
    input_stream = create_InputStream(elements_data)
    while input_stream.size() > 0:
      windowed_val_coder_impl: WindowedValueCoderImpl = (
          self._windowed_value_coder.get_impl())
      windowed_value = windowed_val_coder_impl.decode_from_stream(
          input_stream, True)
      key, value = self._kv_extractor(windowed_value.value)
      for window in windowed_value.windows:
        self._values_by_window[key, window].append(value)

  def encoded_items(self) -> Iterator[Tuple[bytes, bytes, bytes, int]]:
    value_coder_impl = self._value_coder.get_impl()
    key_coder_impl = self._key_coder.get_impl()
    for (key, window), values in self._values_by_window.items():
      encoded_window = self._window_coder.encode(window)
      encoded_key = key_coder_impl.encode_nested(key)
      output_stream = create_OutputStream()
      for value in values:
        value_coder_impl.encode_to_stream(value, output_stream, True)
      yield encoded_key, encoded_window, output_stream.get(), len(values)


class GenericNonMergingWindowFn(window.NonMergingWindowFn):

  URN = 'internal-generic-non-merging'

  def __init__(self, coder: coders.Coder) -> None:
    self._coder = coder

  def assign(
      self,
      assign_context: window.WindowFn.AssignContext) -> Iterable[BoundedWindow]:
    raise NotImplementedError()

  def get_window_coder(self) -> coders.Coder:
    return self._coder

  @staticmethod
  @window.urns.RunnerApiFn.register_urn(URN, bytes)
  def from_runner_api_parameter(
      window_coder_id: bytes, context: Any) -> 'GenericNonMergingWindowFn':
    return GenericNonMergingWindowFn(
        context.coders[window_coder_id.decode('utf-8')])


QUEUE_KEY_TYPE = TypeVar('QUEUE_KEY_TYPE')


class _ProcessingQueueManager(object):
  """Manages the queues for ProcessBundle inputs.
  There are three queues:
   - ready_inputs(_ProcessingQueueManager.KeyedQueue). This queue contains input
       data that is ready to be processed. These are data such as timers past
       their trigger time, and data to be processed.
       The ready_inputs_queue contains tuples of (stage_name, inputs), where
       inputs are dictionaries mapping PCollection name to data buffers.
   - watermark_pending_inputs(_ProcessingQueueManager.KeyedQueue). This queue
       contains input data that is not yet ready to be processed, and is blocked
       on the watermark advancing. ((stage_name, watermark), inputs), where
       the watermark is the watermark at which the inputs should be scheduled,
       and inputs are dictionaries mapping PCollection name to data buffers.
   - time_pending_inputs(_ProcessingQueueManager.KeyedQueue). This queue
       contains input data that is not yet ready to be processed, and is blocked
       on time advancing. ((stage_name, time), inputs), where
       the time is the real time point at which the inputs should be scheduled,
       and inputs are dictionaries mapping PCollection name to data buffers.
  """
  class KeyedQueue(Generic[QUEUE_KEY_TYPE]):
    def __init__(self) -> None:
      self._q: typing.Deque[Tuple[QUEUE_KEY_TYPE,
                                  DataInput]] = collections.deque()
      self._keyed_elements: MutableMapping[QUEUE_KEY_TYPE,
                                           Tuple[QUEUE_KEY_TYPE,
                                                 DataInput]] = {}

    def enque(self, elm: Tuple[QUEUE_KEY_TYPE, DataInput]) -> None:
      key = elm[0]
      incoming_inputs: DataInput = elm[1]
      if not incoming_inputs:
        return
      if key in self._keyed_elements:
        existing_inputs = self._keyed_elements[key][1]
        for pcoll in incoming_inputs.data:
          if incoming_inputs.data[pcoll] and existing_inputs.data.get(pcoll):
            existing_inputs.data[pcoll].extend(incoming_inputs.data[pcoll])
          elif incoming_inputs.data[pcoll]:
            existing_inputs.data[pcoll] = incoming_inputs.data[pcoll]
        for timer_family in (incoming_inputs.timers or []):
          if (incoming_inputs.timers[timer_family] and
              existing_inputs.timers.get(timer_family)):
            existing_inputs.timers[timer_family].extend(
                incoming_inputs.timers[timer_family])
          elif incoming_inputs.timers[timer_family]:
            existing_inputs.timers[timer_family] = incoming_inputs.timers[
                timer_family]
      else:
        self._keyed_elements[key] = elm
        self._q.appendleft(elm)

    def deque(self) -> Tuple[QUEUE_KEY_TYPE, DataInput]:
      elm = self._q.pop()
      key = elm[0]
      del self._keyed_elements[key]
      return elm

    def __len__(self) -> int:
      return len(self._q)

    def __repr__(self) -> str:
      return '<%s at 0x%x>' % (str(self), id(self))

    def __str__(self) -> str:
      return '<%s len: %s %s>' % (
          self.__class__.__name__, len(self), list(self._q))

  def __init__(self) -> None:
    # For time-pending and watermark-pending inputs, the key type is
    # STAGE+TIMESTAMP, while for the ready inputs, the key type is only STAGE.
    self.time_pending_inputs = _ProcessingQueueManager.KeyedQueue[Tuple[
        str, Timestamp]]()
    self.watermark_pending_inputs = _ProcessingQueueManager.KeyedQueue[Tuple[
        str, Timestamp]]()
    self.ready_inputs = _ProcessingQueueManager.KeyedQueue[str]()

  def __str__(self) -> str:
    return '_ProcessingQueueManager(%s)' % self.__dict__


class GenericMergingWindowFn(window.WindowFn):

  URN = 'internal-generic-merging'

  TO_SDK_TRANSFORM = 'read'
  FROM_SDK_TRANSFORM = 'write'

  _HANDLES: Dict[str, 'GenericMergingWindowFn'] = {}

  def __init__(
      self,
      execution_context: 'FnApiRunnerExecutionContext',
      windowing_strategy_proto: beam_runner_api_pb2.WindowingStrategy) -> None:
    self._worker_handler: Optional[worker_handlers.WorkerHandler] = None
    self._handle_id = handle_id = uuid.uuid4().hex
    self._HANDLES[handle_id] = self
    # ExecutionContexts are expensive, we don't want to keep them in the
    # static dictionary forever.  Instead we hold a weakref and pop self
    # out of the dict once this context goes away.
    self._execution_context_ref_obj = weakref.ref(
        execution_context, lambda _: self._HANDLES.pop(handle_id, None))
    self._windowing_strategy_proto = windowing_strategy_proto
    self._counter = 0
    # Lazily created in make_process_bundle_descriptor()
    self._process_bundle_descriptor = None
    self._bundle_processor_id: str = ''
    self.windowed_input_coder_impl: Optional[CoderImpl] = None
    self.windowed_output_coder_impl: Optional[CoderImpl] = None

  def _execution_context_ref(self) -> 'FnApiRunnerExecutionContext':
    result = self._execution_context_ref_obj()
    assert result is not None
    return result

  def payload(self) -> bytes:
    return self._handle_id.encode('utf-8')

  @staticmethod
  @window.urns.RunnerApiFn.register_urn(URN, bytes)
  def from_runner_api_parameter(
      handle_id: bytes, unused_context: Any) -> 'GenericMergingWindowFn':
    return GenericMergingWindowFn._HANDLES[handle_id.decode('utf-8')]

  def assign(
      self, assign_context: window.WindowFn.AssignContext
  ) -> Iterable[window.BoundedWindow]:
    raise NotImplementedError()

  def merge(self, merge_context: window.WindowFn.MergeContext) -> None:
    worker_handler = self.worker_handle()

    assert self.windowed_input_coder_impl is not None
    assert self.windowed_output_coder_impl is not None
    process_bundle_id = self.uid('process')
    to_worker = worker_handler.data_conn.output_stream(
        process_bundle_id, self.TO_SDK_TRANSFORM)
    to_worker.write(
        self.windowed_input_coder_impl.encode_nested(
            window.GlobalWindows.windowed_value((b'', merge_context.windows))))
    to_worker.close()

    process_bundle_req = beam_fn_api_pb2.InstructionRequest(
        instruction_id=process_bundle_id,
        process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
            process_bundle_descriptor_id=self._bundle_processor_id))
    result_future = worker_handler.control_conn.push(process_bundle_req)
    for output in worker_handler.data_conn.input_elements(
        process_bundle_id, [self.FROM_SDK_TRANSFORM],
        abort_callback=lambda: bool(result_future.is_done() and result_future.
                                    get().error)):
      if isinstance(output, beam_fn_api_pb2.Elements.Data):
        windowed_result = self.windowed_output_coder_impl.decode_nested(
            output.data)
        for merge_result, originals in windowed_result.value[1][1]:
          merge_context.merge(originals, merge_result)
      else:
        raise RuntimeError("Unexpected data: %s" % output)

    result = result_future.get()
    if result.error:
      raise RuntimeError(result.error)
    # The result was "returned" via the merge callbacks on merge_context above.

  def get_window_coder(self) -> coders.Coder:
    return self._execution_context_ref().pipeline_context.coders[
        self._windowing_strategy_proto.window_coder_id]

  def worker_handle(self) -> 'worker_handlers.WorkerHandler':
    if self._worker_handler is None:
      worker_handler_manager = self._execution_context_ref(
      ).worker_handler_manager
      self._worker_handler = worker_handler_manager.get_worker_handlers(
          self._windowing_strategy_proto.environment_id, 1)[0]
      process_bundle_decriptor = self.make_process_bundle_descriptor(
          self._worker_handler.data_api_service_descriptor(),
          self._worker_handler.state_api_service_descriptor())
      worker_handler_manager.register_process_bundle_descriptor(
          process_bundle_decriptor)
    return self._worker_handler

  def make_process_bundle_descriptor(
      self,
      data_api_service_descriptor: Optional[endpoints_pb2.ApiServiceDescriptor],
      state_api_service_descriptor: Optional[endpoints_pb2.ApiServiceDescriptor]
  ) -> beam_fn_api_pb2.ProcessBundleDescriptor:
    """Creates a ProcessBundleDescriptor for invoking the WindowFn's
    merge operation.
    """
    def make_channel_payload(coder_id: str) -> bytes:
      data_spec = beam_fn_api_pb2.RemoteGrpcPort(coder_id=coder_id)
      if data_api_service_descriptor:
        data_spec.api_service_descriptor.url = (data_api_service_descriptor.url)
      return data_spec.SerializeToString()

    pipeline_context = self._execution_context_ref().pipeline_context
    global_windowing_strategy_id = self.uid('global_windowing_strategy')
    global_windowing_strategy_proto = core.Windowing(
        window.GlobalWindows()).to_runner_api(pipeline_context)
    coders = dict(pipeline_context.coders.get_id_to_proto_map())

    def make_coder(urn: str, *components: str) -> str:
      coder_proto = beam_runner_api_pb2.Coder(
          spec=beam_runner_api_pb2.FunctionSpec(urn=urn),
          component_coder_ids=components)
      coder_id = self.uid('coder')
      coders[coder_id] = coder_proto
      pipeline_context.coders.put_proto(coder_id, coder_proto)
      return coder_id

    bytes_coder_id = make_coder(common_urns.coders.BYTES.urn)
    window_coder_id = self._windowing_strategy_proto.window_coder_id
    global_window_coder_id = make_coder(common_urns.coders.GLOBAL_WINDOW.urn)
    iter_window_coder_id = make_coder(
        common_urns.coders.ITERABLE.urn, window_coder_id)
    input_coder_id = make_coder(
        common_urns.coders.KV.urn, bytes_coder_id, iter_window_coder_id)
    output_coder_id = make_coder(
        common_urns.coders.KV.urn,
        bytes_coder_id,
        make_coder(
            common_urns.coders.KV.urn,
            iter_window_coder_id,
            make_coder(
                common_urns.coders.ITERABLE.urn,
                make_coder(
                    common_urns.coders.KV.urn,
                    window_coder_id,
                    iter_window_coder_id))))
    windowed_input_coder_id = make_coder(
        common_urns.coders.WINDOWED_VALUE.urn,
        input_coder_id,
        global_window_coder_id)
    windowed_output_coder_id = make_coder(
        common_urns.coders.WINDOWED_VALUE.urn,
        output_coder_id,
        global_window_coder_id)

    self.windowed_input_coder_impl = pipeline_context.coders[
        windowed_input_coder_id].get_impl()
    self.windowed_output_coder_impl = pipeline_context.coders[
        windowed_output_coder_id].get_impl()

    self._bundle_processor_id = self.uid('merge_windows')
    return beam_fn_api_pb2.ProcessBundleDescriptor(
        id=self._bundle_processor_id,
        transforms={
            self.TO_SDK_TRANSFORM: beam_runner_api_pb2.PTransform(
                unique_name='MergeWindows/Read',
                spec=beam_runner_api_pb2.FunctionSpec(
                    urn=bundle_processor.DATA_INPUT_URN,
                    payload=make_channel_payload(windowed_input_coder_id)),
                outputs={'input': 'input'}),
            'Merge': beam_runner_api_pb2.PTransform(
                unique_name='MergeWindows/Merge',
                spec=beam_runner_api_pb2.FunctionSpec(
                    urn=common_urns.primitives.MERGE_WINDOWS.urn,
                    payload=self._windowing_strategy_proto.window_fn.
                    SerializeToString()),
                inputs={'input': 'input'},
                outputs={'output': 'output'}),
            self.FROM_SDK_TRANSFORM: beam_runner_api_pb2.PTransform(
                unique_name='MergeWindows/Write',
                spec=beam_runner_api_pb2.FunctionSpec(
                    urn=bundle_processor.DATA_OUTPUT_URN,
                    payload=make_channel_payload(windowed_output_coder_id)),
                inputs={'output': 'output'}),
        },
        pcollections={
            'input': beam_runner_api_pb2.PCollection(
                unique_name='input',
                windowing_strategy_id=global_windowing_strategy_id,
                coder_id=input_coder_id),
            'output': beam_runner_api_pb2.PCollection(
                unique_name='output',
                windowing_strategy_id=global_windowing_strategy_id,
                coder_id=output_coder_id),
        },
        coders=coders,
        windowing_strategies={
            global_windowing_strategy_id: global_windowing_strategy_proto,
        },
        environments=dict(
            self._execution_context_ref().pipeline_components.environments.
            items()),
        state_api_service_descriptor=state_api_service_descriptor,
        timer_api_service_descriptor=data_api_service_descriptor)

  def uid(self, name: str = '') -> str:
    self._counter += 1
    return '%s_%s_%s' % (self._handle_id, name, self._counter)


class FnApiRunnerExecutionContext(object):
  """
 :var pcoll_buffers: (dict): Mapping of
       PCollection IDs to list that functions as buffer for the
       ``beam.PCollection``.
 """
  def __init__(
      self,
      stages: List[translations.Stage],
      worker_handler_manager: 'worker_handlers.WorkerHandlerManager',
      pipeline_components: beam_runner_api_pb2.Components,
      safe_coders: translations.SafeCoderMapping,
      data_channel_coders: Dict[str, str],
      num_workers: int,
      uses_teststream: bool = False,
      split_managers: Sequence[Tuple[str, Callable[[int],
                                                   Iterable[float]]]] = ()
  ) -> None:
    """
    :param worker_handler_manager: This class manages the set of worker
        handlers, and the communication with state / control APIs.
    :param pipeline_components:  (beam_runner_api_pb2.Components)
    :param safe_coders: A map from Coder ID to Safe Coder ID.
    :param data_channel_coders: A map from PCollection ID to the ID of the Coder
        for that PCollection.
    """
    self.stages = {s.name: s for s in stages}
    self.side_input_descriptors_by_stage = (
        self._build_data_side_inputs_map(stages))
    self.pcoll_buffers: MutableMapping[bytes, PartitionableBuffer] = {}
    self.timer_buffers: MutableMapping[bytes, ListBuffer] = {}
    self.worker_handler_manager = worker_handler_manager
    self.pipeline_components = pipeline_components
    self.safe_coders = safe_coders
    self.data_channel_coders = data_channel_coders
    self.num_workers = num_workers
    self.split_managers = split_managers
    # TODO(pabloem): Move Clock classes out of DirectRunner and into FnApiRnr
    self.clock: Union[TestClock, RealClock] = (
        TestClock() if uses_teststream else RealClock())
    self.queues = _ProcessingQueueManager()

    # The following set of dictionaries hold information mapping relationships
    # between various pipeline elements.
    self.input_transform_to_buffer_id: MutableMapping[str, bytes] = {}
    self.pcollection_to_producer_transform: MutableMapping[Union[str, bytes],
                                                           Optional[str]] = {}
    # Map of buffer_id to its consumers. A consumer is the pair of
    # Stage name + Ptransform name that consume that buffer.
    self.buffer_id_to_consumer_pairs: Dict[bytes, Set[Tuple[str, str]]] = {}
    self._compute_pipeline_dictionaries()

    self.watermark_manager = WatermarkManager(stages)
    # from apache_beam.runners.portability.fn_api_runner import \
    #     visualization_tools
    # visualization_tools.show_watermark_manager(self.watermark_manager)
    self.pipeline_context = pipeline_context.PipelineContext(
        self.pipeline_components,
        iterable_state_write=self._iterable_state_write)
    self._last_uid = -1
    self.safe_windowing_strategies = {
        id: self._make_safe_windowing_strategy(id)
        for id in self.pipeline_components.windowing_strategies.keys()
    }

    self._stage_managers: Dict[str, BundleContextManager] = {}

  def bundle_manager_for(
      self,
      stage: Stage,
      num_workers: Optional[int] = None) -> 'BundleContextManager':
    if stage.name not in self._stage_managers:
      self._stage_managers[stage.name] = BundleContextManager(
          self,
          stage,
          num_workers or self.num_workers,
          split_managers=self.split_managers)
    return self._stage_managers[stage.name]

  def _compute_pipeline_dictionaries(self) -> None:
    for s in self.stages.values():
      for t in s.transforms:
        buffer_id = t.spec.payload
        if t.spec.urn == bundle_processor.DATA_INPUT_URN:
          self.input_transform_to_buffer_id[t.unique_name] = buffer_id
          if t.spec.payload == translations.IMPULSE_BUFFER:
            # Impulse data is not produced by any PTransform.
            self.pcollection_to_producer_transform[
                translations.IMPULSE_BUFFER] = None
          else:
            assert t.spec.payload != translations.IMPULSE_BUFFER
            _, input_pcoll = split_buffer_id(buffer_id)
            # Adding PCollections that may not have a producer.
            # This is necessary, for example, for the case where we pass an
            # empty list of PCollections into a Flatten transform.
            if input_pcoll not in self.pcollection_to_producer_transform:
              self.pcollection_to_producer_transform[input_pcoll] = None
            if buffer_id not in self.buffer_id_to_consumer_pairs:
              self.buffer_id_to_consumer_pairs[buffer_id] = set()
            if (s.name, t.unique_name
                ) not in self.buffer_id_to_consumer_pairs[buffer_id]:
              self.buffer_id_to_consumer_pairs[buffer_id].add(
                  (s.name, t.unique_name))
        elif t.spec.urn == bundle_processor.DATA_OUTPUT_URN:
          _, output_pcoll = split_buffer_id(buffer_id)
          self.pcollection_to_producer_transform[output_pcoll] = t.unique_name
        elif t.spec.urn in translations.PAR_DO_URNS:
          pass

  def setup(self) -> None:
    """This sets up the pipeline to begin running.

    1. This function enqueues all initial pipeline bundles to be executed.
    2. It also updates payload fields on DATA_INPUT and DATA_OUTPUT operations
      to the Data API endpoints that are live.
    """
    for stage in self.stages.values():
      self._enqueue_stage_initial_inputs(stage)

  def _enqueue_stage_initial_inputs(self, stage: Stage) -> None:
    """Sets up IMPULSE inputs for a stage, and the data GRPC API endpoint."""
    data_input: MutableMapping[str, PartitionableBuffer] = {}
    ready_to_schedule = True
    for transform in stage.transforms:
      if (transform.spec.urn in {bundle_processor.DATA_INPUT_URN,
                                 bundle_processor.DATA_OUTPUT_URN}):
        if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
          coder_id = self.data_channel_coders[only_element(
              transform.outputs.values())]
          coder = self.pipeline_context.coders[self.safe_coders.get(
              coder_id, coder_id)]
          if transform.spec.payload == translations.IMPULSE_BUFFER:
            data_input[transform.unique_name] = ListBuffer(coder.get_impl())
            data_input[transform.unique_name].append(ENCODED_IMPULSE_VALUE)
          else:
            # If this is not an IMPULSE input, then it is not part of the
            # initial inputs of a pipeline, and we'll ignore it.
            pass
        else:
          coder_id = self.data_channel_coders[only_element(
              transform.inputs.values())]
        # For every DATA_INPUT or DATA_OUTPUT operation, we need to replace the
        # payload with the GRPC configuration for the Data channel.
        bundle_manager = self.bundle_manager_for(stage)
        data_spec = beam_fn_api_pb2.RemoteGrpcPort(coder_id=coder_id)
        data_api_service_descriptor = (
            bundle_manager.data_api_service_descriptor())
        if data_api_service_descriptor:
          data_spec.api_service_descriptor.url = (
              data_api_service_descriptor.url)
        transform.spec.payload = data_spec.SerializeToString()
      elif transform.spec.urn in translations.PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        if payload.side_inputs:
          # If the stage needs side inputs, then it's not ready to be
          # executed.
          ready_to_schedule = False
    if data_input and ready_to_schedule:
      # We push the data inputs, along with the name of the consuming stage.
      _LOGGER.debug('Scheduling bundle in stage for execution: %s', stage.name)
      self.queues.ready_inputs.enque((stage.name, DataInput(data_input, {})))
    elif data_input and not ready_to_schedule:
      _LOGGER.debug(
          'Enqueuing stage pending watermark. Stage name: %s', stage.name)
      self.queues.watermark_pending_inputs.enque(
          ((stage.name, MAX_TIMESTAMP), DataInput(data_input, {})))

  @staticmethod
  def _build_data_side_inputs_map(
      stages: Iterable[translations.Stage]
  ) -> MutableMapping[str, 'DataSideInput']:
    """Builds an index mapping stages to side input descriptors.

    A side input descriptor is a map of side input IDs to side input access
    patterns for all of the outputs of a stage that will be consumed as a
    side input.
    """
    transform_consumers: DefaultDict[
        str,
        List[beam_runner_api_pb2.PTransform]] = collections.defaultdict(list)
    stage_consumers: DefaultDict[
        str, List[translations.Stage]] = collections.defaultdict(list)

    def get_all_side_inputs() -> Set[str]:
      all_side_inputs: Set[str] = set()
      for stage in stages:
        for transform in stage.transforms:
          for input in transform.inputs.values():
            transform_consumers[input].append(transform)
            stage_consumers[input].append(stage)
        for si in stage.side_inputs():
          all_side_inputs.add(si)
      return all_side_inputs

    all_side_inputs = frozenset(get_all_side_inputs())
    data_side_inputs_by_producing_stage: Dict[str, DataSideInput] = {}

    producing_stages_by_pcoll = {}

    for s in stages:
      data_side_inputs_by_producing_stage[s.name] = {}
      for transform in s.transforms:
        for o in transform.outputs.values():
          if o in s.side_inputs():
            continue
          if o in producing_stages_by_pcoll:
            continue
          producing_stages_by_pcoll[o] = s

    for side_pc in all_side_inputs:
      for consuming_transform in transform_consumers[side_pc]:
        if consuming_transform.spec.urn not in translations.PAR_DO_URNS:
          continue
        producing_stage = producing_stages_by_pcoll[side_pc]
        payload = proto_utils.parse_Bytes(
            consuming_transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for si_tag in payload.side_inputs:
          if consuming_transform.inputs[si_tag] == side_pc:
            side_input_id = (consuming_transform.unique_name, si_tag)
            data_side_inputs_by_producing_stage[
                producing_stage.name][side_input_id] = (
                    translations.create_buffer_id(side_pc),
                    payload.side_inputs[si_tag].access_pattern)

    return data_side_inputs_by_producing_stage

  def _make_safe_windowing_strategy(self, id: str) -> str:
    windowing_strategy_proto = self.pipeline_components.windowing_strategies[id]
    if windowing_strategy_proto.window_fn.urn in SAFE_WINDOW_FNS:
      return id
    else:
      safe_id = id + '_safe'
      while safe_id in self.pipeline_components.windowing_strategies:
        safe_id += '_'
      safe_proto = copy.copy(windowing_strategy_proto)
      if (windowing_strategy_proto.merge_status ==
          beam_runner_api_pb2.MergeStatus.NON_MERGING):
        safe_proto.window_fn.urn = GenericNonMergingWindowFn.URN
        safe_proto.window_fn.payload = (
            windowing_strategy_proto.window_coder_id.encode('utf-8'))
      elif (windowing_strategy_proto.merge_status ==
            beam_runner_api_pb2.MergeStatus.NEEDS_MERGE):
        window_fn = GenericMergingWindowFn(self, windowing_strategy_proto)
        safe_proto.window_fn.urn = GenericMergingWindowFn.URN
        safe_proto.window_fn.payload = window_fn.payload()
      else:
        raise NotImplementedError(
            'Unsupported merging strategy: %s' %
            windowing_strategy_proto.merge_status)
      self.pipeline_context.windowing_strategies.put_proto(safe_id, safe_proto)
      return safe_id

  @property
  def state_servicer(self) -> 'worker_handlers.StateServicer':
    # TODO(BEAM-9625): Ensure FnApiRunnerExecutionContext owns StateServicer
    return self.worker_handler_manager.state_servicer

  def next_uid(self) -> str:
    self._last_uid += 1
    return str(self._last_uid)

  def _iterable_state_write(
      self, values: Iterable, element_coder_impl: CoderImpl) -> bytes:
    token = unique_name(None, 'iter').encode('ascii')
    out = create_OutputStream()
    for element in values:
      element_coder_impl.encode_to_stream(element, out, True)
    self.worker_handler_manager.state_servicer.append_raw(
        beam_fn_api_pb2.StateKey(
            runner=beam_fn_api_pb2.StateKey.Runner(key=token)),
        out.get())
    return token

  def commit_side_inputs_to_state(
      self,
      data_side_input: 'DataSideInput',
  ) -> None:
    for (consuming_transform_id, tag), (buffer_id,
                                        func_spec) in data_side_input.items():
      _, pcoll_id = split_buffer_id(buffer_id)
      value_coder = self.pipeline_context.coders[self.safe_coders[
          self.data_channel_coders[pcoll_id]]]
      elements_by_window = WindowGroupingBuffer(func_spec, value_coder)
      if buffer_id not in self.pcoll_buffers:
        self.pcoll_buffers[buffer_id] = ListBuffer(
            coder_impl=value_coder.get_impl())
      for element_data in self.pcoll_buffers[buffer_id]:
        elements_by_window.append(element_data)

      if func_spec.urn == common_urns.side_inputs.ITERABLE.urn:
        for _, window, elements_data, _ in elements_by_window.encoded_items():
          state_key = beam_fn_api_pb2.StateKey(
              iterable_side_input=beam_fn_api_pb2.StateKey.IterableSideInput(
                  transform_id=consuming_transform_id,
                  side_input_id=tag,
                  window=window))
          self.state_servicer.append_raw(state_key, elements_data)
      elif func_spec.urn == common_urns.side_inputs.MULTIMAP.urn:
        # TODO(robertwb): Consider computing these lazily on demand rather than
        # anticipating all potentail state requests which will be more cpu and
        # memory efficient for large side inputs.
        for (key, window, elements_data,
             elements_count) in elements_by_window.encoded_items():
          state_key = beam_fn_api_pb2.StateKey(
              multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
                  transform_id=consuming_transform_id,
                  side_input_id=tag,
                  window=window,
                  key=key))
          self.state_servicer.append_raw(state_key, elements_data)

          key_iter_state_key = beam_fn_api_pb2.StateKey(
              multimap_keys_side_input=beam_fn_api_pb2.StateKey.
              MultimapKeysSideInput(
                  transform_id=consuming_transform_id,
                  side_input_id=tag,
                  window=window))
          self.state_servicer.append_raw(key_iter_state_key, key)

          kv_iter_state_key = beam_fn_api_pb2.StateKey(
              multimap_keys_values_side_input=beam_fn_api_pb2.StateKey.
              MultimapKeysValuesSideInput(
                  transform_id=consuming_transform_id,
                  side_input_id=tag,
                  window=window))
          self.state_servicer.append_raw(
              kv_iter_state_key,
              # KV<K, Iterable<V>> encoding.
              key + struct.pack('>i', elements_count) + elements_data)
      else:
        raise ValueError("Unknown access pattern: '%s'" % func_spec.urn)


class BundleContextManager(object):
  def __init__(
      self,
      execution_context: FnApiRunnerExecutionContext,
      stage: translations.Stage,
      num_workers: int,
      split_managers: Sequence[Tuple[str, Callable[[int], Iterable[float]]]],
  ) -> None:
    self.execution_context = execution_context
    self.stage = stage
    self.bundle_uid = self.execution_context.next_uid()
    self.num_workers = num_workers
    self.split_managers = split_managers

    # Properties that are lazily initialized
    self._process_bundle_descriptor: Optional[
        beam_fn_api_pb2.ProcessBundleDescriptor] = None
    self._worker_handlers: Optional[List[worker_handlers.WorkerHandler]] = None
    # a mapping of {(transform_id, timer_family_id): timer_coder_id}. The map
    # is built after self._process_bundle_descriptor is initialized.
    # This field can be used to tell whether current bundle has timers.
    self._timer_coder_ids: Optional[Dict[Tuple[str, str], str]] = None

    # A mapping from transform_name to Buffer ID
    self.stage_data_outputs: DataOutput = {}
    # A mapping of {(transform_id, timer_family_id) : buffer_id}
    self.stage_timer_outputs: OutputTimers = {}
    self._compute_expected_outputs()

  def _compute_expected_outputs(self) -> None:
    for transform in self.stage.transforms:
      if transform.spec.urn == bundle_processor.DATA_OUTPUT_URN:
        buffer_id = transform.spec.payload
        self.stage_data_outputs[transform.unique_name] = buffer_id
      elif transform.spec.urn in translations.PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for timer_family_id in payload.timer_family_specs.keys():
          time_domain = payload.timer_family_specs[timer_family_id].time_domain
          self.stage_timer_outputs[(transform.unique_name, timer_family_id)] = (
              create_buffer_id(timer_family_id, 'timers'), time_domain)

  @property
  def worker_handlers(self) -> List['worker_handlers.WorkerHandler']:
    if self._worker_handlers is None:
      self._worker_handlers = (
          self.execution_context.worker_handler_manager.get_worker_handlers(
              self.stage.environment, self.num_workers))
    return self._worker_handlers

  def data_api_service_descriptor(
      self) -> Optional[endpoints_pb2.ApiServiceDescriptor]:
    # All worker_handlers share the same grpc server, so we can read grpc server
    # info from any worker_handler and read from the first worker_handler.
    return self.worker_handlers[0].data_api_service_descriptor()

  def state_api_service_descriptor(
      self) -> Optional[endpoints_pb2.ApiServiceDescriptor]:
    # All worker_handlers share the same grpc server, so we can read grpc server
    # info from any worker_handler and read from the first worker_handler.
    return self.worker_handlers[0].state_api_service_descriptor()

  @property
  def process_bundle_descriptor(
      self) -> beam_fn_api_pb2.ProcessBundleDescriptor:
    if self._process_bundle_descriptor is None:
      self._process_bundle_descriptor = self._build_process_bundle_descriptor()
      self._timer_coder_ids = self._build_timer_coders_id_map()
    return self._process_bundle_descriptor

  def _build_process_bundle_descriptor(
      self) -> beam_fn_api_pb2.ProcessBundleDescriptor:
    # Cannot be invoked until *after* _extract_endpoints is called.
    # Always populate the timer_api_service_descriptor.
    return beam_fn_api_pb2.ProcessBundleDescriptor(
        id=self.bundle_uid,
        transforms={
            transform.unique_name: transform
            for transform in self.stage.transforms
        },
        pcollections=dict(
            self.execution_context.pipeline_components.pcollections.items()),
        coders=dict(self.execution_context.pipeline_components.coders.items()),
        windowing_strategies=dict(
            self.execution_context.pipeline_components.windowing_strategies.
            items()),
        environments=dict(
            self.execution_context.pipeline_components.environments.items()),
        state_api_service_descriptor=self.state_api_service_descriptor(),
        timer_api_service_descriptor=self.data_api_service_descriptor())

  def get_input_coder_impl(self, transform_id: str) -> CoderImpl:
    coder_id = beam_fn_api_pb2.RemoteGrpcPort.FromString(
        self.process_bundle_descriptor.transforms[transform_id].spec.payload
    ).coder_id
    assert coder_id
    return self.get_coder_impl(coder_id)

  def _build_timer_coders_id_map(self) -> Dict[Tuple[str, str], str]:
    assert self._process_bundle_descriptor is not None
    timer_coder_ids = {}
    for transform_id, transform_proto in (self._process_bundle_descriptor
        .transforms.items()):
      if transform_proto.spec.urn == common_urns.primitives.PAR_DO.urn:
        pardo_payload = proto_utils.parse_Bytes(
            transform_proto.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for id, timer_family_spec in pardo_payload.timer_family_specs.items():
          timer_coder_ids[(transform_id, id)] = (
              timer_family_spec.timer_family_coder_id)
    return timer_coder_ids

  def get_coder_impl(self, coder_id: str) -> CoderImpl:
    if coder_id in self.execution_context.safe_coders:
      return self.execution_context.pipeline_context.coders[
          self.execution_context.safe_coders[coder_id]].get_impl()
    else:
      return self.execution_context.pipeline_context.coders[coder_id].get_impl()

  def get_timer_coder_impl(
      self, transform_id: str, timer_family_id: str) -> CoderImpl:
    assert self._timer_coder_ids is not None
    return self.get_coder_impl(
        self._timer_coder_ids[(transform_id, timer_family_id)])

  def get_buffer(
      self, buffer_id: bytes, transform_id: str) -> PartitionableBuffer:
    """Returns the buffer for a given (operation_type, PCollection ID).
    For grouping-typed operations, we produce a ``GroupingBuffer``. For
    others, we produce a ``ListBuffer``.
    """
    kind, name = split_buffer_id(buffer_id)
    if kind == 'materialize':
      if buffer_id not in self.execution_context.pcoll_buffers:
        self.execution_context.pcoll_buffers[buffer_id] = ListBuffer(
            coder_impl=self.get_input_coder_impl(transform_id))
      return self.execution_context.pcoll_buffers[buffer_id]
    # For timer buffer, name = timer_family_id
    elif kind == 'timers':
      if buffer_id not in self.execution_context.timer_buffers:
        timer_coder_impl = self.get_timer_coder_impl(transform_id, name)
        self.execution_context.timer_buffers[buffer_id] = ListBuffer(
            timer_coder_impl)
      return self.execution_context.timer_buffers[buffer_id]
    elif kind == 'group':
      # This is a grouping write, create a grouping buffer if needed.
      if buffer_id not in self.execution_context.pcoll_buffers:
        original_gbk_transform = name
        transform_proto = self.execution_context.pipeline_components.transforms[
            original_gbk_transform]
        input_pcoll = only_element(list(transform_proto.inputs.values()))
        output_pcoll = only_element(list(transform_proto.outputs.values()))
        pre_gbk_coder = self.execution_context.pipeline_context.coders[
            self.execution_context.safe_coders[
                self.execution_context.data_channel_coders[input_pcoll]]]
        post_gbk_coder = self.execution_context.pipeline_context.coders[
            self.execution_context.safe_coders[
                self.execution_context.data_channel_coders[output_pcoll]]]
        windowing_strategy = (
            self.execution_context.pipeline_context.windowing_strategies[
                self.execution_context.safe_windowing_strategies[
                    self.execution_context.pipeline_components.
                    pcollections[input_pcoll].windowing_strategy_id]])
        self.execution_context.pcoll_buffers[buffer_id] = GroupingBuffer(
            pre_gbk_coder, post_gbk_coder, windowing_strategy)
    else:
      # These should be the only two identifiers we produce for now,
      # but special side input writes may go here.
      raise NotImplementedError(buffer_id)
    return self.execution_context.pcoll_buffers[buffer_id]

  def input_for(self, transform_id: str, input_id: str) -> str:
    """Returns the name of the transform producing the given PCollection."""
    input_pcoll = self.process_bundle_descriptor.transforms[
        transform_id].inputs[input_id]
    for read_id, proto in self.process_bundle_descriptor.transforms.items():
      # The GrpcRead is followed by the SDF/Process.
      if (proto.spec.urn == bundle_processor.DATA_INPUT_URN and
          input_pcoll in proto.outputs.values()):
        return read_id
      # The GrpcRead is followed by the SDF/Truncate -> SDF/Process.
      if (proto.spec.urn
          == common_urns.sdf_components.TRUNCATE_SIZED_RESTRICTION.urn and
          input_pcoll in proto.outputs.values()):
        read_input = list(
            self.process_bundle_descriptor.transforms[read_id].inputs.values()
        )[0]
        for (grpc_read,
             transform_proto) in self.process_bundle_descriptor.transforms.items():  # pylint: disable=line-too-long
          if (transform_proto.spec.urn == bundle_processor.DATA_INPUT_URN and
              read_input in transform_proto.outputs.values()):
            return grpc_read

    raise RuntimeError('No IO transform feeds %s' % transform_id)
