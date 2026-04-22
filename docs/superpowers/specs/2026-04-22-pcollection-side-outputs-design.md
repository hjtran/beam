# PCollection Side Outputs — Design Spec

**Author:** Joey Tran
**Date:** 2026-04-22
**Status:** Approved (brainstorming)

## Problem

In the Python SDK today, transform authors face a tension: return a `PCollection`
(chainable and ergonomic, but loses side outputs) or return a `dict` /
`PCollectionTuple` (preserves side outputs but breaks chaining). Most pick
chaining and discard the side outputs.

## Goal

Extend `PCollection` so a transform can return a single PCollection that *also*
carries named "side output" PCollections, accessible via `result.side_outputs.<tag>`.
The result remains a true PCollection — `result | NextTransform()` works
unchanged.

## Non-goals (this spec)

- `ParDo.with_side_outputs` convenience method (a follow-up).
- Beam YAML dot-modifier integration. (YAML's `get_pcollection`
  already resolves `Transform.tag` by walking the producer's outputs dict, so
  registering side outputs on the `AppliedPTransform` should make YAML work
  for free; verifying this is part of the implementation, but no YAML code is
  added in this spec.)
- A new `_PCollectionWithSideOutputs` wrapper type. We extend `PCollection`
  itself.

## Public API

```python
class PCollection(PValue, Generic[T]):
    @property
    def side_outputs(self) -> _SideOutputsContainer: ...

    def with_side_outputs(self, **side_outputs: "PCollection") -> "PCollection":
        """Return a copy of this PCollection carrying the given side outputs.

        Each kwarg becomes accessible as ``result.side_outputs.<tag>``. Tags
        must be valid Python identifiers (enforced by ``**`` syntax).

        Calling ``with_side_outputs`` again replaces any previously-set side
        outputs on the new copy; the original is unchanged.
        """
```

```python
class _SideOutputsContainer:
    """Lightweight accessor over named side-output PCollections.

    Supports attribute access (``container.dropped``), indexing
    (``container["dropped"]``), iteration over tag names, ``len()``, and
    ``in``. ``__getattr__`` on a missing tag raises ``AttributeError`` with
    a message listing available tags.
    """
```

### Validation

`with_side_outputs` validates each kwarg:

1. The value must be a `PCollection` — `TypeError` otherwise.
2. The value's `pipeline` must equal `self.pipeline` — `ValueError` otherwise.

Tag names are validated implicitly by `**` (must be valid identifiers). No
restriction on overlap with other tag names elsewhere in the graph.

### Access

- `pcoll.side_outputs.dropped` — primary access.
- `pcoll.side_outputs["dropped"]` — equivalent.
- `pcoll.side_outputs` on a PCollection that never had side outputs set —
  returns an empty container; iteration yields nothing; missing-tag access
  raises `AttributeError` with a helpful message.

## Internal Design

### Storage

`PCollection` gains one new attribute:

```python
_side_outputs: Optional[dict[str, "PCollection"]] = None
```

Default `None` (not an empty dict) so we can distinguish "never set" from
"explicitly empty". Stored directly on the instance to keep `copy.copy`
behavior natural.

### Pipeline graph integration (the key change)

When a `PTransform.expand()` returns a PCollection that carries side outputs,
those side outputs must be registered as real outputs of the wrapping
`AppliedPTransform`. This is what makes side outputs first-class in the
graph (visible to runners, the proto, visualization tools, and YAML's
`Transform.tag` resolution).

The hook is in `Pipeline._apply_internal` (`pipeline.py` ~line 818). The
existing loop walks the result via `get_named_nested_pvalues` and registers
each PValue on `current.outputs`, with a special case for `DoOutputsTuple`.
We add a parallel special case: after registering the main PCollection,
if it has non-empty `_side_outputs`, iterate them and call
`current.add_output(side_pcoll, side_tag)` (skipping any tag already present
in `current.outputs`).

Pseudocode for the addition:

```python
# Inside _apply_internal, after current.add_output(result, tag):
if isinstance(result, pvalue.PCollection) and result._side_outputs:
    for side_tag, side_pcoll in result._side_outputs.items():
        if side_tag not in current.outputs:
            current.add_output(side_pcoll, side_tag)
```

This is the *only* change to `pipeline.py`.

`add_output` (`pipeline.py:1371`) only sets `self.outputs[tag] = output`; it
does not modify the side output's `producer`. The side output's true
producer (e.g. the inner `ParDo` from `with_outputs`) is preserved, which
is what we want — the outer composite just additionally lists the side
output among its outputs. This is the standard pattern for composite
transforms that surface outputs from inner transforms.

### Chaining behavior

`pc | A | B` is unchanged. `A` returns a `PCollection` (which happens to
carry side outputs); the `|` operator passes that PCollection to `B` as
input. `B` sees a normal PCollection. `A`'s side outputs are not carried
through to `B`'s output — to capture them, the caller must save the
intermediate result:

```python
result = pc | A()
result.side_outputs.dropped  # accessible
result | B()                  # B's main input is result's main output
```

This matches the proposal's "Chaining Behavior" paragraph and requires no
special code.

### Pickling and round-tripping

- **Pickling:** `PCollection.__reduce_ex__` already returns
  `_InvalidUnpickledPCollection`. Side outputs ride along with that — no
  change needed.
- **Proto round-trip:** Side outputs are real outputs on the producer's
  `AppliedPTransform`, so they're already in the runner API proto. The
  Python-side `_side_outputs` annotation is *not* serialized. After
  `from_runner_api`, the reconstructed PCollection will not have
  `_side_outputs` populated, but the side outputs themselves still exist
  on their producer transform. This is acceptable; the convenience accessor
  is a construction-time ergonomic, not a serialized graph property.

### `_SideOutputsContainer` implementation sketch

```python
class _SideOutputsContainer:
    def __init__(self, side_outputs: dict[str, PCollection]):
        object.__setattr__(self, "_side_outputs", dict(side_outputs))

    def __getattr__(self, tag: str) -> PCollection:
        if tag.startswith("__"):
            raise AttributeError(tag)
        try:
            return self._side_outputs[tag]
        except KeyError:
            available = sorted(self._side_outputs)
            raise AttributeError(
                f"No side output named {tag!r}. Available: {available}")

    def __getitem__(self, tag: str) -> PCollection:
        return self._side_outputs[tag]

    def __iter__(self):       return iter(self._side_outputs)
    def __len__(self):        return len(self._side_outputs)
    def __contains__(self, tag): return tag in self._side_outputs
```

## Files Changed

- `sdks/python/apache_beam/pvalue.py` — add `_SideOutputsContainer` class,
  `PCollection._side_outputs` attribute, `PCollection.side_outputs` property,
  `PCollection.with_side_outputs` method.
- `sdks/python/apache_beam/pipeline.py` — add the side-output registration
  block inside `_apply_internal`'s result-walking loop.
- `sdks/python/apache_beam/pvalue_test.py` — unit tests for the container,
  validation rules, and the `with_side_outputs` copy semantics.
- New end-to-end test (location TBD by implementation plan; likely in
  `pvalue_test.py` or `transforms/ptransform_test.py`) verifying that a
  composite transform returning `with_side_outputs(...)` produces a graph
  in which the side outputs are accessible from the `AppliedPTransform`'s
  outputs and from the returned PCollection.

## Testing Strategy

Unit tests:

- `with_side_outputs` returns a copy; original unchanged.
- Side output access via attribute and index; missing-tag error message
  lists available tags.
- `TypeError` when a non-PCollection is passed.
- `ValueError` when a side output is on a different pipeline.
- Empty-side-outputs container behavior.
- Calling `with_side_outputs` twice replaces (does not merge).

End-to-end test (`TestPipeline` based):

- A composite `PTransform` whose `expand()` calls
  `result.with_side_outputs(dropped=...)`.
- Apply it: `out = pcoll | MyFilter()`.
- Assert `out.side_outputs.dropped` is a PCollection.
- Assert the wrapping `AppliedPTransform.outputs` contains both the main
  output (under `None`) and the `"dropped"` tag.
- Assert chaining (`out | NextTransform()`) still uses the main output
  and that the next transform's input is the main PCollection.
- Materialize and `assert_that` on both the main output and the side
  output to confirm correctness end-to-end.

## Risks & Mitigations

- **Risk:** A user attaches side outputs to a PCollection that was *not*
  produced by their composite transform (e.g. they grab some unrelated
  PCollection and pass it as a side output). The pipeline graph would
  then list a foreign PCollection under the composite's outputs.
  **Mitigation:** `add_output` is idempotent for already-registered tags
  (we skip if `tag in current.outputs`), so we won't double-register; but
  we deliberately do *not* validate that side outputs are descendants
  of the wrapping transform — the user opted in by calling
  `with_side_outputs`. Document this limitation; consider a future warning.

- **Risk:** Tag collision with the main output's tag (`None` is the main
  tag). Kwargs cannot be `None`, so this can't happen accidentally. A
  user passing `with_side_outputs(main=...)` would create a tag named
  `"main"` which is fine and matches the proposal's example.

## Open Questions (to defer)

- Should `with_side_outputs` chain (i.e., merge with existing `_side_outputs`)
  rather than replace? The spec says replace, matching `copy.copy`'s
  shallow semantics. If chaining ever becomes desired, the current contract
  permits adding it (additive, non-breaking).
- Static type checking: `pcoll.side_outputs.dropped` is opaque to mypy.
  Could be addressed later with a typed factory or a generic protocol;
  out of scope here.
