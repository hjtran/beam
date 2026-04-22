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

Validation happens in two places:

**At call time (`with_side_outputs`):**

1. Each value must be a `PCollection` — `TypeError` otherwise.
2. Each value's `pipeline` must equal `self.pipeline` — `ValueError`
   otherwise.

Tag names are validated implicitly by `**` (must be valid identifiers).
Tags with hyphens, spaces, or other punctuation are not supported because
attribute access (`pcoll.side_outputs.dropped`) is the primary access
pattern; users with arbitrary string tags should continue to use the
existing dict-return / `DoOutputsTuple` pattern.

**At apply time (inside `Pipeline._apply_internal` and `Pipeline._replace`):**

3. **Provenance check.** Each side output must be produced by a transform
   inside the wrapping composite's subtree (i.e. the side output's
   `producer` must be `current` or a descendant of `current.parts`). This
   prevents transforms from exposing unrelated PCollections as their
   outputs, which would corrupt the pipeline graph. Raises `ValueError`
   if violated.

4. **Tag collision check.** If a side-output tag already exists in
   `current.outputs`, the value at that tag must be the *same*
   `PCollection` object as the side output being registered. Otherwise
   raises `ValueError`. (Silent skip would let `out.side_outputs.foo`
   disagree with `out.producer.outputs['foo']`.)

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

There are **two** output-registration sites in `pipeline.py`, both of which
need the new hook:

1. `Pipeline._apply_internal` (~line 818) — the normal apply path.
2. `Pipeline._replace` (~line 401–413) — the override / replacement path,
   which currently only handles plain `PValue`, `dict`, and
   `DoOutputsTuple` returns. Without an update here, transform overrides
   would silently drop side outputs.

**Scope:** The hook only fires for the *top-level* returned object. We do
not walk nested PCollections inside dict/tuple/list returns looking for
attached side outputs — those are an opt-in, top-level convenience.

**Behavior at the hook:**

```python
# After current.add_output(result, tag), where result is the top-level
# returned PCollection:
if isinstance(result, pvalue.PCollection) and result._side_outputs:
    for side_tag, side_pcoll in result._side_outputs.items():
        # Provenance check: the side output's producer must be inside
        # current's subtree.
        _verify_descendant(side_pcoll, current)
        # Collision check: same-tag must mean same PCollection.
        existing = current.outputs.get(side_tag)
        if existing is not None and existing is not side_pcoll:
            raise ValueError(
                f"Side output tag {side_tag!r} conflicts with an existing "
                f"output of the same transform.")
        if existing is None:
            current.add_output(side_pcoll, side_tag)
```

`_verify_descendant` walks `current.parts` (and their `parts`, recursively)
collecting the set of `AppliedPTransform`s, then asserts that
`side_pcoll.producer` is in that set or is `current` itself.

`add_output` (`pipeline.py:1371`) only sets `self.outputs[tag] = output`;
it does not modify the side output's `producer`. The side output's true
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
- **Proto round-trip:** Side outputs are real outputs on the wrapping
  composite's `AppliedPTransform`, so they appear in the runner API proto
  via `named_outputs()`. The Python-side `_side_outputs` annotation on the
  returned PCollection is *not* serialized. After `from_runner_api`, the
  reconstructed PCollection will not have `_side_outputs` populated.
  **`.side_outputs` is a construction-time ergonomic, not a durable graph
  property.** Users who need to access side outputs of a deserialized
  pipeline should walk `AppliedPTransform.outputs` directly. This is
  documented on `with_side_outputs`.

### Type checking

Beam's composite-boundary type checking (`type_check_outputs` in
`Pipeline._apply_internal`, ~line 815) inspects the value returned from
`expand()`. With this proposal, only the *main* PCollection's element type
participates in that check — attached side outputs are not type-checked
at the composite boundary. They retain whatever element type their inner
producer assigned. This matches the behavior of side outputs accessed via
`DoOutputsTuple` and is documented on `with_side_outputs`.

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
  block in two locations: inside `_apply_internal`'s result-walking loop
  (only for the top-level returned PCollection), and inside `_replace`'s
  output-handling block. Both call a shared helper for
  provenance/collision validation.
- `sdks/python/apache_beam/pvalue_test.py` — unit tests for the container,
  call-time validation rules, and the `with_side_outputs` copy semantics.
- `sdks/python/apache_beam/pipeline_test.py` — end-to-end and graph-level
  tests (see Testing Strategy below).

## Testing Strategy

**Unit tests (`pvalue_test.py`):**

- `with_side_outputs` returns a copy; original unchanged.
- Side output access via attribute and index; missing-tag error message
  lists available tags.
- `TypeError` when a non-PCollection is passed.
- `ValueError` when a side output is on a different pipeline.
- Empty-side-outputs container behavior.
- Calling `with_side_outputs` twice replaces (does not merge).

**Pipeline graph tests (`pipeline_test.py`):**

- End-to-end: composite `PTransform` whose `expand()` returns
  `result.with_side_outputs(dropped=...)`. Apply it, assert
  `out.side_outputs.dropped` is a PCollection, assert the wrapping
  `AppliedPTransform.outputs` contains both `None` (main) and `"dropped"`,
  assert chaining (`out | Next()`) uses the main output. Materialize and
  `assert_that` on both outputs.
- Wrapping a `ParDo(...).with_outputs(...)` inside a composite that returns
  `result.with_side_outputs(...)` (the canonical `MyFilter` example).
- Provenance violation: returning `with_side_outputs(other=foreign_pcoll)`
  where `foreign_pcoll` was not produced inside the composite — must raise
  `ValueError` at apply time.
- Tag collision with a non-identical PCollection — must raise `ValueError`.
- Tag collision with the *same* PCollection — must succeed (idempotent).
- `Pipeline.replace_all`: replacement transform that returns a PCollection
  with side outputs — assert the side outputs end up registered on the
  replacement's `AppliedPTransform`.
- Runner API round-trip: build a pipeline using `with_side_outputs`,
  serialize via `to_runner_api`, deserialize via `from_runner_api`, and
  confirm the named outputs survive on the producer's `AppliedPTransform`.
- Nested-return non-flattening: `expand()` returns a dict containing a
  PCollection that has `_side_outputs` set — confirm those side outputs
  are NOT auto-registered (they're only registered when the top-level
  result is itself the PCollection-with-side-outputs).

## Risks & Mitigations

- **Risk:** A user attaches a foreign PCollection as a side output, which
  would corrupt the pipeline graph by listing it under the wrong producer.
  **Mitigation:** Apply-time provenance check (`_verify_descendant`) raises
  `ValueError` if the side output's `producer` is not `current` or one of
  its descendants.
- **Risk:** Tag collision with an existing output of the wrapping
  transform produces inconsistent state.
  **Mitigation:** Apply-time collision check raises `ValueError` unless
  the existing entry refers to the same `PCollection` object (idempotent
  re-registration).
- **Risk:** Tag collision with the main output's tag (`None` is the main
  tag). Kwargs cannot be `None`, so this can't happen accidentally.
- **Risk:** Tag flexibility — Beam's existing tag mechanism allows
  arbitrary strings. `with_side_outputs(**kwargs)` only allows valid
  Python identifiers. **Mitigation:** Documented limitation; users with
  arbitrary tags should continue to use the existing dict-return /
  `DoOutputsTuple` patterns. A future overload could accept a mapping
  for non-identifier tags.

## Open Questions (to defer)

- Should `with_side_outputs` chain (i.e., merge with existing `_side_outputs`)
  rather than replace? The spec says replace, matching `copy.copy`'s
  shallow semantics. If chaining ever becomes desired, the current contract
  permits adding it (additive, non-breaking).
- Static type checking: `pcoll.side_outputs.dropped` is opaque to mypy.
  Could be addressed later with a typed factory or a generic protocol;
  out of scope here.
