"""Strict, **independent** re-parser for Prometheus text exposition 0.0.4.

Role
----
This is the *checker*. :mod:`ano.telemetry.prometheus` is the *producer*.
``VALIDATION_STRATEGY.md`` §3.1 requires them to be independent:

    "the parser must not import generator code, must not share a grammar
    constant module, and should be written against ``PROMETHEUS_SPEC.md``
    rather than against the emitter. Shared code between producer and checker
    is how format bugs survive."

Accordingly every regex, charset, limit and escape table below is declared
**here**, from the specification, even though the writer declares something
that looks similar. The duplication is the point; do not factor it out. The
only symbol imported from the writer is the :class:`MetricSample` dataclass,
because contract C7 fixes it as this function's return type — and by then all
checks have already run.

Structure mirrors the reference implementation's single-pass state machine
(``expfmt/text_parse.go``) and reproduces its error conditions as enumerated
verbatim in ``PROMETHEUS_SPEC.md`` §1.12.

Rule codes (on :class:`~ano.telemetry.errors.ExpositionError.rule`):

============== ===============================================================
 ``P01``        CRLF / stray carriage return; document must end with LF
 ``P02``        invalid metric name (exporter charset)
 ``P03``        invalid label name, or reserved ``__`` prefix
 ``P04``        label syntax: missing ``=``, missing quote, unterminated value
 ``P05``        invalid escape sequence (only ``\\\\`` ``\\n`` ``\\"`` exist)
 ``P06``        value is not a float64 token
 ``P07``        timestamp is not an integer
 ``P08``        bad ``# TYPE`` token
 ``P09``        duplicate HELP / TYPE, or HELP / TYPE after the family's samples
 ``P10``        duplicate label name within one sample
 ``P11``        duplicate (metric name, label set) series
 ``P12``        histogram: missing ``le="+Inf"``
 ``P13``        histogram: ``_bucket{le="+Inf"}`` != ``_count``
 ``P14``        histogram: bucket counts not monotonic in ``le``
 ``P15``        ``_sum`` / ``_count`` carries an ``le`` or ``quantile`` label
 ``P16``        ``le`` / ``quantile`` not a float; ``quantile`` outside [0,1]
 ``P17``        GMP target label emitted by an exporter
 ``P18``        GMP label key / value length limit exceeded
 ``P19``        trailing junk on a line
============== ===============================================================

Deliberate deviation from the reference parser
----------------------------------------------
Go's ``ParseFloat`` also accepts hexadecimal float literals (``0x1p-2``). No
exporter emits them and the canonical writer never produces one, so this parser
rejects them under ``P06``. That makes it *stricter* than the reference, never
more permissive — which is the safe direction for a conformance checker.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from ano.telemetry.errors import ExpositionError
from ano.telemetry.prometheus import MetricSample  # return type fixed by C7

__all__ = [
    "ParsedSample",
    "ParsedDocument",
    "parse_document",
    "parse_exposition",
    "validate_exposition",
    "assert_roundtrip",
]

# --- grammar, declared independently from the specification ----------------
# Exporter charset. The full Prometheus metric-name charset also allows ':',
# but that is reserved for recording rules (PROMETHEUS_SPEC.md §1.3).
_NAME = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
_LABEL = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
#: Go ParseFloat-compatible decimal token, minus hex literals (see module note).
_FLOAT = re.compile(r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$")
_INTEGER = re.compile(r"^[+-]?[0-9]+$")
_SPECIAL_FLOATS = {"NaN": math.nan, "+Inf": math.inf, "-Inf": -math.inf}
_TYPE_TOKENS = frozenset({"counter", "gauge", "histogram", "summary", "untyped"})
_ESCAPES = {"\\": "\\", "n": "\n", '"': '"'}
_RESERVED_TARGET_LABELS = frozenset(
    {"project_id", "location", "cluster", "namespace", "job", "instance"}
)
_MAX_KEY_CHARS = 100
_MAX_VALUE_CHARS = 1024
_WS = " \t"


@dataclass(frozen=True, slots=True)
class ParsedSample:
    """One parsed sample, with provenance for diagnostics."""

    name: str
    labels: tuple[tuple[str, str], ...]
    value: float
    timestamp: int | None
    line: int

    def label(self, key: str) -> str | None:
        for name, value in self.labels:
            if name == key:
                return value
        return None

    def without(self, key: str) -> tuple[tuple[str, str], ...]:
        return tuple((n, v) for n, v in self.labels if n != key)


@dataclass(frozen=True, slots=True)
class ParsedDocument:
    """The result of a successful strict parse."""

    samples: tuple[ParsedSample, ...]
    types: dict[str, str]
    helps: dict[str, str]

    def type_of(self, family: str) -> str:
        """Return the declared type of ``family``, or ``untyped`` if undeclared."""
        return self.types.get(family, "untyped")


class _LineCursor:
    """Character cursor over one line, mirroring the reference state machine."""

    __slots__ = ("text", "pos", "line")

    def __init__(self, text: str, line: int) -> None:
        self.text = text
        self.pos = 0
        self.line = line

    def eof(self) -> bool:
        return self.pos >= len(self.text)

    def peek(self) -> str:
        return "" if self.eof() else self.text[self.pos]

    def take(self) -> str:
        char = self.text[self.pos]
        self.pos += 1
        return char

    def skip_ws(self) -> None:
        while not self.eof() and self.text[self.pos] in _WS:
            self.pos += 1

    def fail(self, rule: str, message: str) -> None:
        raise ExpositionError(rule, message, self.line)


def _parse_name(cursor: _LineCursor, rule: str, what: str) -> str:
    match = _NAME.match(cursor.text, cursor.pos)
    if match is None or match.start() != cursor.pos:
        cursor.fail(
            rule,
            f"invalid {what} at column {cursor.pos + 1}: "
            f"{cursor.text[cursor.pos:cursor.pos + 24]!r}",
        )
    cursor.pos = match.end()
    return match.group(0)


def _parse_labels(cursor: _LineCursor) -> tuple[tuple[str, str], ...]:
    """Parse ``{a="1",b="2",}``; the cursor sits on ``{`` on entry."""
    cursor.take()  # consume '{'
    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()
    while True:
        cursor.skip_ws()
        if cursor.eof():
            cursor.fail("P04", "unexpected end of label set: missing '}'")
        if cursor.peek() == "}":
            cursor.take()
            return tuple(pairs)

        match = _LABEL.match(cursor.text, cursor.pos)
        if match is None or match.start() != cursor.pos:
            cursor.fail(
                "P03",
                f"invalid label name at column {cursor.pos + 1}: "
                f"{cursor.text[cursor.pos:cursor.pos + 24]!r}",
            )
        key = match.group(0)
        cursor.pos = match.end()
        # The charset regex stops at the first illegal character, so a name
        # like ``cpu-id`` would otherwise surface as "missing '='". Report the
        # real cause. Label-set punctuation is excluded: a '"', ',' or '}'
        # here means the '=' is missing, which is a different defect (P04).
        if cursor.peek() not in ("", "=", '"', ",", "}") and cursor.peek() not in _WS:
            cursor.fail(
                "P03",
                f"invalid label name: {key + cursor.text[cursor.pos:cursor.pos + 8]!r} "
                "contains a character outside ^[a-zA-Z_][a-zA-Z0-9_]*$",
            )
        if key.startswith("__"):
            cursor.fail("P03", f"label name {key!r} uses the reserved '__' prefix")
        if key in seen:
            cursor.fail("P10", f"duplicate label name {key!r} within one sample")
        if len(key) > _MAX_KEY_CHARS:
            cursor.fail("P18", f"label key {key!r} exceeds {_MAX_KEY_CHARS} characters")
        if key in _RESERVED_TARGET_LABELS:
            cursor.fail(
                "P17",
                f"label {key!r} is a Managed Prometheus target label attached by "
                "the collector at export time; an exporter must not emit it",
            )
        seen.add(key)

        cursor.skip_ws()
        if cursor.peek() != "=":
            cursor.fail(
                "P04", f"expected '=' after label name {key!r}, found {cursor.peek()!r}"
            )
        cursor.take()
        cursor.skip_ws()
        if cursor.peek() != '"':
            cursor.fail(
                "P04",
                f"expected '\"' at start of label value, found {cursor.peek()!r}",
            )
        cursor.take()
        value = _parse_label_value(cursor)
        if len(value) > _MAX_VALUE_CHARS:
            cursor.fail("P18", f"label {key!r} value exceeds {_MAX_VALUE_CHARS} characters")
        pairs.append((key, value))

        cursor.skip_ws()
        char = cursor.peek()
        if char == ",":
            cursor.take()
            continue
        if char == "}":
            cursor.take()
            return tuple(pairs)
        cursor.fail("P04", f"expected ',' or '}}' after label value, found {char!r}")


def _parse_label_value(cursor: _LineCursor) -> str:
    """Read a quoted label value; the opening quote is already consumed."""
    out: list[str] = []
    while True:
        if cursor.eof():
            cursor.fail("P04", "unexpected end of label value: missing closing '\"'")
        char = cursor.take()
        if char == '"':
            return "".join(out)
        if char != "\\":
            out.append(char)
            continue
        if cursor.eof():
            cursor.fail("P05", "unexpected end of input after '\\'")
        escaped = cursor.take()
        if escaped not in _ESCAPES:
            cursor.fail(
                "P05",
                f"invalid escape sequence '\\{escaped}'. Only \\\\, \\n and \\\" "
                "exist in this format — in particular there is no \\t and no \\r",
            )
        out.append(_ESCAPES[escaped])


def _parse_value(token: str, cursor: _LineCursor) -> float:
    if token in _SPECIAL_FLOATS:
        return _SPECIAL_FLOATS[token]
    lowered = token.lower()
    if lowered in ("nan", "inf", "+inf", "-inf", "infinity") or lowered.endswith(
        "infinity"
    ):
        cursor.fail(
            "P06",
            f"expected float as value, got {token!r}. The only accepted "
            "non-finite spellings are exactly 'NaN', '+Inf' and '-Inf'",
        )
    if _FLOAT.match(token) is None:
        cursor.fail("P06", f"expected float as value, got {token!r}")
    return float(token)


def _read_token(cursor: _LineCursor) -> str:
    start = cursor.pos
    while not cursor.eof() and cursor.text[cursor.pos] not in _WS:
        cursor.pos += 1
    return cursor.text[start : cursor.pos]


def parse_document(text: str) -> ParsedDocument:
    """Parse and fully validate an exposition document.

    Args:
        text: The whole document.

    Returns:
        A :class:`ParsedDocument` with every sample, and the declared types
        and help strings.

    Raises:
        ano.telemetry.errors.ExpositionError: on the first violation, carrying
            a ``rule`` code from the table in this module's docstring and the
            1-based ``line`` it was detected on.
    """
    if not isinstance(text, str):
        raise ExpositionError("P01", f"expected str, got {type(text).__name__}")
    if "\r" in text:
        line = text.count("\n", 0, text.index("\r")) + 1
        raise ExpositionError(
            "P01",
            "carriage return found: the line terminator is LF only, and a "
            "trailing '\\r' is a parse error",
            line,
        )
    if text and not text.endswith("\n"):
        raise ExpositionError(
            "P01",
            "unexpected end of input stream: the document must end with a newline",
            text.count("\n") + 1,
        )

    types: dict[str, str] = {}
    helps: dict[str, str] = {}
    samples: list[ParsedSample] = []
    families_with_samples: set[str] = set()
    series_seen: dict[tuple[str, tuple[tuple[str, str], ...]], int] = {}

    for index, raw in enumerate(text.split("\n")[:-1] if text else []):
        line_no = index + 1
        cursor = _LineCursor(raw, line_no)
        cursor.skip_ws()
        if cursor.eof():
            continue  # blank line

        if cursor.peek() == "#":
            cursor.take()
            cursor.skip_ws()
            keyword = _read_token(cursor)
            if keyword == "HELP":
                cursor.skip_ws()
                name = _parse_name(cursor, "P02", "metric name in comment")
                if name in helps:
                    cursor.fail("P09", f"second HELP line for metric name {name!r}")
                if name in families_with_samples:
                    cursor.fail(
                        "P09", f"HELP for {name!r} must precede its samples"
                    )
                if not cursor.eof():
                    cursor.take()  # the single separating space
                helps[name] = _unescape_help(cursor.text[cursor.pos :], cursor)
            elif keyword == "TYPE":
                cursor.skip_ws()
                name = _parse_name(cursor, "P02", "metric name in comment")
                if name in types:
                    cursor.fail("P09", f"second TYPE line for metric name {name!r}")
                if name in families_with_samples:
                    cursor.fail(
                        "P09", f"TYPE for {name!r} must precede its samples"
                    )
                cursor.skip_ws()
                token = _read_token(cursor)
                if token not in _TYPE_TOKENS:
                    cursor.fail(
                        "P08",
                        f"unexpected type {token!r} for metric name {name!r}; "
                        f"permitted: {sorted(_TYPE_TOKENS)}",
                    )
                cursor.skip_ws()
                if not cursor.eof():
                    cursor.fail("P19", f"trailing junk after TYPE: {cursor.text[cursor.pos:]!r}")
                types[name] = token
            # Anything else after '#' is a free comment: ignored.
            continue

        # --- sample line ---
        name = _parse_name(cursor, "P02", "metric name")
        # The charset regex stops at the first illegal character. Only '{' or
        # whitespace may legally follow a metric name, so anything else means
        # the name itself is invalid -- report that, not "missing value".
        # ':' is legal Prometheus but reserved for recording rules, so an
        # exporter emitting it is an invalid metric name here (§1.3).
        if not cursor.eof() and cursor.peek() not in _WS and cursor.peek() != "{":
            cursor.fail(
                "P02",
                f"invalid metric name: "
                f"{name + cursor.text[cursor.pos:cursor.pos + 8]!r} contains a "
                "character outside ^[a-zA-Z_][a-zA-Z0-9_]*$ (exporters must not "
                "use ':', which is reserved for recording rules, nor dashes or "
                "dots, which the GMP collector does not scrape)",
            )
        labels: tuple[tuple[str, str], ...] = ()
        if cursor.peek() == "{":
            labels = _parse_labels(cursor)
        if cursor.eof() or cursor.peek() not in _WS:
            cursor.fail("P06", f"expected whitespace then a value after {name!r}")
        cursor.skip_ws()
        value_token = _read_token(cursor)
        if not value_token:
            cursor.fail("P06", f"missing value for {name!r}")
        value = _parse_value(value_token, cursor)

        timestamp: int | None = None
        cursor.skip_ws()
        if not cursor.eof():
            ts_token = _read_token(cursor)
            if _INTEGER.match(ts_token) is None:
                cursor.fail("P07", f"expected integer as timestamp, got {ts_token!r}")
            timestamp = int(ts_token)
            cursor.skip_ws()
            if not cursor.eof():
                cursor.fail("P19", f"trailing junk: {cursor.text[cursor.pos:]!r}")

        series = (name, tuple(sorted(labels)))
        if series in series_seen:
            cursor.fail(
                "P11",
                f"duplicate series {name!r} with the same label set (first seen "
                f"on line {series_seen[series]})",
            )
        series_seen[series] = line_no
        families_with_samples.add(name)
        families_with_samples.add(_family_name_of(name, types))
        samples.append(ParsedSample(name, labels, value, timestamp, line_no))

    document = ParsedDocument(tuple(samples), types, helps)
    _check_family_semantics(document)
    return document


def _unescape_help(text: str, cursor: _LineCursor) -> str:
    """HELP uses the *unquoted* escaper: only ``\\\\`` and ``\\n`` are defined."""
    out: list[str] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char != "\\":
            out.append(char)
            index += 1
            continue
        if index + 1 >= len(text):
            cursor.fail("P05", "HELP text ends with a dangling backslash")
        escaped = text[index + 1]
        if escaped == "\\":
            out.append("\\")
        elif escaped == "n":
            out.append("\n")
        else:
            cursor.fail(
                "P05",
                f"invalid escape sequence '\\{escaped}' in HELP text. HELP is not "
                "a quoted context, so '\\\"' is invalid there too",
            )
        index += 2
    return "".join(out)


def _family_name_of(sample_name: str, types: dict[str, str]) -> str:
    """Fold ``foo_bucket`` / ``foo_sum`` / ``foo_count`` back onto ``foo``."""
    for suffix in ("_bucket", "_sum", "_count"):
        if sample_name.endswith(suffix):
            base = sample_name[: -len(suffix)]
            if types.get(base) in ("histogram", "summary"):
                return base
    return sample_name


def _check_family_semantics(document: ParsedDocument) -> None:
    """Rules P12..P16: histogram and summary invariants."""
    by_family: dict[str, list[ParsedSample]] = {}
    for sample in document.samples:
        by_family.setdefault(
            _family_name_of(sample.name, document.types), []
        ).append(sample)

    for family, samples in by_family.items():
        kind = document.types.get(family)
        if kind == "histogram":
            _check_histogram(family, samples)
        elif kind == "summary":
            _check_summary(family, samples)
        else:
            for sample in samples:
                if sample.label("le") is not None:
                    raise ExpositionError(
                        "P15",
                        f"{sample.name!r} carries an 'le' label but {family!r} is "
                        f"not declared a histogram",
                        sample.line,
                    )


def _check_histogram(family: str, samples: list[ParsedSample]) -> None:
    counts_by_group: dict[tuple[tuple[str, str], ...], float] = {}
    buckets: dict[tuple[tuple[str, str], ...], list[tuple[float, float, int]]] = {}
    inf_by_group: dict[tuple[tuple[str, str], ...], tuple[float, int]] = {}

    for sample in samples:
        if sample.name == f"{family}_bucket":
            raw_le = sample.label("le")
            if raw_le is None:
                raise ExpositionError(
                    "P12", f"{sample.name!r} has no 'le' label", sample.line
                )
            bound = _parse_le(raw_le, sample)
            group = sample.without("le")
            if math.isinf(bound) and bound > 0:
                inf_by_group[group] = (sample.value, sample.line)
            else:
                buckets.setdefault(group, []).append((bound, sample.value, sample.line))
        elif sample.name in (f"{family}_sum", f"{family}_count"):
            if sample.label("le") is not None or sample.label("quantile") is not None:
                raise ExpositionError(
                    "P15",
                    f"{sample.name!r} must not carry an 'le' or 'quantile' label",
                    sample.line,
                )
            if sample.name.endswith("_count"):
                counts_by_group[sample.labels] = sample.value

    groups = set(buckets) | set(inf_by_group) | set(counts_by_group)
    for group in groups:
        if group not in inf_by_group:
            raise ExpositionError(
                "P12",
                f"histogram {family!r} is missing the mandatory le=\"+Inf\" bucket "
                f"for label set {dict(group)}",
            )
        inf_value, inf_line = inf_by_group[group]
        if group not in counts_by_group:
            raise ExpositionError(
                "P13", f"histogram {family!r} has no _count series for {dict(group)}", inf_line
            )
        if inf_value != counts_by_group[group]:
            raise ExpositionError(
                "P13",
                f"histogram {family!r}: le=\"+Inf\" bucket is {inf_value} but "
                f"_count is {counts_by_group[group]}; they must be equal",
                inf_line,
            )
        previous = -math.inf
        for bound, value, line in sorted(buckets.get(group, [])):
            if value < previous:
                raise ExpositionError(
                    "P14",
                    f"histogram {family!r}: bucket counts must be non-decreasing "
                    f"with ascending le (got {value} after {previous})",
                    line,
                )
            previous = value
        if previous > inf_value:
            raise ExpositionError(
                "P14",
                f"histogram {family!r}: a finite bucket ({previous}) exceeds the "
                f"+Inf bucket ({inf_value})",
                inf_line,
            )


def _check_summary(family: str, samples: list[ParsedSample]) -> None:
    for sample in samples:
        if sample.name in (f"{family}_sum", f"{family}_count"):
            if sample.label("le") is not None or sample.label("quantile") is not None:
                raise ExpositionError(
                    "P15",
                    f"{sample.name!r} must not carry an 'le' or 'quantile' label",
                    sample.line,
                )
            continue
        raw = sample.label("quantile")
        if raw is None:
            continue
        try:
            quantile = float(raw)
        except ValueError:
            raise ExpositionError(
                "P16", f"expected float as value for 'quantile' label, got {raw!r}",
                sample.line,
            ) from None
        if math.isnan(quantile) or not 0.0 <= quantile <= 1.0:
            raise ExpositionError(
                "P16", f"quantile must be in [0, 1], got {raw!r}", sample.line
            )


def _parse_le(raw: str, sample: ParsedSample) -> float:
    if raw in _SPECIAL_FLOATS:
        return _SPECIAL_FLOATS[raw]
    if _FLOAT.match(raw) is None:
        raise ExpositionError(
            "P16", f"expected float as value for 'le' label, got {raw!r}", sample.line
        )
    return float(raw)


# ---------------------------------------------------------------------------
# Contract C7 entry points
# ---------------------------------------------------------------------------
def parse_exposition(text: str) -> Iterator[MetricSample]:
    """Strictly parse exposition text into :class:`MetricSample` records.

    The whole document is validated before the first sample is yielded, so a
    violation anywhere raises immediately rather than part-way through
    iteration.
    """
    document = parse_document(text)
    return iter(
        [
            MetricSample(
                name=sample.name,
                labels=dict(sample.labels),
                value=sample.value,
                timestamp=sample.timestamp,
            )
            for sample in document.samples
        ]
    )


def validate_exposition(text: str) -> None:
    """Validate exposition text. Returns ``None``; raises on non-conformance."""
    parse_document(text)


def assert_roundtrip(text: str) -> None:
    """Assert that parse → serialise → parse is idempotent.

    ``VALIDATION_STRATEGY.md`` §3.3 calls this the strongest single check: it
    catches mis-escaped label values that re-parse as *different* values, float
    formatting that loses precision, unstable label ordering, and families that
    silently drop a bucket — none of which a single parse can see.

    Raises:
        ano.telemetry.errors.ExpositionError: if the re-serialised document
            fails to parse, or parses to different samples.
    """
    # Imported here, not at module scope, to keep the parser's import graph
    # free of the writer for everything except the C7 return type.
    from ano.telemetry.prometheus import serialize_exposition

    first = parse_document(text)
    metadata = {
        name: (kind, first.helps.get(name, "")) for name, kind in first.types.items()
    }
    rendered = serialize_exposition(
        [
            MetricSample(s.name, dict(s.labels), s.value, s.timestamp)
            for s in first.samples
        ],
        metadata,
    )
    second = parse_document(rendered)
    lhs = _comparable(first.samples)
    rhs = _comparable(second.samples)
    if lhs != rhs:
        only_first = sorted(set(lhs) - set(rhs))[:3]
        only_second = sorted(set(rhs) - set(lhs))[:3]
        raise ExpositionError(
            "P20",
            "parse/serialise is not idempotent; "
            f"first-only={only_first} second-only={only_second}",
        )


def _comparable(samples: Iterable[ParsedSample]) -> list[tuple]:
    out = []
    for sample in samples:
        # NaN != NaN, so compare its bit pattern by name instead.
        value = "NaN" if math.isnan(sample.value) else sample.value
        out.append((sample.name, tuple(sorted(sample.labels)), value, sample.timestamp))
    return sorted(out, key=repr)
