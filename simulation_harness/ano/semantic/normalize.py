"""Structural normalisation of log messages into an embeddable token stream.

What this module is
-------------------
A **tokeniser**. It rewrites the *volatile* parts of a log line — numbers,
identifiers, addresses, timestamps, paths — into typed placeholders, then
splits what remains into linguistic tokens. The output is the input to
:mod:`ano.semantic.embedding`.

What this module is deliberately **not**
----------------------------------------
It is not a rule list and not an error dictionary. Every rewrite below is keyed
on the *lexical shape* of a substring (is it a run of digits? does it look like
a dotted quad? is it 32 hex characters?) and never on its meaning. There is no
table of "known bad" words, no severity mapping, and nothing anywhere in this
package that says a particular word implies a fault. Requirement R1 forbids
exactly that, and ``tests/m3_semantic/test_no_pattern_list.py`` enforces it by
scanning this package's string literals.

Why normalise at all
--------------------
Two log lines that differ only in a request id, a port number or a millisecond
count describe the *same* event. Without normalisation every occurrence hashes
into different buckets and the embedding space degenerates into one point per
line: nothing clusters, novelty detection becomes meaningless, and the model
would report every line as unseen. Collapsing volatile spans structurally is
what makes "semantically identical messages embed near each other" true.

The placeholder vocabulary is itself a feature: ``<num>`` appearing four times
is a different shape from ``<num>`` appearing once, and the embedding sees that.

Determinism
-----------
Pure text in, pure text out. No randomness, no clock, no locale dependence
(matching is ASCII-scoped and case folding uses :meth:`str.lower`).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator, Sequence

__all__ = [
    "PLACEHOLDERS",
    "normalize_message",
    "signature",
    "tokenize",
    "token_shingles",
    "char_ngrams",
    "structural_shape",
    "iter_features",
]

#: Every placeholder this tokeniser can emit. Each names a *lexical class*,
#: never a semantic category. Exported so tests and the demo surface can show
#: the whole vocabulary — it is small, closed and auditable on sight.
PLACEHOLDERS: tuple[str, ...] = (
    "<ts>",      # a date/time literal
    "<uuid>",    # 8-4-4-4-12 hex
    "<url>",     # scheme://...
    "<path>",    # a filesystem path with at least one separator
    "<ip>",      # IPv4 / IPv6 literal, optionally with :port
    "<mac>",     # six colon- or dash-separated hex octets
    "<hex>",     # a bare hex run of 8+ digits (handles, addresses, digests)
    "<dur>",     # a number immediately followed by a time unit
    "<size>",    # a number immediately followed by a byte unit
    "<ver>",     # a dotted numeric version
    "<num>",     # any other integer / decimal / scientific literal
)

# ---------------------------------------------------------------------------
# Lexical-shape patterns.
#
# Ordering matters: the most specific shape must win, otherwise <num> would eat
# the digits inside a timestamp. Each entry is (compiled pattern, placeholder).
# Read every one of these as "this substring *looks like* X", never as "this
# substring *means* X".
# ---------------------------------------------------------------------------

_HEX = "[0-9a-fA-F]"

_SHAPE_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    # ISO-8601 / RFC 3339 date-time, with or without a time part or zone.
    (
        re.compile(
            r"\d{4}-\d{2}-\d{2}"
            r"(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?"
        ),
        "<ts>",
    ),
    # Bare wall-clock time, e.g. 14:03:59 or 14:03:59.123
    (re.compile(r"\b\d{2}:\d{2}:\d{2}(?:\.\d+)?\b"), "<ts>"),
    # Canonical UUID.
    (
        re.compile(
            rf"\b{_HEX}{{8}}-{_HEX}{{4}}-{_HEX}{{4}}-{_HEX}{{4}}-{_HEX}{{12}}\b"
        ),
        "<uuid>",
    ),
    # MAC address (before <hex>, which would otherwise not match anyway, and
    # before <ip>, which must not see the colons).
    (re.compile(rf"\b(?:{_HEX}{{2}}[:-]){{5}}{_HEX}{{2}}\b"), "<mac>"),
    # URL with an explicit scheme.
    (re.compile(r"\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s\"'<>]+"), "<url>"),
    # IPv6 literal in brackets, optionally with a port.
    (re.compile(r"\[[0-9a-fA-F:]{2,}\](?::\d{1,5})?"), "<ip>"),
    # Bare IPv6 (at least two colons keeps it away from times and label lists).
    (re.compile(rf"\b(?:{_HEX}{{1,4}}:){{2,7}}(?:{_HEX}{{1,4}}|:)\b"), "<ip>"),
    # IPv4, optional CIDR suffix, optional :port.
    (
        re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?:/\d{1,2})?(?::\d{1,5})?\b"),
        "<ip>",
    ),
    # Absolute POSIX path or Windows path. Requires a separator so bare words
    # are untouched.
    (re.compile(r"(?:[A-Za-z]:)?[\\/](?:[\w.@%+-]+[\\/])*[\w.@%+-]+"), "<path>"),
    # Dotted numeric version with three or more components.
    (re.compile(r"\bv?\d+\.\d+\.\d+(?:[.-]\w+)?\b"), "<ver>"),
    # Number + time unit, written together.
    (
        re.compile(
            r"\b\d+(?:\.\d+)?(?:ns|us|\u00b5s|ms|s|m|h|d)\b"
        ),
        "<dur>",
    ),
    # Number + byte unit, written together.
    (
        re.compile(
            r"\b\d+(?:\.\d+)?(?:(?:[kK]|[mM]|[gG]|[tT]|[pP])i?)?[bB](?:ps|/s)?\b"
        ),
        "<size>",
    ),
    # Hex run, with or without the 0x prefix. 8+ digits keeps ordinary words
    # like "added" (all hex letters) out of it.
    (re.compile(rf"\b(?:0[xX])?{_HEX}{{8,}}\b"), "<hex>"),
    # Anything else numeric: signed, decimal, scientific, percent-adjacent.
    (re.compile(r"[+-]?\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b"), "<num>"),
)

#: Splits identifiers written in camelCase or PascalCase.
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")

#: Splits a digit run away from adjacent letters (``user4821`` -> ``user`` +
#: ``4821``, which :func:`tokenize` then folds to ``<num>``). Purely a shape
#: rule: it knows nothing about which words it is separating.
_ALNUM_BOUNDARY = re.compile(r"(?<=[A-Za-z])(?=\d)|(?<=\d)(?=[A-Za-z])")

#: A token is a run of letters/digits/underscore, or one of our placeholders.
_TOKEN = re.compile(r"<[a-z]+>|[A-Za-z0-9_]+")

#: Collapses runs of whitespace.
_SPACES = re.compile(r"\s+")


def normalize_message(message: str) -> str:
    """Rewrite volatile spans of ``message`` into typed placeholders.

    Args:
        message: A raw log message. ``None``-ish input is treated as empty.

    Returns:
        The message with every volatile span replaced by a placeholder from
        :data:`PLACEHOLDERS`, whitespace collapsed, and surrounding whitespace
        stripped. Case is preserved here; :func:`tokenize` folds it.

    Example:
        >>> normalize_message("pool exhausted after 1200ms on 10.2.3.4:8080")
        'pool exhausted after <dur> on <ip>'
    """
    if not message:
        return ""
    text = str(message)
    for pattern, placeholder in _SHAPE_RULES:
        text = pattern.sub(placeholder, text)
    return _SPACES.sub(" ", text).strip()


def tokenize(message: str, *, normalized: bool = False) -> tuple[str, ...]:
    """Split ``message`` into lower-cased tokens after normalisation.

    Three structural splits happen here, all of them about shape:

    * ``camelCase`` and ``PascalCase`` are split into their parts;
    * ``snake_case`` is split on the underscore;
    * a digit run glued to letters is split off and replaced by ``<num>``.

    The first two mean ``poolExhausted`` and ``pool_exhausted`` produce the
    same two tokens, so components that spell the same event differently land
    near each other. The third means ``user4821`` and ``user97`` produce
    ``user <num>`` rather than two unrelated vocabulary items — without it,
    every identifier with a counter in it would be its own template and the
    embedding space would fill with singletons.

    Args:
        message: A raw log message, or an already-normalised one.
        normalized: Set True if ``message`` has already been through
            :func:`normalize_message`, to skip the rewrite pass.

    Returns:
        Tokens in order. Placeholders survive intact as single tokens.
    """
    text = message if normalized else normalize_message(message)
    tokens: list[str] = []
    for raw in _TOKEN.findall(text):
        if raw.startswith("<") and raw.endswith(">"):
            tokens.append(raw)
            continue
        for part in _CAMEL.split(raw):
            for piece in part.split("_"):
                if not piece:
                    continue
                for atom in _ALNUM_BOUNDARY.split(piece):
                    if not atom:
                        continue
                    tokens.append("<num>" if atom.isdigit() else atom.lower())
    return tuple(tokens)


def signature(message: str) -> str:
    """The canonical template string for ``message``.

    Two messages share a signature exactly when they are the same event with
    different volatile values. The signature is what the outlier model
    de-duplicates on, what the training-window audit counts, and what a human
    reads in the evidence field of a semantic finding.

    Args:
        message: A raw log message.

    Returns:
        Space-joined normalised tokens, e.g. ``"pool exhausted after dur on ip"``
        becomes ``"pool exhausted after <dur> on <ip>"``.
    """
    return " ".join(tokenize(message))


def token_shingles(
    tokens: Sequence[str], *, max_order: int = 2
) -> Iterator[str]:
    """Yield token n-grams of order 1..``max_order``.

    Unigrams carry the vocabulary; bigrams carry word order, which is what
    separates "connection refused by peer" from "peer refused by connection".

    Args:
        tokens: Output of :func:`tokenize`.
        max_order: Highest n-gram order. 1 disables bigrams.

    Yields:
        Feature strings, prefixed with their order so a unigram and a bigram
        can never collide in the hash space.
    """
    if max_order < 1:
        raise ValueError(f"max_order must be >= 1, got {max_order}")
    count = len(tokens)
    for order in range(1, max_order + 1):
        if order > count:
            break
        prefix = f"w{order}:"
        for index in range(count - order + 1):
            yield prefix + " ".join(tokens[index : index + order])


def char_ngrams(
    text: str, *, min_n: int = 3, max_n: int = 5
) -> Iterator[str]:
    """Yield character n-grams over a boundary-padded, lower-cased ``text``.

    Word boundaries are marked with ``#`` so a prefix n-gram is distinguishable
    from the same letters mid-word. Character n-grams are what give the
    embedding graceful behaviour on words it has never seen: an unknown token
    still shares sub-strings with related known tokens, so the vector moves in
    a sensible direction instead of becoming pure noise.

    Args:
        text: Normalised message text (placeholders included).
        min_n: Smallest n-gram length.
        max_n: Largest n-gram length.

    Yields:
        Feature strings prefixed with their order.
    """
    if min_n < 1 or max_n < min_n:
        raise ValueError(f"bad n-gram range {min_n}..{max_n}")
    padded = "#" + text.lower().replace(" ", "#") + "#"
    length = len(padded)
    for order in range(min_n, max_n + 1):
        if order > length:
            break
        prefix = f"c{order}:"
        for index in range(length - order + 1):
            yield prefix + padded[index : index + order]


def structural_shape(tokens: Sequence[str]) -> Iterator[str]:
    """Yield coarse shape features describing the message's structure.

    These are bucketed counts, not content: how many tokens, how many of each
    placeholder class. They let the model notice "this line has the same words
    but eleven numeric fields instead of two" — a structural novelty that
    word features alone would miss.

    Args:
        tokens: Output of :func:`tokenize`.

    Yields:
        Feature strings prefixed with ``s:``.
    """
    yield f"s:len:{_bucket(len(tokens))}"
    counts: dict[str, int] = {}
    for token in tokens:
        if token in _PLACEHOLDER_SET:
            counts[token] = counts.get(token, 0) + 1
    for placeholder in PLACEHOLDERS:
        count = counts.get(placeholder, 0)
        if count:
            yield f"s:{placeholder}:{_bucket(count)}"


_PLACEHOLDER_SET = frozenset(PLACEHOLDERS)


def _bucket(count: int) -> int:
    """Map a count to a coarse log-ish bucket so near counts share a feature."""
    if count <= 4:
        return count
    if count <= 8:
        return 8
    if count <= 16:
        return 16
    if count <= 32:
        return 32
    return 64


def iter_features(
    message: str,
    *,
    max_token_order: int = 2,
    min_char_n: int = 3,
    max_char_n: int = 5,
    include_shape: bool = True,
) -> Iterable[str]:
    """Yield every feature string for ``message``.

    This is the single place that decides what the embedding sees. It is a
    union of three shape families — token shingles, character n-grams and
    structural counts — none of which reference message semantics.

    Args:
        message: A raw log message.
        max_token_order: Highest token n-gram order.
        min_char_n: Smallest character n-gram length.
        max_char_n: Largest character n-gram length. Set below ``min_char_n``
            to disable character n-grams entirely.
        include_shape: Emit the structural count features.

    Yields:
        Feature strings, each already namespaced by family.
    """
    normalized = normalize_message(message)
    tokens = tokenize(normalized, normalized=True)
    yield from token_shingles(tokens, max_order=max_token_order)
    if max_char_n >= min_char_n:
        yield from char_ngrams(normalized, min_n=min_char_n, max_n=max_char_n)
    if include_shape:
        yield from structural_shape(tokens)
