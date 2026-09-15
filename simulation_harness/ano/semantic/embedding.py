"""Vector embeddings of log messages — the hashing trick + signed projection.

Requirement R1 asks for log messages "converted into vector embeddings so that
semantically unusual log behaviour can be detected without pre-written regex
rules or a fixed error dictionary". This module produces those vectors.

The scheme
----------
For a message we extract a bag of **shape features** (:func:`
ano.semantic.normalize.iter_features`): token unigrams and bigrams over the
structurally normalised text, character 3–5-grams, and coarse structural counts.
Each feature string ``f`` is then projected into a fixed ``dim``-dimensional
space by the classic *hashing trick* with a *signed random projection*:

.. code-block:: text

    b = stable_bucket(f, dim, namespace)      # which coordinate
    s = stable_sign(f, namespace)             # +1 or -1
    acc[b] += s * weight(f)

This is the Weinberger et al. feature-hashing sketch: an explicit random
projection matrix of shape ``|features| x dim`` whose entries are ±1, evaluated
lazily from a hash instead of stored. It preserves inner products in
expectation, which is precisely the property a distance-based novelty model
needs. It requires no vocabulary, no training corpus and no model file, so it
works on tokens the system has never seen — the property AC-15 depends on.

Weighting
---------
Within a family, repeated features get sublinear term frequency
``1 + log(count)``. Each family (token shingles, character n-grams, structural
counts) is L2-normalised **separately** and then combined with the weights in
:data:`DEFAULT_FAMILY_WEIGHTS`. Without per-family normalisation the character
n-grams — of which there are roughly one per character — would swamp the
handful of token features and the vector would stop reflecting word content.
The final vector is L2-normalised, so cosine similarity is a plain dot product.

Determinism
-----------
Every hash is :mod:`ano.contracts.determinism` (blake2b), never builtin
``hash()``. The embedding of a message is a pure function of the message text
and :class:`EmbeddingConfig`; there is no RNG and no fitted state. Two
processes, two machines and two ``PYTHONHASHSEED`` values produce byte-identical
vectors.

What this module does **not** contain
-------------------------------------
No list of error patterns, no keyword table, no severity weighting, no
per-message rules. It cannot: it never sees anything but feature strings and a
hash function. ``tests/m3_semantic/test_no_pattern_list.py`` asserts this by
scanning the package's literals.
"""

from __future__ import annotations

import math
import operator
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from ano.contracts.determinism import stable_bucket, stable_hash_hex, stable_sign
from ano.semantic.normalize import (
    char_ngrams,
    signature,
    structural_shape,
    token_shingles,
)


__all__ = [
    "DEFAULT_DIM",
    "EMBEDDING_NAMESPACE",
    "FAMILIES",
    "DEFAULT_FAMILY_WEIGHTS",
    "EmbeddingConfig",
    "HashedEmbedder",
    "cosine_similarity",
    "cosine_distance",
    "l2_normalize",
    "dot",
]

#: Default vector dimensionality. 96 coordinates is enough that random-hash
#: collisions between the few hundred distinct signatures a real estate emits
#: stay negligible, and small enough that a full pairwise cosine scan over
#: thousands of vectors costs milliseconds in pure Python (D7 capacity budget).
DEFAULT_DIM = 96

#: Domain-separation namespace for every hash in this module. **Frozen**:
#: changing it changes every vector the system has ever produced.
EMBEDDING_NAMESPACE = "ano.semantic.embedding.v1"

#: The three feature families, in a fixed order.
FAMILIES: tuple[str, ...] = ("token", "char", "shape")

#: How much each family contributes to the final vector. Token content
#: dominates; character n-grams provide graceful degradation on unseen words;
#: structural counts are a small nudge, not a vote.
DEFAULT_FAMILY_WEIGHTS: dict[str, float] = {
    "token": 0.60,
    "char": 0.34,
    "shape": 0.06,
}


@dataclass(frozen=True, slots=True)
class EmbeddingConfig:
    """Everything that determines a vector, in one hashable record.

    Attributes:
        dim: Vector dimensionality.
        max_token_order: Highest token n-gram order (2 = unigrams + bigrams).
        min_char_n: Smallest character n-gram length.
        max_char_n: Largest character n-gram length. Set below ``min_char_n``
            to switch character n-grams off.
        include_shape: Include the structural count family.
        namespace: Hash domain-separation namespace.
        family_weights: Per-family contribution weights.
    """

    dim: int = DEFAULT_DIM
    max_token_order: int = 2
    min_char_n: int = 3
    max_char_n: int = 5
    include_shape: bool = True
    namespace: str = EMBEDDING_NAMESPACE
    family_weights: tuple[tuple[str, float], ...] = field(
        default=tuple(sorted(DEFAULT_FAMILY_WEIGHTS.items()))
    )

    def __post_init__(self) -> None:
        if self.dim < 2:
            raise ValueError(f"dim must be >= 2, got {self.dim}")
        if self.max_token_order < 1:
            raise ValueError(
                f"max_token_order must be >= 1, got {self.max_token_order}"
            )
        if self.min_char_n < 1:
            raise ValueError(f"min_char_n must be >= 1, got {self.min_char_n}")
        if not self.namespace:
            raise ValueError("namespace must be non-empty")
        names = {name for name, _ in self.family_weights}
        unknown = sorted(names - set(FAMILIES))
        if unknown:
            raise ValueError(f"unknown feature families: {unknown}")
        for name, weight in self.family_weights:
            if weight < 0.0:
                raise ValueError(f"family weight {name} must be >= 0")

    def weight_for(self, family: str) -> float:
        """Weight of ``family``, or 0.0 if it was not configured."""
        for name, weight in self.family_weights:
            if name == family:
                return weight
        return 0.0

    def to_dict(self) -> dict[str, Any]:
        """Plain-data form, for embedding in an artifact or a report."""
        return {
            "dim": self.dim,
            "max_token_order": self.max_token_order,
            "min_char_n": self.min_char_n,
            "max_char_n": self.max_char_n,
            "include_shape": self.include_shape,
            "namespace": self.namespace,
            "family_weights": {
                name: weight for name, weight in self.family_weights
            },
        }

    def fingerprint(self) -> str:
        """Short stable id of this configuration.

        Stored beside the vectors so a consumer can tell at a glance whether
        two sets of embeddings are comparable.
        """
        parts = [
            str(self.dim),
            str(self.max_token_order),
            f"{self.min_char_n}-{self.max_char_n}",
            "shape" if self.include_shape else "noshape",
            self.namespace,
        ] + [f"{name}={weight:.6f}" for name, weight in self.family_weights]
        return stable_hash_hex("|".join(parts), "embedding-config", digest_size=6)


def l2_normalize(vector: Sequence[float]) -> tuple[float, ...]:
    """Scale ``vector`` to unit length. An all-zero vector is returned as-is."""
    norm = math.sqrt(sum(value * value for value in vector))
    if norm <= 0.0:
        return tuple(float(value) for value in vector)
    return tuple(value / norm for value in vector)


def dot(left: Sequence[float], right: Sequence[float]) -> float:
    """Inner product of two equal-length vectors."""
    if len(left) != len(right):
        raise ValueError(
            f"dimension mismatch: {len(left)} vs {len(right)}"
        )
    return sum(map(operator.mul, left, right))


def cosine_similarity(
    left: Sequence[float], right: Sequence[float], *, normalized: bool = True
) -> float:
    """Cosine similarity in ``[-1, 1]``.

    Args:
        left: First vector.
        right: Second vector.
        normalized: True when both inputs are already unit length (which every
            vector out of :class:`HashedEmbedder` is), letting this skip two
            square roots per comparison.
    """
    product = dot(left, right)
    if normalized:
        return product
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm <= 0.0 or right_norm <= 0.0:
        return 0.0
    return product / (left_norm * right_norm)


def cosine_distance(
    left: Sequence[float], right: Sequence[float], *, normalized: bool = True
) -> float:
    """Cosine distance ``1 - cos`` in ``[0, 2]``, clamped away from -0.0."""
    value = 1.0 - cosine_similarity(left, right, normalized=normalized)
    if value < 0.0:
        return 0.0
    return value


class HashedEmbedder:
    """Turns a log message into a unit-length vector. Stateless and pure.

    The only mutable state is a memoisation cache; it changes speed, never
    results.

    Args:
        config: The embedding configuration. Defaults to
            :class:`EmbeddingConfig`'s defaults.
        cache_size: Maximum number of message signatures to memoise. Log
            streams are extremely repetitive — a few hundred distinct templates
            can back millions of lines — so this is the difference between
            milliseconds and minutes. Set 0 to disable.

    Example:
        >>> embedder = HashedEmbedder()
        >>> a = embedder.embed("thread pool exhausted, 512 of 512 busy")
        >>> b = embedder.embed("thread pool exhausted, 256 of 256 busy")
        >>> c = embedder.embed("scheduled backup completed successfully")
        >>> cosine_similarity(a, b) > cosine_similarity(a, c)
        True
    """

    def __init__(
        self,
        config: EmbeddingConfig | None = None,
        *,
        cache_size: int = 65536,
    ) -> None:
        self._config = config or EmbeddingConfig()
        if cache_size < 0:
            raise ValueError(f"cache_size must be >= 0, got {cache_size}")
        self._cache_size = cache_size
        self._vector_cache: dict[str, tuple[float, ...]] = {}
        # (bucket, sign) per feature string. Character n-grams recur constantly
        # across messages, so this saves the overwhelming majority of digests.
        self._projection_cache: dict[str, tuple[int, int]] = {}

    # -- properties --------------------------------------------------------
    @property
    def config(self) -> EmbeddingConfig:
        """The configuration this embedder was built with."""
        return self._config

    @property
    def dim(self) -> int:
        """Vector dimensionality."""
        return self._config.dim

    def fingerprint(self) -> str:
        """Stable id of the configuration; see :meth:`EmbeddingConfig.fingerprint`."""
        return self._config.fingerprint()

    # -- the public surface ------------------------------------------------
    def signature(self, message: str) -> str:
        """Canonical template of ``message`` (see :func:`normalize.signature`)."""
        return signature(message)

    def embed(self, message: str) -> tuple[float, ...]:
        """Embed one log message.

        The vector is a pure function of the message's **signature** — its
        structurally normalised template — so two lines differing only in a
        request id or a byte count embed to exactly the same point. That
        identity is what makes the memoisation below exact rather than
        approximate, and it is the property the outlier model relies on when it
        de-duplicates a million-line stream down to a few hundred templates.

        Args:
            message: Raw log message text.

        Returns:
            A unit-length tuple of ``dim`` floats. An empty or feature-less
            message embeds to the zero vector, which is a legitimate, stable
            answer rather than an error.
        """
        return self.embed_signature(signature(message))

    def embed_many(self, messages: Iterable[str]) -> list[tuple[float, ...]]:
        """Embed a batch, in order."""
        return [self.embed(message) for message in messages]

    def embed_signature(self, template: str) -> tuple[float, ...]:
        """Embed a signature string produced by :func:`normalize.signature`.

        Normalisation is idempotent, so passing a raw message here is also
        correct — it simply costs one extra normalisation pass.
        """
        key = signature(template)
        cached = self._vector_cache.get(key)
        if cached is not None:
            return cached
        vector = self._embed_uncached(key)
        if self._cache_size:
            if len(self._vector_cache) >= self._cache_size:
                self._vector_cache.clear()
            self._vector_cache[key] = vector
        return vector

    # -- internals ---------------------------------------------------------
    def _embed_uncached(self, normalized: str) -> tuple[float, ...]:
        """Compute the vector for an already-normalised signature string."""
        config = self._config
        tokens = tuple(normalized.split(" ")) if normalized else ()

        counts: dict[str, dict[str, int]] = {
            "token": {},
            "char": {},
            "shape": {},
        }
        _tally(
            counts["token"],
            token_shingles(tokens, max_order=config.max_token_order),
        )
        if config.max_char_n >= config.min_char_n:
            _tally(
                counts["char"],
                char_ngrams(
                    normalized,
                    min_n=config.min_char_n,
                    max_n=config.max_char_n,
                ),
            )
        if config.include_shape:
            _tally(counts["shape"], structural_shape(tokens))

        accumulator = [0.0] * config.dim
        for family in FAMILIES:
            weight = config.weight_for(family)
            family_counts = counts[family]
            if weight <= 0.0 or not family_counts:
                continue
            sketch = self._sketch(family_counts)
            norm = math.sqrt(sum(value * value for value in sketch))
            if norm <= 0.0:
                continue
            scale = weight / norm
            for index, value in enumerate(sketch):
                if value:
                    accumulator[index] += value * scale
        return l2_normalize(accumulator)

    def _sketch(self, counts: dict[str, int]) -> list[float]:
        """Hash one family's weighted feature counts into ``dim`` coordinates."""
        dim = self._config.dim
        namespace = self._config.namespace
        projection = self._projection_cache
        sketch = [0.0] * dim
        for feature, count in counts.items():
            entry = projection.get(feature)
            if entry is None:
                entry = (
                    stable_bucket(feature, dim, namespace),
                    stable_sign(feature, namespace),
                )
                projection[feature] = entry
            bucket, sign = entry
            # Sublinear term frequency: the fifth occurrence of a token says
            # much less than the first.
            sketch[bucket] += sign * (1.0 + math.log(count))
        return sketch

    def __repr__(self) -> str:
        return (
            f"HashedEmbedder(dim={self.dim}, "
            f"fingerprint={self.fingerprint()!r})"
        )


def _tally(target: dict[str, int], features: Iterable[str]) -> None:
    """Count ``features`` into ``target`` in place."""
    for feature in features:
        target[feature] = target.get(feature, 0) + 1
