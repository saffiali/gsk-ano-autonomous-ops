"""Novelty detection **in the embedding space**.

The model
---------
A k-nearest-neighbour novelty detector over the log-message vectors produced by
:mod:`ano.semantic.embedding`. It holds no rules and no vocabulary — only
vectors, their occurrence counts, and one calibrated distance scale. Scoring a
message is a geometry question: *how far is this point from the points the
system saw while it was learning what normal looks like?*

Scoring, precisely
------------------
Training is a **multiset** of vectors: one per observed log line, stored
compactly as (signature, vector, occurrence count). For a query vector ``q``:

``distance`` = the mean cosine distance to the ``k`` nearest training
*occurrences*. Occurrences, not distinct templates — so a message whose
template was logged five thousand times during training finds five thousand
copies of itself at distance zero and scores zero. Repetition is what "normal"
means in a log stream, and the multiset is how that enters the arithmetic.

``score`` = ``distance / (distance + scale)``, a bounded, strictly increasing
function of distance in ``[0, 1)``. It is a continuous novelty score, not a
verdict; ``is_outlier`` is provided for convenience and is simply
``score >= threshold``, with the default threshold placing the decision
boundary exactly at ``distance == scale``.

Calibration — where ``scale`` comes from
----------------------------------------
``scale`` is learned from the training window and from nothing else. For each
distinct training signature we compute its leave-one-out distance: the mean
cosine distance to the ``k`` nearest **other distinct signatures**, with its
own duplicates excluded. That distribution answers "how far apart are the
templates this estate normally emits?". ``scale`` is its
``calibration_quantile`` (90th percentile by default), floored at
``min_scale``.

So the decision boundary reads: *a message is an outlier when it is farther
from everything known than 90% of known templates are from their neighbours.*
A new wording inside a familiar family lands well inside that radius and scores
low; a failure mode phrased in vocabulary the system has never seen lands
outside it and scores high. That is the whole mechanism, and it is why an
unseen signature is surfaced without anyone having written a pattern for it.

What is deliberately absent
---------------------------
No keyword list, no severity input, no regular expression describing a failure,
no special case for any particular message. The model cannot recognise "error"
as a concept; it only measures distance.

Cost
----
Fitting is ``O(U^2 * dim)`` over ``U`` distinct signatures, scoring
``O(U * dim)`` per distinct query signature, and both are memoised by
signature. Log streams collapse to a few hundred templates, so a run over
hundreds of thousands of lines costs milliseconds of vector arithmetic.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ano.contracts.determinism import stable_hash_hex
from ano.semantic.embedding import cosine_distance

__all__ = [
    "DEFAULT_K",
    "DEFAULT_CALIBRATION_QUANTILE",
    "DEFAULT_MIN_SCALE",
    "DEFAULT_THRESHOLD",
    "TrainingProfile",
    "OutlierScore",
    "SemanticOutlierModel",
    "weighted_quantile",
]

#: Neighbours considered. 3 is enough to stop a single outlying training point
#: from dominating, and small enough that a sparse family still registers.
DEFAULT_K = 3

#: Quantile of the leave-one-out template-gap distribution used as the scale.
DEFAULT_CALIBRATION_QUANTILE = 0.90

#: Floor on the scale. Without it a degenerate training window — one template,
#: or every template identical — would give a zero scale and make every
#: subsequent message infinitely anomalous.
DEFAULT_MIN_SCALE = 0.05

#: ``score >= threshold`` is reported as an outlier. 0.5 puts the boundary at
#: exactly ``distance == scale``.
DEFAULT_THRESHOLD = 0.5


def weighted_quantile(
    values: Sequence[float], quantile: float, weights: Sequence[float] | None = None
) -> float:
    """Return the ``quantile`` of ``values``, optionally weighted.

    Uses the lower-value convention: the smallest value whose cumulative weight
    reaches ``quantile`` of the total. No interpolation, so the result is
    always a value that actually occurred — which keeps the calibration
    explainable ("the scale is this template's gap").

    Args:
        values: The sample. Must be non-empty.
        quantile: Desired quantile in ``[0, 1]``.
        weights: Per-value weights. Uniform if omitted.

    Raises:
        ValueError: for an empty sample, a bad quantile, or a length mismatch.
    """
    if not values:
        raise ValueError("cannot take a quantile of an empty sample")
    if not 0.0 <= quantile <= 1.0:
        raise ValueError(f"quantile must be in [0, 1], got {quantile}")
    if weights is None:
        weights = [1.0] * len(values)
    if len(weights) != len(values):
        raise ValueError(
            f"{len(values)} values but {len(weights)} weights"
        )
    pairs = sorted(zip(values, weights))
    total = math.fsum(weight for _, weight in pairs)
    if total <= 0.0:
        return pairs[-1][0]
    target = quantile * total
    cumulative = 0.0
    for value, weight in pairs:
        cumulative += weight
        if cumulative >= target:
            return value
    return pairs[-1][0]


@dataclass(frozen=True, slots=True)
class TrainingProfile:
    """A description of what the model learned. Fully reportable.

    Attributes:
        dim: Vector dimensionality.
        distinct_signatures: Number of distinct message templates in training.
        total_occurrences: Number of log lines in training.
        k: Neighbours used.
        scale: The calibrated characteristic distance.
        calibration_quantile: Quantile used to derive :attr:`scale`.
        threshold: Score at or above which a message is called an outlier.
        gap_quantiles: A few quantiles of the leave-one-out template-gap
            distribution, so a reviewer can see the shape rather than one
            number.
        fingerprint: Stable digest of the fitted training set. Two models with
            the same fingerprint were fitted on exactly the same data.
    """

    dim: int
    distinct_signatures: int
    total_occurrences: int
    k: int
    scale: float
    calibration_quantile: float
    threshold: float
    gap_quantiles: Mapping[str, float] = field(default_factory=dict)
    fingerprint: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Plain data, for a report or a demo panel."""
        return {
            "dim": self.dim,
            "distinct_signatures": self.distinct_signatures,
            "total_occurrences": self.total_occurrences,
            "k": self.k,
            "scale": self.scale,
            "calibration_quantile": self.calibration_quantile,
            "threshold": self.threshold,
            "gap_quantiles": dict(self.gap_quantiles),
            "fingerprint": self.fingerprint,
        }


@dataclass(frozen=True, slots=True)
class OutlierScore:
    """The verdict on one message, with everything needed to justify it.

    Attributes:
        signature: The normalised template that was scored.
        distance: Mean cosine distance to the ``k`` nearest training
            occurrences, in ``[0, 2]``.
        score: ``distance / (distance + scale)``, in ``[0, 1)``.
        is_outlier: ``score >= threshold``.
        novelty_ratio: ``distance / scale`` — how many characteristic template
            gaps away this message sits. 1.0 is the decision boundary.
        nearest_signature: The closest training template, or ``None`` when the
            model was fitted on nothing.
        nearest_distance: Cosine distance to that template.
        nearest_support: How many times that template occurred in training.
        seen_in_training: Whether this exact signature occurred in training.
            Reported as a **fact**, never used as the score — a detector that
            simply flagged unseen strings would be a lookup table, not a
            semantic model.
    """

    signature: str
    distance: float
    score: float
    is_outlier: bool
    novelty_ratio: float
    nearest_signature: str | None = None
    nearest_distance: float = 0.0
    nearest_support: int = 0
    seen_in_training: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Plain data, for evidence fields and artifacts."""
        return {
            "signature": self.signature,
            "distance": round(self.distance, 6),
            "score": round(self.score, 6),
            "is_outlier": self.is_outlier,
            "novelty_ratio": round(self.novelty_ratio, 6),
            "nearest_signature": self.nearest_signature,
            "nearest_distance": round(self.nearest_distance, 6),
            "nearest_support": self.nearest_support,
            "seen_in_training": self.seen_in_training,
        }


class SemanticOutlierModel:
    """k-NN novelty detection over log-message embeddings.

    Args:
        k: Neighbours averaged when measuring distance.
        calibration_quantile: Quantile of the leave-one-out template-gap
            distribution that becomes the scale.
        min_scale: Floor on the scale, guarding a degenerate training window.
        threshold: Score at or above which :attr:`OutlierScore.is_outlier` is
            true.

    Example::

        model = SemanticOutlierModel()
        model.fit((sig, vec) for sig, vec in training_pairs)
        result = model.score(query_signature, query_vector)
        result.score        # continuous novelty in [0, 1)
    """

    def __init__(
        self,
        *,
        k: int = DEFAULT_K,
        calibration_quantile: float = DEFAULT_CALIBRATION_QUANTILE,
        min_scale: float = DEFAULT_MIN_SCALE,
        threshold: float = DEFAULT_THRESHOLD,
    ) -> None:
        if k < 1:
            raise ValueError(f"k must be >= 1, got {k}")
        if not 0.0 < calibration_quantile <= 1.0:
            raise ValueError(
                f"calibration_quantile must be in (0, 1], got {calibration_quantile}"
            )
        if min_scale <= 0.0:
            raise ValueError(f"min_scale must be > 0, got {min_scale}")
        if not 0.0 <= threshold < 1.0:
            raise ValueError(f"threshold must be in [0, 1), got {threshold}")

        self._k = k
        self._quantile = calibration_quantile
        self._min_scale = min_scale
        self._threshold = threshold

        self._signatures: tuple[str, ...] = ()
        self._vectors: tuple[tuple[float, ...], ...] = ()
        self._counts: tuple[int, ...] = ()
        self._index: dict[str, int] = {}
        self._scale = min_scale
        self._dim = 0
        self._total = 0
        self._gap_quantiles: dict[str, float] = {}
        self._fingerprint = ""
        self._fitted = False
        self._cache: dict[str, OutlierScore] = {}

    # -- properties --------------------------------------------------------
    @property
    def fitted(self) -> bool:
        """True once :meth:`fit` has run."""
        return self._fitted

    @property
    def scale(self) -> float:
        """The calibrated characteristic distance."""
        return self._scale

    @property
    def threshold(self) -> float:
        """Score at or above which a message is called an outlier."""
        return self._threshold

    @property
    def k(self) -> int:
        """Neighbours averaged."""
        return self._k

    # -- fitting -----------------------------------------------------------
    def fit(
        self, observations: Iterable[tuple[str, Sequence[float]]]
    ) -> "SemanticOutlierModel":
        """Learn the distribution of normal log embeddings.

        Args:
            observations: ``(signature, vector)`` per **observed log line**.
                Repeats are expected and meaningful: they are what makes a
                template normal. Vectors must all share a dimensionality.

        Returns:
            ``self``, so a fit can be chained.

        Raises:
            ValueError: on an empty training set or mixed dimensionality.
        """
        counts: dict[str, int] = {}
        vectors: dict[str, tuple[float, ...]] = {}
        for signature, vector in observations:
            counts[signature] = counts.get(signature, 0) + 1
            if signature not in vectors:
                vectors[signature] = tuple(float(value) for value in vector)

        if not counts:
            raise ValueError(
                "cannot fit a semantic outlier model on an empty training "
                "window; widen the window or check the ingest step"
            )
        dims = {len(vector) for vector in vectors.values()}
        if len(dims) > 1:
            raise ValueError(
                f"training vectors have mixed dimensionality {sorted(dims)}; "
                "they must all come from one EmbeddingConfig"
            )

        ordered = sorted(counts)
        self._signatures = tuple(ordered)
        self._vectors = tuple(vectors[name] for name in ordered)
        self._counts = tuple(counts[name] for name in ordered)
        self._index = {name: position for position, name in enumerate(ordered)}
        self._dim = dims.pop()
        self._total = sum(self._counts)
        self._cache = {}
        self._calibrate()
        self._fingerprint = stable_hash_hex(
            "\n".join(f"{name}\t{counts[name]}" for name in ordered),
            "semantic",
            "training-set",
            digest_size=8,
        )
        self._fitted = True
        return self

    def fit_messages(
        self, messages: Iterable[str], embedder: Any = None
    ) -> "SemanticOutlierModel":
        """Convenience: embed ``messages`` with ``embedder`` and fit on them."""
        if embedder is None:
            from ano.semantic.embedding import HashedEmbedder

            embedder = HashedEmbedder()
        return self.fit(
            (embedder.signature(message), embedder.embed(message))
            for message in messages
        )

    def _calibrate(self) -> None:
        """Derive the scale from the leave-one-out template-gap distribution.

        Own duplicates are excluded here — the question this distribution
        answers is how far apart *distinct* normal templates sit, which is the
        radius inside which a new message still counts as familiar. Including
        duplicates would collapse the scale to zero for any repetitive stream
        and turn the model into an exact-match lookup.
        """
        size = len(self._signatures)
        if size < 2:
            self._scale = self._min_scale
            self._gap_quantiles = {}
            return

        gaps: list[float] = []
        for position, vector in enumerate(self._vectors):
            neighbours = sorted(
                cosine_distance(vector, other)
                for index, other in enumerate(self._vectors)
                if index != position
            )
            take = neighbours[: min(self._k, len(neighbours))]
            gaps.append(math.fsum(take) / len(take))

        self._scale = max(
            weighted_quantile(gaps, self._quantile), self._min_scale
        )
        self._gap_quantiles = {
            f"p{int(q * 100):02d}": round(weighted_quantile(gaps, q), 6)
            for q in (0.10, 0.50, 0.90, 0.99)
        }

    # -- scoring -----------------------------------------------------------
    def score(
        self,
        signature: str,
        vector: Sequence[float] | None = None,
        *,
        embedder: Any = None,
    ) -> OutlierScore:
        """Score one message's embedding.

        Args:
            signature: The message's normalised template, or the message itself
                if ``vector`` is omitted.
            vector: The embedding, from the same configuration training used.
            embedder: Embedder to use when ``vector`` is omitted.

        Returns:
            An :class:`OutlierScore`.

        Raises:
            RuntimeError: if the model has not been fitted.
            ValueError: on a dimensionality mismatch.
        """
        if not self._fitted:
            raise RuntimeError(
                "SemanticOutlierModel.score called before fit(); a novelty "
                "model with no notion of normal cannot say anything"
            )
        if vector is None:
            if embedder is None:
                from ano.semantic.embedding import HashedEmbedder

                embedder = HashedEmbedder()
            message = signature
            signature = embedder.signature(message)
            vector = embedder.embed(message)
        cached = self._cache.get(signature)
        if cached is not None:
            return cached
        if len(vector) != self._dim:
            raise ValueError(
                f"vector has dimension {len(vector)} but the model was fitted "
                f"on dimension {self._dim}"
            )

        distances = [
            cosine_distance(vector, training) for training in self._vectors
        ]
        order = sorted(range(len(distances)), key=lambda i: (distances[i], i))

        # Mean distance to the k nearest *occurrences*: a template that
        # occurred many times supplies many neighbours at its own distance.
        remaining = self._k
        accumulated = 0.0
        used = 0
        for position in order:
            if remaining <= 0:
                break
            take = min(remaining, self._counts[position])
            accumulated += distances[position] * take
            used += take
            remaining -= take
        distance = accumulated / used if used else 0.0

        nearest = order[0]
        result = OutlierScore(
            signature=signature,
            distance=distance,
            score=distance / (distance + self._scale),
            is_outlier=(distance / (distance + self._scale)) >= self._threshold,
            novelty_ratio=distance / self._scale,
            nearest_signature=self._signatures[nearest],
            nearest_distance=distances[nearest],
            nearest_support=self._counts[nearest],
            seen_in_training=signature in self._index,
        )
        self._cache[signature] = result
        return result

    def score_many(
        self, items: Iterable[tuple[str, Sequence[float]]]
    ) -> list[OutlierScore]:
        """Score a batch, in order. Repeated signatures cost one lookup."""
        return [self.score(signature, vector) for signature, vector in items]

    # -- introspection -----------------------------------------------------
    def profile(self) -> TrainingProfile:
        """Everything about the fitted model, as reportable data."""
        if not self._fitted:
            raise RuntimeError("profile() called before fit()")
        return TrainingProfile(
            dim=self._dim,
            distinct_signatures=len(self._signatures),
            total_occurrences=self._total,
            k=self._k,
            scale=self._scale,
            calibration_quantile=self._quantile,
            threshold=self._threshold,
            gap_quantiles=dict(self._gap_quantiles),
            fingerprint=self._fingerprint,
        )

    def training_signatures(self) -> tuple[str, ...]:
        """Every distinct template seen during training, sorted.

        This is what makes feature 35 checkable: the harness can demonstrate
        that an injected novel signature is absent from training rather than
        assert it.
        """
        return self._signatures

    def support(self, signature: str) -> int:
        """How many times ``signature`` occurred in training. 0 if never."""
        position = self._index.get(signature)
        return self._counts[position] if position is not None else 0

    def __contains__(self, signature: object) -> bool:
        return signature in self._index

    def __len__(self) -> int:
        return len(self._signatures)

    def __repr__(self) -> str:
        if not self._fitted:
            return "SemanticOutlierModel(unfitted)"
        return (
            f"SemanticOutlierModel(signatures={len(self._signatures)}, "
            f"occurrences={self._total}, scale={self._scale:.4f}, "
            f"k={self._k})"
        )
