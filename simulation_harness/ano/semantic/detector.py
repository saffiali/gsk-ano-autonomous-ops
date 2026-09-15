"""End-to-end semantic log intelligence over the analytical store.

This is where requirement R1's second half becomes a runnable thing. Given a
:class:`ano.ingest.features.FeatureReader`, the detector:

1. **Declares a training window** — the ingested ``history`` phase when the
   generator marked one, otherwise the leading fraction of the observed log
   time range. Either way the choice is recorded in
   :class:`TrainingWindowAudit`, with the bounds, the line count and a digest
   of the exact signature set. Feature 34 asks for the window to be "bounded
   and declared"; this is that, and it is derived from ingested telemetry
   alone.
2. **Fits** :class:`ano.semantic.outlier.SemanticOutlierModel` on the
   embeddings the ingest pipeline stored for that window. It reads the
   persisted vectors — it does not recompute them from strings — so the claim
   that detection operates on the embeddings is a property of the query, not a
   comment.
3. **Scores** every log line after the training window and reports a continuous
   novelty score per line, plus per-entity per-window aggregates for the
   detection layer to consume.

Requirement R5
--------------
The detector never sees a label. Its training window comes from telemetry
timestamps, its model from telemetry vectors, and its threshold from the
training window's own geometry. Nothing here can open ``labels.json``:
:func:`ano.ingest.sources.guard_path` refuses, and this module never touches
the file system at all.

Auditing "the signature was not in training"
---------------------------------------------
:meth:`TrainingWindowAudit.contains_signature` and
:meth:`SemanticDetector.training_signatures` let the evaluation harness
*demonstrate* absence rather than assert it (feature 35). The detector itself
does not use that information to score — see
:attr:`ano.semantic.outlier.OutlierScore.seen_in_training`, which is reported
as a fact and excluded from the arithmetic. A detector that flagged unseen
strings would be a lookup table; this one measures distance.
"""

from __future__ import annotations

import datetime as _dt
import math
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ano.contracts.determinism import hash_stream
from ano.ingest.features import FeatureReader, LogRow, TimeRange
from ano.semantic.embedding import EmbeddingConfig, HashedEmbedder
from ano.semantic.normalize import signature as message_signature
from ano.semantic.outlier import (
    OutlierScore,
    SemanticOutlierModel,
    TrainingProfile,
)

__all__ = [
    "DEFAULT_TRAIN_FRACTION",
    "TRAINING_PHASE",
    "SemanticAnomaly",
    "SemanticScoreWindow",
    "TrainingWindowAudit",
    "SemanticReport",
    "SemanticDetector",
]

#: Share of the observed log range used for training when the generator did not
#: mark a history phase.
DEFAULT_TRAIN_FRACTION = 0.4

#: The ingest phase preferred as the training window.
TRAINING_PHASE = "history"


@dataclass(frozen=True, slots=True)
class TrainingWindowAudit:
    """What the model trained on, in a form a third party can check.

    Attributes:
        window: The interval, half-open.
        source: How it was chosen — ``"phase:history"`` when the generator
            marked one, ``"fraction:0.40"`` when it was derived from the
            observed range.
        log_lines: Log lines in the window.
        distinct_signatures: Distinct templates among them.
        signature_digest: Order-independent digest of the signature set. Two
            audits with the same digest describe the same training vocabulary.
        signatures: The templates themselves, sorted.
    """

    window: TimeRange
    source: str
    log_lines: int
    distinct_signatures: int
    signature_digest: str
    signatures: tuple[str, ...] = ()

    def contains_signature(self, value: str) -> bool:
        """True if ``value`` — a raw message or a signature — occurred here.

        Accepts either form: a raw message is normalised first, so the caller
        does not have to know how templating works to ask the question.
        """
        if value in self._signature_set:
            return True
        return message_signature(value) in self._signature_set

    @property
    def _signature_set(self) -> frozenset[str]:
        return frozenset(self.signatures)

    def to_dict(self, *, include_signatures: bool = False) -> dict[str, Any]:
        """Plain data. Signatures are omitted unless asked for; there can be
        hundreds and they belong in an evidence artifact, not a summary."""
        payload: dict[str, Any] = {
            "window": self.window.to_dict(),
            "source": self.source,
            "log_lines": self.log_lines,
            "distinct_signatures": self.distinct_signatures,
            "signature_digest": self.signature_digest,
        }
        if include_signatures:
            payload["signatures"] = list(self.signatures)
        return payload


@dataclass(frozen=True, slots=True)
class SemanticAnomaly:
    """One log line the model considers semantically unusual."""

    insert_id: str
    entity_id: str | None
    timestamp: _dt.datetime
    severity: str
    message: str
    signature: str
    score: float
    distance: float
    novelty_ratio: float
    nearest_signature: str | None
    nearest_distance: float
    nearest_support: int
    seen_in_training: bool

    def to_dict(self) -> dict[str, Any]:
        """Plain data, suitable for a C2 evidence field."""
        return {
            "insert_id": self.insert_id,
            "entity_id": self.entity_id,
            "timestamp": self.timestamp.isoformat().replace("+00:00", "Z"),
            "severity": self.severity,
            "message": self.message,
            "signature": self.signature,
            "score": round(self.score, 6),
            "distance": round(self.distance, 6),
            "novelty_ratio": round(self.novelty_ratio, 6),
            "nearest_signature": self.nearest_signature,
            "nearest_distance": round(self.nearest_distance, 6),
            "nearest_support": self.nearest_support,
            "seen_in_training": self.seen_in_training,
        }


@dataclass(frozen=True, slots=True)
class SemanticScoreWindow:
    """Per-entity, per-window summary of semantic novelty.

    This is the shape the detection layer consumes: one number per entity per
    window that says "how unlike anything we have seen did this entity sound".

    Attributes:
        entity_id: The entity.
        window: The interval.
        lines: Log lines in the window.
        max_score: Highest novelty score in the window.
        mean_score: Mean novelty score over the window's lines.
        anomaly_count: Lines at or above the model's threshold.
        anomaly_rate: ``anomaly_count / lines``.
        unseen_signatures: Distinct templates in the window that did not occur
            in training. Reported for evidence; not part of the score.
        top_signature: The template responsible for :attr:`max_score`.
    """

    entity_id: str
    window: TimeRange
    lines: int
    max_score: float
    mean_score: float
    anomaly_count: int
    anomaly_rate: float
    unseen_signatures: int
    top_signature: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Plain data."""
        return {
            "entity_id": self.entity_id,
            "window": self.window.to_dict(),
            "lines": self.lines,
            "max_score": round(self.max_score, 6),
            "mean_score": round(self.mean_score, 6),
            "anomaly_count": self.anomaly_count,
            "anomaly_rate": round(self.anomaly_rate, 6),
            "unseen_signatures": self.unseen_signatures,
            "top_signature": self.top_signature,
        }


@dataclass(frozen=True, slots=True)
class SemanticReport:
    """The outcome of one semantic analysis run."""

    training: TrainingWindowAudit
    profile: TrainingProfile
    scoring_window: TimeRange
    scored_lines: int
    scored_signatures: int
    anomaly_count: int
    anomalous_signatures: tuple[str, ...]
    anomalies: tuple[SemanticAnomaly, ...]
    embedding_config: Mapping[str, Any] = field(default_factory=dict)
    embedding_fingerprint: str = ""
    elapsed_s: float = 0.0

    @property
    def anomaly_rate(self) -> float:
        """Share of scored lines flagged. 0.0 when nothing was scored."""
        return self.anomaly_count / self.scored_lines if self.scored_lines else 0.0

    def to_dict(self, *, max_anomalies: int = 25) -> dict[str, Any]:
        """Plain data, with the anomaly list truncated for readability."""
        return {
            "training": self.training.to_dict(),
            "profile": self.profile.to_dict(),
            "scoring_window": self.scoring_window.to_dict(),
            "scored_lines": self.scored_lines,
            "scored_signatures": self.scored_signatures,
            "anomaly_count": self.anomaly_count,
            "anomaly_rate": round(self.anomaly_rate, 6),
            "anomalous_signatures": list(self.anomalous_signatures),
            "anomalies": [
                item.to_dict() for item in self.anomalies[:max_anomalies]
            ],
            "anomalies_truncated": max(
                0, len(self.anomalies) - max_anomalies
            ),
            "embedding_config": dict(self.embedding_config),
            "embedding_fingerprint": self.embedding_fingerprint,
            "elapsed_s": round(self.elapsed_s, 6),
        }


class SemanticDetector:
    """Fits a novelty model on a declared training window and scores the rest.

    Args:
        reader: The feature-read API over the analytical store.
        embedder: Embedder used when a message has no stored vector. Defaults
            to :class:`ano.semantic.embedding.HashedEmbedder` with the default
            configuration — which must match what ingest used, or the vectors
            will not be comparable.
        model: A pre-configured outlier model. Defaults to
            :class:`ano.semantic.outlier.SemanticOutlierModel`'s defaults.
        train_fraction: Fallback training share when no ``history`` phase was
            ingested.
        training_phase: Ingest phase preferred as the training window.

    Example::

        detector = SemanticDetector(FeatureReader(store))
        report = detector.run()
        report.anomaly_count
        detector.score_windows("app-tomcat-04", window_s=300)
    """

    def __init__(
        self,
        reader: FeatureReader,
        *,
        embedder: HashedEmbedder | None = None,
        model: SemanticOutlierModel | None = None,
        train_fraction: float = DEFAULT_TRAIN_FRACTION,
        training_phase: str = TRAINING_PHASE,
    ) -> None:
        if not 0.0 < train_fraction < 1.0:
            raise ValueError(
                f"train_fraction must be in (0, 1), got {train_fraction}"
            )
        self._reader = reader
        self._embedder = embedder or HashedEmbedder(EmbeddingConfig())
        self._model = model or SemanticOutlierModel()
        self._train_fraction = train_fraction
        self._training_phase = training_phase
        self._audit: TrainingWindowAudit | None = None

    # -- properties --------------------------------------------------------
    @property
    def model(self) -> SemanticOutlierModel:
        """The novelty model, fitted or not."""
        return self._model

    @property
    def embedder(self) -> HashedEmbedder:
        """The embedder used for messages with no stored vector."""
        return self._embedder

    @property
    def audit(self) -> TrainingWindowAudit | None:
        """The training-window audit, once :meth:`fit` has run."""
        return self._audit

    # -- window selection --------------------------------------------------
    def select_training_window(self) -> tuple[TimeRange, str]:
        """Choose and justify the training window.

        Returns:
            ``(window, source)``. ``source`` names the rule that was applied,
            so the choice appears in the report rather than being implicit.

        Raises:
            ValueError: if the store holds no telemetry.
        """
        phase = self._reader.phase_range(self._training_phase, "log")
        if phase is not None and phase.duration_s > 0:
            return phase, f"phase:{self._training_phase}"
        bounds = self._reader.time_bounds()
        if bounds is None:
            raise ValueError(
                "the store holds no telemetry, so no training window exists; "
                "run the ingest pipeline first"
            )
        return (
            self._reader.training_range(self._train_fraction, bounds=bounds),
            f"fraction:{self._train_fraction:.2f}",
        )

    # -- fitting -----------------------------------------------------------
    def fit(
        self, window: TimeRange | None = None, *, source: str | None = None
    ) -> TrainingWindowAudit:
        """Fit the model on the embeddings stored for ``window``.

        Args:
            window: Training interval. Chosen by
                :meth:`select_training_window` when omitted.
            source: Description of how ``window`` was chosen. Derived when
                omitted.

        Returns:
            The :class:`TrainingWindowAudit`.

        Raises:
            ValueError: if the window contains no embedded log lines.
        """
        if window is None:
            window, derived = self.select_training_window()
            source = source or derived
        source = source or "explicit"

        rows = self._reader.embedded_log_rows(window=window)
        if not rows:
            raise ValueError(
                f"training window {window.to_dict()} contains no embedded log "
                "lines; either the window is empty or ingest ran with "
                "embed_logs disabled"
            )

        observations = [
            (message_signature(row.message), vector) for row, vector in rows
        ]
        self._model.fit(observations)
        distinct = self._model.training_signatures()
        audit = TrainingWindowAudit(
            window=window,
            source=source,
            log_lines=len(rows),
            distinct_signatures=len(distinct),
            signature_digest=hash_stream(distinct, "semantic", "training"),
            signatures=distinct,
        )
        self._audit = audit
        return audit

    # -- scoring -----------------------------------------------------------
    def score_rows(
        self, rows: Sequence[tuple[LogRow, Sequence[float]]]
    ) -> tuple[SemanticAnomaly, ...]:
        """Score pre-fetched ``(LogRow, vector)`` pairs.

        Returns:
            One :class:`SemanticAnomaly` per row — every row, not just the
            flagged ones. The score is continuous, so the caller decides what
            to do with a 0.31 as much as with a 0.94.
        """
        out: list[SemanticAnomaly] = []
        for row, vector in rows:
            signature = message_signature(row.message)
            result = self._model.score(signature, vector)
            out.append(_to_anomaly(row, signature, result))
        return tuple(out)

    def score_window(
        self,
        window: TimeRange | None = None,
        *,
        entity_id: str | None = None,
    ) -> tuple[SemanticAnomaly, ...]:
        """Score every embedded log line in ``window``.

        Args:
            window: Interval to score. Defaults to everything after the
                training window.
            entity_id: Restrict to one entity.
        """
        target = window if window is not None else self.scoring_window()
        rows = self._reader.embedded_log_rows(entity_id, target)
        return self.score_rows(rows)

    def scoring_window(self) -> TimeRange:
        """Everything after the training window, up to the end of telemetry."""
        if self._audit is None:
            raise RuntimeError("scoring_window() called before fit()")
        bounds = self._reader.time_bounds()
        if bounds is None:  # pragma: no cover - fit() would have failed first
            raise ValueError("the store holds no telemetry")
        start = max(self._audit.window.end, bounds.start)
        return TimeRange(start, max(start, bounds.end))

    def score_windows(
        self,
        entity_id: str,
        window: TimeRange | None = None,
        *,
        window_s: int = 300,
        include_empty: bool = False,
    ) -> tuple[SemanticScoreWindow, ...]:
        """Per-window semantic novelty for one entity.

        This is the detection layer's entry point: a small time series of
        "how unusual did this entity sound", aligned to the same window grid
        as :meth:`ano.ingest.features.FeatureReader.feature_windows`.

        Args:
            entity_id: The entity.
            window: Interval to cover. Defaults to :meth:`scoring_window`.
            window_s: Window width in seconds.
            include_empty: Emit windows in which the entity logged nothing.
        """
        if window_s <= 0:
            raise ValueError(f"window_s must be > 0, got {window_s}")
        span = window if window is not None else self.scoring_window()
        anomalies = self.score_window(span, entity_id=entity_id)

        buckets: dict[int, list[SemanticAnomaly]] = {}
        for item in anomalies:
            index = int((item.timestamp - span.start).total_seconds() // window_s)
            buckets.setdefault(index, []).append(item)

        out: list[SemanticScoreWindow] = []
        for index, sub in enumerate(span.split(window_s)):
            found = buckets.get(index)
            if not found and not include_empty:
                continue
            found = found or []
            scores = [item.score for item in found]
            best = max(found, key=lambda item: (item.score, item.insert_id), default=None)
            flagged = sum(1 for item in found if item.score >= self._model.threshold)
            out.append(
                SemanticScoreWindow(
                    entity_id=entity_id,
                    window=sub,
                    lines=len(found),
                    max_score=max(scores) if scores else 0.0,
                    mean_score=(math.fsum(scores) / len(scores)) if scores else 0.0,
                    anomaly_count=flagged,
                    anomaly_rate=(flagged / len(found)) if found else 0.0,
                    unseen_signatures=len(
                        {
                            item.signature
                            for item in found
                            if not item.seen_in_training
                        }
                    ),
                    top_signature=best.signature if best is not None else None,
                )
            )
        return tuple(out)

    # -- the whole thing ---------------------------------------------------
    def run(
        self,
        *,
        training_window: TimeRange | None = None,
        scoring_window: TimeRange | None = None,
        keep_all: bool = False,
    ) -> SemanticReport:
        """Fit, score and summarise in one call.

        Args:
            training_window: Override the automatic choice.
            scoring_window: Override "everything after training".
            keep_all: Keep every scored line in the report, not just the
                flagged ones. Useful for analysis; large.

        Returns:
            The :class:`SemanticReport`.
        """
        started = time.perf_counter()
        audit = self.fit(training_window)
        target = scoring_window if scoring_window is not None else self.scoring_window()
        scored = self.score_window(target)

        flagged = tuple(
            item for item in scored if item.score >= self._model.threshold
        )
        kept = scored if keep_all else flagged
        ordered = tuple(
            sorted(kept, key=lambda item: (-item.score, item.timestamp, item.insert_id))
        )
        return SemanticReport(
            training=audit,
            profile=self._model.profile(),
            scoring_window=target,
            scored_lines=len(scored),
            scored_signatures=len({item.signature for item in scored}),
            anomaly_count=len(flagged),
            anomalous_signatures=tuple(
                sorted({item.signature for item in flagged})
            ),
            anomalies=ordered,
            embedding_config=self._embedder.config.to_dict(),
            embedding_fingerprint=self._embedder.fingerprint(),
            elapsed_s=time.perf_counter() - started,
        )

    # -- audit helpers -----------------------------------------------------
    def training_signatures(self) -> tuple[str, ...]:
        """Every template the model trained on, sorted.

        Exposed so the evaluation harness can demonstrate that an injected
        novel signature is absent from the training corpus (feature 35).
        """
        return self._model.training_signatures()

    def signature_of(self, message: str) -> str:
        """The normalised template of ``message``."""
        return message_signature(message)


def _to_anomaly(
    row: LogRow, signature: str, result: OutlierScore
) -> SemanticAnomaly:
    """Combine a log row and its score into a reportable finding."""
    return SemanticAnomaly(
        insert_id=row.insert_id,
        entity_id=row.entity_id,
        timestamp=row.timestamp,
        severity=row.severity,
        message=row.message,
        signature=signature,
        score=result.score,
        distance=result.distance,
        novelty_ratio=result.novelty_ratio,
        nearest_signature=result.nearest_signature,
        nearest_distance=result.nearest_distance,
        nearest_support=result.nearest_support,
        seen_in_training=result.seen_in_training,
    )
