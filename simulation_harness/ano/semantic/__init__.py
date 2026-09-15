"""``ano.semantic`` — vector embeddings of log messages and novelty detection.

Requirement R1: *"convert log messages into vector embeddings so that
semantically unusual log behaviour can be detected without pre-written regex
rules or a fixed error dictionary. Semantic outlier detection must surface
novel failure signatures the system has never been given a pattern for."*

Three modules, in the order data flows through them:

============================  =============================================
:mod:`ano.semantic.normalize` Structural tokenisation. Volatile spans
                              (numbers, ids, addresses, times, paths) become
                              typed placeholders, so two lines describing the
                              same event reduce to the same template.
:mod:`ano.semantic.embedding` The hashing trick with a signed random
                              projection over token shingles, character
                              n-grams and structural counts. Output is a
                              unit-length dense vector. No vocabulary, no
                              model file, no training.
:mod:`ano.semantic.outlier`   Novelty scoring **in the vector space**:
                              occurrence-weighted k-nearest-neighbour cosine
                              distance to a fitted training window, calibrated
                              against that window's own leave-one-out
                              distances.
:mod:`ano.semantic.detector`  Ties them to the store: fit on the training
                              window, score everything after it, emit findings.
============================  =============================================

Why this is genuinely semantic, and not a rule list in disguise
---------------------------------------------------------------
Nothing in this package knows what an error is. There is no keyword table, no
severity weighting and no regular expression that mentions a failure mode; the
only regular expressions describe *lexical shapes* (a run of digits, a dotted
quad) and are used for tokenisation. Novelty is decided entirely by geometry:
how far a message's vector sits from the vectors of the messages the system
observed during training. A failure whose wording the system has never seen
lands far from every training vector and scores high — which is precisely what
acceptance criterion AC-15 requires, and precisely what a pattern list cannot
do.

``tests/m3_semantic/test_no_pattern_list.py`` enforces the claim mechanically
by scanning this package's string literals for error vocabulary.
"""

from ano.semantic.detector import (
    SemanticAnomaly,
    SemanticDetector,
    SemanticReport,
    SemanticScoreWindow,
    TrainingWindowAudit,
)
from ano.semantic.embedding import (
    DEFAULT_DIM,
    EMBEDDING_NAMESPACE,
    EmbeddingConfig,
    HashedEmbedder,
    cosine_distance,
    cosine_similarity,
    dot,
    l2_normalize,
)
from ano.semantic.normalize import (
    PLACEHOLDERS,
    iter_features,
    normalize_message,
    signature,
    tokenize,
)
from ano.semantic.outlier import (
    OutlierScore,
    SemanticOutlierModel,
    TrainingProfile,
)

__all__ = [
    # normalisation
    "normalize_message",
    "signature",
    "tokenize",
    "iter_features",
    "PLACEHOLDERS",
    # embeddings
    "HashedEmbedder",
    "EmbeddingConfig",
    "DEFAULT_DIM",
    "EMBEDDING_NAMESPACE",
    "cosine_similarity",
    "cosine_distance",
    "l2_normalize",
    "dot",
    # outlier model
    "SemanticOutlierModel",
    "OutlierScore",
    "TrainingProfile",
    # end-to-end detector
    "SemanticDetector",
    "SemanticAnomaly",
    "SemanticReport",
    "SemanticScoreWindow",
    "TrainingWindowAudit",
]
