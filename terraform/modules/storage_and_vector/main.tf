resource "google_bigquery_dataset" "ano_dataset" {
  dataset_id                 = var.dataset_id
  project                    = var.project_id
  location                   = var.region
  description                = "GSK Autonomous Operations (ANO) Analytical and Vector Storage Dataset"
  delete_contents_on_destroy = false
  labels                     = var.labels
}

resource "google_bigquery_table" "raw_logs" {
  dataset_id          = google_bigquery_dataset.ano_dataset.dataset_id
  table_id            = "raw_logs"
  project             = var.project_id
  description         = "Time-partitioned and entity-clustered table storing raw operational logs from Cloud Logging."
  deletion_protection = false
  labels              = var.labels

  time_partitioning {
    type          = "DAY"
    field         = "timestamp"
    expiration_ms = 7776000000
  }

  clustering = ["service_name", "severity", "host_id", "environment"]

  schema = <<-EOF
[
  {
    "name": "log_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique Cloud Logging insertId"
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "Log emission timestamp (Partition key)"
  },
  {
    "name": "receive_timestamp",
    "type": "TIMESTAMP",
    "mode": "NULLABLE",
    "description": "Log sink ingestion timestamp"
  },
  {
    "name": "severity",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Log severity (INFO, WARNING, ERROR, CRITICAL)"
  },
  {
    "name": "service_name",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Workload or service name (apache-web, tomcat-app, postgres-db)"
  },
  {
    "name": "host_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Compute VM, GKE node, or network switch hostname"
  },
  {
    "name": "environment",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Environment (prod, staging, dr)"
  },
  {
    "name": "domain",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Operational domain (COMPUTE, APPLICATION, DATABASE, NETWORK)"
  },
  {
    "name": "message_template",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Normalized message template with masked dynamic tokens"
  },
  {
    "name": "raw_payload",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Full raw text or JSON log message and stack trace"
  },
  {
    "name": "trace_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Distributed trace ID"
  },
  {
    "name": "span_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Span ID"
  },
  {
    "name": "labels",
    "type": "JSON",
    "mode": "NULLABLE",
    "description": "Resource and log labels JSON"
  }
]
EOF
}

resource "google_bigquery_table" "log_embeddings" {
  dataset_id          = google_bigquery_dataset.ano_dataset.dataset_id
  table_id            = "log_embeddings"
  project             = var.project_id
  description         = "Time-partitioned and entity-clustered table storing 768-dim Vertex AI text-embedding-005 log vectors."
  deletion_protection = false
  labels              = var.labels

  time_partitioning {
    type          = "DAY"
    field         = "timestamp"
    expiration_ms = 7776000000
  }

  clustering = ["service_name", "domain", "host_id"]

  schema = <<-EOF
[
  {
    "name": "chunk_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique ID for sliding-window log chunk"
  },
  {
    "name": "log_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Anchor log_id from raw_logs"
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "Chunk window timestamp (Partition key)"
  },
  {
    "name": "service_name",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Emitting service name"
  },
  {
    "name": "host_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Host or network device ID"
  },
  {
    "name": "domain",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Domain (COMPUTE, APPLICATION, DATABASE, NETWORK)"
  },
  {
    "name": "severity",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Maximum severity within the chunk"
  },
  {
    "name": "chunk_text",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Normalized sliding-window text with preserved stack trace"
  },
  {
    "name": "embedding",
    "type": "FLOAT64",
    "mode": "REPEATED",
    "description": "ARRAY<FLOAT64> 768-dimensional vector from Vertex AI text-embedding-005"
  },
  {
    "name": "embedding_model",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Model ID (text-embedding-005)"
  },
  {
    "name": "token_count",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "Input token count"
  },
  {
    "name": "is_outlier",
    "type": "BOOL",
    "mode": "NULLABLE",
    "description": "Semantic outlier flag"
  }
]
EOF
}

resource "google_bigquery_table" "gmp_metrics" {
  dataset_id          = google_bigquery_dataset.ano_dataset.dataset_id
  table_id            = "gmp_metrics"
  project             = var.project_id
  description         = "Time-partitioned and entity-clustered table storing Google Managed Service for Prometheus (GMP) telemetry."
  deletion_protection = false
  labels              = var.labels

  time_partitioning {
    type          = "DAY"
    field         = "timestamp"
    expiration_ms = 7776000000
  }

  clustering = ["host_id", "service_name", "domain"]

  schema = <<-EOF
[
  {
    "name": "metric_sample_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique sample ID"
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "Scrape timestamp (Partition key)"
  },
  {
    "name": "host_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Target host or switch ID"
  },
  {
    "name": "service_name",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Service name"
  },
  {
    "name": "domain",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Domain (COMPUTE, APPLICATION, DATABASE, NETWORK)"
  },
  {
    "name": "environment",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Environment (prod, staging)"
  },
  {
    "name": "cpu_utilization_pct",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "CPU usage percentage (0 to 100)"
  },
  {
    "name": "throughput_rps",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Application or system request throughput RPS"
  },
  {
    "name": "cpu_to_throughput_ratio",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Ratio detecting CPU spikes without throughput"
  },
  {
    "name": "thread_starvation_count",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "Starved or blocked OS or JVM worker threads"
  },
  {
    "name": "io_wait_pct",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "CPU I/O wait percentage"
  },
  {
    "name": "disk_latency_ms",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Storage read/write latency in milliseconds"
  },
  {
    "name": "socket_exhaustion_count",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "Sockets in TIME_WAIT/CLOSE_WAIT or epoll saturation"
  },
  {
    "name": "apache_tomcat_latency_ms",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Apache/Tomcat HTTP p95 latency in milliseconds"
  },
  {
    "name": "db_lock_wait_ms",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Database row/table lock wait time in milliseconds"
  },
  {
    "name": "db_active_sessions",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "Active concurrent DB sessions"
  },
  {
    "name": "ospf_flap_count",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "Network switch OSPF neighbor state flaps"
  },
  {
    "name": "packet_loss_pct",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Network interface packet loss percentage"
  },
  {
    "name": "unresponsive_in_15_30m",
    "type": "BOOL",
    "mode": "NULLABLE",
    "description": "Training label for Capability 1 (TRUE if host locked up in 15 to 30 minutes)"
  },
  {
    "name": "root_cause_domain",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Training label for Capability 2 (APPLICATION, DATABASE, NETWORK, COMPUTE, NONE)"
  }
]
EOF
}

resource "google_bigquery_table" "topology_edges" {
  dataset_id          = google_bigquery_dataset.ano_dataset.dataset_id
  table_id            = "topology_edges"
  project             = var.project_id
  description         = "Time-partitioned and entity-clustered table representing cross-domain dependency graph edges."
  deletion_protection = false
  labels              = var.labels

  time_partitioning {
    type  = "DAY"
    field = "updated_at"
  }

  clustering = ["source_entity_id", "target_entity_id", "relationship_type"]

  schema = <<-EOF
[
  {
    "name": "edge_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique directed graph edge ID"
  },
  {
    "name": "updated_at",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "Topology discovery timestamp (Partition key)"
  },
  {
    "name": "source_entity_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Upstream caller or dependent host/service ID"
  },
  {
    "name": "source_domain",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Upstream domain (APPLICATION, DATABASE, NETWORK, COMPUTE)"
  },
  {
    "name": "target_entity_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Downstream dependency host, DB, or switch ID"
  },
  {
    "name": "target_domain",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Downstream domain (APPLICATION, DATABASE, NETWORK, COMPUTE)"
  },
  {
    "name": "relationship_type",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "CONNECTS_TO, HOSTED_ON, ROUTES_THROUGH, DEPENDS_ON"
  },
  {
    "name": "criticality_weight",
    "type": "FLOAT64",
    "mode": "REQUIRED",
    "description": "Dependency impact weight (0.0 to 1.0)"
  },
  {
    "name": "hop_distance",
    "type": "INT64",
    "mode": "REQUIRED",
    "description": "Graph hop distance"
  },
  {
    "name": "metadata",
    "type": "JSON",
    "mode": "NULLABLE",
    "description": "BGP/OSPF area, VLAN, or connection pool metadata"
  }
]
EOF
}

resource "google_bigquery_table" "change_calendar" {
  dataset_id          = google_bigquery_dataset.ano_dataset.dataset_id
  table_id            = "change_calendar"
  project             = var.project_id
  description         = "Time-partitioned and entity-clustered table storing ServiceNow/CMDB scheduled maintenance windows."
  deletion_protection = false
  labels              = var.labels

  time_partitioning {
    type  = "DAY"
    field = "start_time"
  }

  clustering = ["target_entity_id", "change_type", "status"]

  schema = <<-EOF
[
  {
    "name": "change_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Change ticket ID (e.g. CHG0049281)"
  },
  {
    "name": "start_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "Scheduled window start timestamp (Partition key)"
  },
  {
    "name": "end_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "Scheduled window end timestamp"
  },
  {
    "name": "target_entity_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Target host_id or service_name undergoing maintenance"
  },
  {
    "name": "target_domain",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Domain (COMPUTE, APPLICATION, DATABASE, NETWORK)"
  },
  {
    "name": "change_type",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "OS_PATCHING, DB_MAINTENANCE, NETWORK_UPGRADE, APP_DEPLOYMENT"
  },
  {
    "name": "status",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "SCHEDULED, IN_PROGRESS, COMPLETED, CANCELLED"
  },
  {
    "name": "suppress_alerts",
    "type": "BOOL",
    "mode": "REQUIRED",
    "description": "Boolean flag to suppress false-positive alerts during window"
  },
  {
    "name": "change_risk_level",
    "type": "INT64",
    "mode": "REQUIRED",
    "description": "Exogenous risk regressor (1=Low, 2=Medium, 3=High)"
  },
  {
    "name": "owner_team",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Responsible engineering team"
  }
]
EOF
}

resource "google_bigquery_table" "incidents_predictions" {
  dataset_id          = google_bigquery_dataset.ano_dataset.dataset_id
  table_id            = "incidents_predictions"
  project             = var.project_id
  description         = "Time-partitioned and entity-clustered table storing correlated predictive incidents and remediation state."
  deletion_protection = false
  labels              = var.labels

  time_partitioning {
    type  = "DAY"
    field = "prediction_timestamp"
  }

  clustering = ["root_cause_entity_id", "capability_type", "suppressed_by_change_window", "remediation_status"]

  schema = <<-EOF
[
  {
    "name": "incident_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique incident or prediction UUID"
  },
  {
    "name": "prediction_timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "Generation timestamp (Partition key)"
  },
  {
    "name": "capability_type",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "CAP1_UNRESPONSIVENESS_15_30M, CAP2_CROSS_DOMAIN_30_60M, CAP3_DYNAMIC_BASELINE_ANOMALY, SEMANTIC_LOG_OUTLIER"
  },
  {
    "name": "affected_entity_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Primary entity exhibiting symptoms"
  },
  {
    "name": "root_cause_entity_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Attributed root-cause entity ID"
  },
  {
    "name": "root_cause_domain",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Attributed root-cause domain (COMPUTE, APPLICATION, DATABASE, NETWORK)"
  },
  {
    "name": "lead_time_minutes",
    "type": "INT64",
    "mode": "REQUIRED",
    "description": "Forecast lead time in minutes (15 to 30 or 30 to 60)"
  },
  {
    "name": "anomaly_probability",
    "type": "FLOAT64",
    "mode": "REQUIRED",
    "description": "Confidence or probability score (0.0 to 1.0)"
  },
  {
    "name": "semantic_nearest_neighbor_log_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Nearest historical incident log_id from VECTOR_SEARCH"
  },
  {
    "name": "cosine_distance",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Vector cosine distance to matched pattern"
  },
  {
    "name": "suppressed_by_change_window",
    "type": "BOOL",
    "mode": "REQUIRED",
    "description": "TRUE if suppressed by active change_calendar window"
  },
  {
    "name": "active_change_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Matching change_id if suppressed"
  },
  {
    "name": "recommended_action",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "DRAIN_AND_RESTART_WORKERS, KILL_BLOCKING_DB_SESSIONS, REROUTE_OSPF_TRAFFIC, SCALE_UP_INSTANCE_GROUP, NO_ACTION_SUPPRESSED"
  },
  {
    "name": "remediation_status",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "PENDING, TRIGGERED, COMPLETED, SUPPRESSED, FAILED"
  },
  {
    "name": "remediation_execution_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Cloud Run or Workflows execution trace ID"
  }
]
EOF
}

resource "google_bigquery_routine" "create_vector_index" {
  dataset_id      = google_bigquery_dataset.ano_dataset.dataset_id
  routine_id      = "sp_create_vector_index"
  routine_type    = "PROCEDURE"
  language        = "SQL"
  project         = var.project_id
  description     = "Stored procedure executing CREATE VECTOR INDEX with TREE_AH and COSINE distance on log_embeddings."
  definition_body = <<-EOF
BEGIN
  CREATE VECTOR INDEX IF NOT EXISTS `log_embeddings_vector_idx`
  ON `${var.project_id}.${var.dataset_id}.log_embeddings`(embedding)
  STORING(chunk_id, log_id, timestamp, service_name, host_id, domain, severity, chunk_text)
  OPTIONS(
    index_type = 'TREE_AH',
    distance_type = 'COSINE',
    tree_ah_options = '{"leaf_node_embedding_count": 1000, "normalization_type": "L2"}'
  );
END;
EOF
}

resource "google_bigquery_routine" "semantic_outlier_vector_search" {
  dataset_id      = google_bigquery_dataset.ano_dataset.dataset_id
  routine_id      = "sp_semantic_outlier_search"
  routine_type    = "PROCEDURE"
  language        = "SQL"
  project         = var.project_id
  description     = "Stored procedure executing real-time semantic outlier and incident pattern matching via VECTOR_SEARCH."
  definition_body = <<-EOF
BEGIN
  SELECT
    query.chunk_id AS query_chunk_id,
    query.log_id AS query_log_id,
    query.service_name AS query_service_name,
    query.host_id AS query_host_id,
    query.domain AS query_domain,
    base.log_id AS semantic_nearest_neighbor_log_id,
    base.chunk_text AS historical_failure_signature,
    distance AS cosine_distance
  FROM
    VECTOR_SEARCH(
      TABLE `${var.project_id}.${var.dataset_id}.log_embeddings`,
      'embedding',
      (
        SELECT chunk_id, log_id, service_name, host_id, domain, embedding
        FROM `${var.project_id}.${var.dataset_id}.log_embeddings`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
      ),
      top_k => 3,
      distance_type => 'COSINE',
      options => '{"fraction_lists_to_search": 0.05}'
    )
  WHERE
    distance < 0.25;
END;
EOF
}
