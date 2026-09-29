resource "google_bigquery_dataset" "gsk_observability_demo" {
  project                    = var.project_id
  dataset_id                 = var.observability_dataset_id
  friendly_name              = "GSK Enterprise Observability Lakehouse"
  description                = "Unified dataset storing EBRS raw telemetry, system logs, ServiceNow maintenance windows, BQML ARIMA/TimesFM baselines, and ISO GQL topology graphs"
  location                   = var.region
  delete_contents_on_destroy = false
  labels                     = var.labels
}

resource "google_logging_project_sink" "gsk_bq_telemetry_sink" {
  name                   = "gsk_bq_telemetry_sink"
  project                = var.project_id
  destination            = "bigquery.googleapis.com/projects/${var.project_id}/datasets/${google_bigquery_dataset.gsk_observability_demo.dataset_id}"
  filter                 = "logName=\"projects/${var.project_id}/logs/gsk_host_telemetry\""
  unique_writer_identity = true

  bigquery_options {
    use_partitioned_tables = true
  }
}

resource "google_bigquery_dataset_iam_member" "sink_dataset_writer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.gsk_observability_demo.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_logging_project_sink.gsk_bq_telemetry_sink.writer_identity
}

resource "google_bigquery_table" "enterprise_telemetry_partitioned" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "enterprise_telemetry_partitioned"
  project             = var.project_id
  description         = "Partitioned and hierarchically clustered raw telemetry table for GSK enterprise monitoring"
  deletion_protection = false
  labels              = var.labels

  time_partitioning {
    type          = "DAY"
    field         = "timestamp"
    expiration_ms = 7776000000
  }

  clustering = ["site_location", "system_id", "application_tier", "host_id"]

  schema = <<-EOF
[
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "UTC telemetry emission timestamp (Partition key)"
  },
  {
    "name": "enterprise_domain",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Enterprise domain (Pharma_Manufacturing, R_and_D, Commercial)"
  },
  {
    "name": "site_location",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Physical site location (Site_A_London, Site_B_Stevenage, Site_C_Ware)"
  },
  {
    "name": "system_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Manufacturing or enterprise system ID (EBRS, LIMS, MES_BATCH)"
  },
  {
    "name": "cluster_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Kubernetes or VMware ESXi cluster identifier"
  },
  {
    "name": "application_tier",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Application tier (Web_Frontend, Database_Backend, Batch_Processing)"
  },
  {
    "name": "host_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Canonical host identifier"
  },
  {
    "name": "hostname",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Server hostname used as TIME_SERIES_ID_COL"
  },
  {
    "name": "cpu_usage_pct",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Host CPU utilization percentage (0.0 preserved for silent drops)"
  },
  {
    "name": "memory_usage_pct",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Host memory utilization percentage"
  },
  {
    "name": "io_wait_ms",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Storage I/O wait latency in milliseconds"
  },
  {
    "name": "network_bytes_sec",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "Network throughput in bytes per second"
  },
  {
    "name": "status",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Operational host status (HEALTHY, ANOMALY, DOWN)"
  },
  {
    "name": "active_connections",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "Active application or database connection pool count"
  }
]
EOF
}

resource "google_bigquery_table" "system_logs" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "system_logs"
  project             = var.project_id
  description         = "Partitioned and clustered unstructured/structured system logs for 5-minute metric-to-log correlation"
  deletion_protection = false
  labels              = var.labels

  time_partitioning {
    type          = "DAY"
    field         = "timestamp"
    expiration_ms = 7776000000
  }

  clustering = ["host_id", "severity"]

  schema = <<-EOF
[
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "UTC log emission timestamp (Partition key)"
  },
  {
    "name": "log_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Unique log entry identifier"
  },
  {
    "name": "host_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Canonical host identifier"
  },
  {
    "name": "hostname",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Server hostname"
  },
  {
    "name": "site_location",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Physical site location"
  },
  {
    "name": "system_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "System identifier (EBRS, LIMS, MES_BATCH)"
  },
  {
    "name": "application_tier",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Application tier"
  },
  {
    "name": "severity",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Log severity (INFO, WARNING, ERROR, CRITICAL, FATAL)"
  },
  {
    "name": "service_name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Emitting process or service name"
  },
  {
    "name": "message",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unstructured log message or stack trace"
  }
]
EOF
}

resource "google_bigquery_table" "servicenow_maintenance_windows" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "servicenow_maintenance_windows"
  project             = var.project_id
  description         = "ServiceNow planned maintenance windows used as external regressor in ARIMA_PLUS_XREG"
  deletion_protection = false
  labels              = var.labels

  time_partitioning {
    type  = "DAY"
    field = "start_time"
  }

  clustering = ["hostname"]

  schema = <<-EOF
[
  {
    "name": "change_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "ServiceNow change request ID (e.g. CHG0049281)"
  },
  {
    "name": "host_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Canonical host identifier"
  },
  {
    "name": "hostname",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Target server hostname under maintenance"
  },
  {
    "name": "site_location",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Physical site location"
  },
  {
    "name": "system_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "System identifier"
  },
  {
    "name": "start_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "Scheduled maintenance window start timestamp"
  },
  {
    "name": "end_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "Scheduled maintenance window end timestamp"
  },
  {
    "name": "is_maintenance_window",
    "type": "INT64",
    "mode": "REQUIRED",
    "description": "Binary external regressor (1 during maintenance, 0 otherwise)"
  },
  {
    "name": "change_type",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Type of planned change"
  },
  {
    "name": "description",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Human-readable summary of the maintenance activity"
  }
]
EOF
}

resource "google_bigquery_table" "incident_root_cause_analysis" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "incident_root_cause_analysis"
  project             = var.project_id
  description         = "Materialized 3-sentence SRE incident root-cause analyses synthesized by Gemini 2.5 Flash"
  deletion_protection = false
  labels              = var.labels

  schema = <<-EOF
[
  {
    "name": "event_timestamp",
    "type": "TIMESTAMP",
    "mode": "NULLABLE",
    "description": "Incident anomaly timestamp"
  },
  {
    "name": "hostname",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Impacted host hostname"
  },
  {
    "name": "anomaly_score",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Measured anomaly score or metric spike"
  },
  {
    "name": "prompt",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Constructed SRE triage prompt sent to Gemini 2.5 Flash"
  },
  {
    "name": "gemini_root_cause_analysis",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "3-sentence SRE summary generated via ML.GENERATE_TEXT"
  }
]
EOF
}

resource "google_bigquery_table" "structured_log_entities" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "structured_log_entities"
  project             = var.project_id
  description         = "Schema-enforced structured incident entities extracted from unstructured error logs via AI.GENERATE_TABLE"
  deletion_protection = false
  labels              = var.labels

  schema = <<-EOF
[
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "NULLABLE",
    "description": "Log emission timestamp"
  },
  {
    "name": "hostname",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Emitting host hostname"
  },
  {
    "name": "root_cause_category",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Extracted root cause category"
  },
  {
    "name": "failed_component",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Extracted failed software/hardware component"
  },
  {
    "name": "error_code",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Extracted error code or exception class"
  },
  {
    "name": "recommended_action",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Recommended SRE remediation action"
  },
  {
    "name": "confidence_score",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Extraction confidence score (0.0 to 1.0)"
  }
]
EOF
}

resource "google_bigquery_table" "nodes_switches" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "nodes_switches"
  project             = var.project_id
  description         = "ISO GQL Property Graph Node Table (Step 1): Physical Switch Nodes from SolarWinds CMDB"
  deletion_protection = false
  labels              = var.labels

  schema = <<-EOF
[
  {
    "name": "switch_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique physical switch identifier (Graph KEY)"
  },
  {
    "name": "hostname",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Switch hostname (e.g. sw-core-stv-01)"
  },
  {
    "name": "site_location",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Physical site location"
  },
  {
    "name": "management_ip",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Switch management IPv4 address"
  },
  {
    "name": "model",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Switch hardware model"
  }
]
EOF
}

resource "google_bigquery_table" "nodes_hypervisors" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "nodes_hypervisors"
  project             = var.project_id
  description         = "ISO GQL Property Graph Node Table (Step 2): VMware ESXi Hypervisor Nodes"
  deletion_protection = false
  labels              = var.labels

  schema = <<-EOF
[
  {
    "name": "hypervisor_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique hypervisor identifier (Graph KEY)"
  },
  {
    "name": "hostname",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Hypervisor hostname (e.g. esxi-cluster-04)"
  },
  {
    "name": "site_location",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Physical site location"
  },
  {
    "name": "cluster_name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "VMware vSphere cluster name"
  },
  {
    "name": "esxi_version",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "ESXi hypervisor version"
  }
]
EOF
}

resource "google_bigquery_table" "nodes_hosts" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "nodes_hosts"
  project             = var.project_id
  description         = "ISO GQL Property Graph Node Table (Step 3): Virtual Host Nodes from ServiceNow CMDB"
  deletion_protection = false
  labels              = var.labels

  schema = <<-EOF
[
  {
    "name": "host_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique virtual host identifier (Graph KEY)"
  },
  {
    "name": "hostname",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Virtual machine hostname (e.g. srv-b-batch-02)"
  },
  {
    "name": "site_location",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Physical site location"
  },
  {
    "name": "application_type",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Hosted workload role or tier"
  },
  {
    "name": "operating_system",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Guest OS distribution and version"
  }
]
EOF
}

resource "google_bigquery_table" "nodes_applications" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "nodes_applications"
  project             = var.project_id
  description         = "ISO GQL Property Graph Node Table (Step 4): Logical Application Tier Nodes"
  deletion_protection = false
  labels              = var.labels

  schema = <<-EOF
[
  {
    "name": "app_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique application identifier (Graph KEY)"
  },
  {
    "name": "name",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Logical application name (e.g. EBRS-Batch-Engine, EBRS-Web-Portal)"
  },
  {
    "name": "tier",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Application tier (Frontend, Backend, Database, Batch)"
  },
  {
    "name": "system_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Parent enterprise system (EBRS, LIMS, MES_BATCH)"
  },
  {
    "name": "criticality",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "GxP criticality classification (Tier-1-GXP, Tier-2)"
  }
]
EOF
}

resource "google_bigquery_table" "edges_connected_to" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "edges_connected_to"
  project             = var.project_id
  description         = "ISO GQL Property Graph Edge Table (Step 5): Switch-to-Hypervisor CONNECTED_TO Edges"
  deletion_protection = false
  labels              = var.labels

  schema = <<-EOF
[
  {
    "name": "edge_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique edge identifier (Graph KEY)"
  },
  {
    "name": "switch_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Source physical switch ID"
  },
  {
    "name": "hypervisor_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Destination hypervisor ID"
  },
  {
    "name": "port_name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Physical switch uplink port name"
  },
  {
    "name": "speed_gbps",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "Link speed in Gbps"
  }
]
EOF
}

resource "google_bigquery_table" "edges_hosts_vm" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "edges_hosts_vm"
  project             = var.project_id
  description         = "ISO GQL Property Graph Edge Table (Step 6): Hypervisor-to-Host HOSTS Edges"
  deletion_protection = false
  labels              = var.labels

  schema = <<-EOF
[
  {
    "name": "edge_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique edge identifier (Graph KEY)"
  },
  {
    "name": "hypervisor_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Source hypervisor ID"
  },
  {
    "name": "host_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Destination virtual host ID"
  },
  {
    "name": "allocated_vcpus",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "Allocated virtual CPUs"
  },
  {
    "name": "allocated_ram_gb",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "Allocated RAM in GB"
  }
]
EOF
}

resource "google_bigquery_table" "edges_runs_app" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "edges_runs_app"
  project             = var.project_id
  description         = "ISO GQL Property Graph Edge Table (Step 7): Host-to-Application RUNS Edges"
  deletion_protection = false
  labels              = var.labels

  schema = <<-EOF
[
  {
    "name": "edge_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique edge identifier (Graph KEY)"
  },
  {
    "name": "host_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Source virtual host ID"
  },
  {
    "name": "app_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Destination application ID"
  },
  {
    "name": "process_id",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "OS process identifier"
  },
  {
    "name": "listen_port",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "TCP listen port"
  }
]
EOF
}

resource "google_bigquery_table" "edges_app_communicates" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "edges_app_communicates"
  project             = var.project_id
  description         = "ISO GQL Property Graph Edge Table (Step 8): Inter-Application COMMUNICATES_WITH Edges"
  deletion_protection = false
  labels              = var.labels

  schema = <<-EOF
[
  {
    "name": "edge_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique edge identifier (Graph KEY)"
  },
  {
    "name": "source_app_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Upstream calling application ID"
  },
  {
    "name": "target_app_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Downstream target application ID"
  },
  {
    "name": "protocol",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Application protocol (HTTPS, JDBC, gRPC)"
  },
  {
    "name": "avg_latency_ms",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Average RPC/query latency in milliseconds"
  }
]
EOF
}

resource "google_bigquery_table" "edges_network_flows" {
  dataset_id          = google_bigquery_dataset.gsk_observability_demo.dataset_id
  table_id            = "edges_network_flows"
  project             = var.project_id
  description         = "ISO GQL Property Graph Edge Table (Step 9): Host-to-Host CommunicatesWith Network Flow Edges"
  deletion_protection = false
  labels              = var.labels

  schema = <<-EOF
[
  {
    "name": "edge_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique edge identifier (Graph KEY)"
  },
  {
    "name": "source_host_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Source host ID"
  },
  {
    "name": "destination_host_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Destination host ID"
  },
  {
    "name": "avg_traffic",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": "Average network traffic in bytes per second"
  }
]
EOF
}

resource "google_bigquery_routine" "deploy_property_graph_gql" {
  dataset_id      = google_bigquery_dataset.gsk_observability_demo.dataset_id
  routine_id      = "sp_deploy_property_graph_gql"
  routine_type    = "PROCEDURE"
  language        = "SQL"
  project         = var.project_id
  description     = "Step 10: Compiles the multi-domain ISO GQL Property Graph (Switch -> Hypervisor -> Host -> Application) across 4 node tables and 5 edge tables."
  definition_body = <<-EOF
BEGIN
  EXECUTE IMMEDIATE """
    CREATE OR REPLACE PROPERTY GRAPH `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.gsk_infrastructure_dependency_graph`
      NODE TABLES (
        `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.nodes_switches` AS switches
          KEY (switch_id)
          LABEL Switch
          PROPERTIES (switch_id, hostname, site_location, management_ip, model),
        `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.nodes_hypervisors` AS hypervisors
          KEY (hypervisor_id)
          LABEL Hypervisor
          PROPERTIES (hypervisor_id, hostname, site_location, cluster_name, esxi_version),
        `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.nodes_hosts` AS hosts
          KEY (host_id)
          LABEL Host
          PROPERTIES (host_id, hostname, site_location, application_type, operating_system),
        `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.nodes_applications` AS applications
          KEY (app_id)
          LABEL Application
          PROPERTIES (app_id, name, tier, system_id, criticality)
      )
      EDGE TABLES (
        `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.edges_connected_to` AS connected_to
          KEY (edge_id)
          SOURCE KEY (switch_id) REFERENCES switches (switch_id)
          DESTINATION KEY (hypervisor_id) REFERENCES hypervisors (hypervisor_id)
          LABEL CONNECTED_TO
          PROPERTIES (port_name, speed_gbps),
        `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.edges_hosts_vm` AS hosts_vm
          KEY (edge_id)
          SOURCE KEY (hypervisor_id) REFERENCES hypervisors (hypervisor_id)
          DESTINATION KEY (host_id) REFERENCES hosts (host_id)
          LABEL HOSTS
          PROPERTIES (allocated_vcpus, allocated_ram_gb),
        `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.edges_runs_app` AS runs_app
          KEY (edge_id)
          SOURCE KEY (host_id) REFERENCES hosts (host_id)
          DESTINATION KEY (app_id) REFERENCES applications (app_id)
          LABEL RUNS
          PROPERTIES (process_id, listen_port),
        `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.edges_app_communicates` AS app_communicates
          KEY (edge_id)
          SOURCE KEY (source_app_id) REFERENCES applications (app_id)
          DESTINATION KEY (target_app_id) REFERENCES applications (app_id)
          LABEL COMMUNICATES_WITH
          PROPERTIES (protocol, avg_latency_ms),
        `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.edges_network_flows` AS network_flows
          KEY (edge_id)
          SOURCE KEY (source_host_id) REFERENCES hosts (host_id)
          DESTINATION KEY (destination_host_id) REFERENCES hosts (host_id)
          LABEL CommunicatesWith
          PROPERTIES (avg_traffic)
      )
  """;
END;
EOF
}

resource "google_bigquery_routine" "train_host_cpu_arima_model" {
  dataset_id      = google_bigquery_dataset.gsk_observability_demo.dataset_id
  routine_id      = "sp_train_host_cpu_arima_model"
  routine_type    = "PROCEDURE"
  language        = "SQL"
  project         = var.project_id
  description     = "Trains multi-series ARIMA_PLUS model with HORIZON = 10000, HOLIDAY_REGION = 'GB', and GAP_FILL() zero-filling."
  definition_body = <<-EOF
BEGIN
  CREATE OR REPLACE MODEL `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.host_cpu_arima_model`
  OPTIONS (
    MODEL_TYPE = 'ARIMA_PLUS',
    TIME_SERIES_TIMESTAMP_COL = 'ts_minute',
    TIME_SERIES_DATA_COL = 'cpu_usage',
    TIME_SERIES_ID_COL = 'hostname',
    DATA_FREQUENCY = 'PER_MINUTE',
    HORIZON = 10000,
    HOLIDAY_REGION = 'GB',
    AUTO_ARIMA = TRUE,
    CLEAN_SPIKES_AND_DIPS = TRUE
  ) AS
  WITH time_bounds AS (
    SELECT
      TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE), INTERVAL 14 DAY) AS start_ts,
      TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE), INTERVAL 1 MINUTE) AS end_ts
  ),
  raw_minute_telemetry AS (
    SELECT
      TIMESTAMP_TRUNC(timestamp, MINUTE) AS ts_minute,
      hostname,
      AVG(cpu_usage_pct) AS cpu_usage
    FROM `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.enterprise_telemetry_partitioned`
    CROSS JOIN time_bounds b
    WHERE timestamp >= b.start_ts
      AND timestamp <= b.end_ts
    GROUP BY ts_minute, hostname
  ),
  active_hosts AS (
    SELECT DISTINCT hostname FROM raw_minute_telemetry
  ),
  anchored_telemetry AS (
    SELECT ts_minute, hostname, cpu_usage FROM raw_minute_telemetry
    UNION ALL
    SELECT b.start_ts AS ts_minute, h.hostname, 0.0 AS cpu_usage FROM active_hosts h CROSS JOIN time_bounds b
    UNION ALL
    SELECT b.end_ts AS ts_minute, h.hostname, 0.0 AS cpu_usage FROM active_hosts h CROSS JOIN time_bounds b
  ),
  deduped_telemetry AS (
    SELECT ts_minute, hostname, MAX(cpu_usage) AS cpu_usage
    FROM anchored_telemetry
    GROUP BY ts_minute, hostname
  )
  SELECT
    ts_minute,
    hostname,
    COALESCE(cpu_usage, 0.0) AS cpu_usage
  FROM GAP_FILL(
    TABLE deduped_telemetry,
    ts_column => 'ts_minute',
    bucket_width => INTERVAL 1 MINUTE,
    partitioning_columns => ['hostname'],
    value_columns => [('cpu_usage', 'null')]
  );
END;
EOF
}

resource "google_bigquery_routine" "train_host_cpu_arimax_model" {
  dataset_id      = google_bigquery_dataset.gsk_observability_demo.dataset_id
  routine_id      = "sp_train_host_cpu_arimax_model"
  routine_type    = "PROCEDURE"
  language        = "SQL"
  project         = var.project_id
  description     = "Trains multivariate ARIMA_PLUS_XREG model incorporating ServiceNow planned maintenance windows (is_maintenance_window regressor)."
  definition_body = <<-EOF
BEGIN
  CREATE OR REPLACE MODEL `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.host_cpu_arimax_model`
  OPTIONS (
    MODEL_TYPE = 'ARIMA_PLUS_XREG',
    TIME_SERIES_TIMESTAMP_COL = 'ts_minute',
    TIME_SERIES_DATA_COL = 'cpu_usage',
    TIME_SERIES_ID_COL = 'hostname',
    DATA_FREQUENCY = 'PER_MINUTE',
    HORIZON = 10000,
    HOLIDAY_REGION = 'GB',
    AUTO_ARIMA = TRUE,
    CLEAN_SPIKES_AND_DIPS = TRUE
  ) AS
  SELECT
    TIMESTAMP_TRUNC(t.timestamp, MINUTE) AS ts_minute,
    t.hostname,
    AVG(t.cpu_usage_pct) AS cpu_usage,
    MAX(COALESCE(m.is_maintenance_window, 0)) AS is_maintenance
  FROM `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.enterprise_telemetry_partitioned` t
  LEFT JOIN `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.servicenow_maintenance_windows` m
    ON t.hostname = m.hostname
    AND TIMESTAMP_TRUNC(t.timestamp, MINUTE) BETWEEN m.start_time AND m.end_time
  WHERE t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
  GROUP BY ts_minute, t.hostname;
END;
EOF
}

resource "google_bigquery_routine" "train_composite_host_signals_arima" {
  dataset_id      = google_bigquery_dataset.gsk_observability_demo.dataset_id
  routine_id      = "sp_train_composite_host_signals_arima"
  routine_type    = "PROCEDURE"
  language        = "SQL"
  project         = var.project_id
  description     = "Trains composite multi-signal ARIMA_PLUS model across CPU utilization and log ingestion volume (TIME_SERIES_ID_COL = ['hostname', 'metric_name'])."
  definition_body = <<-EOF
BEGIN
  CREATE OR REPLACE VIEW `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.unified_telemetry_signal_view` AS
  SELECT
    TIMESTAMP_TRUNC(timestamp, MINUTE) AS ts_minute,
    hostname,
    'cpu_pct' AS metric_name,
    AVG(cpu_usage_pct) AS metric_value
  FROM `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.enterprise_telemetry_partitioned`
  GROUP BY ts_minute, hostname
  UNION ALL
  SELECT
    TIMESTAMP_TRUNC(timestamp, MINUTE) AS ts_minute,
    hostname,
    'log_volume' AS metric_name,
    CAST(COUNT(*) AS FLOAT64) AS metric_value
  FROM `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.system_logs`
  GROUP BY ts_minute, hostname;

  CREATE OR REPLACE MODEL `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.composite_host_signals_arima`
  OPTIONS (
    MODEL_TYPE = 'ARIMA_PLUS',
    TIME_SERIES_TIMESTAMP_COL = 'ts_minute',
    TIME_SERIES_DATA_COL = 'metric_value',
    TIME_SERIES_ID_COL = ['hostname', 'metric_name'],
    DATA_FREQUENCY = 'PER_MINUTE',
    HORIZON = 10000,
    HOLIDAY_REGION = 'GB',
    AUTO_ARIMA = TRUE,
    CLEAN_SPIKES_AND_DIPS = TRUE
  ) AS
  SELECT ts_minute, hostname, metric_name, metric_value
  FROM `${var.project_id}.${google_bigquery_dataset.gsk_observability_demo.dataset_id}.unified_telemetry_signal_view`
  WHERE ts_minute >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY);
END;
EOF
}
