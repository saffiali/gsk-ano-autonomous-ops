-- =============================================================================
-- GSK Enterprise Observability Platform — 05: Multi-Domain ISO GQL Property Graph
-- Full-Stack Topology: Switch -> Hypervisor -> Host -> Application
-- Includes Complete 10-Step DDL, Standalone ISO GQL Queries, and Compliant
-- SQL GRAPH_TABLE Queries Projecting Scalar Properties & JSON Visualization Objects.
-- =============================================================================

-- =============================================================================
-- PART A: Direct Schema DDL for 4 Node Tables & 5 Edge Tables
-- =============================================================================

-- Step 1: Physical Switch Nodes Table (SolarWinds CMDB)
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.nodes_switches`
(
  switch_id STRING NOT NULL,
  hostname STRING NOT NULL,
  site_location STRING,
  management_ip STRING,
  model STRING
);

-- Step 2: Hypervisor Nodes Table (VMware ESXi Clusters)
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.nodes_hypervisors`
(
  hypervisor_id STRING NOT NULL,
  hostname STRING NOT NULL,
  site_location STRING,
  cluster_name STRING,
  esxi_version STRING
);

-- Step 3: Virtual Host Nodes Table (ServiceNow VM Records)
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.nodes_hosts`
(
  host_id STRING NOT NULL,
  hostname STRING NOT NULL,
  site_location STRING,
  application_type STRING,
  operating_system STRING
);

-- Step 4: Logical Application Tier Nodes Table
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.nodes_applications`
(
  app_id STRING NOT NULL,
  name STRING NOT NULL,
  tier STRING,              -- 'Frontend', 'Backend', 'Database', 'Batch'
  system_id STRING,         -- 'EBRS', 'LIMS', 'MES_BATCH'
  criticality STRING        -- 'Tier-1-GXP', 'Tier-2'
);

-- Step 5: Switch-to-Hypervisor Connection Edges Table
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.edges_connected_to`
(
  edge_id STRING NOT NULL,
  switch_id STRING NOT NULL,
  hypervisor_id STRING NOT NULL,
  port_name STRING,
  speed_gbps INT64
);

-- Step 6: Hypervisor-to-VM Hosting Edges Table
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.edges_hosts_vm`
(
  edge_id STRING NOT NULL,
  hypervisor_id STRING NOT NULL,
  host_id STRING NOT NULL,
  allocated_vcpus INT64,
  allocated_ram_gb INT64
);

-- Step 7: Host-to-Application Execution Edges Table
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.edges_runs_app`
(
  edge_id STRING NOT NULL,
  host_id STRING NOT NULL,
  app_id STRING NOT NULL,
  process_id INT64,
  listen_port INT64
);

-- Step 8: Inter-Application Communication Edges Table
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.edges_app_communicates`
(
  edge_id STRING NOT NULL,
  source_app_id STRING NOT NULL,
  target_app_id STRING NOT NULL,
  protocol STRING,
  avg_latency_ms FLOAT64
);

-- Step 9: Host Network Flow Edges Table
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.edges_network_flows`
(
  edge_id STRING NOT NULL,
  source_host_id STRING NOT NULL,
  destination_host_id STRING NOT NULL,
  avg_traffic FLOAT64
);

-- =============================================================================
-- PART B: Blueprint CMDB Inventory Materialization Statements (Steps 1–9 CTAS)
-- =============================================================================
/*
-- Step 1: Materialize Physical Switch Nodes (SolarWinds CMDB)
CREATE OR REPLACE TABLE `gke-demos-363017.gsk_observability_demo.nodes_switches` AS
SELECT switch_id, hostname, site_location, management_ip, model
FROM `gke-demos-363017.gsk_observability_demo.solarwinds_switch_inventory`;

-- Step 2: Materialize Hypervisor Nodes (VMware ESXi Clusters)
CREATE OR REPLACE TABLE `gke-demos-363017.gsk_observability_demo.nodes_hypervisors` AS
SELECT hypervisor_id, hostname, site_location, cluster_name, esxi_version
FROM `gke-demos-363017.gsk_observability_demo.vmware_host_inventory`;

-- Step 3: Materialize Virtual Host Nodes (ServiceNow VM Records)
CREATE OR REPLACE TABLE `gke-demos-363017.gsk_observability_demo.nodes_hosts` AS
SELECT host_id, hostname, site_location, application_type, operating_system
FROM `gke-demos-363017.gsk_observability_demo.servicenow_server_inventory`;

-- Step 4: Materialize Logical Application Tier Nodes
CREATE OR REPLACE TABLE `gke-demos-363017.gsk_observability_demo.nodes_applications` AS
SELECT app_id, name, tier, system_id, criticality
FROM `gke-demos-363017.gsk_observability_demo.cmdb_applications`;

-- Step 5: Materialize Switch-to-Hypervisor Connection Edges
CREATE OR REPLACE TABLE `gke-demos-363017.gsk_observability_demo.edges_connected_to` AS
SELECT GENERATE_UUID() AS edge_id, switch_id, hypervisor_id, port_name, speed_gbps
FROM `gke-demos-363017.gsk_observability_demo.network_topology_links`;

-- Step 6: Materialize Hypervisor-to-VM Hosting Edges
CREATE OR REPLACE TABLE `gke-demos-363017.gsk_observability_demo.edges_hosts_vm` AS
SELECT GENERATE_UUID() AS edge_id, hypervisor_id, host_id, allocated_vcpus, allocated_ram_gb
FROM `gke-demos-363017.gsk_observability_demo.vmware_vm_allocations`;

-- Step 7: Materialize Host-to-Application Execution Edges
CREATE OR REPLACE TABLE `gke-demos-363017.gsk_observability_demo.edges_runs_app` AS
SELECT GENERATE_UUID() AS edge_id, host_id, app_id, process_id, listen_port
FROM `gke-demos-363017.gsk_observability_demo.process_bindings`;

-- Step 8: Materialize Inter-Application Communication Edges
CREATE OR REPLACE TABLE `gke-demos-363017.gsk_observability_demo.edges_app_communicates` AS
SELECT GENERATE_UUID() AS edge_id, source_app_id, target_app_id, protocol, avg_latency_ms
FROM `gke-demos-363017.gsk_observability_demo.app_network_matrix`;

-- Step 9: Materialize Host Network Flow Edges
CREATE OR REPLACE TABLE `gke-demos-363017.gsk_observability_demo.edges_network_flows` AS
SELECT GENERATE_UUID() AS edge_id, source_host_id, destination_host_id, AVG(avg_traffic_bytes_sec) AS avg_traffic
FROM `gke-demos-363017.gsk_observability_demo.network_telemetry`
GROUP BY source_host_id, destination_host_id;
*/

-- =============================================================================
-- Step 10: Compile Multi-Domain ISO GQL Property Graph
-- =============================================================================
CREATE OR REPLACE PROPERTY GRAPH `gke-demos-363017.gsk_observability_demo.gsk_infrastructure_dependency_graph`
  NODE TABLES (
    `gke-demos-363017.gsk_observability_demo.nodes_switches` AS switches
      KEY (switch_id)
      LABEL Switch
      PROPERTIES (switch_id, hostname, site_location, management_ip, model),

    `gke-demos-363017.gsk_observability_demo.nodes_hypervisors` AS hypervisors
      KEY (hypervisor_id)
      LABEL Hypervisor
      PROPERTIES (hypervisor_id, hostname, site_location, cluster_name, esxi_version),

    `gke-demos-363017.gsk_observability_demo.nodes_hosts` AS hosts
      KEY (host_id)
      LABEL Host
      PROPERTIES (host_id, hostname, site_location, application_type, operating_system),

    `gke-demos-363017.gsk_observability_demo.nodes_applications` AS applications
      KEY (app_id)
      LABEL Application
      PROPERTIES (app_id, name, tier, system_id, criticality)
  )
  EDGE TABLES (
    `gke-demos-363017.gsk_observability_demo.edges_connected_to` AS connected_to
      KEY (edge_id)
      SOURCE KEY (switch_id) REFERENCES switches (switch_id)
      DESTINATION KEY (hypervisor_id) REFERENCES hypervisors (hypervisor_id)
      LABEL CONNECTED_TO
      PROPERTIES (port_name, speed_gbps),

    `gke-demos-363017.gsk_observability_demo.edges_hosts_vm` AS hosts_vm
      KEY (edge_id)
      SOURCE KEY (hypervisor_id) REFERENCES hypervisors (hypervisor_id)
      DESTINATION KEY (host_id) REFERENCES hosts (host_id)
      LABEL HOSTS
      PROPERTIES (allocated_vcpus, allocated_ram_gb),

    `gke-demos-363017.gsk_observability_demo.edges_runs_app` AS runs_app
      KEY (edge_id)
      SOURCE KEY (host_id) REFERENCES hosts (host_id)
      DESTINATION KEY (app_id) REFERENCES applications (app_id)
      LABEL RUNS
      PROPERTIES (process_id, listen_port),

    `gke-demos-363017.gsk_observability_demo.edges_app_communicates` AS app_communicates
      KEY (edge_id)
      SOURCE KEY (source_app_id) REFERENCES applications (app_id)
      DESTINATION KEY (target_app_id) REFERENCES applications (app_id)
      LABEL COMMUNICATES_WITH
      PROPERTIES (protocol, avg_latency_ms),

    `gke-demos-363017.gsk_observability_demo.edges_network_flows` AS network_flows
      KEY (edge_id)
      SOURCE KEY (source_host_id) REFERENCES hosts (host_id)
      DESTINATION KEY (destination_host_id) REFERENCES hosts (host_id)
      LABEL CommunicatesWith
      PROPERTIES (avg_traffic)
  );

-- =============================================================================
-- PART C: Standalone ISO GQL & SQL GRAPH_TABLE Traversal Queries
-- =============================================================================

-- Query 1: Full-Stack ISO GQL Query (Switch -> Hypervisor -> Host -> Application)
GRAPH `gke-demos-363017.gsk_observability_demo.gsk_infrastructure_dependency_graph`
MATCH (sw:Switch {switch_id: 'sw-core-stv-01'})-[:CONNECTED_TO]->(hyp:Hypervisor)-[:HOSTS]->(vm:Host)-[:RUNS]->(backend:Application)<-[:COMMUNICATES_WITH]-(frontend:Application)
RETURN 
  sw.hostname AS failing_switch,
  hyp.hostname AS impacted_hypervisor,
  vm.hostname AS impacted_vm,
  backend.name AS impacted_backend,
  frontend.name AS impacted_frontend;

-- Query 2: Equivalent SQL GRAPH_TABLE Full-Stack Traversal (Projecting Explicit Scalar Attributes)
SELECT
  failing_switch,
  impacted_hypervisor,
  impacted_vm,
  impacted_backend,
  impacted_frontend
FROM GRAPH_TABLE(
  `gke-demos-363017.gsk_observability_demo.gsk_infrastructure_dependency_graph`,
  MATCH (sw:Switch {switch_id: 'sw-core-stv-01'})-[:CONNECTED_TO]->(hyp:Hypervisor)-[:HOSTS]->(vm:Host)-[:RUNS]->(backend:Application)<-[:COMMUNICATES_WITH]-(frontend:Application)
  RETURN
    sw.hostname AS failing_switch,
    hyp.hostname AS impacted_hypervisor,
    vm.hostname AS impacted_vm,
    backend.name AS impacted_backend,
    frontend.name AS impacted_frontend
);

-- Query 3: Standalone ISO GQL Query — Upstream & Downstream Host Blast Radius
GRAPH `gke-demos-363017.gsk_observability_demo.gsk_infrastructure_dependency_graph`
MATCH (source:Host)-[flow:CommunicatesWith]->(downstream:Host)
RETURN 
  source.hostname AS source_server,
  source.site_location AS source_site,
  downstream.hostname AS downstream_server,
  downstream.site_location AS downstream_site,
  flow.avg_traffic AS traffic_bytes_sec
ORDER BY traffic_bytes_sec DESC
LIMIT 20;

-- Query 4: SQL Interoperability & Visualization JSON via GRAPH_TABLE
-- Projects explicit scalar properties (src.hostname AS src_host, dst.hostname AS dst_host)
-- and constructs structured JSON for Grafana / Cytoscape topology panels.
WITH path_matches AS (
  SELECT 
    src_host,
    src_site,
    src_app,
    dst_host,
    dst_site,
    dst_app,
    traffic_bytes_sec
  FROM GRAPH_TABLE(
    `gke-demos-363017.gsk_observability_demo.gsk_infrastructure_dependency_graph`,
    MATCH (src:Host)-[flow:CommunicatesWith]->(dst:Host)
    WHERE src.host_id != dst.host_id
    RETURN 
      src.hostname AS src_host,
      src.site_location AS src_site,
      src.application_type AS src_app,
      dst.hostname AS dst_host,
      dst.site_location AS dst_site,
      dst.application_type AS dst_app,
      flow.avg_traffic AS traffic_bytes_sec
  )
)
SELECT 
  JSON_OBJECT(
    'total_dependency_paths', COUNT(*),
    'nodes', (
      SELECT ARRAY_AGG(DISTINCT node) FROM (
        SELECT src_host AS node FROM path_matches
        UNION ALL
        SELECT dst_host AS node FROM path_matches
      )
    ),
    'edges', (
      SELECT ARRAY_AGG(
        JSON_OBJECT(
          'source', src_host,
          'source_site', src_site,
          'target', dst_host,
          'target_site', dst_site,
          'traffic_bytes_sec', traffic_bytes_sec
        )
      ) FROM path_matches
    )
  ) AS graph_visualization_json
FROM path_matches;
