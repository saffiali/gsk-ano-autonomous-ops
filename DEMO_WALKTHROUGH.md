# GSK Autonomous Operations (ANO) — Live Customer Demo Walkthrough Guide
**Target GCP Project:** `gke-demos-363017` (`europe-west2` / BigQuery `EU` Location)  
**Dataset:** `gsk_ano_ops`  
**Live Executive Dashboard URL:** [http://saffi-jetski-dev.c.googlers.com:8080](http://saffi-jetski-dev.c.googlers.com:8080) *(or `http://localhost:8080` inside Cloudtop)*  
**GitHub Repository:** [https://github.com/saffiali/gsk-ano-autonomous-ops](https://github.com/saffiali/gsk-ano-autonomous-ops)

---

## 🎯 1. Executive Demo Narrative & Business Value Hook

Use this opening framing before sharing your screen with GSK IT, Hosting, or Network Operations leaders:

> *"Today, enterprise IT and network operations across 12,000 VMs and 1,500 applications generate 71.2 billion events per day. Traditional static monitoring thresholds drown SREs in alert fatigue, miss silent hangs where CPU looks normal, and fail to connect application symptoms back to network infrastructure root causes.*
>
> *GSK’s Autonomous Operations (ANO) platform on Google Cloud shifts operations from reactive firefighting to predictive, self-healing AI-Ops across three core capabilities:*
> 1. **Zero-Regex Semantic Log Outlier Detection & 15–30 Minute Server Hang Prediction** *(Vertex AI `text-embedding-005` + BigQuery Vector Search + BQML Logistic Regression)*
> 2. **Cross-Domain Root Cause Attribution (30–60 Min Outage Forecast)** *(correlating Tomcat HTTP 500s $\rightarrow$ Oracle DB locks $\rightarrow$ Core Switch OSPF flaps)*
> 3. **Dynamic 90-Day Seasonal Baselines & ServiceNow Change Window Noise Suppression** *(BQML `ARIMA_PLUS_XREG` joined with `change_calendar` + Eventarc/Cloud Workflows automated self-healing)*
>
> *Across GSK's estate, this architecture eliminates **~50% of change-related outages**, slashes **MTTR by 78.6% (from 140 minutes to <30 minutes)**, and reclaims **85,500 engineer hours/year (\$6.41M/yr net value, 27.2x ROI)**."*

---

## ⚙️ 2. Solution Deployment & Telemetry Seeding (`gke-demos-363017`)

The platform features a **Dual-Mode Analytical Engine (`src/mirror_store.py`)**:
* **Live GCP Mode (`LIVE_GCP`)**: When authenticated via `gcloud auth login && gcloud auth application-default login` with access to `gke-demos-363017`, the deployment script provisions the dataset `gsk_ano_ops` in BigQuery `EU`, creates all 6 partitioned/clustered tables, builds the `TREE_AH` Vector Index (`log_embeddings_vector_idx`), and deploys all 3 BQML models directly into Google Cloud.
* **Deterministic Local Analytical Mirror Mode (`LOCAL_MIRROR`)**: If live credentials are refreshing or offline during a customer presentation, the script automatically provisions a 1:1 local SQLite3 mirror (`.cache/gsk_ano_local_mirror.db` with custom 768-dim L2 `COSINE_DISTANCE` UDFs) so the live Web UI Dashboard and CLI Demo Runner operate with **100% mathematical and SQL parity and zero downtime**.

### Step-by-Step Deployment & Seeding Commands

```bash
cd /usr/local/google/home/saffi/teamwork_projects/gsk_ano_production_tf

# 1. (Optional) Authenticate your Google Cloud session for live BigQuery/Vertex AI deployment:
gcloud auth login
gcloud auth application-default login
gcloud config set project gke-demos-363017

# 2. Trigger the Automated Solution Deployment to gke-demos-363017:
python3 scripts/deploy_to_gcp.py --project gke-demos-363017

# 3. Seed 90 Days of GSK Telemetry, CMDB Topology Graph, ServiceNow Windows, & Live Incidents:
python3 src/seed_live_demo.py --project gke-demos-363017 --inject-live-incidents
```

**Expected Verification Output:**
* **6/6 BigQuery Partitioned & Clustered Tables Active:** `raw_logs`, `log_embeddings`, `gmp_metrics`, `topology_edges`, `change_calendar`, `incidents_predictions`.
* **BigQuery Vector Index Active:** `log_embeddings_vector_idx` (`TREE_AH` ScaNN index with `COSINE` distance).
* **3 BQML Models Deployed:** `model_server_unresponsiveness` (`LOGISTIC_REG`), `model_cross_domain_rca` (`BOOSTED_TREE_CLASSIFIER`), and `model_rolling_baseline` (`ARIMA_PLUS_XREG`).

---

## 🖥️ 3. Interactive Executive Web UI Dashboard Walkthrough (4 Acts)

Open the live dashboard in your browser:
👉 **[http://saffi-jetski-dev.c.googlers.com:8080](http://saffi-jetski-dev.c.googlers.com:8080)** *(or `http://localhost:8080`)*

If the server is ever stopped, start it with:
```bash
python3 src/demo_dashboard.py --port 8080
```

### 🟢 Pre-Demo Orientation (KPI Header & Architecture Overview)
Point to the **Top KPI Bar** on the dashboard:
* **Estate Scale:** `12,000 VMs / 1,500 Apps` | `71.2B Events/Day`
* **MTTR Reduction:** `78.6%` (`140 min` $\rightarrow$ `< 30 min`)
* **Operational Toil Saved:** `85,500 hrs/yr` (`$6.41M/yr` Annual Net Value)
* **Change Outage Reduction:** `~50%` eliminated via ServiceNow `change_calendar` integration.

---

### 🎬 Act 1: Zero-Regex Semantic Log Outlier Detection (`< 2s Lead Time`)

#### 🖱️ What to Click
Click the **[Trigger Act 1: Novel Log Outlier]** button in the top navigation bar.

#### 👀 What Happens on Screen
1. Watch the **768-Dim Embedding & Semantic Outlier Inspector** panel update in real time.
2. A multi-line Java/Oracle XA distributed transaction deadlock stack trace (`ORA-02049: timeout: distributed transaction waiting for lock` / `ORA-00060: deadlock detected`) arrives from `tomcat-app-stv-01`.
3. `StackTraceCoalescer` and `LogNormalizer` mask dynamic memory addresses, thread IDs, and timestamps into `<HEX>`, `<THREAD>`, and `<TIMESTAMP>` while preserving the exact exception hierarchy and Java package/class frames.
4. Vertex AI `text-embedding-005` encodes the chunk into a 768-dimensional vector and queries BigQuery `VECTOR_SEARCH` (`TREE_AH` index).
5. The **Cosine Novelty Distance Gauge** jumps to **`0.421`** (exceeding the `0.350` anomaly threshold), flagging **`OUTLIER DETECTED (ZERO REGEX)`**.

#### 🗣️ Presenter Talking Track
> *"Notice that nobody wrote a single regex rule or Splunk query for `ORA-02049`. In a traditional environment, if an application upgrade introduces a brand-new distributed XA deadlock signature, rule-based monitoring is completely blind until users call the helpdesk. Here, Vertex AI `text-embedding-005` embeds the normalized stack trace into 768-dimensional vector space, and BigQuery `VECTOR_SEARCH` immediately flags it because its cosine distance (`0.421`) is far outside the 90-day historical cluster (`> 0.35`). We catch zero-day failure modes in under 2 seconds."*

---

### 🎬 Act 2: Server Unresponsiveness Prediction (`22 Minutes Ahead of Outage`)

#### 🖱️ What to Click
Click the **[Trigger Act 2: 22m Hang Prediction]** button in the top navigation bar.

#### 👀 What Happens on Screen
1. Look at the **Server Unresponsiveness Forecast** panel and the **`tomcat-app-stv-01` (`vm-stv-app-01`)** node in the Topology Graph.
2. Notice the **CPU Utilization** gauge sits at a healthy-looking **`65.2%`**—well below a traditional `85%` static threshold alert.
3. However, BQML `ML.PREDICT` (`model_server_unresponsiveness`) evaluates multivariate signals:
   * **CPU Utilization:** `65.2%` *(Normal)*
   * **HTTP Request Throughput:** Collapsed from `420 RPS` $\rightarrow$ **`38.4 RPS`** *(CPU spike without throughput)*
   * **JVM Blocked Threads:** **`142 threads`** *(Thread starvation)*
   * **OS Socket State:** **`4,850 CLOSE_WAIT sockets`** + **`42.8ms` disk I/O wait**
4. The model outputs a **`94.2%` Hang Probability** with an estimated **Lead Time of `22 Minutes`** before total JVM/OS lockup.

#### 🗣️ Presenter Talking Track
> *"This is the classic 'silent hang' that frustrates every hosting team. If you only monitor CPU at 85%, this server looks totally fine at 65.2% CPU. In reality, threads are deadlocked waiting on I/O, sockets are stuck in `CLOSE_WAIT` at 4,850, and actual HTTP throughput has collapsed to 38 requests per second. Our BigQuery ML multivariate model detects this divergence between resource consumption and useful work, predicting a total server hang **22 minutes before the VM stops responding to ping or SSH**."*

---

### 🎬 Act 3: Cross-Domain Root Cause Attribution (`47 Minutes Ahead of Outage`)

#### 🖱️ What to Click
Click the **[Trigger Act 3: Cross-Domain RCA]** button in the top navigation bar.

#### 👀 What Happens on Screen
1. Watch the **Interactive Topology Graph Visualizer** animate the 3-tier CMDB dependency chain:
   * **Tier 1 (Application):** `tomcat-app-stv-01` turns **AMBER** (`HTTP 500 Latency = 2,850ms`)
   * **Tier 2 (Database):** `ora-db-stv-01` turns **AMBER** (`Row Lock Wait = 1,420ms`, `Active Sessions = 218`)
   * **Tier 3 (Network Infrastructure):** `core-sw-lon-01` flashes **PULSING RED (ROOT CAUSE)** (`19 OSPF Neighbor Flaps`, `14.8% Packet Loss`)
2. The **Cross-Domain Attribution Table** displays the BQML `BOOSTED_TREE_CLASSIFIER` probabilities:
   * **Network Domain (`core-sw-lon-01`):** **`88.9%` Root Cause Attribution** *(Primary Culprit)*
   * **Database Domain (`ora-db-stv-01`):** `8.4%` *(Downstream Victim)*
   * **Application Domain (`tomcat-app-stv-01`):** `2.7%` *(Upstream Symptom)*
3. **Alert Collapse:** 47 separate symptom alerts across App, DB, and Host monitoring are collapsed into **1 actionable Root Cause Incident (`inc-act3-rca-003`)** routed to the Network Engineering queue **47 minutes ahead** of total cluster partition.

#### 🗣️ Presenter Talking Track
> *"When this incident hits, the Application team sees HTTP 500 timeouts and blames the Database team. The DBA sees session locks and blames the Storage or App team. Meanwhile, the real culprit is top-of-rack switch `core-sw-lon-01` suffering 19 OSPF flaps and 14.8% packet loss, which is dropping TCP ACKs between the database and app tiers.*
>
> *Instead of paging three separate war rooms of 15 engineers for 2 hours, our Cross-Domain BQML model joins telemetry across `topology_edges`, attributes **88.9% root cause probability** to `core-sw-lon-01`, suppresses the 46 symptomatic App and DB alerts, and routes a single root-cause ticket to Network Ops—slashing MTTR by 70%."*

---

### 🎬 Act 4: Dynamic 90-Day Baselines & ServiceNow Noise Suppression vs. Eventarc Self-Healing

#### 🖱️ What to Click
Click the **[Trigger Act 4: Suppression & Self-Healing]** button in the top navigation bar.

#### 👀 What Happens on Screen
Look at the **ServiceNow Change Suppression & Eventarc Self-Healing Stream** panel at the bottom right, which demonstrates two side-by-side operational scenarios:

1. **Scenario A — Scheduled OS Patching (`SUPPRESSED BY SERVICENOW CHG0049281`):**
   * Host `ora-db-stv-01` spikes to `96.4%` CPU at 02:15 UTC during Sunday night maintenance.
   * BQML `ARIMA_PLUS_XREG` recognizes both the 90-day weekly seasonal pattern **and** queries `change_calendar`, finding active approved ServiceNow ticket **`CHG0049281`** (`suppress_alerts = TRUE`).
   * **Result:** Badge displays **`SUPPRESSED_MAINTENANCE_WINDOW`** (`0 Pages Sent`, `0 Engineer Toil`).

2. **Scenario B — Unscheduled Network Failure (`AUTO-HEALED BY CLOUD WORKFLOWS`):**
   * Switch `core-sw-lon-01` suffers an unscheduled OSPF flap outside any active ServiceNow window (`change_calendar` lookup returns `FALSE`).
   * **Result:** Eventarc immediately triggers the Cloud Run remediation webhook (`src/remediation_webhook.py`), which enforces a 900s rate-limit cooldown check and executes Cloud Workflow **`gsk-ano-network-ospf-reroute`** (`exec-live-9f82a1b4c3d2`), shifting OSPF link cost to drain traffic to the standby core switch in **`42 seconds`**.

#### 🗣️ Presenter Talking Track
> *"Roughly 50% of enterprise outages and alert storms are triggered by planned changes and patching windows. In Scenario A, when `ora-db-stv-01` spikes during an approved ServiceNow maintenance window (`CHG0049281`), our pipeline automatically suppresses the noise—nobody gets woken up at 2 AM for planned patching.*
>
> *Conversely, in Scenario B, when `core-sw-lon-01` degrades outside a change window, Eventarc triggers our Cloud Run self-healing webhook and Cloud Workflows (`gsk-ano-network-ospf-reroute`) to automatically drain traffic to the redundant switch in **42 seconds**—before a single customer transaction fails."*

---

## 💻 4. CLI Presenter & BigQuery Console SQL Walkthrough

If your audience includes Principal Engineers, Data Scientists, or SREs who want to see the terminal CLI presenter and raw BigQuery SQL execution:

### Run the Interactive 4-Act CLI Demo Presenter
```bash
# Run all 4 Acts sequentially with formatted ASCII tables, verbatim BigQuery SQL, and talking points:
python3 src/demo_runner.py --project gke-demos-363017 --act all

# Or run individual Acts during Q&A:
python3 src/demo_runner.py --project gke-demos-363017 --act 1
python3 src/demo_runner.py --project gke-demos-363017 --act 2
python3 src/demo_runner.py --project gke-demos-363017 --act 3
python3 src/demo_runner.py --project gke-demos-363017 --act 4
```

### Run the 24-Check Automated Verification Suite
To prove production readiness, Terraform HCL validity (`/google/bin/releases/g3terraform/hclfmt -check`), BigQuery schema parity, and zero secrets exposure:
```bash
python3 tests/validate_all.py
```
**Output:**
```text
Ran 24 tests in 13.417s
OK
SUMMARY: Executed 24 verification checks | Passed: 24 | Failures: 0 | Errors: 0 | Pass Rate: 100.0%
```
