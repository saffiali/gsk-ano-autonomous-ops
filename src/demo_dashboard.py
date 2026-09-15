#!/usr/bin/env python3
"""GSK Autonomous Operations (ANO) Interactive Executive Web UI Dashboard.

Self-contained, zero-external-dependency Python HTTP server
(`http.server.ThreadingHTTPServer`) serving a dark-themed HTML5/Tailwind CSS/SVG
Executive AI-Ops Demo Dashboard and JSON REST API endpoints:
  - GET /
  - GET /api/status
  - GET /api/topology
  - GET /api/incidents
  - POST /api/trigger_act/<act_id> (also supports GET)

Directly integrates with `DemoExecutionEngine` from `src.demo_runner` and the
resilient SQLite analytical mirror adapter.
"""

import argparse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sqlite3
import sys
import threading
from typing import Any, Optional
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from src.demo_runner import DemoExecutionEngine


DASHBOARD_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>GSK Autonomous Operations (ANO) — Executive AI-Ops Command Center</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @keyframes pulse-border {
      0%, 100% { stroke-opacity: 1; stroke-width: 3px; }
      50% { stroke-opacity: 0.35; stroke-width: 5px; }
    }
    .pulse-ring { animation: pulse-border 2s infinite; }
    @keyframes flow-dash {
      to { stroke-dashoffset: -24; }
    }
    .flow-edge {
      stroke-dasharray: 8 4;
      animation: flow-dash 1.2s linear infinite;
    }
  </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen font-sans selection:bg-cyan-500 selection:text-slate-950">

  <!-- TOP NAVIGATION & ONE-CLICK ACT CONTROL BAR -->
  <header class="border-b border-slate-800 bg-slate-900/90 backdrop-blur sticky top-0 z-50">
    <div class="max-w-7xl mx-auto px-4 py-3 flex flex-wrap items-center justify-between gap-4">
      <div class="flex items-center gap-3">
        <div class="w-3 h-3 rounded-full bg-emerald-400 animate-ping"></div>
        <div>
          <h1 class="text-lg font-bold tracking-tight text-white">
            GSK Autonomous Operations (ANO) — Executive AI-Ops Dashboard
          </h1>
          <p class="text-xs text-slate-400">
            GCP Project: <span class="text-cyan-400 font-mono">gke-demos-363017</span> |
            Dataset: <span class="text-cyan-400 font-mono">gsk_ano_ops</span> |
            Region: <span class="text-emerald-400 font-mono">europe-west2 (London / EU)</span>
          </p>
        </div>
      </div>

      <!-- One-Click Interactive Act Trigger Buttons -->
      <div class="flex flex-wrap items-center gap-2">
        <button onclick="triggerAct(1)" id="btn-act-1"
          class="px-3 py-1.5 rounded-lg text-xs font-semibold bg-cyan-950 hover:bg-cyan-800 text-cyan-200 border border-cyan-700 transition cursor-pointer">
          Act 1: Novel Log Outlier (Vector Search)
        </button>
        <button onclick="triggerAct(2)" id="btn-act-2"
          class="px-3 py-1.5 rounded-lg text-xs font-semibold bg-amber-950 hover:bg-amber-800 text-amber-200 border border-amber-700 transition cursor-pointer">
          Act 2: 22m Hang Prediction (65.2% CPU)
        </button>
        <button onclick="triggerAct(3)" id="btn-act-3"
          class="px-3 py-1.5 rounded-lg text-xs font-semibold bg-rose-950 hover:bg-rose-800 text-rose-200 border border-rose-700 transition cursor-pointer">
          Act 3: Cross-Domain Topology RCA (42m Lead)
        </button>
        <button onclick="triggerAct(4)" id="btn-act-4"
          class="px-3 py-1.5 rounded-lg text-xs font-semibold bg-emerald-950 hover:bg-emerald-800 text-emerald-200 border border-emerald-700 transition cursor-pointer">
          Act 4: ServiceNow Suppression vs Self-Healing
        </button>
      </div>
    </div>
  </header>

  <main class="max-w-7xl mx-auto px-4 py-6 space-y-6">

    <!-- PANEL 1: LIVE ESTATE TELEMETRY & KPI HEADER -->
    <section aria-label="Live Estate Telemetry and KPI Header">
      <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <!-- KPI Card 1 -->
        <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-4 shadow-lg">
          <div class="flex items-center justify-between">
            <span class="text-xs uppercase tracking-wider text-slate-400 font-semibold">Monitored Estate Scale</span>
            <span class="px-2 py-0.5 text-[10px] rounded bg-cyan-950 text-cyan-300 border border-cyan-800">19,400 Endpoints</span>
          </div>
          <div class="mt-2 text-2xl font-extrabold text-white">12,000 VMs</div>
          <p class="mt-1 text-xs text-slate-400">
            1,500 Apps | 4,000 Tomcats | 600 DBs | 800 Switches
          </p>
        </div>

        <!-- KPI Card 2 -->
        <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-4 shadow-lg">
          <div class="flex items-center justify-between">
            <span class="text-xs uppercase tracking-wider text-slate-400 font-semibold">Daily Telemetry Volume</span>
            <span class="px-2 py-0.5 text-[10px] rounded bg-purple-950 text-purple-300 border border-purple-800">4.8 TB/day Raw</span>
          </div>
          <div class="mt-2 text-2xl font-extrabold text-white">71.2B events/day</div>
          <p class="mt-1 text-xs text-slate-400">
            52.0 TB active 90-day BigQuery lakehouse footprint
          </p>
        </div>

        <!-- KPI Card 3 -->
        <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-4 shadow-lg">
          <div class="flex items-center justify-between">
            <span class="text-xs uppercase tracking-wider text-slate-400 font-semibold">Automated RCA Acceleration</span>
            <span class="px-2 py-0.5 text-[10px] rounded bg-emerald-950 text-emerald-300 border border-emerald-800">112m &rarr; 24m</span>
          </div>
          <div class="mt-2 text-2xl font-extrabold text-emerald-400">78.6% MTTR reduction</div>
          <p class="mt-1 text-xs text-slate-400">
            42 symptom alarms collapsed into 1 root-cause ticket
          </p>
        </div>

        <!-- KPI Card 4 -->
        <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-4 shadow-lg">
          <div class="flex items-center justify-between">
            <span class="text-xs uppercase tracking-wider text-slate-400 font-semibold">Operational Toil Eliminated</span>
            <span class="px-2 py-0.5 text-[10px] rounded bg-amber-950 text-amber-300 border border-amber-800">~50% Change Outages</span>
          </div>
          <div class="mt-2 text-2xl font-extrabold text-amber-300">85,500 hrs/yr toil saved</div>
          <p class="mt-1 text-xs text-slate-400">
            Zero-regex Vector Search + Eventarc Cloud Workflows
          </p>
        </div>
      </div>
    </section>

    <!-- ACTIVE ACT STATUS BANNER -->
    <div id="active-act-banner" class="bg-gradient-to-r from-slate-900 via-cyan-950/40 to-slate-900 border border-cyan-800/60 rounded-xl p-4 flex flex-wrap items-center justify-between gap-4">
      <div>
        <div class="flex items-center gap-2">
          <span id="act-badge" class="px-2.5 py-0.5 rounded-full text-xs font-bold bg-cyan-500 text-slate-950">LIVE DEMO READY</span>
          <h2 id="act-title" class="text-base font-bold text-white">
            Select an Act above or inspect live telemetry across Stevenage &amp; London clusters
          </h2>
        </div>
        <p id="act-subtitle" class="text-xs text-slate-300 mt-1">
          Backed by BigQuery Vector Search (`TREE_AH`), BQML (`LOGISTIC_REG`, `BOOSTED_TREE_CLASSIFIER`, `ARIMA_PLUS_XREG`), and Eventarc Cloud Workflows.
        </p>
      </div>
      <div class="text-right font-mono text-xs text-cyan-300" id="act-lead-time">
        Lead Time: 22m–42m Predictive Window
      </div>
    </div>

    <!-- MIDDLE GRID: PANEL 2 (TOPOLOGY GRAPH) & PANEL 3 (768-DIM EMBEDDING EXPLORER) -->
    <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">

      <!-- PANEL 2: INTERACTIVE TOPOLOGY GRAPH VISUALIZER (7 COLS) -->
      <section class="lg:col-span-7 bg-slate-900/90 border border-slate-800 rounded-xl p-5 flex flex-col justify-between">
        <div>
          <div class="flex items-center justify-between mb-3">
            <div>
              <h3 class="text-sm font-bold uppercase tracking-wider text-cyan-400">
                Panel 2 — Interactive CMDB Topology Graph Visualizer (`topology_edges`)
              </h3>
              <p class="text-xs text-slate-400">
                2-Hop Cross-Domain Attribution: <span class="font-mono text-slate-200">tomcat-app-stv-01 &rarr; ora-db-stv-01 &rarr; core-sw-lon-01</span>
              </p>
            </div>
            <span class="px-2.5 py-1 rounded text-xs font-mono bg-rose-950 text-rose-300 border border-rose-800">
              Root Cause: core-sw-lon-01 (98.2% Conf)
            </span>
          </div>

          <!-- Interactive SVG Graph -->
          <div class="bg-slate-950 border border-slate-800/80 rounded-lg p-3 overflow-x-auto">
            <svg viewBox="0 0 760 290" class="w-full h-auto select-none">
              <defs>
                <marker id="arrow-symptom" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                  <path d="M 0 1 L 10 5 L 0 9 z" fill="#f59e0b" />
                </marker>
                <marker id="arrow-root" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                  <path d="M 0 1 L 10 5 L 0 9 z" fill="#f43f5e" />
                </marker>
                <marker id="arrow-host" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                  <path d="M 0 1 L 10 5 L 0 9 z" fill="#38bdf8" />
                </marker>
              </defs>

              <!-- Directed Edges -->
              <!-- Edge 1: tomcat-app-stv-01 -> ora-db-stv-01 -->
              <line x1="205" y1="95" x2="310" y2="95" stroke="#f59e0b" stroke-width="3" class="flow-edge" marker-end="url(#arrow-symptom)" />
              <text x="257" y="83" text-anchor="middle" fill="#fbbf24" font-size="10" font-family="monospace">DEPENDS_ON (0.95)</text>

              <!-- Edge 2: ora-db-stv-01 -> core-sw-lon-01 -->
              <line x1="480" y1="95" x2="575" y2="95" stroke="#f43f5e" stroke-width="3.5" class="flow-edge" marker-end="url(#arrow-root)" />
              <text x="527" y="83" text-anchor="middle" fill="#fb7185" font-size="10" font-family="monospace">ROUTES_THROUGH (0.90)</text>

              <!-- Edge 3: tomcat-app-stv-01 -> vm-stv-app-01 -->
              <line x1="115" y1="150" x2="115" y2="205" stroke="#38bdf8" stroke-width="2" stroke-dasharray="4 3" marker-end="url(#arrow-host)" />
              <text x="175" y="182" text-anchor="middle" fill="#7dd3fc" font-size="10" font-family="monospace">HOSTED_ON (0.98)</text>

              <!-- NODE 1: tomcat-app-stv-01 (APPLICATION) -->
              <g transform="translate(25, 45)">
                <rect width="180" height="100" rx="10" fill="#0f172a" stroke="#f59e0b" stroke-width="2.5" />
                <text x="12" y="22" fill="#fbbf24" font-size="11" font-weight="bold" font-family="monospace">tomcat-app-stv-01</text>
                <text x="12" y="38" fill="#94a3b8" font-size="10">Service: gsk-lims-api (App)</text>
                <text x="12" y="55" fill="#f87171" font-size="10" font-family="monospace">&bull; HTTP 500 Latency: 2,850ms</text>
                <text x="12" y="71" fill="#fde047" font-size="10" font-family="monospace">&bull; Cosine Dist: 0.421 (&gt;0.35)</text>
                <text x="12" y="87" fill="#cbd5e1" font-size="10" font-family="monospace">&bull; Hang Lead: 22m (96.4%)</text>
              </g>

              <!-- NODE 2: ora-db-stv-01 (DATABASE) -->
              <g transform="translate(310, 45)">
                <rect width="170" height="100" rx="10" fill="#0f172a" stroke="#06b6d4" stroke-width="2.5" />
                <text x="12" y="22" fill="#22d3ee" font-size="11" font-weight="bold" font-family="monospace">ora-db-stv-01</text>
                <text x="12" y="38" fill="#94a3b8" font-size="10">Service: ora-lims-prod (DB)</text>
                <text x="12" y="55" fill="#fbbf24" font-size="10" font-family="monospace">&bull; Row Lock Wait: 1,420ms</text>
                <text x="12" y="71" fill="#67e8f9" font-size="10" font-family="monospace">&bull; Window: CHG0049281</text>
                <rect x="10" y="78" width="150" height="16" rx="4" fill="#083344" />
                <text x="85" y="89" text-anchor="middle" fill="#22d3ee" font-size="9" font-weight="bold" font-family="monospace">SUPPRESSED (0 PAGER NOISE)</text>
              </g>

              <!-- NODE 3: core-sw-lon-01 (NETWORK ROOT CAUSE) -->
              <g transform="translate(575, 40)">
                <rect width="170" height="110" rx="10" fill="#1e1b4b" stroke="#f43f5e" stroke-width="3" class="pulse-ring" />
                <text x="12" y="22" fill="#fb7185" font-size="11" font-weight="bold" font-family="monospace">core-sw-lon-01</text>
                <text x="12" y="38" fill="#fda4af" font-size="10">London Core Switch (Net)</text>
                <text x="12" y="55" fill="#f43f5e" font-size="10" font-weight="bold" font-family="monospace">&bull; OSPF Flaps: 19 flaps</text>
                <text x="12" y="71" fill="#f43f5e" font-size="10" font-weight="bold" font-family="monospace">&bull; Packet Loss: 14.8%</text>
                <text x="12" y="86" fill="#34d399" font-size="9.5" font-family="monospace">&bull; Lead Time: 42m (98.2%)</text>
                <rect x="8" y="92" width="154" height="14" rx="3" fill="#064e3b" />
                <text x="85" y="102" text-anchor="middle" fill="#6ee7b7" font-size="8.5" font-weight="bold" font-family="monospace">gsk-ano-network-ospf-reroute</text>
              </g>

              <!-- NODE 4: vm-stv-app-01 (COMPUTE VM) -->
              <g transform="translate(25, 205)">
                <rect width="180" height="72" rx="8" fill="#0f172a" stroke="#38bdf8" stroke-width="2" />
                <text x="12" y="20" fill="#38bdf8" font-size="11" font-weight="bold" font-family="monospace">vm-stv-app-01</text>
                <text x="12" y="36" fill="#cbd5e1" font-size="10" font-family="monospace">CPU: 65.2% (Normal) | 18.5 RPS</text>
                <text x="12" y="52" fill="#fca5a5" font-size="10" font-family="monospace">Threads: 142 | Sockets: 4,850</text>
                <text x="12" y="66" fill="#fde047" font-size="9.5" font-family="monospace">Act 2 Prediction: 22m Lead Time</text>
              </g>
            </svg>
          </div>
        </div>

        <!-- Live Presenter Talking Points Box -->
        <div class="mt-4 bg-slate-950/90 border border-slate-800 rounded-lg p-3">
          <div class="text-xs font-bold text-cyan-400 uppercase tracking-wider mb-1">Active Presenter Talking Points</div>
          <ul id="talking-points-list" class="text-xs text-slate-300 space-y-1.5 list-disc list-inside">
            <li><strong>Zero-Regex Novelty Detection (Act 1):</strong> 768-dim <code>text-embedding-005</code> + BigQuery <code>TREE_AH</code> Vector Search isolates novel XA distributed deadlock at <code>cosine_distance = 0.421 (&gt; 0.35)</code>.</li>
            <li><strong>Silent Hang Prediction (Act 2):</strong> Catches <code>vm-stv-app-01</code> thread starvation (<code>142</code>) &amp; socket exhaustion (<code>4,850</code>) <strong>22 minutes ahead</strong> (<code>96.4%</code> prob) while CPU sits at <strong>65.2%</strong>.</li>
            <li><strong>2-Hop Graph RCA (Act 3):</strong> Proves Tomcat HTTP 500s (<code>2,850ms</code>) and DB locks (<code>1,420ms</code>) stem from <code>core-sw-lon-01</code> OSPF flaps (<code>19 flaps</code>, <code>14.8% packet loss</code>, <code>98.2%</code> confidence).</li>
          </ul>
        </div>
      </section>

      <!-- PANEL 3: 768-DIM EMBEDDING & SEMANTIC OUTLIER EXPLORER (5 COLS) -->
      <section class="lg:col-span-5 bg-slate-900/90 border border-slate-800 rounded-xl p-5 flex flex-col justify-between space-y-4">
        <div>
          <div class="flex items-center justify-between mb-2">
            <h3 class="text-sm font-bold uppercase tracking-wider text-amber-400">
              Panel 3 — 768-Dim Embedding &amp; SQL Inspector
            </h3>
            <span class="px-2 py-0.5 rounded text-xs font-mono bg-amber-950 text-amber-300 border border-amber-800">
              OUTLIER DETECTED (ZERO REGEX)
            </span>
          </div>

          <!-- Novelty Cosine Distance Gauge -->
          <div class="bg-slate-950 border border-slate-800 rounded-lg p-3 mb-3">
            <div class="flex justify-between text-xs font-mono mb-1">
              <span class="text-slate-400">Semantic Cosine Distance (`text-embedding-005`)</span>
              <span id="cosine-gauge-label" class="text-amber-400 font-bold">0.421 &gt; 0.35 Threshold</span>
            </div>
            <div class="w-full h-3 bg-slate-800 rounded-full overflow-hidden relative">
              <div class="h-full bg-gradient-to-r from-emerald-500 via-amber-500 to-rose-500" style="width: 84%;"></div>
              <!-- Threshold marker at 0.35 (70% width) -->
              <div class="absolute top-0 bottom-0 left-[70%] w-0.5 bg-white"></div>
            </div>
            <div class="flex justify-between text-[10px] font-mono text-slate-400 mt-1">
              <span>0.00 (Identical)</span>
              <span>0.35 (Outlier Threshold)</span>
              <span>0.50+ (Novel Zero-Day)</span>
            </div>
          </div>

          <!-- Normalized Stack Trace Template -->
          <div class="mb-3">
            <div class="text-xs font-semibold text-slate-300 mb-1">
              Normalized Token Template (`LogNormalizer` — Stack Frame Preserved):
            </div>
            <pre id="normalized-template-box" class="bg-slate-950 border border-slate-800 rounded p-2.5 text-[11px] font-mono text-cyan-300 overflow-x-auto whitespace-pre-wrap leading-relaxed">&lt;TIMESTAMP&gt; ERROR [tomcat-app@tomcat-app-stv-01] org.apache.catalina.core.StandardWrapperValve.invoke: ORA-&lt;NUM&gt;: deadlock detected while waiting for resource at IP &lt;IP_PORT&gt; session &lt;UUID&gt;
  at oracle.jdbc.driver.T4CTTIoer11.processError(T4CTTIoer11.java:&lt;LINE&gt;)
  at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:&lt;LINE&gt;)</pre>
          </div>

          <!-- Live Verbatim BigQuery SQL Query Inspector -->
          <div>
            <div class="text-xs font-semibold text-slate-300 mb-1">
              Live Verbatim BigQuery SQL Query Inspector (`VECTOR_SEARCH` / `ML.PREDICT`):
            </div>
            <pre id="sql-query-box" class="bg-slate-950 border border-slate-800 rounded p-2.5 text-[11px] font-mono text-emerald-300 overflow-x-auto max-h-48 leading-relaxed">SELECT
  query.chunk_id AS incoming_chunk_id,
  query.host_id,
  ROUND(distance, 4) AS cosine_distance,
  IF(distance &gt; 0.35, 'OUTLIER DETECTED (ZERO REGEX)', 'NORMAL') AS status
FROM VECTOR_SEARCH(
  TABLE `gke-demos-363017.gsk_ano_ops.log_embeddings`,
  'embedding',
  (SELECT * FROM `gke-demos-363017.gsk_ano_ops.log_embeddings` WHERE host_id = 'tomcat-app-stv-01'),
  top_k =&gt; 3, distance_type =&gt; 'COSINE'
);</pre>
          </div>
        </div>
      </section>

    </div>

    <!-- PANEL 4: SERVICENOW CHANGE SUPPRESSION & EVENTARC SELF-HEALING LOG -->
    <section class="bg-slate-900/90 border border-slate-800 rounded-xl p-5">
      <div class="flex flex-wrap items-center justify-between gap-2 mb-4">
        <div>
          <h3 class="text-sm font-bold uppercase tracking-wider text-emerald-400">
            Panel 4 — ServiceNow Change-Window Noise Suppression (`ARIMA_PLUS_XREG`) &amp; Eventarc Self-Healing Audit Log
          </h3>
          <p class="text-xs text-slate-400">
            Contrasting Scheduled Maintenance (<span class="font-mono text-cyan-300">CHG0049281</span> on <span class="font-mono">ora-db-stv-01</span>) vs Unscheduled Network Failure (<span class="font-mono text-emerald-300">gsk-ano-network-ospf-reroute</span> on <span class="font-mono">core-sw-lon-01</span>)
          </p>
        </div>
        <div class="flex items-center gap-2 text-xs font-mono">
          <span class="px-2.5 py-1 rounded bg-cyan-950 text-cyan-300 border border-cyan-800">
            CHG0049281: STATUS: SUPPRESSED — ZERO PAGER NOISE
          </span>
          <span class="px-2.5 py-1 rounded bg-emerald-950 text-emerald-300 border border-emerald-800">
            Eventarc &rarr; gsk-ano-network-ospf-reroute: TRIGGERED
          </span>
        </div>
      </div>

      <div class="overflow-x-auto">
        <table class="w-full text-left border-collapse text-xs font-mono">
          <thead>
            <tr class="border-b border-slate-800 text-slate-400 bg-slate-950/60">
              <th class="py-2.5 px-3">INCIDENT_ID</th>
              <th class="py-2.5 px-3">CAPABILITY / MODEL</th>
              <th class="py-2.5 px-3">ROOT_CAUSE_ENTITY</th>
              <th class="py-2.5 px-3">LEAD / CONFIDENCE</th>
              <th class="py-2.5 px-3">SERVICENOW_WINDOW</th>
              <th class="py-2.5 px-3">AUTONOMOUS_REMEDIATION_STATUS</th>
            </tr>
          </thead>
          <tbody id="incidents-table-body" class="divide-y divide-slate-800/70">
            <tr class="bg-slate-950/30">
              <td class="py-2.5 px-3 text-cyan-300">inc-act4-chg0049281-suppressed</td>
              <td class="py-2.5 px-3">ARIMA_PLUS_XREG (Cap 3)</td>
              <td class="py-2.5 px-3 font-bold text-white">ora-db-stv-01</td>
              <td class="py-2.5 px-3">30m | 99.1%</td>
              <td class="py-2.5 px-3 text-cyan-300 font-bold">CHG0049281 (Active)</td>
              <td class="py-2.5 px-3">
                <span class="px-2 py-0.5 rounded bg-cyan-950 text-cyan-300 border border-cyan-700 font-bold">
                  STATUS: SUPPRESSED — ZERO PAGER NOISE
                </span>
              </td>
            </tr>
            <tr class="bg-slate-950/30">
              <td class="py-2.5 px-3 text-rose-300">inc-act4-ospf-reroute-triggered</td>
              <td class="py-2.5 px-3">BOOSTED_TREE_RCA (Cap 2)</td>
              <td class="py-2.5 px-3 font-bold text-rose-400">core-sw-lon-01</td>
              <td class="py-2.5 px-3">42m | 98.2%</td>
              <td class="py-2.5 px-3 text-slate-400">NONE (Unscheduled)</td>
              <td class="py-2.5 px-3">
                <span class="px-2 py-0.5 rounded bg-emerald-950 text-emerald-300 border border-emerald-700 font-bold">
                  STATUS: TRIGGERED — Eventarc &rarr; Cloud Workflow gsk-ano-network-ospf-reroute
                </span>
              </td>
            </tr>
            <tr class="bg-slate-950/30">
              <td class="py-2.5 px-3 text-amber-300">inc-act2-hang-predict-02</td>
              <td class="py-2.5 px-3">LOGISTIC_REG (Cap 1)</td>
              <td class="py-2.5 px-3 font-bold text-amber-300">tomcat-app-stv-01 / vm-stv-app-01</td>
              <td class="py-2.5 px-3">22m | 96.4%</td>
              <td class="py-2.5 px-3 text-slate-400">NONE (65.2% CPU)</td>
              <td class="py-2.5 px-3">
                <span class="px-2 py-0.5 rounded bg-amber-950 text-amber-300 border border-amber-700 font-bold">
                  PREDICTED 22M AHEAD (Thread Starvation: 142 | Sockets: 4,850)
                </span>
              </td>
            </tr>
            <tr class="bg-slate-950/30">
              <td class="py-2.5 px-3 text-purple-300">inc-act1-semantic-outlier-01</td>
              <td class="py-2.5 px-3">text-embedding-005 (TREE_AH)</td>
              <td class="py-2.5 px-3 font-bold text-purple-300">tomcat-app-stv-01</td>
              <td class="py-2.5 px-3">Cosine: 0.421 (&gt;0.35)</td>
              <td class="py-2.5 px-3 text-slate-400">NONE (Zero-Day XA)</td>
              <td class="py-2.5 px-3">
                <span class="px-2 py-0.5 rounded bg-purple-950 text-purple-300 border border-purple-700 font-bold">
                  OUTLIER DETECTED (ZERO REGEX)
                </span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

  </main>

  <script>
    async function triggerAct(actId) {
      try {
        const response = await fetch('/api/trigger_act/' + actId, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ act_id: actId })
        });
        const data = await response.json();
        if (data.status === 'SUCCESS') {
          document.getElementById('act-badge').textContent = 'ACT ' + data.act_id + ' EXECUTED';
          document.getElementById('act-title').textContent = data.title;
          document.getElementById('act-lead-time').textContent = 'Lead Time: ' + data.lead_time;
          if (data.sql_query) {
            document.getElementById('sql-query-box').textContent = data.sql_query;
          }
          if (data.talking_points && Array.isArray(data.talking_points)) {
            const ul = document.getElementById('talking-points-list');
            ul.innerHTML = data.talking_points.map(tp => `<li>${tp}</li>`).join('');
          }
        }
      } catch (err) {
        console.error('Error triggering Act:', err);
      }
    }
  </script>
</body>
</html>
"""


class DashboardAppState:
  """Thread-safe state container shared across HTTP requests."""

  def __init__(
      self,
      project_id: str = "gke-demos-363017",
      dataset_id: str = "gsk_ano_ops",
      mirror_path: Optional[str] = None,
  ) -> None:
    self.project_id = project_id
    self.dataset_id = dataset_id
    self.engine = DemoExecutionEngine(
        project_id=project_id,
        dataset_id=dataset_id,
        mode="auto",
        mirror_path=mirror_path,
    )
    self._lock = threading.Lock()
    self.active_act: int = 4
    self.acts_completed: list[int] = [1, 2, 3, 4]

    # Pre-seed all 4 acts so initial GET /api/incidents has rich data
    for act_num in [1, 2, 3, 4]:
      self.engine.run_act(act_num)

  def get_status_payload(self) -> dict[str, Any]:
    with self._lock:
      return {
          "status": "HEALTHY",
          "project_id": self.project_id,
          "dataset": self.dataset_id,
          "dataset_id": self.dataset_id,
          "execution_mode": self.engine.execution_mode,
          "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
          "kpis": {
              "monitored_vms": 12000,
              "monitored_vms_display": "12,000 VMs",
              "monitored_apps": 1500,
              "daily_events_billions": 71.2,
              "daily_events_display": "71.2B events/day",
              "daily_volume_tb": 4.8,
              "active_bq_footprint_tb": 52.0,
              "mttr_reduction_pct": 78.6,
              "mttr_reduction_display": "78.6% MTTR reduction",
              "mttr_before_mins": 112,
              "mttr_after_mins": 24,
              "change_outage_reduction_pct": 50.0,
              "annual_toil_saved_hours": 85500,
              "annual_toil_display": "85,500 hrs/yr toil saved",
          },
          "models": [
              "model_server_unresponsiveness",
              "model_cap1_server_unresponsiveness",
              "model_cross_domain_rca",
              "model_cap2_cross_domain_rca",
              "model_rolling_baseline",
              "model_cap3_rolling_baseline_arima",
          ],
          "tables": [
              "raw_logs",
              "log_embeddings",
              "gmp_metrics",
              "topology_edges",
              "change_calendar",
              "incidents_predictions",
          ],
          "active_act": self.active_act,
          "acts_completed": list(self.acts_completed),
      }

  def get_topology_payload(self) -> dict[str, Any]:
    return {
        "status": "SUCCESS",
        "project_id": self.project_id,
        "root_cause_entity_id": "core-sw-lon-01",
        "root_cause_domain": "NETWORK",
        "collapsed_symptom_alerts": 42,
        "nodes": [
            {
                "id": "tomcat-app-stv-01",
                "label": "tomcat-app-stv-01",
                "service": "gsk-lims-api",
                "domain": "APPLICATION",
                "site": "Stevenage (UK)",
                "status": "SYMPTOM_HTTP_500_AND_OUTLIER",
                "is_root_cause": False,
                "metrics": {
                    "apache_tomcat_latency_ms": 2850.0,
                    "cosine_distance": 0.421,
                    "cpu_utilization_pct": 65.2,
                    "throughput_rps": 18.5,
                    "thread_starvation_count": 142,
                    "socket_exhaustion_count": 4850,
                    "lead_time_minutes": 22,
                    "hang_probability_pct": 96.4,
                },
            },
            {
                "id": "vm-stv-app-01",
                "label": "vm-stv-app-01",
                "service": "gsk-lims-api-host",
                "domain": "COMPUTE",
                "site": "Stevenage (UK)",
                "status": "PREDICTED_UNRESPONSIVE_22M",
                "is_root_cause": False,
                "metrics": {
                    "cpu_utilization_pct": 65.2,
                    "thread_starvation_count": 142,
                    "socket_exhaustion_count": 4850,
                    "lead_time_minutes": 22,
                    "probability_pct": 96.4,
                },
            },
            {
                "id": "ora-db-stv-01",
                "label": "ora-db-stv-01",
                "service": "ora-lims-prod",
                "domain": "DATABASE",
                "site": "Stevenage (UK)",
                "status": "MAINTENANCE_SUPPRESSED",
                "is_root_cause": False,
                "active_change_id": "CHG0049281",
                "metrics": {
                    "db_lock_wait_ms": 1420.0,
                    "db_active_sessions": 168,
                    "suppressed_by_change_window": True,
                },
            },
            {
                "id": "core-sw-lon-01",
                "label": "core-sw-lon-01",
                "service": "lon-core-routing",
                "domain": "NETWORK",
                "site": "London (UK)",
                "status": "ROOT_CAUSE_CRITICAL",
                "is_root_cause": True,
                "remediation_workflow": "gsk-ano-network-ospf-reroute",
                "remediation_status": "TRIGGERED",
                "metrics": {
                    "ospf_flap_count": 19,
                    "packet_loss_pct": 14.8,
                    "lead_time_minutes": 42,
                    "confidence_pct": 98.2,
                },
            },
        ],
        "edges": [
            {
                "edge_id": "edge-stv-app-to-db-01",
                "source": "tomcat-app-stv-01",
                "target": "ora-db-stv-01",
                "relationship_type": "DEPENDS_ON",
                "criticality_weight": 0.95,
            },
            {
                "edge_id": "edge-stv-db-to-net-01",
                "source": "ora-db-stv-01",
                "target": "core-sw-lon-01",
                "relationship_type": "ROUTES_THROUGH",
                "criticality_weight": 0.90,
            },
            {
                "edge_id": "edge-stv-app-to-vm-01",
                "source": "tomcat-app-stv-01",
                "target": "vm-stv-app-01",
                "relationship_type": "HOSTED_ON",
                "criticality_weight": 0.98,
            },
        ],
    }

  def get_incidents_payload(self) -> dict[str, Any]:
    incidents: list[dict[str, Any]] = []
    try:
      conn = sqlite3.connect(str(self.engine.store_adapter.db_path))
      conn.row_factory = sqlite3.Row
      cursor = conn.cursor()
      cursor.execute(
          "SELECT * FROM incidents_predictions ORDER BY prediction_timestamp DESC"
      )
      for row in cursor.fetchall():
        incidents.append(dict(row))
      conn.close()
    except Exception:  # pylint: disable=broad-except
      pass

    # Guarantee rich default records if empty
    if not incidents:
      incidents = [
          {
              "incident_id": "inc-act4-ospf-reroute-triggered",
              "prediction_timestamp": "2026-09-15T21:20:00Z",
              "capability_type": "CAP2_CROSS_DOMAIN_30_60M",
              "affected_entity_id": "tomcat-app-stv-01",
              "root_cause_entity_id": "core-sw-lon-01",
              "root_cause_domain": "NETWORK",
              "lead_time_minutes": 42,
              "anomaly_probability": 0.982,
              "suppressed_by_change_window": False,
              "active_change_id": None,
              "recommended_action": "REROUTE_OSPF_TRAFFIC",
              "remediation_status": "TRIGGERED",
              "remediation_execution_id": (
                  f"projects/{self.project_id}/locations/europe-west2/workflows/"
                  "gsk-ano-network-ospf-reroute/executions/exec-live-9f82a1b4c3d2"
              ),
          },
          {
              "incident_id": "inc-act4-chg0049281-suppressed",
              "prediction_timestamp": "2026-09-15T21:18:00Z",
              "capability_type": "CAP3_DYNAMIC_BASELINE_ANOMALY",
              "affected_entity_id": "ora-db-stv-01",
              "root_cause_entity_id": "ora-db-stv-01",
              "root_cause_domain": "DATABASE",
              "lead_time_minutes": 30,
              "anomaly_probability": 0.991,
              "suppressed_by_change_window": True,
              "active_change_id": "CHG0049281",
              "recommended_action": "NO_ACTION_SUPPRESSED",
              "remediation_status": "SUPPRESSED",
              "remediation_execution_id": None,
          },
      ]

    return {
        "status": "SUCCESS",
        "project_id": self.project_id,
        "total_count": len(incidents),
        "incidents": incidents,
    }

  def trigger_act(self, act_id: int) -> dict[str, Any]:
    with self._lock:
      act_res = self.engine.run_act(act_id)
      self.active_act = act_id
      if act_id not in self.acts_completed:
        self.acts_completed.append(act_id)

    incidents_payload = self.get_incidents_payload()
    latest_inc = (
        incidents_payload["incidents"][0]
        if incidents_payload["incidents"]
        else {}
    )

    response_payload = {
        "status": "SUCCESS",
        "act_id": act_id,
        "title": act_res["title"],
        "capability": act_res["capability"],
        "lead_time": act_res["lead_time"],
        "execution_mode": act_res["execution_mode"],
        "sql_query": act_res["sql_query"],
        "results": act_res["results"],
        "table_headers": act_res["table_headers"],
        "table_rows": act_res["table_rows"],
        "talking_points": act_res["talking_points"],
        "kpi_impact": act_res["kpi_impact"],
        "incident": latest_inc,
        "topology": self.get_topology_payload(),
        "embedding_analysis": {
            "chunk_id": act_res["pipeline_artifacts"].get(
                "chunk_id", "c8f92a1b4e7d01928374655a9b8c7d6e"
            ),
            "host_id": "tomcat-app-stv-01",
            "service_name": "gsk-lims-api",
            "embedding_model": "text-embedding-005",
            "dimensions": 768,
            "cosine_distance": 0.421,
            "outlier_threshold": 0.35,
            "is_outlier": True,
            "detection_banner": "OUTLIER DETECTED (ZERO REGEX)",
        },
    }
    return response_payload


class DashboardRequestHandler(BaseHTTPRequestHandler):
  """HTTP request handler serving the Executive UI and REST API."""

  app_state: DashboardAppState

  def log_message(self, format_str: str, *args: Any) -> None:
    """Suppress noisy console logging during automated unit tests."""
    return

  def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, indent=2).encode("utf-8")
    self.send_response(status_code)
    self.send_header("Content-Type", "application/json; charset=utf-8")
    self.send_header("Content-Length", str(len(body)))
    self.send_header("Cache-Control", "no-store")
    self.end_headers()
    self.wfile.write(body)

  def _send_html(self, status_code: int, html_text: str) -> None:
    body = html_text.encode("utf-8")
    self.send_response(status_code)
    self.send_header("Content-Type", "text/html; charset=utf-8")
    self.send_header("Content-Length", str(len(body)))
    self.end_headers()
    self.wfile.write(body)

  def do_GET(self) -> None:  # pylint: disable=invalid-name
    parsed = urlparse(self.path)
    path = parsed.path.rstrip("/") or "/"

    if path == "/":
      self._send_html(200, DASHBOARD_HTML_TEMPLATE)
      return

    if path == "/api/status":
      self._send_json(200, self.app_state.get_status_payload())
      return

    if path == "/api/topology":
      self._send_json(200, self.app_state.get_topology_payload())
      return

    if path == "/api/incidents":
      self._send_json(200, self.app_state.get_incidents_payload())
      return

    if path.startswith("/api/trigger_act/"):
      act_str = path.split("/api/trigger_act/", 1)[1]
      self._handle_trigger_act(act_str)
      return

    self._send_json(404, {"status": "ERROR", "message": f"Not found: {path}"})

  def do_POST(self) -> None:  # pylint: disable=invalid-name
    content_len = int(self.headers.get("Content-Length", "0") or "0")
    if content_len > 0:
      _ = self.rfile.read(content_len)

    parsed = urlparse(self.path)
    path = parsed.path.rstrip("/")

    if path.startswith("/api/trigger_act/"):
      act_str = path.split("/api/trigger_act/", 1)[1]
      self._handle_trigger_act(act_str)
      return

    self._send_json(404, {"status": "ERROR", "message": f"Not found: {path}"})

  def _handle_trigger_act(self, act_str: str) -> None:
    try:
      if act_str == "all":
        for act_num in [1, 2, 3, 4]:
          res = self.app_state.trigger_act(act_num)
        self._send_json(200, res)
        return
      act_id = int(act_str)
      if act_id not in (1, 2, 3, 4):
        raise ValueError("act_id must be 1, 2, 3, or 4")
      res = self.app_state.trigger_act(act_id)
      self._send_json(200, res)
    except Exception as exc:  # pylint: disable=broad-except
      self._send_json(
          400,
          {"status": "ERROR", "message": f"Invalid act trigger request: {exc}"},
      )


def create_dashboard_server(
    host: str = "127.0.0.1",
    port: int = 0,
    project_id: str = "gke-demos-363017",
    mirror_path: Optional[str] = None,
) -> ThreadingHTTPServer:
  """Factory function creating and binding a ThreadingHTTPServer instance.

  Supports port=0 ephemeral OS port binding for clean programmatic unit testing.
  """
  state = DashboardAppState(
      project_id=project_id,
      dataset_id="gsk_ano_ops",
      mirror_path=mirror_path,
  )

  class BoundDashboardHandler(DashboardRequestHandler):
    app_state = state

  server = ThreadingHTTPServer((host, port), BoundDashboardHandler)
  return server


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
  parser = argparse.ArgumentParser(
      description="GSK Autonomous Operations (ANO) Interactive Executive Web UI Dashboard"
  )
  parser.add_argument(
      "--host",
      default="0.0.0.0",
      help="Host interface to bind (default: 0.0.0.0)",
  )
  parser.add_argument(
      "--port",
      type=int,
      default=8080,
      help="TCP port to listen on (default: 8080)",
  )
  parser.add_argument(
      "--project",
      default="gke-demos-363017",
      help="Target GCP Project ID (default: gke-demos-363017)",
  )
  parser.add_argument(
      "--mirror-path",
      default=None,
      help="Optional path to local SQLite analytical mirror database",
  )
  return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
  args = parse_args(argv)
  server = create_dashboard_server(
      host=args.host,
      port=args.port,
      project_id=args.project,
      mirror_path=args.mirror_path,
  )
  actual_host, actual_port = server.server_address
  print("=" * 88)
  print("GSK AUTONOMOUS OPERATIONS (ANO) — EXECUTIVE WEB UI DASHBOARD")
  print(f"Listening on: http://{actual_host}:{actual_port}")
  print(f"Target GCP Project: {args.project} | Dataset: gsk_ano_ops")
  print("Press Ctrl+C to stop the server.")
  print("=" * 88)
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    print("\nShutting down dashboard server...")
  finally:
    server.shutdown()
    server.server_close()
  return 0


if __name__ == "__main__":
  sys.exit(main())
