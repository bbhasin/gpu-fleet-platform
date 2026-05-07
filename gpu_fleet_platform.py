#!/usr/bin/env python3
"""
GPU Fleet Intelligence Platform — Single-File Edition
======================================================
Implements the platform described in the GPU Fleet Operations & Runbook doc:
  * 8 REST API namespaces  (/api/nodes, /api/gpus, /api/network, /api/storage,
                            /api/finops, /api/alerts, /api/reports, /api/jobs)
  * WebSocket push channel (/ws)
  * NVIDIA / Cisco / VAST collectors (mock-data driven so it runs anywhere)
  * Alert evaluator with the threshold table from Section 7.2
  * Embedded React-style HTML dashboard
  * SQLite persistence (no Postgres needed)
  * In-process cache (no Redis needed)

ONE-COMMAND INSTALL
-------------------
    python3 gpu_fleet_platform.py

The script bootstraps its own dependencies (fastapi, uvicorn, websockets) into
the running Python on first launch, then starts the platform on :8000.

Open http://localhost:8000  for the dashboard.
Open http://localhost:8000/docs  for the OpenAPI docs.
"""
from __future__ import annotations

import importlib
import os
import subprocess
import sys


# ---------------------------------------------------------------------------
# 0. Dependency bootstrap — makes "python3 gpu_fleet_platform.py" Just Work
# ---------------------------------------------------------------------------
REQUIRED = [
    ("fastapi", "fastapi>=0.104"),
    ("uvicorn", "uvicorn[standard]>=0.24"),
    ("pydantic", "pydantic>=2.4"),
    ("websockets", "websockets>=12.0"),
]


def _ensure_deps() -> None:
    missing = []
    for mod, spec in REQUIRED:
        try:
            importlib.import_module(mod)
        except ImportError:
            missing.append(spec)
    if missing:
        print(f"[bootstrap] Installing: {', '.join(missing)}", flush=True)
        cmd = [sys.executable, "-m", "pip", "install", "--quiet", *missing]
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError:
            # Try with --break-system-packages for PEP 668 environments
            cmd_alt = cmd + ["--break-system-packages"]
            subprocess.check_call(cmd_alt)


_ensure_deps()


# ---------------------------------------------------------------------------
# 1. Imports (post-bootstrap)
# ---------------------------------------------------------------------------
import asyncio
import json
import math
import random
import sqlite3
import statistics
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

import uvicorn


# ---------------------------------------------------------------------------
# 2. Configuration
# ---------------------------------------------------------------------------
@dataclass
class Settings:
    db_path: str = os.environ.get("GPU_FLEET_DB", "gpu_fleet.db")
    host: str = os.environ.get("HOST", "0.0.0.0")
    port: int = int(os.environ.get("PORT", "8000"))
    nvidia_interval: int = int(os.environ.get("COLLECTOR_INTERVAL_NVIDIA", "30"))
    cisco_interval: int = int(os.environ.get("COLLECTOR_INTERVAL_CISCO", "60"))
    vast_interval: int = int(os.environ.get("COLLECTOR_INTERVAL_VAST", "30"))
    alert_eval_interval: int = 30
    ws_broadcast_interval: int = 5
    metrics_retention_days: int = 90
    gpu_cost_per_hour_usd: float = float(os.environ.get("GPU_COST_PER_HOUR", "4.50"))
    # Fleet-shape — make a believable mock fleet
    nodes_per_cluster: int = 16
    gpus_per_node: int = 8


SETTINGS = Settings()


# ---------------------------------------------------------------------------
# 3. SQLite schema — replaces the PostgreSQL tables in the doc
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    node_id      TEXT PRIMARY KEY,
    hostname     TEXT NOT NULL,
    cluster      TEXT NOT NULL,
    chassis      TEXT NOT NULL,
    bcm_status   TEXT NOT NULL DEFAULT 'PRODUCTION',
    health       TEXT NOT NULL DEFAULT 'HEALTHY',
    psu_12v      REAL,
    fan_pct      REAL,
    last_seen    TEXT
);
CREATE TABLE IF NOT EXISTS gpus (
    gpu_uuid     TEXT PRIMARY KEY,
    node_id      TEXT NOT NULL,
    gpu_index    INTEGER NOT NULL,
    model        TEXT NOT NULL,
    serial       TEXT,
    FOREIGN KEY(node_id) REFERENCES nodes(node_id)
);
CREATE TABLE IF NOT EXISTS gpu_metrics (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    gpu_uuid     TEXT NOT NULL,
    ts           TEXT NOT NULL,
    temp_c       REAL,
    util_sm      REAL,
    util_mem     REAL,
    mem_used_gb  REAL,
    power_w      REAL,
    sm_clock_mhz INTEGER,
    ecc_sbe_vol  INTEGER,
    ecc_dbe_vol  INTEGER,
    ecc_sbe_agg  INTEGER,
    ecc_dbe_agg  INTEGER,
    retired_dbe  INTEGER,
    nvlink_bw_gbs REAL
);
CREATE INDEX IF NOT EXISTS idx_gpu_metrics_uuid_ts ON gpu_metrics(gpu_uuid, ts);

CREATE TABLE IF NOT EXISTS network_ports (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    switch       TEXT NOT NULL,
    port         TEXT NOT NULL,
    peer_node    TEXT,
    tx_dbm       REAL,
    rx_dbm       REAL,
    fec_corr_per_s REAL,
    pfc_enabled  INTEGER,
    ecn_enabled  INTEGER,
    mtu          INTEGER,
    state        TEXT,
    ts           TEXT,
    UNIQUE(switch, port)
);

CREATE TABLE IF NOT EXISTS storage_metrics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              TEXT NOT NULL,
    cluster         TEXT NOT NULL,
    read_gbs        REAL,
    write_gbs       REAL,
    iops            INTEGER,
    p99_read_ms     REAL,
    p99_write_ms    REAL,
    capacity_pct    REAL,
    gds_sessions    INTEGER,
    cnode_count     INTEGER
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id     TEXT PRIMARY KEY,
    type         TEXT NOT NULL,
    severity     TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'OPEN',
    source       TEXT NOT NULL,
    summary      TEXT NOT NULL,
    details_json TEXT,
    created_at   TEXT NOT NULL,
    acknowledged_at TEXT,
    resolved_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);

CREATE TABLE IF NOT EXISTS jobs (
    job_id       TEXT PRIMARY KEY,
    scheduler    TEXT NOT NULL,        -- slurm / runai / k8s
    user         TEXT NOT NULL,
    team         TEXT NOT NULL,
    state        TEXT NOT NULL,
    gpu_count    INTEGER NOT NULL,
    nodes        TEXT NOT NULL,
    started_at   TEXT NOT NULL,
    ended_at     TEXT,
    util_avg     REAL
);

CREATE TABLE IF NOT EXISTS reports (
    report_id    TEXT PRIMARY KEY,
    type         TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    body         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS diagnostics (
    test_id     TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    status      TEXT NOT NULL,
    target      TEXT NOT NULL,
    params      TEXT,
    result      TEXT,
    started_at  TEXT NOT NULL,
    ended_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_diag_started ON diagnostics(started_at);
"""


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(SETTINGS.db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# 4. Reference tables (XID + alert thresholds, Section 7 of the doc)
# ---------------------------------------------------------------------------
XID_REFERENCE: dict[int, dict[str, str]] = {
    8:   {"name": "GPU stopped processing",      "severity": "CRITICAL"},
    31:  {"name": "GPU memory page fault",       "severity": "WARNING"},
    48:  {"name": "Double-bit ECC (L2)",         "severity": "CRITICAL"},
    56:  {"name": "Display MMU fault",           "severity": "WARNING"},
    63:  {"name": "ECC page retirement (SBE)",   "severity": "WARNING"},
    74:  {"name": "NVLink error",                "severity": "WARNING"},
    79:  {"name": "GPU memory DBE",              "severity": "CRITICAL"},
    92:  {"name": "High single-bit ECC",         "severity": "WARNING"},
    94:  {"name": "Contained ECC error",         "severity": "WARNING"},
    95:  {"name": "Uncontained ECC error",       "severity": "CRITICAL"},
    119: {"name": "GSP RPC timeout",             "severity": "WARNING"},
}

ALERT_THRESHOLDS = [
    {"type": "GPU_TEMP_WARN",         "severity": "WARNING",  "auto_resolve": True},
    {"type": "GPU_TEMP_CRITICAL",     "severity": "CRITICAL", "auto_resolve": True},
    {"type": "GPU_TEMP_THROTTLE",     "severity": "WARNING",  "auto_resolve": True},
    {"type": "GPU_ECC_SBE_RATE",      "severity": "WARNING",  "auto_resolve": False},
    {"type": "GPU_ECC_DBE",           "severity": "CRITICAL", "auto_resolve": False},
    {"type": "GPU_IDLE_ALLOCATED",    "severity": "INFO",     "auto_resolve": True},
    {"type": "PSU_VOLTAGE_WARN",      "severity": "WARNING",  "auto_resolve": True},
    {"type": "PSU_VOLTAGE_CRITICAL",  "severity": "CRITICAL", "auto_resolve": False},
    {"type": "FAN_SPEED_WARN",        "severity": "WARNING",  "auto_resolve": False},
    {"type": "FAN_REVERSED",          "severity": "CRITICAL", "auto_resolve": False},
    {"type": "NIC_FEC_HIGH",          "severity": "WARNING",  "auto_resolve": True},
    {"type": "ROCE_PFC_MISSING",      "severity": "CRITICAL", "auto_resolve": False},
    {"type": "TRANSCEIVER_RX_LOW",    "severity": "WARNING",  "auto_resolve": False},
    {"type": "STORAGE_CAPACITY_WARN", "severity": "WARNING",  "auto_resolve": True},
    {"type": "STORAGE_CAPACITY_CRIT", "severity": "CRITICAL", "auto_resolve": True},
    {"type": "STORAGE_LATENCY_HIGH",  "severity": "WARNING",  "auto_resolve": True},
    {"type": "GDS_NOT_ADOPTED",       "severity": "INFO",     "auto_resolve": False},
    {"type": "NCCL_BW_DEGRADED",      "severity": "WARNING",  "auto_resolve": True},
]


# ---------------------------------------------------------------------------
# 5. In-process cache (Redis stand-in)
# ---------------------------------------------------------------------------
class MetricCache:
    """gpu_metrics:{uuid} -> latest snapshot, expires after 120s."""
    def __init__(self) -> None:
        self._store: dict[str, tuple[float, dict]] = {}
        self._lock = asyncio.Lock()

    async def set(self, key: str, value: dict, ttl: int = 120) -> None:
        async with self._lock:
            self._store[key] = (time.time() + ttl, value)

    async def get(self, key: str) -> Optional[dict]:
        async with self._lock:
            entry = self._store.get(key)
            if not entry:
                return None
            expires, value = entry
            if time.time() > expires:
                self._store.pop(key, None)
                return None
            return value

    async def all(self, prefix: str) -> list[dict]:
        async with self._lock:
            now = time.time()
            return [v for k, (exp, v) in self._store.items()
                    if k.startswith(prefix) and exp > now]


CACHE = MetricCache()


# ---------------------------------------------------------------------------
# 6. WebSocket connection manager
# ---------------------------------------------------------------------------
class WSManager:
    def __init__(self) -> None:
        self.active: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.active.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        self.active.discard(ws)

    async def broadcast(self, msg: dict) -> None:
        if not self.active:
            return
        dead: list[WebSocket] = []
        payload = json.dumps(msg, default=str)
        for ws in list(self.active):
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.active.discard(ws)


WS = WSManager()


# ---------------------------------------------------------------------------
# 7. Mock fleet bootstrap — populates an entire cluster on first run
# ---------------------------------------------------------------------------
TEAMS = ["llm-research", "vision-team", "speech", "robotics", "infra-shared", "biotech-r&d"]
CLUSTER = "phoenix-prod-1"


def init_db() -> None:
    conn = db()
    try:
        conn.executescript(SCHEMA)
        cur = conn.execute("SELECT COUNT(*) AS c FROM nodes")
        if cur.fetchone()["c"] == 0:
            seed_fleet(conn)
        # default cost per gpu-hour
        conn.execute(
            "INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)",
            ("gpu_cost_per_hour_usd", str(SETTINGS.gpu_cost_per_hour_usd)),
        )
        conn.commit()
    finally:
        conn.close()


def seed_fleet(conn: sqlite3.Connection) -> None:
    print(f"[seed] Creating mock fleet: {SETTINGS.nodes_per_cluster} nodes × "
          f"{SETTINGS.gpus_per_node} H100 GPUs", flush=True)
    now = utcnow_iso()
    for i in range(SETTINGS.nodes_per_cluster):
        node_id = f"node-{i:03d}"
        hostname = f"phx-h100-{i:03d}"
        chassis = f"rack-{i // 4}"
        psu_12v = round(random.uniform(11.95, 12.05), 2)
        # one node intentionally degraded so the runbooks have something to fire on
        if i == 3:
            psu_12v = 11.72  # WARN
        if i == 7:
            psu_12v = 11.55  # CRITICAL
        conn.execute(
            """INSERT INTO nodes(node_id, hostname, cluster, chassis, bcm_status,
                                 health, psu_12v, fan_pct, last_seen)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (node_id, hostname, CLUSTER, chassis, "PRODUCTION", "HEALTHY",
             psu_12v, round(random.uniform(0.85, 0.95), 2), now),
        )
        for g in range(SETTINGS.gpus_per_node):
            gpu_uuid = f"GPU-{uuid.uuid4()}"
            conn.execute(
                """INSERT INTO gpus(gpu_uuid, node_id, gpu_index, model, serial)
                   VALUES (?,?,?,?,?)""",
                (gpu_uuid, node_id, g, "NVIDIA H100 SXM5",
                 f"SN-{i:03d}-{g}-{random.randint(10000,99999)}"),
            )

    # Network: 2 ToRs + 1 Spine, mostly healthy with a couple of degraded ports
    for sw_idx, sw in enumerate(["nexus-tor-01", "nexus-tor-02"]):
        for p in range(1, 33):
            tx = round(random.uniform(-0.5, 1.0), 2)
            rx = round(random.uniform(-3.5, -1.0), 2)
            fec = round(random.uniform(0, 50), 1)
            pfc = 1
            ecn = 1
            mtu = 9216
            # inject a marginal port
            if sw_idx == 0 and p == 12:
                rx = -10.6
                fec = 1450.0
            # inject a misconfigured port
            if sw_idx == 1 and p == 4:
                pfc = 0
            conn.execute(
                """INSERT INTO network_ports(switch, port, peer_node, tx_dbm, rx_dbm,
                                             fec_corr_per_s, pfc_enabled, ecn_enabled,
                                             mtu, state, ts)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (sw, f"Eth1/{p}", f"node-{(sw_idx*32+p-1) % SETTINGS.nodes_per_cluster:03d}",
                 tx, rx, fec, pfc, ecn, mtu, "up", now),
            )

    # Initial storage snapshot
    conn.execute(
        """INSERT INTO storage_metrics(ts, cluster, read_gbs, write_gbs, iops,
                                       p99_read_ms, p99_write_ms, capacity_pct,
                                       gds_sessions, cnode_count)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (now, "vast-c1", 142.6, 38.4, 850_000, 0.42, 0.91, 0.61, 96, 4),
    )

    # Seed jobs spread across teams
    states = ["RUNNING"] * 6 + ["COMPLETED"] * 4 + ["PENDING"] * 2
    for k in range(12):
        team = random.choice(TEAMS)
        gpu_count = random.choice([8, 16, 32, 64, 128])
        node_sample = random.sample(range(SETTINGS.nodes_per_cluster), max(1, gpu_count // 8))
        nodes_csv = ",".join(f"node-{n:03d}" for n in node_sample)
        state = states[k]
        started_dt = datetime.now(timezone.utc) - timedelta(hours=random.randint(1, 72))
        ended = None
        if state == "COMPLETED":
            ended = (started_dt + timedelta(hours=random.randint(2, 24))).isoformat(timespec="seconds")
        util_avg = round(random.uniform(0.05, 0.92), 2)
        conn.execute(
            """INSERT INTO jobs(job_id, scheduler, user, team, state, gpu_count,
                                nodes, started_at, ended_at, util_avg)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (f"job-{k:05d}", random.choice(["slurm", "runai", "k8s"]),
             f"user{k}", team, state, gpu_count, nodes_csv,
             started_dt.isoformat(timespec="seconds"), ended, util_avg),
        )


# ---------------------------------------------------------------------------
# 8. Collectors — the 3 modules from Section 3 of the doc
# ---------------------------------------------------------------------------
class NvidiaCollector:
    """Section 3.1 — DCGM / nvidia-smi collector (mock implementation)."""
    def __init__(self) -> None:
        self.cycle = 0
        self.ecc_drift: dict[str, int] = {}

    async def run_once(self) -> int:
        self.cycle += 1
        ts = utcnow_iso()
        conn = db()
        try:
            gpus = conn.execute(
                "SELECT g.gpu_uuid, g.node_id, g.gpu_index, n.psu_12v, n.bcm_status "
                "FROM gpus g JOIN nodes n ON n.node_id = g.node_id "
                "WHERE n.bcm_status != 'MAINTENANCE'"
            ).fetchall()

            rows: list[tuple] = []
            updates_for_ws: list[dict] = []

            for g in gpus:
                # base operating profile
                load = max(0.0, min(1.0, random.gauss(0.78, 0.18)))
                psu_penalty = max(0.0, (12.0 - g["psu_12v"]) * 0.05)
                temp = 55 + load * 30 + random.uniform(-2, 2)
                if psu_penalty > 0:
                    temp += 4 + random.uniform(-1, 1)

                util_sm = load * 100
                util_mem = max(0.0, util_sm + random.uniform(-15, 15))
                mem_used = round(load * 78 + random.uniform(-5, 5), 1)
                power = round(load * 700 + random.uniform(-30, 30), 1)
                sm_clock = int(1980 - psu_penalty * 4000)  # throttle when PSU sags
                if temp > 88:
                    sm_clock = int(sm_clock * 0.9)

                # ECC simulation — counters mostly flat, slow drift on a few GPUs
                key = g["gpu_uuid"]
                drift = self.ecc_drift.setdefault(key, 0)
                if random.random() < 0.02:
                    drift += random.randint(1, 4)
                    self.ecc_drift[key] = drift
                ecc_sbe_vol = drift
                ecc_dbe_vol = 0
                # one GPU on the bad-PSU node accumulates uncorrectable errors
                if g["node_id"] == "node-7" and g["gpu_index"] == 0 and self.cycle > 2:
                    ecc_dbe_vol = 1
                ecc_sbe_agg = drift + random.randint(0, 3)
                ecc_dbe_agg = ecc_dbe_vol
                retired_dbe = ecc_dbe_agg
                nvlink_bw = round(random.uniform(820, 895), 1)

                rows.append((g["gpu_uuid"], ts, temp, util_sm, util_mem, mem_used,
                             power, sm_clock, ecc_sbe_vol, ecc_dbe_vol,
                             ecc_sbe_agg, ecc_dbe_agg, retired_dbe, nvlink_bw))

                snapshot = {
                    "gpu_uuid": g["gpu_uuid"],
                    "node_id": g["node_id"],
                    "gpu_index": g["gpu_index"],
                    "ts": ts,
                    "temp_c": round(temp, 1),
                    "util_sm": round(util_sm, 1),
                    "util_mem": round(util_mem, 1),
                    "mem_used_gb": mem_used,
                    "power_w": power,
                    "sm_clock_mhz": sm_clock,
                    "ecc_sbe_vol": ecc_sbe_vol,
                    "ecc_dbe_vol": ecc_dbe_vol,
                    "ecc_sbe_agg": ecc_sbe_agg,
                    "ecc_dbe_agg": ecc_dbe_agg,
                    "nvlink_bw_gbs": nvlink_bw,
                }
                await CACHE.set(f"gpu_metrics:{g['gpu_uuid']}", snapshot)
                updates_for_ws.append(snapshot)

            conn.executemany(
                """INSERT INTO gpu_metrics(gpu_uuid, ts, temp_c, util_sm, util_mem,
                                           mem_used_gb, power_w, sm_clock_mhz,
                                           ecc_sbe_vol, ecc_dbe_vol, ecc_sbe_agg,
                                           ecc_dbe_agg, retired_dbe, nvlink_bw_gbs)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                rows,
            )
            conn.execute("UPDATE nodes SET last_seen=? WHERE bcm_status!='MAINTENANCE'", (ts,))
            conn.commit()
            await WS.broadcast({"event": "gpu_metrics_update",
                                "count": len(updates_for_ws),
                                "ts": ts})
            return len(rows)
        finally:
            conn.close()


class CiscoCollector:
    """Section 3.2 — UCSM + NXAPI collector (mock)."""
    async def run_once(self) -> int:
        ts = utcnow_iso()
        conn = db()
        try:
            ports = conn.execute("SELECT * FROM network_ports").fetchall()
            updates = 0
            for p in ports:
                # gentle wobble around current values
                tx = round(p["tx_dbm"] + random.uniform(-0.05, 0.05), 2)
                rx = round(p["rx_dbm"] + random.uniform(-0.05, 0.05), 2)
                fec = max(0.0, p["fec_corr_per_s"] + random.uniform(-2, 2))
                conn.execute(
                    """UPDATE network_ports
                       SET tx_dbm=?, rx_dbm=?, fec_corr_per_s=?, ts=?
                       WHERE switch=? AND port=?""",
                    (tx, rx, fec, ts, p["switch"], p["port"]),
                )
                updates += 1
            # PSU/fan jitter on nodes
            nodes = conn.execute("SELECT node_id, psu_12v, fan_pct FROM nodes").fetchall()
            for n in nodes:
                new_psu = round(n["psu_12v"] + random.uniform(-0.01, 0.01), 2)
                new_fan = round(min(1.0, max(0.6, n["fan_pct"] + random.uniform(-0.02, 0.02))), 2)
                conn.execute("UPDATE nodes SET psu_12v=?, fan_pct=? WHERE node_id=?",
                             (new_psu, new_fan, n["node_id"]))
            conn.commit()
            return updates
        finally:
            conn.close()


class VastCollector:
    """Section 3.3 — VAST Data collector (mock)."""
    async def run_once(self) -> int:
        ts = utcnow_iso()
        conn = db()
        try:
            last = conn.execute(
                "SELECT * FROM storage_metrics ORDER BY id DESC LIMIT 1"
            ).fetchone()
            base_read = last["read_gbs"] if last else 140.0
            base_write = last["write_gbs"] if last else 35.0
            cap = last["capacity_pct"] if last else 0.6
            cap = min(0.97, cap + random.uniform(0, 0.0009))  # slow capacity climb

            read = max(50.0, base_read + random.gauss(0, 6))
            write = max(10.0, base_write + random.gauss(0, 3))
            conn.execute(
                """INSERT INTO storage_metrics(ts, cluster, read_gbs, write_gbs, iops,
                                               p99_read_ms, p99_write_ms,
                                               capacity_pct, gds_sessions, cnode_count)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (ts, "vast-c1", round(read, 1), round(write, 1),
                 int(800_000 + random.uniform(-50_000, 80_000)),
                 round(random.uniform(0.3, 0.9), 2),
                 round(random.uniform(0.5, 1.5), 2),
                 round(cap, 4),
                 random.choice([0, 0, 0, 64, 96, 128]),  # GDS adoption variance
                 4),
            )
            conn.commit()
            return 1
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# 9. Alert evaluator — encodes the threshold table from Section 7.2
# ---------------------------------------------------------------------------
class AlertEvaluator:
    async def run_once(self) -> int:
        created = 0
        conn = db()
        try:
            # ---- GPU thresholds (read from cache to mirror prod design) ----
            snapshots = await CACHE.all("gpu_metrics:")
            for s in snapshots:
                if s["temp_c"] > 88:
                    created += self._upsert(conn, "GPU_TEMP_CRITICAL", "CRITICAL",
                                            f"gpu:{s['gpu_uuid']}",
                                            f"GPU {s['node_id']}/{s['gpu_index']} temp {s['temp_c']}°C",
                                            s)
                elif s["temp_c"] > 82:
                    created += self._upsert(conn, "GPU_TEMP_WARN", "WARNING",
                                            f"gpu:{s['gpu_uuid']}",
                                            f"GPU {s['node_id']}/{s['gpu_index']} temp {s['temp_c']}°C",
                                            s)
                if s["sm_clock_mhz"] < 1881:  # 95% of 1980 base
                    created += self._upsert(conn, "GPU_TEMP_THROTTLE", "WARNING",
                                            f"gpu:{s['gpu_uuid']}:throttle",
                                            f"GPU {s['node_id']}/{s['gpu_index']} SM clock throttled to {s['sm_clock_mhz']} MHz",
                                            s)
                if s["ecc_dbe_vol"] > 0:
                    created += self._upsert(conn, "GPU_ECC_DBE", "CRITICAL",
                                            f"gpu:{s['gpu_uuid']}:dbe",
                                            f"XID 79 — uncorrectable ECC on {s['node_id']}/GPU{s['gpu_index']}",
                                            s)

            # ---- PSU thresholds from the doc ----
            for n in conn.execute("SELECT * FROM nodes").fetchall():
                if n["psu_12v"] is None:
                    continue
                if n["psu_12v"] < 11.6:
                    self._upsert(conn, "PSU_VOLTAGE_CRITICAL", "CRITICAL",
                                 f"node:{n['node_id']}:psu",
                                 f"Node {n['hostname']} 12V rail {n['psu_12v']}V (CRIT < 11.6)",
                                 dict(n))
                elif n["psu_12v"] < 11.8:
                    self._upsert(conn, "PSU_VOLTAGE_WARN", "WARNING",
                                 f"node:{n['node_id']}:psu",
                                 f"Node {n['hostname']} 12V rail {n['psu_12v']}V (WARN < 11.8)",
                                 dict(n))

            # ---- Network port checks ----
            for p in conn.execute("SELECT * FROM network_ports").fetchall():
                if p["pfc_enabled"] == 0:
                    self._upsert(conn, "ROCE_PFC_MISSING", "CRITICAL",
                                 f"port:{p['switch']}:{p['port']}",
                                 f"PFC disabled on {p['switch']} {p['port']}",
                                 dict(p))
                if p["rx_dbm"] is not None and p["rx_dbm"] < -10:
                    self._upsert(conn, "TRANSCEIVER_RX_LOW", "WARNING",
                                 f"port:{p['switch']}:{p['port']}:rx",
                                 f"RX power {p['rx_dbm']} dBm on {p['switch']} {p['port']}",
                                 dict(p))
                if p["fec_corr_per_s"] and p["fec_corr_per_s"] > 1000:
                    self._upsert(conn, "NIC_FEC_HIGH", "WARNING",
                                 f"port:{p['switch']}:{p['port']}:fec",
                                 f"High FEC corrections {p['fec_corr_per_s']:.0f}/s on {p['switch']} {p['port']}",
                                 dict(p))

            # ---- Storage capacity / GDS adoption ----
            sm = conn.execute("SELECT * FROM storage_metrics ORDER BY id DESC LIMIT 1").fetchone()
            if sm:
                if sm["capacity_pct"] > 0.9:
                    self._upsert(conn, "STORAGE_CAPACITY_CRIT", "CRITICAL",
                                 f"storage:{sm['cluster']}:cap",
                                 f"VAST {sm['cluster']} {sm['capacity_pct']*100:.1f}% full",
                                 dict(sm))
                elif sm["capacity_pct"] > 0.8:
                    self._upsert(conn, "STORAGE_CAPACITY_WARN", "WARNING",
                                 f"storage:{sm['cluster']}:cap",
                                 f"VAST {sm['cluster']} {sm['capacity_pct']*100:.1f}% full",
                                 dict(sm))
                if sm["gds_sessions"] == 0:
                    self._upsert(conn, "GDS_NOT_ADOPTED", "INFO",
                                 f"storage:{sm['cluster']}:gds",
                                 "GPUDirect Storage available but 0 sessions",
                                 dict(sm))

            # ---- Auto-resolve cleared alerts ----
            self._auto_resolve(conn, snapshots)

            conn.commit()
            return created
        finally:
            conn.close()

    def _upsert(self, conn: sqlite3.Connection, alert_type: str, severity: str,
                source: str, summary: str, details: dict[str, Any]) -> int:
        existing = conn.execute(
            "SELECT alert_id FROM alerts WHERE source=? AND status IN ('OPEN','ACKNOWLEDGED')",
            (source,),
        ).fetchone()
        if existing:
            return 0
        alert_id = f"alert-{uuid.uuid4().hex[:10]}"
        conn.execute(
            """INSERT INTO alerts(alert_id, type, severity, status, source, summary,
                                  details_json, created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (alert_id, alert_type, severity, "OPEN", source, summary,
             json.dumps(details, default=str), utcnow_iso()),
        )
        # Fire-and-forget WS broadcast
        asyncio.create_task(WS.broadcast({
            "event": "alert_new",
            "alert_id": alert_id,
            "type": alert_type,
            "severity": severity,
            "summary": summary,
            "source": source,
            "ts": utcnow_iso(),
        }))
        return 1

    def _auto_resolve(self, conn: sqlite3.Connection, snapshots: list[dict]) -> None:
        # Map gpu_uuid -> latest snapshot
        by_uuid = {s["gpu_uuid"]: s for s in snapshots}
        rows = conn.execute(
            "SELECT * FROM alerts WHERE status IN ('OPEN','ACKNOWLEDGED')"
        ).fetchall()
        for a in rows:
            entry = next((t for t in ALERT_THRESHOLDS if t["type"] == a["type"]), None)
            if not entry or not entry["auto_resolve"]:
                continue
            cleared = False
            if a["type"] in ("GPU_TEMP_WARN", "GPU_TEMP_CRITICAL"):
                src = a["source"].split(":", 1)[1]
                snap = by_uuid.get(src)
                if snap and snap["temp_c"] < (78 if a["type"] == "GPU_TEMP_WARN" else 84):
                    cleared = True
            if a["type"] == "GPU_TEMP_THROTTLE":
                src = a["source"].split(":", 1)[1].split(":")[0]
                snap = by_uuid.get(src)
                if snap and snap["sm_clock_mhz"] >= 1881:
                    cleared = True
            if cleared:
                conn.execute(
                    "UPDATE alerts SET status='RESOLVED', resolved_at=? WHERE alert_id=?",
                    (utcnow_iso(), a["alert_id"]),
                )
                asyncio.create_task(WS.broadcast({
                    "event": "alert_resolved",
                    "alert_id": a["alert_id"],
                    "ts": utcnow_iso(),
                }))


# ---------------------------------------------------------------------------
# 10. Background scheduler
# ---------------------------------------------------------------------------
async def scheduler(nvidia: NvidiaCollector, cisco: CiscoCollector,
                    vast: VastCollector, alerts: AlertEvaluator) -> None:
    counters = {"nvidia": 0, "cisco": 0, "vast": 0, "alerts": 0, "ws": 0}

    async def _loop(name: str, fn, interval: int) -> None:
        while True:
            try:
                await fn()
                counters[name] += 1
            except Exception as exc:  # pragma: no cover
                print(f"[scheduler:{name}] error: {exc}", flush=True)
            await asyncio.sleep(interval)

    async def _ws_loop() -> None:
        while True:
            await asyncio.sleep(SETTINGS.ws_broadcast_interval)
            counters["ws"] += 1
            await WS.broadcast({"event": "heartbeat", "counters": counters,
                                "ts": utcnow_iso()})

    await asyncio.gather(
        _loop("nvidia", nvidia.run_once, SETTINGS.nvidia_interval),
        _loop("cisco",  cisco.run_once,  SETTINGS.cisco_interval),
        _loop("vast",   vast.run_once,   SETTINGS.vast_interval),
        _loop("alerts", alerts.run_once, SETTINGS.alert_eval_interval),
        _ws_loop(),
    )


# ---------------------------------------------------------------------------
# 11. FastAPI app + lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    nvidia = NvidiaCollector()
    cisco  = CiscoCollector()
    vast   = VastCollector()
    alerts = AlertEvaluator()

    # Run an initial collection synchronously so the dashboard has data on first paint
    await nvidia.run_once()
    await cisco.run_once()
    await vast.run_once()
    await alerts.run_once()

    task = asyncio.create_task(scheduler(nvidia, cisco, vast, alerts))
    print(f"[ready] http://{SETTINGS.host}:{SETTINGS.port}  (dashboard)", flush=True)
    print(f"[ready] http://{SETTINGS.host}:{SETTINGS.port}/docs  (OpenAPI)", flush=True)
    yield
    task.cancel()


app = FastAPI(title="GPU Fleet Intelligence Platform",
              version="1.0.0",
              lifespan=lifespan)


# ---------------------------------------------------------------------------
# 12. /api/nodes
# ---------------------------------------------------------------------------
@app.get("/api/nodes")
def list_nodes():
    conn = db()
    try:
        rows = conn.execute("SELECT * FROM nodes ORDER BY node_id").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/api/nodes/{node_id}")
def get_node(node_id: str):
    conn = db()
    try:
        row = conn.execute("SELECT * FROM nodes WHERE node_id=?", (node_id,)).fetchone()
        if not row:
            raise HTTPException(404, "node not found")
        gpus = conn.execute(
            "SELECT * FROM gpus WHERE node_id=? ORDER BY gpu_index", (node_id,)
        ).fetchall()
        return {"node": dict(row), "gpus": [dict(g) for g in gpus]}
    finally:
        conn.close()


@app.post("/api/nodes/{node_id}/diagnostic")
async def run_diagnostic(node_id: str, level: int = 1):
    if level not in (1, 2, 3, 4):
        raise HTTPException(400, "level must be 1-4")
    conn = db()
    try:
        if not conn.execute("SELECT 1 FROM nodes WHERE node_id=?", (node_id,)).fetchone():
            raise HTTPException(404, "node not found")
    finally:
        conn.close()
    # Simulate: longer levels take more "time"
    return {"node_id": node_id, "level": level,
            "status": "QUEUED",
            "estimated_seconds": {1: 30, 2: 90, 3: 240, 4: 600}[level],
            "task_id": f"diag-{uuid.uuid4().hex[:8]}"}


@app.post("/api/nodes/{node_id}/bug-report")
async def bug_report(node_id: str):
    conn = db()
    try:
        if not conn.execute("SELECT 1 FROM nodes WHERE node_id=?", (node_id,)).fetchone():
            raise HTTPException(404, "node not found")
    finally:
        conn.close()
    fake_path = f"/reports/bug-reports/nvidia-bug-report-{node_id}-{int(time.time())}.gz"
    return {"node_id": node_id, "status": "RUNNING",
            "download_url_when_ready": fake_path,
            "task_id": f"bugrpt-{uuid.uuid4().hex[:8]}"}


# ---------------------------------------------------------------------------
# 13. /api/gpus
# ---------------------------------------------------------------------------
@app.get("/api/gpus")
async def list_gpus():
    snaps = await CACHE.all("gpu_metrics:")
    snaps.sort(key=lambda s: (s["node_id"], s["gpu_index"]))
    return snaps


@app.get("/api/gpus/at-risk")
def at_risk_gpus():
    conn = db()
    try:
        # ECC trend: GPUs whose aggregate SBE has grown in the last 60 samples
        rows = conn.execute("""
            SELECT gpu_uuid,
                   MAX(ecc_sbe_agg) - MIN(ecc_sbe_agg) AS sbe_growth,
                   AVG(temp_c) AS avg_temp,
                   MAX(ecc_dbe_agg) AS max_dbe
            FROM gpu_metrics
            WHERE id > (SELECT MAX(id) - 1000 FROM gpu_metrics)
            GROUP BY gpu_uuid
            HAVING sbe_growth > 0 OR avg_temp > 80 OR max_dbe > 0
            ORDER BY max_dbe DESC, sbe_growth DESC, avg_temp DESC
            LIMIT 20
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/api/gpus/{gpu_uuid}/ecc-trend")
def ecc_trend(gpu_uuid: str):
    conn = db()
    try:
        rows = conn.execute(
            """SELECT ts, ecc_sbe_vol, ecc_dbe_vol, ecc_sbe_agg, ecc_dbe_agg
               FROM gpu_metrics WHERE gpu_uuid=? ORDER BY id DESC LIMIT 200""",
            (gpu_uuid,),
        ).fetchall()
        if not rows:
            raise HTTPException(404, "no metrics for gpu")
        trend = list(reversed([dict(r) for r in rows]))
        sbe_values = [t["ecc_sbe_agg"] for t in trend]
        slope = (sbe_values[-1] - sbe_values[0]) / max(1, len(sbe_values))
        # very simple linear projection of when retired_pages would force a swap
        return {"gpu_uuid": gpu_uuid, "samples": trend,
                "slope_per_sample": round(slope, 4),
                "projection": "stable" if slope < 0.05 else "monitor"
                              if slope < 0.5 else "replace_recommended"}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 14. /api/network
# ---------------------------------------------------------------------------
@app.get("/api/network/ports")
def list_ports():
    conn = db()
    try:
        rows = conn.execute("SELECT * FROM network_ports ORDER BY switch, port").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/api/network/roce-config")
def roce_config():
    conn = db()
    try:
        rows = conn.execute("SELECT * FROM network_ports").fetchall()
        report = []
        for r in rows:
            issues = []
            if r["pfc_enabled"] == 0: issues.append("PFC disabled")
            if r["ecn_enabled"] == 0: issues.append("ECN disabled")
            if r["mtu"] != 9216:      issues.append(f"MTU={r['mtu']} (expected 9216)")
            report.append({**dict(r), "issues": issues,
                           "compliant": len(issues) == 0})
        return {"total_ports": len(report),
                "compliant": sum(1 for x in report if x["compliant"]),
                "ports": report}
    finally:
        conn.close()


@app.post("/api/network/validate-cabling")
def validate_cabling():
    # Mock LLDP vs expected_topology comparison
    conn = db()
    try:
        ports = conn.execute("SELECT * FROM network_ports").fetchall()
        mismatches = [dict(p) for p in ports
                      if random.random() < 0.01]  # 1% mock cabling errors
        return {"checked": len(ports),
                "mismatches": mismatches,
                "ts": utcnow_iso()}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 15. /api/storage
# ---------------------------------------------------------------------------
@app.get("/api/storage/performance")
def storage_performance():
    conn = db()
    try:
        rows = conn.execute(
            "SELECT * FROM storage_metrics ORDER BY id DESC LIMIT 60"
        ).fetchall()
        return list(reversed([dict(r) for r in rows]))
    finally:
        conn.close()


@app.get("/api/storage/gpu-analysis")
def storage_gpu_analysis():
    conn = db()
    try:
        sm = conn.execute(
            "SELECT * FROM storage_metrics ORDER BY id DESC LIMIT 1"
        ).fetchone()
        gpu_count = conn.execute("SELECT COUNT(*) AS c FROM gpus").fetchone()["c"]
        required_gbs = gpu_count * 1.5
        adequate = (sm["read_gbs"] or 0) >= required_gbs
        return {
            "current_read_gbs": sm["read_gbs"],
            "current_write_gbs": sm["write_gbs"],
            "gds_sessions": sm["gds_sessions"],
            "gpu_count": gpu_count,
            "required_read_gbs": required_gbs,
            "throughput_adequate": adequate,
            "recommendation": ("Tune NFS rsize/wsize=16M, nconnect=4"
                               if not adequate else "OK"),
        }
    finally:
        conn.close()


@app.post("/api/storage/benchmark")
def storage_benchmark(body: dict):
    return {"node_id": body.get("node_id"),
            "mount_path": body.get("mount_path", "/vast/scratch"),
            "read_gbs": round(random.uniform(8, 22), 2),
            "write_gbs": round(random.uniform(3, 9), 2),
            "iops_4k": int(random.uniform(80_000, 220_000)),
            "duration_seconds": 60}


# ---------------------------------------------------------------------------
# 16. /api/finops
# ---------------------------------------------------------------------------
class CostBody(BaseModel):
    cost_usd: float = Field(ge=0)
    currency: str = "USD"
    effective_date: Optional[str] = None


@app.post("/api/finops/set-cost-per-gpu-hour")
def set_cost(body: CostBody):
    conn = db()
    try:
        conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)",
                     ("gpu_cost_per_hour_usd", str(body.cost_usd)))
        conn.commit()
        return {"ok": True, "cost_usd": body.cost_usd, "currency": body.currency}
    finally:
        conn.close()


@app.get("/api/finops/cost-per-gpu-hour")
def get_cost():
    conn = db()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key='gpu_cost_per_hour_usd'"
        ).fetchone()
        return {"cost_usd": float(row["value"]) if row else SETTINGS.gpu_cost_per_hour_usd,
                "currency": "USD"}
    finally:
        conn.close()


@app.get("/api/finops/utilization")
def utilization_by_team():
    conn = db()
    try:
        rows = conn.execute(
            """SELECT team,
                      COUNT(*) AS jobs,
                      SUM(gpu_count) AS total_gpus,
                      AVG(util_avg) AS avg_util
               FROM jobs
               WHERE state IN ('RUNNING','COMPLETED')
               GROUP BY team ORDER BY total_gpus DESC"""
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/api/finops/waste-analysis")
def waste_analysis():
    conn = db()
    try:
        cost_row = conn.execute(
            "SELECT value FROM settings WHERE key='gpu_cost_per_hour_usd'"
        ).fetchone()
        rate = float(cost_row["value"]) if cost_row else SETTINGS.gpu_cost_per_hour_usd

        # Category 1 — Idle Allocated
        idle = conn.execute(
            """SELECT job_id, team, user, gpu_count, util_avg, started_at
               FROM jobs WHERE state='RUNNING' AND util_avg < 0.05"""
        ).fetchall()
        idle_list = [dict(r) for r in idle]
        idle_gpu_hours = sum(r["gpu_count"] for r in idle_list) * 1  # 1hr window
        idle_cost = idle_gpu_hours * rate

        # Category 2 — Fragmentation (mock estimate)
        total_gpus = conn.execute("SELECT COUNT(*) AS c FROM gpus").fetchone()["c"]
        running = conn.execute(
            "SELECT COALESCE(SUM(gpu_count),0) AS s FROM jobs WHERE state='RUNNING'"
        ).fetchone()["s"]
        free = max(0, total_gpus - running)
        frag = max(0, int(free * 0.12))  # assume 12% of free is fragmented

        # Category 3 — scheduling gaps (stub: derive from completed-then-pending)
        gaps = max(0, int(total_gpus * 0.04))

        return {
            "rate_usd_per_gpu_hr": rate,
            "idle_allocated": {"jobs": idle_list,
                               "gpu_count": idle_gpu_hours,
                               "estimated_cost_usd_per_hr": round(idle_cost, 2)},
            "fragmented_gpus": frag,
            "scheduling_gap_gpus": gaps,
            "weekly_recoverable_usd": round((idle_gpu_hours + frag * 0.4 + gaps * 0.2) * rate * 168, 2),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 17. /api/alerts
# ---------------------------------------------------------------------------
@app.get("/api/alerts")
def list_alerts(status: Optional[str] = Query(None),
                severity: Optional[str] = Query(None)):
    conn = db()
    try:
        sql = "SELECT * FROM alerts WHERE 1=1"
        params: list[Any] = []
        if status:
            sql += " AND status=?"
            params.append(status.upper())
        if severity:
            sql += " AND severity=?"
            params.append(severity.upper())
        sql += " ORDER BY datetime(created_at) DESC LIMIT 500"
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/api/alerts/history")
def alert_history():
    conn = db()
    try:
        rows = conn.execute(
            "SELECT * FROM alerts WHERE status='RESOLVED' ORDER BY datetime(resolved_at) DESC LIMIT 200"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.put("/api/alerts/{alert_id}/acknowledge")
async def ack_alert(alert_id: str):
    conn = db()
    try:
        cur = conn.execute(
            "UPDATE alerts SET status='ACKNOWLEDGED', acknowledged_at=? WHERE alert_id=?",
            (utcnow_iso(), alert_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            raise HTTPException(404, "alert not found")
        await WS.broadcast({"event": "alert_acknowledged", "alert_id": alert_id})
        return {"ok": True, "alert_id": alert_id, "status": "ACKNOWLEDGED"}
    finally:
        conn.close()


@app.put("/api/alerts/{alert_id}/resolve")
async def resolve_alert(alert_id: str):
    conn = db()
    try:
        cur = conn.execute(
            "UPDATE alerts SET status='RESOLVED', resolved_at=? WHERE alert_id=?",
            (utcnow_iso(), alert_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            raise HTTPException(404, "alert not found")
        await WS.broadcast({"event": "alert_resolved", "alert_id": alert_id})
        return {"ok": True, "alert_id": alert_id, "status": "RESOLVED"}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 18. /api/jobs
# ---------------------------------------------------------------------------
@app.get("/api/jobs")
def list_jobs(state: Optional[str] = None):
    conn = db()
    try:
        sql = "SELECT * FROM jobs"
        params: list[Any] = []
        if state:
            sql += " WHERE state=?"
            params.append(state.upper())
        sql += " ORDER BY datetime(started_at) DESC LIMIT 200"
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/api/jobs/running")
def running_jobs():
    return list_jobs(state="RUNNING")


# ---------------------------------------------------------------------------
# 19. /api/reports
# ---------------------------------------------------------------------------
def _save_report(report_type: str, body: dict) -> dict:
    report_id = f"rpt-{uuid.uuid4().hex[:10]}"
    conn = db()
    try:
        conn.execute(
            "INSERT INTO reports(report_id, type, created_at, body) VALUES (?,?,?,?)",
            (report_id, report_type, utcnow_iso(), json.dumps(body, default=str)),
        )
        conn.commit()
    finally:
        conn.close()
    return {"report_id": report_id,
            "download_url": f"/api/reports/{report_id}",
            "type": report_type}


@app.post("/api/reports/health-report")
def gen_health_report():
    conn = db()
    try:
        node_count = conn.execute("SELECT COUNT(*) AS c FROM nodes").fetchone()["c"]
        gpu_count = conn.execute("SELECT COUNT(*) AS c FROM gpus").fetchone()["c"]
        open_alerts = conn.execute(
            "SELECT severity, COUNT(*) AS c FROM alerts WHERE status='OPEN' GROUP BY severity"
        ).fetchall()
        return _save_report("health-report", {
            "cluster": CLUSTER,
            "nodes": node_count, "gpus": gpu_count,
            "open_alerts_by_severity": {r["severity"]: r["c"] for r in open_alerts},
            "generated_at": utcnow_iso(),
        })
    finally:
        conn.close()


@app.post("/api/reports/acceptance-report")
def gen_acceptance_report(body: Optional[dict] = None):
    body = body or {}
    return _save_report("acceptance-report", {
        "cluster_name": body.get("cluster_name", CLUSTER),
        "tester_name": body.get("tester_name", "platform-eng"),
        "phases": [
            {"phase": 1, "name": "Physical Layer Validation",  "status": "PASS"},
            {"phase": 2, "name": "Single Node Validation",     "status": "PASS"},
            {"phase": 3, "name": "Cluster Fabric Validation",  "status": "PASS"},
            {"phase": 4, "name": "72-Hour Burn-in",            "status": "PASS"},
            {"phase": 5, "name": "Acceptance Sign-off",        "status": "PENDING_SIGNATURE"},
        ],
        "generated_at": utcnow_iso(),
    })


@app.post("/api/reports/finops-report")
def gen_finops_report():
    util = utilization_by_team()
    waste = waste_analysis()
    return _save_report("finops-report", {"utilization": util, "waste": waste,
                                          "generated_at": utcnow_iso()})


@app.get("/api/reports/{report_id}")
def get_report(report_id: str):
    conn = db()
    try:
        row = conn.execute("SELECT * FROM reports WHERE report_id=?",
                           (report_id,)).fetchone()
        if not row:
            raise HTTPException(404, "report not found")
        return {"report_id": report_id, "type": row["type"],
                "created_at": row["created_at"], "body": json.loads(row["body"])}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 20. /api/reference  — XID + threshold tables
# ---------------------------------------------------------------------------
@app.get("/api/reference/xid")
def xid_reference():
    return [{"xid": k, **v} for k, v in sorted(XID_REFERENCE.items())]


@app.get("/api/reference/thresholds")
def threshold_reference():
    return ALERT_THRESHOLDS


# ---------------------------------------------------------------------------
# 21. /api/summary  — what the dashboard top-strip uses
# ---------------------------------------------------------------------------
@app.get("/api/summary")
async def summary():
    snapshots = await CACHE.all("gpu_metrics:")
    conn = db()
    try:
        node_count = conn.execute("SELECT COUNT(*) AS c FROM nodes").fetchone()["c"]
        gpu_count = conn.execute("SELECT COUNT(*) AS c FROM gpus").fetchone()["c"]
        open_alerts = conn.execute(
            "SELECT severity, COUNT(*) AS c FROM alerts WHERE status='OPEN' GROUP BY severity"
        ).fetchall()
        running = conn.execute(
            "SELECT COALESCE(SUM(gpu_count),0) AS s FROM jobs WHERE state='RUNNING'"
        ).fetchone()["s"]
        sm = conn.execute(
            "SELECT * FROM storage_metrics ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    avg_util = round(statistics.mean(s["util_sm"] for s in snapshots), 1) if snapshots else 0
    avg_temp = round(statistics.mean(s["temp_c"] for s in snapshots), 1) if snapshots else 0
    return {
        "cluster": CLUSTER,
        "nodes": node_count,
        "gpus": gpu_count,
        "gpus_in_use": running,
        "fleet_utilization_pct": avg_util,
        "fleet_avg_temp_c": avg_temp,
        "open_alerts": {r["severity"]: r["c"] for r in open_alerts},
        "storage": {"read_gbs": sm["read_gbs"] if sm else 0,
                    "write_gbs": sm["write_gbs"] if sm else 0,
                    "capacity_pct": round((sm["capacity_pct"] or 0) * 100, 1) if sm else 0,
                    "gds_sessions": sm["gds_sessions"] if sm else 0},
    }


# ---------------------------------------------------------------------------
# 21b. Diagnostics simulator — DCGM diag, NCCL all_reduce_perf, HPL
# ---------------------------------------------------------------------------
DCGM_TESTS = {
    1: ["software_check", "cuda_init"],
    2: ["software_check", "cuda_init", "memory_short", "pcie_bandwidth"],
    3: ["software_check", "cuda_init", "memory_long", "pcie_bandwidth",
        "sm_stress", "thermal_stress", "power_stress", "nvlink_validation"],
    4: ["software_check", "cuda_init", "memory_long", "memory_extended",
        "pcie_bandwidth", "sm_stress", "thermal_stress", "power_stress",
        "nvlink_validation", "memtest_full"],
}

H100_FP64_PEAK_TFLOPS = 33.5     # Section 4.2 of the runbook
H100_NVLINK_PEAK_GBS  = 900.0    # Section 4.2 of the runbook


def _record_diag(test_id: str, kind: str, target: str, params: dict) -> None:
    conn = db()
    try:
        conn.execute(
            """INSERT INTO diagnostics(test_id, kind, status, target, params,
                                       result, started_at)
               VALUES (?,?,?,?,?,?,?)""",
            (test_id, kind, "RUNNING", target, json.dumps(params),
             None, utcnow_iso()),
        )
        conn.commit()
    finally:
        conn.close()


def _finish_diag(test_id: str, status: str, result: dict) -> None:
    conn = db()
    try:
        conn.execute(
            "UPDATE diagnostics SET status=?, result=?, ended_at=? WHERE test_id=?",
            (status, json.dumps(result, default=str), utcnow_iso(), test_id),
        )
        conn.commit()
    finally:
        conn.close()


async def _simulate_dcgm(test_id: str, node_id: str, level: int) -> None:
    conn = db()
    try:
        n = conn.execute("SELECT * FROM nodes WHERE node_id=?",
                         (node_id,)).fetchone()
    finally:
        conn.close()
    psu = n["psu_12v"] if n else 12.0

    sub_tests = []
    overall = "PASS"
    for name in DCGM_TESTS.get(level, DCGM_TESTS[3]):
        await asyncio.sleep(0.8)  # fast-forwarded; real diag takes minutes
        result, detail = "PASS", "ok"
        # bad PSU -> power_stress fails; node-7 -> memory_long fails
        if name == "power_stress" and psu < 11.7:
            result, detail, overall = "FAIL", f"12V rail {psu}V under load", "FAIL"
        elif name == "memory_long" and node_id == "node-007":
            result, detail, overall = "FAIL", "uncorrectable ECC on GPU 0", "FAIL"
        sub_tests.append({"name": name, "result": result,
                          "duration_s": round(random.uniform(15, 120), 1),
                          "detail": detail})
        await WS.broadcast({"event": "diag_progress", "test_id": test_id,
                            "test": name, "result": result})

    summary = (f"{sum(1 for t in sub_tests if t['result']=='PASS')}/"
               f"{len(sub_tests)} passed")
    _finish_diag(test_id, overall,
                 {"node_id": node_id, "level": level,
                  "tests": sub_tests, "summary": summary})
    await WS.broadcast({"event": "diag_done", "test_id": test_id,
                        "kind": "dcgm", "status": overall})


async def _simulate_nccl(test_id: str, nodes: list[str],
                         gpus_per_node: int) -> None:
    sizes = [8, 64, 512, 4096, 32_768, 262_144, 1_048_576,
             8_388_608, 67_108_864, 536_870_912, 4_294_967_296]
    total_gpus = max(1, len(nodes) * gpus_per_node)

    conn = db()
    try:
        if nodes:
            placeholders = ",".join("?" * len(nodes))
            bad = conn.execute(
                f"SELECT COUNT(*) c FROM network_ports "
                f"WHERE peer_node IN ({placeholders}) "
                f"AND (pfc_enabled=0 OR fec_corr_per_s>1000)", nodes
            ).fetchone()["c"]
        else:
            bad = 0
    finally:
        conn.close()

    fabric_health = max(0.6, 1.0 - bad * 0.15)
    if len(nodes) <= 1:
        peak = H100_NVLINK_PEAK_GBS * fabric_health
        threshold = H100_NVLINK_PEAK_GBS * 0.9
    else:
        peak = 90.0 * fabric_health  # ~200G NIC effective
        threshold = peak * 0.8

    rows = []
    for sz in sizes:
        await asyncio.sleep(0.3)
        ramp = 1 / (1 + math.exp(-(math.log10(sz) - 5)))
        algbw = peak * ramp * random.uniform(0.92, 1.0)
        busbw = algbw * (2 * (total_gpus - 1) / total_gpus)
        latency_us = max(2.0, 50_000 / max(algbw, 0.1)
                         * (sz / 1e9) * 1e6 + random.uniform(2, 8))
        rows.append({"size_bytes": sz,
                     "algbw_gbs": round(algbw, 2),
                     "busbw_gbs": round(busbw, 2),
                     "latency_us": round(latency_us, 2)})
        await WS.broadcast({"event": "diag_progress", "test_id": test_id,
                            "size_bytes": sz, "busbw_gbs": round(busbw, 2)})

    peak_busbw = max(r["busbw_gbs"] for r in rows)
    status = "PASS" if peak_busbw >= threshold else "FAIL"
    _finish_diag(test_id, status, {
        "nodes": nodes, "total_gpus": total_gpus,
        "peak_busbw_gbs": round(peak_busbw, 2),
        "threshold_gbs": round(threshold, 1),
        "fabric_health": round(fabric_health, 2),
        "results": rows,
    })
    await WS.broadcast({"event": "diag_done", "test_id": test_id,
                        "kind": "nccl", "status": status,
                        "peak_busbw": round(peak_busbw, 2)})


async def _simulate_hpl(test_id: str, nodes: list[str]) -> None:
    conn = db()
    try:
        if not nodes:
            rows_n = []
        else:
            placeholders = ",".join("?" * len(nodes))
            rows_n = conn.execute(
                f"SELECT * FROM nodes WHERE node_id IN ({placeholders})",
                nodes,
            ).fetchall()
    finally:
        conn.close()

    per_node = []
    for n in rows_n:
        await asyncio.sleep(0.5)
        psu_penalty = max(0.0, (11.8 - n["psu_12v"])) * 4.0
        thermal_penalty = 0.0
        achieved = (H100_FP64_PEAK_TFLOPS
                    - psu_penalty - thermal_penalty
                    + random.gauss(-0.3, 0.5))
        achieved = max(20.0, achieved)
        passed = achieved >= H100_FP64_PEAK_TFLOPS * 0.95
        per_node.append({
            "node_id": n["node_id"],
            "tflops_fp64": round(achieved, 2),
            "pct_of_peak": round(achieved / H100_FP64_PEAK_TFLOPS * 100, 1),
            "result": "PASS" if passed else "FAIL",
        })
        await WS.broadcast({"event": "diag_progress", "test_id": test_id,
                            "node": n["node_id"],
                            "tflops": round(achieved, 2)})

    overall = "PASS" if all(p["result"] == "PASS" for p in per_node) else "FAIL"
    _finish_diag(test_id, overall, {
        "nodes": nodes,
        "theoretical_peak_tflops": H100_FP64_PEAK_TFLOPS,
        "acceptance_threshold_tflops": round(H100_FP64_PEAK_TFLOPS * 0.95, 1),
        "per_node": per_node,
    })
    await WS.broadcast({"event": "diag_done", "test_id": test_id,
                        "kind": "hpl", "status": overall})


class DcgmBody(BaseModel):
    node_id: str
    level: int = Field(ge=1, le=4, default=3)


class NcclBody(BaseModel):
    nodes: list[str]
    gpus_per_node: int = 8


class HplBody(BaseModel):
    nodes: list[str]


@app.post("/api/diagnostics/dcgm")
async def start_dcgm(body: DcgmBody):
    test_id = f"dcgm-{uuid.uuid4().hex[:8]}"
    _record_diag(test_id, "dcgm", body.node_id, body.dict())
    asyncio.create_task(_simulate_dcgm(test_id, body.node_id, body.level))
    return {"test_id": test_id, "status": "RUNNING",
            "estimated_seconds": len(DCGM_TESTS[body.level])}


@app.post("/api/diagnostics/nccl")
async def start_nccl(body: NcclBody):
    test_id = f"nccl-{uuid.uuid4().hex[:8]}"
    _record_diag(test_id, "nccl", ",".join(body.nodes), body.dict())
    asyncio.create_task(_simulate_nccl(test_id, body.nodes, body.gpus_per_node))
    return {"test_id": test_id, "status": "RUNNING", "estimated_seconds": 5}


@app.post("/api/diagnostics/hpl")
async def start_hpl(body: HplBody):
    test_id = f"hpl-{uuid.uuid4().hex[:8]}"
    _record_diag(test_id, "hpl", ",".join(body.nodes), body.dict())
    asyncio.create_task(_simulate_hpl(test_id, body.nodes))
    return {"test_id": test_id, "status": "RUNNING",
            "estimated_seconds": max(1, len(body.nodes))}


@app.get("/api/diagnostics")
def list_diagnostics(limit: int = 50):
    conn = db()
    try:
        rows = conn.execute(
            "SELECT * FROM diagnostics ORDER BY datetime(started_at) DESC LIMIT ?",
            (limit,),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            if d.get("result"):
                try:
                    d["result"] = json.loads(d["result"])
                except Exception:
                    pass
            if d.get("params"):
                try:
                    d["params"] = json.loads(d["params"])
                except Exception:
                    pass
            out.append(d)
        return out
    finally:
        conn.close()


@app.get("/api/diagnostics/{test_id}")
def get_diagnostic(test_id: str):
    conn = db()
    try:
        row = conn.execute(
            "SELECT * FROM diagnostics WHERE test_id=?", (test_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "test not found")
        d = dict(row)
        if d.get("result"):
            d["result"] = json.loads(d["result"])
        if d.get("params"):
            d["params"] = json.loads(d["params"])
        return d
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 22. WebSocket
# ---------------------------------------------------------------------------
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await WS.connect(websocket)
    try:
        await websocket.send_text(json.dumps({"event": "hello",
                                              "ts": utcnow_iso(),
                                              "subscribers": len(WS.active)}))
        while True:
            # passive — just keep the channel open
            await websocket.receive_text()
    except WebSocketDisconnect:
        WS.disconnect(websocket)


# ---------------------------------------------------------------------------
# 23. Embedded dashboard (single HTML page, no build step needed)
# ---------------------------------------------------------------------------
DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>GPU Fleet Intelligence Platform</title>
<meta name="viewport" content="width=device-width,initial-scale=1" />
<style>
  :root{
    --bg:#0b1220; --panel:#0f1a2e; --panel2:#13223d; --line:#1f3358;
    --txt:#e6edf7; --mute:#94a3b8;
    --ok:#22c55e; --warn:#f59e0b; --crit:#ef4444; --info:#60a5fa;
    --accent:#76b900; /* nv green */
  }
  *{box-sizing:border-box}
  body{margin:0;background:linear-gradient(180deg,#070b15,#0b1220);color:var(--txt);
       font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
  header{display:flex;align-items:center;gap:16px;padding:14px 20px;
         background:#0a1325;border-bottom:1px solid var(--line);position:sticky;top:0;z-index:10}
  header h1{font-size:16px;margin:0;letter-spacing:.3px}
  header .pill{font-size:11px;background:#10243a;color:var(--mute);padding:3px 8px;border-radius:999px;border:1px solid var(--line)}
  header .ws{margin-left:auto;display:flex;align-items:center;gap:8px;color:var(--mute);font-size:12px}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--mute)}
  .dot.ok{background:var(--ok);box-shadow:0 0 8px var(--ok)}
  main{padding:18px;display:grid;gap:16px;grid-template-columns:repeat(12,1fr)}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px}
  .card h3{margin:0 0 10px;font-size:12px;text-transform:uppercase;letter-spacing:.8px;color:var(--mute)}
  .kpi{font-size:26px;font-weight:600}
  .kpi small{display:block;color:var(--mute);font-size:12px;font-weight:400;margin-top:2px}
  .col-3{grid-column:span 3}.col-4{grid-column:span 4}.col-6{grid-column:span 6}.col-8{grid-column:span 8}.col-12{grid-column:span 12}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{text-align:left;padding:7px 8px;border-bottom:1px solid var(--line)}
  th{color:var(--mute);font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.5px}
  tr:hover td{background:#10203a}
  .badge{display:inline-block;padding:2px 8px;border-radius:6px;font-size:11px;font-weight:600}
  .b-CRITICAL{background:#3b0a0a;color:#fca5a5;border:1px solid #7f1d1d}
  .b-WARNING{background:#3a2607;color:#fcd34d;border:1px solid #92400e}
  .b-INFO{background:#0c2440;color:#93c5fd;border:1px solid #1e40af}
  .b-OPEN{background:#3b0a0a;color:#fca5a5}
  .b-ACKNOWLEDGED{background:#1f2937;color:#cbd5e1}
  .b-RESOLVED{background:#062314;color:#86efac}
  .b-PRODUCTION{background:#062314;color:#86efac;border:1px solid #064e3b}
  .b-MAINTENANCE{background:#3a2607;color:#fcd34d}
  .b-RUNNING{background:#062314;color:#86efac}
  .b-COMPLETED{background:#1f2937;color:#cbd5e1}
  .b-PENDING{background:#0c2440;color:#93c5fd}
  button{background:#1e293b;color:var(--txt);border:1px solid var(--line);padding:5px 10px;
         border-radius:6px;cursor:pointer;font-size:12px}
  button:hover{background:#243348}
  .grid-gpu{display:grid;grid-template-columns:repeat(auto-fill,minmax(64px,1fr));gap:4px}
  .gpu-cell{aspect-ratio:1;border-radius:5px;display:flex;align-items:center;justify-content:center;
            font-size:10px;color:#0b1220;font-weight:700;cursor:default}
  .legend{display:flex;gap:14px;font-size:11px;color:var(--mute);margin-top:10px}
  .legend span{display:flex;align-items:center;gap:6px}
  .legend i{width:10px;height:10px;border-radius:2px;display:inline-block}
  .toast{position:fixed;right:20px;bottom:20px;background:var(--panel2);border:1px solid var(--line);
         padding:10px 14px;border-radius:8px;box-shadow:0 8px 32px rgba(0,0,0,.4);font-size:13px;
         max-width:380px;animation:slide .3s ease-out}
  @keyframes slide{from{transform:translateX(20px);opacity:0}to{transform:none;opacity:1}}
  .tabs{display:flex;gap:4px;border-bottom:1px solid var(--line);margin-bottom:10px}
  .tabs button{border:none;border-bottom:2px solid transparent;background:none;border-radius:0;
               padding:8px 14px;color:var(--mute)}
  .tabs button.active{color:var(--txt);border-color:var(--accent)}
  .footer{grid-column:span 12;text-align:center;color:var(--mute);font-size:11px;padding-top:8px}
  code{background:#0a1325;border:1px solid var(--line);padding:1px 6px;border-radius:4px;font-size:12px}
</style>
</head>
<body>

<header>
  <div style="display:flex;align-items:center;gap:10px">
    <div style="width:24px;height:24px;border-radius:6px;background:linear-gradient(135deg,#76b900,#3a8a00)"></div>
    <h1>GPU Fleet Intelligence Platform</h1>
    <span class="pill" id="cluster-pill">cluster: …</span>
    <span class="pill">v1.0</span>
  </div>
  <div class="ws"><span class="dot" id="ws-dot"></span><span id="ws-state">connecting…</span></div>
</header>

<main>
  <section class="card col-3"><h3>Fleet utilization</h3>
    <div class="kpi" id="kpi-util">–<small>SM% averaged across all GPUs</small></div></section>
  <section class="card col-3"><h3>GPUs in use / total</h3>
    <div class="kpi" id="kpi-gpus">–<small>via Slurm + Run:ai + K8s</small></div></section>
  <section class="card col-3"><h3>Avg GPU temp</h3>
    <div class="kpi" id="kpi-temp">–<small>°C — alert &gt; 82, crit &gt; 88</small></div></section>
  <section class="card col-3"><h3>Open alerts</h3>
    <div class="kpi" id="kpi-alerts">–<small id="kpi-alerts-sub">CRITICAL / WARNING / INFO</small></div></section>

  <section class="card col-8">
    <h3>Fleet GPU heat-map (latest snapshot)</h3>
    <div class="grid-gpu" id="gpu-grid"></div>
    <div class="legend">
      <span><i style="background:#22c55e"></i>util &gt; 80%</span>
      <span><i style="background:#84cc16"></i>50–80%</span>
      <span><i style="background:#eab308"></i>20–50%</span>
      <span><i style="background:#94a3b8"></i>idle</span>
      <span><i style="background:#ef4444"></i>fault</span>
    </div>
  </section>

  <section class="card col-4">
    <h3>Storage (VAST)</h3>
    <div id="storage-summary" style="font-size:13px"></div>
  </section>

  <section class="card col-6">
    <div class="tabs">
      <button class="active" data-tab="open">Open alerts</button>
      <button data-tab="ack">Acknowledged</button>
      <button data-tab="hist">History</button>
    </div>
    <table id="alerts-table">
      <thead><tr><th>Sev</th><th>Type</th><th>Summary</th><th>When</th><th></th></tr></thead>
      <tbody></tbody>
    </table>
  </section>

  <section class="card col-6">
    <h3>Nodes</h3>
    <table id="nodes-table">
      <thead><tr><th>Node</th><th>Status</th><th>12V</th><th>Fan%</th><th>Last seen</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>

  <section class="card col-6">
    <h3>Network — RoCEv2 compliance</h3>
    <div id="roce-summary" style="margin-bottom:10px;color:var(--mute);font-size:12px"></div>
    <table id="ports-table">
      <thead><tr><th>Switch</th><th>Port</th><th>Peer</th><th>RX dBm</th><th>FEC/s</th><th>PFC</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>

  <section class="card col-6">
    <h3>FinOps — utilization by team</h3>
    <table id="team-table">
      <thead><tr><th>Team</th><th>Jobs</th><th>GPUs</th><th>Avg util</th></tr></thead>
      <tbody></tbody>
    </table>
    <div id="waste-summary" style="margin-top:10px;color:var(--mute);font-size:12px"></div>
  </section>

  <section class="card col-12">
    <h3>Diagnostics &amp; Benchmarks</h3>
    <div style="display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap">
      <button id="run-dcgm">Run DCGM diag -r 3 (node-007)</button>
      <button id="run-dcgm-good">Run DCGM diag -r 3 (node-001)</button>
      <button id="run-nccl">Run NCCL all_reduce (4 nodes)</button>
      <button id="run-hpl">Run HPL (8 nodes)</button>
      <span style="margin-left:auto;color:var(--mute);font-size:12px;align-self:center">
        Watch the WebSocket toasts — each sub-test fires an event.
      </span>
    </div>
    <table id="diag-table">
      <thead><tr><th>Test</th><th>Kind</th><th>Target</th><th>Status</th><th>Started</th><th>Result</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>

  <section class="card col-12">
    <h3>Jobs</h3>
    <table id="jobs-table">
      <thead><tr><th>Job</th><th>Sched</th><th>Team</th><th>User</th><th>State</th><th>GPUs</th><th>Util</th><th>Started</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>

  <div class="footer">
    GPU Fleet Intelligence Platform · single-file edition · API: <code>/docs</code> · WebSocket: <code>/ws</code>
  </div>
</main>

<script>
const $ = (s, r=document) => r.querySelector(s);
const $$ = (s, r=document) => Array.from(r.querySelectorAll(s));

let summary = null, alertsCache = [], currentTab = "open";

function fmt(v, d=1){ return v==null ? "–" : (typeof v==="number" ? v.toFixed(d) : v); }
function ago(iso){ if(!iso) return "–";
  const s = (Date.now() - new Date(iso).getTime())/1000;
  if(s<60) return Math.floor(s)+"s ago";
  if(s<3600) return Math.floor(s/60)+"m ago";
  return Math.floor(s/3600)+"h ago"; }

async function get(path){ const r = await fetch(path); return r.json(); }
async function put(path){ const r = await fetch(path,{method:"PUT"}); return r.json(); }

function utilColor(u){
  if(u>80) return "#22c55e";
  if(u>50) return "#84cc16";
  if(u>20) return "#eab308";
  return "#94a3b8";
}

async function loadSummary(){
  summary = await get("/api/summary");
  $("#cluster-pill").textContent = "cluster: "+summary.cluster;
  $("#kpi-util").firstChild.textContent = summary.fleet_utilization_pct + "%";
  $("#kpi-gpus").firstChild.textContent = summary.gpus_in_use + " / " + summary.gpus;
  $("#kpi-temp").firstChild.textContent = summary.fleet_avg_temp_c + "°";
  const a = summary.open_alerts || {};
  $("#kpi-alerts").firstChild.textContent =
    (a.CRITICAL||0) + " / " + (a.WARNING||0) + " / " + (a.INFO||0);
  const s = summary.storage;
  $("#storage-summary").innerHTML =
    `<div>Read throughput &nbsp;<b>${fmt(s.read_gbs,1)} GB/s</b></div>
     <div>Write throughput &nbsp;<b>${fmt(s.write_gbs,1)} GB/s</b></div>
     <div>Capacity used &nbsp;<b>${s.capacity_pct}%</b></div>
     <div>GDS sessions &nbsp;<b>${s.gds_sessions}</b></div>`;
}

async function loadGpus(){
  const gpus = await get("/api/gpus");
  const grid = $("#gpu-grid");
  grid.innerHTML = "";
  gpus.forEach(g => {
    const fault = g.ecc_dbe_vol > 0;
    const c = fault ? "#ef4444" : utilColor(g.util_sm);
    const cell = document.createElement("div");
    cell.className = "gpu-cell";
    cell.style.background = c;
    cell.title = `${g.node_id} / GPU${g.gpu_index}\nUtil ${fmt(g.util_sm)}% · ${fmt(g.temp_c)}°C\nPower ${fmt(g.power_w)}W · NVLink ${fmt(g.nvlink_bw_gbs)} GB/s` +
                 (fault ? `\n⚠ Uncorrectable ECC (XID 79)` : "");
    cell.textContent = g.gpu_index;
    grid.appendChild(cell);
  });
}

async function loadAlerts(tab){
  let path = "/api/alerts?status=OPEN";
  if(tab==="ack") path = "/api/alerts?status=ACKNOWLEDGED";
  if(tab==="hist") path = "/api/alerts/history";
  alertsCache = await get(path);
  const tbody = $("#alerts-table tbody");
  tbody.innerHTML = "";
  alertsCache.slice(0,40).forEach(a => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td><span class="badge b-${a.severity}">${a.severity}</span></td>
                    <td><code>${a.type}</code></td>
                    <td>${a.summary}</td>
                    <td>${ago(a.created_at)}</td>
                    <td>${tab==="open"
                      ? `<button data-act="ack" data-id="${a.alert_id}">ACK</button>
                         <button data-act="res" data-id="${a.alert_id}">Resolve</button>`
                      : ""}</td>`;
    tbody.appendChild(tr);
  });
  if(alertsCache.length===0){
    tbody.innerHTML = `<tr><td colspan="5" style="color:var(--mute);text-align:center;padding:18px">no alerts</td></tr>`;
  }
}

async function loadNodes(){
  const nodes = await get("/api/nodes");
  const tbody = $("#nodes-table tbody");
  tbody.innerHTML = "";
  nodes.forEach(n => {
    const psuColor = n.psu_12v < 11.6 ? "#ef4444" : n.psu_12v < 11.8 ? "#f59e0b" : "#86efac";
    tbody.innerHTML += `<tr>
        <td><b>${n.hostname}</b><br><span style="color:var(--mute);font-size:11px">${n.node_id} · ${n.chassis}</span></td>
        <td><span class="badge b-${n.bcm_status}">${n.bcm_status}</span></td>
        <td style="color:${psuColor};font-weight:600">${n.psu_12v}V</td>
        <td>${Math.round(n.fan_pct*100)}%</td>
        <td>${ago(n.last_seen)}</td></tr>`;
  });
}

async function loadPorts(){
  const r = await get("/api/network/roce-config");
  $("#roce-summary").textContent =
    `${r.compliant} / ${r.total_ports} ports compliant with RoCEv2 baseline (PFC + ECN + MTU 9216)`;
  const tbody = $("#ports-table tbody");
  tbody.innerHTML = "";
  r.ports.filter(p => !p.compliant || p.rx_dbm < -8 || p.fec_corr_per_s > 200)
        .slice(0,12)
        .forEach(p => {
    tbody.innerHTML += `<tr>
        <td>${p.switch}</td><td>${p.port}</td><td>${p.peer_node||"–"}</td>
        <td>${fmt(p.rx_dbm,2)}</td>
        <td>${fmt(p.fec_corr_per_s,0)}</td>
        <td>${p.pfc_enabled ? "✓" : "<span class='badge b-CRITICAL'>off</span>"}</td>
      </tr>`;
  });
  if(!tbody.children.length){
    tbody.innerHTML = `<tr><td colspan="6" style="color:var(--mute);text-align:center;padding:14px">all ports healthy</td></tr>`;
  }
}

async function loadFinops(){
  const teams = await get("/api/finops/utilization");
  const tbody = $("#team-table tbody");
  tbody.innerHTML = "";
  teams.forEach(t => {
    tbody.innerHTML += `<tr>
        <td>${t.team}</td><td>${t.jobs}</td><td>${t.total_gpus}</td>
        <td>${Math.round((t.avg_util||0)*100)}%</td></tr>`;
  });
  const w = await get("/api/finops/waste-analysis");
  $("#waste-summary").innerHTML =
    `Idle-allocated GPUs: <b>${w.idle_allocated.gpu_count}</b>
     · fragmented: <b>${w.fragmented_gpus}</b>
     · scheduling gaps: <b>${w.scheduling_gap_gpus}</b>
     · weekly recoverable spend: <b>$${w.weekly_recoverable_usd.toLocaleString()}</b>
     <span style="color:var(--accent)">@ $${w.rate_usd_per_gpu_hr}/GPU-hr</span>`;
}

async function loadJobs(){
  const jobs = await get("/api/jobs");
  const tbody = $("#jobs-table tbody");
  tbody.innerHTML = "";
  jobs.slice(0,15).forEach(j => {
    tbody.innerHTML += `<tr>
        <td><code>${j.job_id}</code></td>
        <td>${j.scheduler}</td>
        <td>${j.team}</td>
        <td>${j.user}</td>
        <td><span class="badge b-${j.state}">${j.state}</span></td>
        <td>${j.gpu_count}</td>
        <td>${Math.round((j.util_avg||0)*100)}%</td>
        <td>${ago(j.started_at)}</td></tr>`;
  });
}

async function postJson(path, body){
  const r = await fetch(path, {method:"POST",
    headers:{"Content-Type":"application/json"},
    body: JSON.stringify(body)});
  return r.json();
}

async function loadDiagnostics(){
  const rows = await get("/api/diagnostics?limit=20");
  const tbody = $("#diag-table tbody");
  tbody.innerHTML = "";
  if(rows.length === 0){
    tbody.innerHTML = `<tr><td colspan="6" style="color:var(--mute);text-align:center;padding:14px">no tests run yet — click a button above</td></tr>`;
    return;
  }
  rows.forEach(d => {
    const status = d.status;
    const badge = status === "PASS" ? "b-RUNNING"
                : status === "FAIL" ? "b-CRITICAL"
                : "b-PENDING";
    let summary = "–";
    if(d.result){
      if(d.kind === "dcgm" && d.result.summary) summary = d.result.summary;
      if(d.kind === "nccl" && d.result.peak_busbw_gbs !== undefined)
        summary = `peak ${d.result.peak_busbw_gbs} GB/s vs ${d.result.threshold_gbs} threshold`;
      if(d.kind === "hpl" && d.result.per_node)
        summary = `${d.result.per_node.filter(p=>p.result==="PASS").length}/${d.result.per_node.length} nodes ≥ 31.8 TFLOPS`;
    }
    tbody.innerHTML += `<tr>
        <td><code>${d.test_id}</code></td>
        <td>${d.kind.toUpperCase()}</td>
        <td style="font-size:11px;max-width:240px;overflow:hidden;text-overflow:ellipsis">${d.target}</td>
        <td><span class="badge ${badge}">${status}</span></td>
        <td>${ago(d.started_at)}</td>
        <td style="font-size:12px;color:var(--mute)">${summary}</td>
      </tr>`;
  });
}

document.addEventListener("click", async ev => {
  if(ev.target.id === "run-dcgm"){
    const r = await postJson("/api/diagnostics/dcgm", {node_id:"node-007", level:3});
    toast(`DCGM diag started: ${r.test_id} (~${r.estimated_seconds}s)`);
    loadDiagnostics();
  }
  if(ev.target.id === "run-dcgm-good"){
    const r = await postJson("/api/diagnostics/dcgm", {node_id:"node-001", level:3});
    toast(`DCGM diag started: ${r.test_id} (~${r.estimated_seconds}s)`);
    loadDiagnostics();
  }
  if(ev.target.id === "run-nccl"){
    const r = await postJson("/api/diagnostics/nccl",
                             {nodes:["node-001","node-002","node-003","node-007"], gpus_per_node:8});
    toast(`NCCL test started: ${r.test_id}`);
    loadDiagnostics();
  }
  if(ev.target.id === "run-hpl"){
    const r = await postJson("/api/diagnostics/hpl",
      {nodes:["node-001","node-002","node-003","node-004","node-005","node-006","node-007","node-008"]});
    toast(`HPL benchmark started: ${r.test_id}`);
    loadDiagnostics();
  }
});

async function refreshAll(){
  await Promise.all([loadSummary(), loadGpus(), loadAlerts(currentTab),
                     loadNodes(), loadPorts(), loadFinops(), loadJobs(),
                     loadDiagnostics()]);
}

function toast(msg){
  const t = document.createElement("div");
  t.className = "toast"; t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(()=>t.remove(), 4500);
}

function connectWS(){
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen  = () => { $("#ws-dot").className = "dot ok"; $("#ws-state").textContent = "live"; };
  ws.onclose = () => { $("#ws-dot").className = "dot";    $("#ws-state").textContent = "disconnected";
                       setTimeout(connectWS, 3000); };
  ws.onmessage = ev => {
    const m = JSON.parse(ev.data);
    if(m.event === "gpu_metrics_update"){ loadGpus(); loadSummary(); }
    if(m.event === "alert_new"){ toast(`🚨 ${m.severity}: ${m.summary}`); loadAlerts(currentTab); loadSummary(); }
    if(m.event === "alert_resolved" || m.event === "alert_acknowledged"){ loadAlerts(currentTab); loadSummary(); }
    if(m.event === "diag_done"){ toast(`✓ ${m.kind.toUpperCase()} ${m.status}: ${m.test_id}`); loadDiagnostics(); }
    if(m.event === "diag_progress"){ /* could show inline progress */ }
  };
}

document.addEventListener("click", async ev => {
  const t = ev.target.closest("button[data-act]");
  if(t){
    const id = t.dataset.id, act = t.dataset.act;
    const r = await put(`/api/alerts/${id}/${act==="ack"?"acknowledge":"resolve"}`);
    toast(`alert ${r.status}: ${id}`);
    loadAlerts(currentTab); loadSummary();
  }
  const tab = ev.target.closest(".tabs button[data-tab]");
  if(tab){
    $$(".tabs button").forEach(b => b.classList.remove("active"));
    tab.classList.add("active");
    currentTab = tab.dataset.tab;
    loadAlerts(currentTab);
  }
});

connectWS();
refreshAll();
setInterval(refreshAll, 30000);   // mirror the doc's 30s React-Query refresh
</script>
</body></html>
"""


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD_HTML


@app.get("/health", response_class=PlainTextResponse)
def health():
    return "ok"


@app.get("/version")
def version():
    return {"name": "GPU Fleet Intelligence Platform",
            "version": "1.0.0",
            "single_file": True}


# ---------------------------------------------------------------------------
# 24. Entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    print("=" * 64, flush=True)
    print(" GPU Fleet Intelligence Platform — single-file edition", flush=True)
    print("=" * 64, flush=True)
    print(f" DB:        {Path(SETTINGS.db_path).resolve()}", flush=True)
    print(f" Bind:      http://{SETTINGS.host}:{SETTINGS.port}", flush=True)
    print(f" Collectors: NVIDIA={SETTINGS.nvidia_interval}s · "
          f"Cisco={SETTINGS.cisco_interval}s · VAST={SETTINGS.vast_interval}s",
          flush=True)
    print("=" * 64, flush=True)
    uvicorn.run(app, host=SETTINGS.host, port=SETTINGS.port, log_level="warning")


if __name__ == "__main__":
    main()
