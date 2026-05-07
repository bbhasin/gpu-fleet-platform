# GPU Fleet Platform — Air-Gapped Observability for NVIDIA Clusters

A single-file, single-command operations platform for NVIDIA H100/H200
GPU clusters. Designed for **air-gapped, regulated environments** —
banks, hospitals, federal agencies, and integrators — where SaaS
monitoring is a non-starter and Mission Control's cloud telemetry can't
leave the perimeter.

> Built as a complement to NVIDIA Mission Control, not a replacement.
> Targets the day-2 operations gap most customers fall into after the
> integrator delivers a DGX SuperPOD or HGX reference design and leaves.

[ADD SCREENSHOT HERE — dashboard with GPU heat-map + alerts table]

---

## Quick start

```bash
git clone https://github.com/<username>/gpu-fleet-platform.git
cd gpu-fleet-platform
python3 gpu_fleet_platform.py
```

The script bootstraps its four dependencies (FastAPI, uvicorn, pydantic,
websockets) on first launch — nothing else to install. Then:

- Dashboard: <http://localhost:8000>
- OpenAPI docs: <http://localhost:8000/docs>
- WebSocket stream: `ws://localhost:8000/ws`

[ADD SCREENSHOT HERE — OpenAPI /docs page showing all 34 routes]

---

## What it gives you

A unified day-2 picture across the layers most customers can't see in
one place today:

| Layer | What you see |
|---|---|
| **GPU compute** | Fleet heat-map, per-GPU temp/util/power/ECC, NVLink bandwidth, XID detection |
| **Network fabric** | Cisco Nexus port health, RoCEv2 compliance (PFC + ECN + MTU 9216), transceiver power, FEC counters |
| **Storage** | VAST/WEKA/GPFS throughput, capacity, GPUDirect Storage adoption, latency p99 |
| **Workload** | Slurm + Kubernetes + Run:ai jobs unified, queue depth, fairshare |
| **FinOps** | Per-team utilization, idle-allocated GPU detection, $/GPU-hour chargeback, weekly recoverable spend |
| **Alerts** | Threshold-based (XID, ECC, PSU, thermal, FEC, RoCEv2, GDS), with auto-resolve |
| **Diagnostics** | DCGM diag (-r 1/2/3/4), NCCL all-reduce sweeps, HPL TFLOPS validation, all auto-rendered |
| **Reports** | Weekly health PDF, monthly FinOps PDF, 5-phase NVIDIA acceptance report |

---

## Why "air-gapped" matters

Most monitoring stacks for AI infrastructure assume telemetry can leave
the cluster — Mission Control sends to NVIDIA's SaaS, Datadog runs in
the cloud, Run:ai's analytics rely on hosted services. For:

- **Regulated banks** under SR 11-7 / SOX
- **Hospital systems** under HIPAA
- **Federal agencies** under FISMA/FedRAMP
- **Defense industrial base** customers under CMMC

…that's a non-starter. This platform installs offline as a single
`.tar.gz`, stores all data on-prem, integrates with on-prem ServiceNow,
and never reaches back to a vendor cloud.

[ADD SCREENSHOT HERE — alerts panel with critical/warning/info]

---

## Architecture

```
gpu_fleet_platform.py            ─── single Python file, ~1700 lines
   ├─ FastAPI app (8 REST namespaces + 1 WebSocket)
   │     /api/nodes      /api/gpus      /api/network
   │     /api/storage    /api/finops    /api/alerts
   │     /api/reports    /api/jobs      /ws
   ├─ Three async collectors
   │     NVIDIA: DCGM REST + nvidia-smi over SSH + BCM API  (every 30s)
   │     Cisco:  UCSM XML API + NXAPI + SNMP                 (every 60s)
   │     VAST:   VAST REST API + GDS adoption checks         (every 30s)
   ├─ Threshold-based alert evaluator (auto-resolve when cleared)
   ├─ Embedded React-style HTML dashboard
   ├─ SQLite persistence (no Postgres in v1)
   └─ In-process cache (no Redis in v1)
```

For production-scale (1000+ nodes), the v1 SQLite + in-process design
is replaced by PostgreSQL + Redis. The interface contracts stay the
same so the swap is mechanical.

---

## API surface

The platform exposes 34 REST routes plus a real-time WebSocket. Highlights:

```
GET  /api/gpus                       fleet GPU snapshot, refreshed every 30s
GET  /api/gpus/at-risk               GPUs with concerning ECC/thermal trends
GET  /api/gpus/{uuid}/ecc-trend      24-hour ECC trajectory + projection
POST /api/nodes/{id}/diagnostic      kick off DCGM diag (level 1-4)
POST /api/nodes/{id}/bug-report      auto-collect nvidia-bug-report.sh
GET  /api/network/roce-config        per-port PFC/ECN/MTU compliance check
POST /api/network/validate-cabling   LLDP-vs-expected-topology audit
GET  /api/storage/gpu-analysis       throughput adequacy vs GPU count
GET  /api/finops/waste-analysis      idle-allocated GPUs by project
POST /api/diagnostics/dcgm           run a DCGM diagnostic
POST /api/diagnostics/nccl           run an NCCL all-reduce sweep
POST /api/diagnostics/hpl            run an HPL TFLOPS validation
POST /api/reports/health-report      generate weekly health PDF
POST /api/reports/acceptance-report  NVIDIA 5-phase acceptance run
```

Full Swagger UI at <http://localhost:8000/docs>.

[ADD SCREENSHOT HERE — diagnostics panel after running NCCL all-reduce]

---

## What's simulated vs. real

This repo runs in **demo mode** out of the box — collectors generate
realistic synthetic data so you can evaluate the platform on a laptop.
Switching to **production mode** is a matter of replacing the body of
each collector's `run_once()` method with real client code:

```python
# Demo mode
async def run_once(self):
    metrics = generate_realistic_mock_metrics()

# Production mode
async def run_once(self):
    metrics = await self.bcm_client.fetch_node_status()
    metrics += await self.dcgm_client.query_gpus()
    ...
```

The seam is clean — the alert evaluator, WebSocket broadcast, dashboard,
and report generators don't change.

---

## Deployment notes

For production, swap:

| Component | v1 (this repo) | Production |
|---|---|---|
| Database | SQLite | PostgreSQL 15 |
| Cache | In-process dict | Redis 7 |
| Async tasks | asyncio | Celery + Redis broker |
| Auth | None (read-only) | JWT with 8-hour expiry |
| Frontend | Embedded HTML | Standalone React/Vite SPA |
| Distribution | Single .py file | Docker Compose / Helm chart |

The v1 single-file design is intentional for evaluations and
air-gapped pilots. Customer ops teams can `scp` it to a monitoring VM
on the management VLAN and have a working dashboard in 5 minutes.

---

## Roadmap

- [ ] Multi-cluster federation
- [ ] LLM-driven triage agent (auto-open ServiceNow tickets with evidence)
- [ ] Native VAST/WEKA/GPFS adapters (currently abstracted)
- [ ] Acceptance test as code (5-phase pipeline auto-runs against a new cluster)
- [ ] Compliance evidence packs (SOC2, HIPAA, FedRAMP control mappings)

---

## Author

Built by **Baljeet Bhasin** as a portfolio piece during the transition
from compute-org TPM at Oracle OCI into AI infrastructure roles.
NCP-AII and NCA-AIIO certified.

Reach me on LinkedIn: <https://www.linkedin.com/in/baljeetbhasin>
or bbhasin@gmail.com.

---

## License

MIT — see `LICENSE`. Use it, fork it, deploy it, build on it. If you
deploy it on a real cluster, I'd value a note on what you found.
