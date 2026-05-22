# Web Dashboard Plan

## Overview

A lightweight web dashboard that provides a live view of the PBS system state — node map, running jobs, queue status, and system utilization. Designed to run on a login node of any ALCF system (Polaris, Aurora, etc.) and be accessed via SSH tunnel from a laptop browser.

## Goals

1. **Live system view** — at-a-glance picture of what the machine is doing right now
2. **System-agnostic** — auto-detects system name, node count, topology from PBS/DB; no hardcoded machine assumptions
3. **Extensible** — architecture that supports adding historical analytics, plots, and new views over time
4. **Minimal footprint** — runs as a single process on a login node; no heavy infrastructure

## Architecture

### Deployment Model

```
[Login Node (e.g. polaris-login-01)]              [Laptop]
  daemon → SQLite DB ← FastAPI :8080 ←── SSH tunnel ──── Browser
         (all on login node)                              localhost:8080
```

- Everything runs on the login node: daemon, SQLite DB, and FastAPI server
- The laptop only provides the SSH tunnel and the browser — no server-side processes
- User starts server manually on the login node: `pbs-monitor web --port 8080`
- User tunnels from laptop: `ssh -L 8080:localhost:8080 <login-node>`
- Opens `localhost:8080` in laptop browser
- No auth needed (single-user, localhost-only binding on login node)

### Tech Stack

| Layer | Choice | Rationale |
|-------|--------|-----------|
| **Backend** | FastAPI | Async, lightweight, auto-generates OpenAPI docs, good WebSocket support for future live push. Already have Python ecosystem. |
| **Frontend** | Vue 3 (via CDN, SFC) | Reactive data binding makes live dashboards easy. CDN-loaded = no Node.js build step needed on login nodes. Lighter than React for this use case. Scales well as we add views. |
| **Charting** | Chart.js or lightweight D3 | For utilization gauges, queue depth bars. Keep it simple. |
| **Node map** | Custom CSS Grid / Canvas | Neither charting lib handles 560–10,000 cell grids well. We'll build this as a dedicated component. |
| **Data transport** | REST (polling) initially | Frontend polls `/api/snapshot` every N seconds. WebSocket/SSE upgrade path exists but isn't needed for v1 since data only changes every 5 min anyway. |

### Why Vue 3 over alternatives

- **vs. vanilla JS**: We'll be adding views (analytics, plots, replay) — component model pays off quickly
- **vs. React**: Vue's single-file components + CDN mode means zero build tooling. React effectively requires a bundler. On a login node where we don't want to install Node.js, this matters.
- **vs. Svelte**: Svelte requires a compile step; can't run from CDN in production mode

### File Structure (proposed)

```
pbs_monitor/
  web/
    __init__.py
    server.py          # FastAPI app, API routes
    static/
      index.html       # SPA shell, loads Vue from CDN
      css/
        dashboard.css
      js/
        app.js          # Vue app + components
        node-map.js     # Node map rendering (Canvas-based)
        charts.js       # Chart.js wrappers
```

No build step. No `package.json`. Just Python + static files served by FastAPI.

## API Design

### System Info (auto-detection)

```
GET /api/system
```
Returns system identity derived from PBS at startup:
```json
{
  "system_name": "polaris",
  "total_nodes": 560,
  "total_cores": 36096,
  "topology": {
    "racks": 40,
    "nodes_per_rack": 14,
    "rack_names": ["x3001", "x3002", ...]
  },
  "daemon_interval_seconds": 300,
  "last_collection": "2026-05-22T08:17:08Z"
}
```

System name: detected from hostname, PBS server name, or DB content — not hardcoded.

Topology: parsed from node naming conventions (Cray `xRRRRc0sSSSbBnN` format). If naming doesn't follow a known pattern, fall back to a flat grid.

### Live Snapshot

```
GET /api/snapshot
```
Returns current system state (most recent DB snapshot):
```json
{
  "timestamp": "2026-05-22T08:17:08Z",
  "system": {
    "running_jobs": 29,
    "queued_jobs": 85,
    "held_jobs": 97,
    "utilization_percent": 62.6,
    "available_nodes": 330,
    "total_nodes": 561
  },
  "nodes": {
    "state_string": "NAAAA...",
    "state_counts": {
      "free": 210,
      "job-exclusive": 180,
      "offline": 45,
      "down": 8,
      ...
    },
    "node_index": ["polaris-login-03", "x3001c0s13b0n0", ...]
  },
  "jobs": {
    "running": [
      {
        "job_id": "7159563",
        "short_id": "7159563",
        "name": "s3_v2_mse",
        "owner": "rama",
        "project": "NLDesignProtein",
        "queue": "preemptable",
        "nodes": 10,
        "walltime": "72:00:00",
        "start_time": "2026-05-19T18:49:05Z",
        "elapsed": "62:28:03",
        "remaining": "09:31:57",
        "node_indices": [45, 46, 47, ...]
      },
      ...
    ],
    "queued_summary": {
      "by_queue": {"debug": 12, "small": 5, "medium": 3, ...},
      "total_queued_node_hours": 14523.5
    }
  },
  "queues": [
    {"name": "debug", "running": 8, "queued": 12, "held": 3},
    ...
  ]
}
```

Key design choices:
- **Node state string** sent as-is from `node_snapshots.snapshot_data` — 560 chars for Polaris, ~10K chars for Aurora. Compact enough even for large systems.
- **Node index** maps position→name. Sent once on page load via `/api/system`, not repeated every poll.
- **Job node indices** computed server-side by matching `execution_node` names against the node index. Enables hover-over-node → show-job-info on the frontend.
- **Short job IDs** stripped of the `.polaris-pbs-01.hsn.cm...` suffix for display.

### Queue Detail

```
GET /api/queues
```
Full queue state with limits, job counts, utilization.

### Job Detail

```
GET /api/jobs/{job_id}
```
Detailed info for a single job (for click-through from node map or job table).

### Historical Snapshots (future, v2+)

```
GET /api/history/utilization?hours=24
GET /api/history/queue-depth?hours=24
```
Time-series data for sparklines and trend charts.

## Frontend Design

### Layout

```
┌─────────────────────────────────────────────────────────────┐
│  [System Name]  ●  Utilization: 62.6%  │  Last update: ...  │
├────────────────────────┬────────────────────────────────────┤
│                        │                                    │
│      NODE MAP          │     RUNNING JOBS TABLE             │
│   (grid of colored     │   job_id | owner | project |      │
│    cells, one per      │   queue | nodes | elapsed |       │
│    node)               │   remaining                       │
│                        │                                    │
│                        │   (sortable, colored by queue)     │
│                        │                                    │
├────────────────────────┼────────────────────────────────────┤
│   QUEUE STATUS         │     SYSTEM STATS                   │
│   (compact bars or     │   (utilization over last 24h       │
│    table showing       │    sparkline, job throughput,      │
│    running/queued/     │    nodes free/busy/down pie)       │
│    held per queue)     │                                    │
└────────────────────────┴────────────────────────────────────┘
```

### Node Map Component — The Scaling Problem

This is the hardest visual challenge. Polaris (560 nodes) is manageable. Aurora (10,624 nodes) is not trivially displayable at one-cell-per-node.

#### Strategy: adaptive rendering

1. **Small systems (≤1,000 nodes):** CSS Grid or HTML Canvas. Each node is a small colored rectangle (~8–12px per side). 560 nodes fits in roughly a 40×14 grid = ~480px × 168px. Plenty of room. Hover shows tooltip with node name + job info.

2. **Large systems (1,000–10,000+ nodes):** HTML5 Canvas rendering (not DOM elements — 10K DOM nodes would be sluggish). Each node becomes ~3–5px. At 4px per cell, a 100×106 grid = 400×424px. Still fits on screen. Tooltip on hover via mouse-position → cell-index math.

3. **Extreme scale fallback (if needed):** aggregate by rack. Each "cell" represents a rack and shows a fill level (e.g., 12/14 nodes busy = 86% filled). This always fits regardless of system size. Could offer a drill-down: click rack → see its individual nodes.

#### Layout within the node map

Respect physical topology when we can detect it:
- **Polaris:** 40 racks × 14 nodes. Render as 40 columns × 14 rows (or vice versa), grouped and labeled by rack. Natural.
- **Aurora:** ~166 racks × 64 nodes (TBD, need to inspect naming). Same approach but at Canvas scale.
- **Unknown topology:** flat grid, row-major order by node index.

#### Color scheme

| State | Color | Notes |
|-------|-------|-------|
| free | light green | Available |
| job-exclusive | blue (shade by job) | Running a job — different shade per distinct job for visual clustering |
| resv-exclusive | purple | Reserved |
| offline | gray | Administratively offline |
| down | red | Down/broken |
| job-exclusive+resv-exclusive | teal | Running inside a reservation |
| unknown | dark gray | No data |

Job-shading: when a single job spans many nodes, color them the same shade of blue so you can visually see job boundaries on the map. Hash the job_id to pick the shade.

#### Interaction

- **Hover a node** → tooltip: node name, state, job_id (if any), owner, project, queue, elapsed time
- **Click a node with a job** → highlight all nodes belonging to that job, scroll to it in the jobs table
- **Click a node that's down/offline** → show since-when (if we have history)

### Running Jobs Table

- Columns: Job ID, Name, Owner, Project, Queue, Nodes, Walltime, Elapsed, Remaining, Progress bar
- Sorted by nodes descending (big jobs first) by default
- Color-code rows by queue (matching queue status colors)
- Hover a row → highlight that job's nodes on the map
- Click → expand to show execution nodes, raw details

### Queue Status Panel

- One row per active queue
- Bar showing: ████ running ░░░ queued ▒▒▒ held
- Numbers alongside
- Compact enough to fit 10–15 queues without scrolling

### System Stats

- Big utilization % number (hero metric)
- Donut chart: free / job-exclusive / offline / down / reserved
- If we add v2 history endpoints: 24h utilization sparkline underneath

## Auto-Refresh Behavior

- Frontend polls `/api/snapshot` every 30 seconds
- Visual indicator: "Last updated: 12s ago" counter, green dot when fresh
- If data hasn't changed (same `timestamp`), skip re-render
- Stale threshold: if `timestamp` is >15 minutes old, show warning (daemon may be down)

## CLI Integration

New subcommand:

```bash
pbs-monitor web [--port PORT] [--host HOST] [--no-browser]
```

- Default: `--host 127.0.0.1 --port 8080`
- Binds localhost only (no auth needed)
- Reads same config file / DB as all other commands
- Prints: "Dashboard running at http://127.0.0.1:8080 — tunnel with: ssh -L 8080:localhost:8080 <this-host>"

### Dependencies

Add to `requirements.txt`:
```
fastapi>=0.100.0
uvicorn[standard]>=0.20.0
```

Minimal additions. Both are pure-Python-friendly and pip-installable on login nodes without system packages.

## Implementation Phases

### Phase 1: Foundation + Node Map (this PR)
- [ ] FastAPI server skeleton with static file serving
- [ ] `/api/system` — auto-detect system name, topology, node index
- [ ] `/api/snapshot` — current node states, running jobs, queue summary
- [ ] Frontend: node map component (Canvas-based, adaptive scaling)
- [ ] Frontend: system utilization header bar
- [ ] Frontend: basic running jobs table
- [ ] `pbs-monitor web` CLI subcommand
- [ ] Test against Polaris DB locally

### Phase 2: Interactivity + Polish
- [ ] Node map hover tooltips (node→job lookup)
- [ ] Click node → highlight job; click job → highlight nodes
- [ ] Queue status panel
- [ ] Job progress bars (elapsed/walltime)
- [ ] Auto-refresh with staleness indicator
- [ ] Responsive layout for different screen sizes

### Phase 3: Historical + Analytics
- [ ] `/api/history/utilization` — 24h utilization time series
- [ ] Utilization sparkline on dashboard
- [ ] Serve pre-generated analytics plots via `/api/plots/latest`
- [ ] Reservation timeline view

### Phase 4: Advanced Features
- [ ] WebSocket push (replace polling)
- [ ] Job lifecycle replay in browser (port existing waffle renderer)
- [ ] Anomaly feed / alerts strip
- [ ] Project burn rate / allocation tracking

## Open Questions

1. **Aurora topology**: Need to inspect Aurora node naming to build the rack layout parser. The Cray `xRRRRc0sSSSbBnN` pattern should be similar but rack/slot counts will differ. May need a sample `pbsnodes` dump from Aurora.

2. **Job-to-node mapping granularity**: The `execution_node` field gives us node assignments for running jobs. For the node map to show which job is on which node, we parse this server-side. Need to verify this field is reliably populated across systems.

3. **Login node resource constraints**: FastAPI + uvicorn is lightweight, but should we add any explicit resource limits (max workers, memory caps) to be a good citizen on shared login nodes?

4. **Multi-DB support**: If you run dashboards for both Polaris and Aurora, do you want one server serving both, or separate instances? Separate seems simpler for v1.
