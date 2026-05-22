"""
PBS Monitor Web Dashboard — FastAPI backend

Serves the live dashboard frontend and provides REST API endpoints
for system info, node state snapshots, running jobs, and queue status.
Reads from the same SQLite database the PBS Monitor daemon writes to.
"""

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import re

import json as _json

from pbs_monitor.database.models import (
    Job, JobState, Node, NodeSnapshot, SystemSnapshot,
    DataCollectionLog,
)

# State character → human-readable label
STATE_CHAR_LABELS = {
    'A': 'free', 'B': 'offline', 'C': 'down', 'D': 'busy',
    'E': 'job-exclusive', 'F': 'job-sharing', 'G': 'reserve',
    'H': 'resv-exclusive', 'I': 'down,offline',
    'J': 'state-unknown,down', 'K': 'state-unknown,down,offline',
    'L': 'job-exclusive,resv-exclusive', 'M': 'offline,resv-exclusive',
    'N': 'unknown',
}

# Static files directory (relative to this package)
STATIC_DIR = Path(__file__).parent / "static"


def _parse_walltime(wt: str | None) -> int | None:
    """Parse HH:MM:SS walltime string to total seconds."""
    if not wt:
        return None
    try:
        parts = wt.split(':')
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except (ValueError, IndexError):
        return None


def _short_job_id(job_id: str) -> str:
    """Strip PBS server suffix: '7159563.polaris-pbs-01...' → '7159563'."""
    return job_id.split('.')[0] if job_id else job_id


def _parse_execution_nodes(exec_node: str | None) -> list[str]:
    """
    Parse PBS execution_node field to a list of node names.
    Format: 'x3001c0s1b0n0/0*64+x3001c0s1b1n0/0*64+...'
    """
    if not exec_node:
        return []
    names = []
    for chunk in exec_node.split('+'):
        name = chunk.split('/')[0].strip()
        if name:
            names.append(name)
    return names


def _parse_eligible_time(et: str | None) -> int:
    """Parse PBS eligible_time 'HH:MM:SS' to seconds."""
    if not et:
        return 0
    try:
        parts = et.split(':')
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except (ValueError, IndexError):
        return 0


def _extract_job_score(job) -> float | None:
    """Compute approximate scheduling score from raw PBS data.

    PBS uses a configurable job_sort_formula. The most common formula on
    Polaris is based on eligible_time (seconds the job has been eligible
    to run). We compute: eligible_time_seconds as the score, which
    mirrors the dominant scheduler ordering.
    """
    if not job.raw_pbs_data:
        return None
    try:
        raw = _json.loads(job.raw_pbs_data) if isinstance(job.raw_pbs_data, str) else job.raw_pbs_data
    except (ValueError, TypeError):
        return None
    et = raw.get('eligible_time')
    if et:
        return float(_parse_eligible_time(et))
    return None


def _detect_system_name(db: Session) -> str:
    """Infer the system name from job IDs in the database."""
    sample = db.query(Job.job_id).filter(Job.job_id.isnot(None)).limit(10).all()
    for (jid,) in sample:
        # e.g. "7159563.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov"
        m = re.search(r'\.(\w+)-pbs', jid)
        if m:
            return m.group(1)
    return "unknown"


def _build_topology(db: Session) -> dict:
    """
    Build rack topology from Cray node naming conventions.
    Returns {rack_names: [...], nodes_per_rack: [...]}
    """
    nodes = (
        db.query(Node.name)
        .filter(Node.name.like('x%'))
        .order_by(Node.snapshot_index)
        .all()
    )
    rack_map: dict[str, list[str]] = {}
    for (name,) in nodes:
        rack_id = name[:5]  # e.g. 'x3001'
        rack_map.setdefault(rack_id, []).append(name)

    rack_names = sorted(rack_map.keys())
    nodes_per_rack = [len(rack_map[r]) for r in rack_names]
    return {"rack_names": rack_names, "nodes_per_rack": nodes_per_rack}


def _build_node_index(db: Session) -> tuple[list[str], list[int]]:
    """Ordered compute node names and their snapshot_data indices."""
    rows = (
        db.query(Node.name, Node.snapshot_index)
        .filter(Node.name.like('x%'))
        .order_by(Node.snapshot_index)
        .all()
    )
    names = [r.name for r in rows]
    indices = [r.snapshot_index for r in rows]
    return names, indices


# ---------------------------------------------------------------------------
# App factory — called by the CLI `web` command
# ---------------------------------------------------------------------------

def create_app(config=None) -> FastAPI:
    """Create and configure the FastAPI application."""
    # Resolve database URL
    if config is None:
        from pbs_monitor.config import Config
        config = Config()

    db_url = config.database.url
    connect_args: dict[str, Any] = {}
    if db_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False

    engine = create_engine(db_url, connect_args=connect_args)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    app = FastAPI(title="PBS Monitor Dashboard")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---- dependency ----
    def get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    # ---- cached system info ----
    _system_cache: dict[str, Any] = {}

    def _populate_system_cache(db: Session) -> dict[str, Any]:
        """Build and cache system info."""
        if _system_cache:
            return _system_cache

        system_name = _detect_system_name(db)
        total_nodes = db.query(func.count(Node.name)).filter(Node.name.like('x%')).scalar() or 0
        topology = _build_topology(db)
        node_names, snapshot_indices = _build_node_index(db)

        last_log = (
            db.query(DataCollectionLog.timestamp)
            .order_by(DataCollectionLog.timestamp.desc())
            .first()
        )

        info = {
            "system_name": system_name,
            "total_nodes": total_nodes,
            "topology": topology,
            "node_index": node_names,
            "snapshot_indices": snapshot_indices,
            "last_collection": last_log[0].isoformat() if last_log else None,
        }
        _system_cache.update(info)
        return info

    @app.get("/api/system")
    def api_system(db: Session = Depends(get_db)):
        return _populate_system_cache(db)

    @app.get("/api/snapshot")
    def api_snapshot(db: Session = Depends(get_db)):
        now = datetime.now(timezone.utc)

        # Ensure system cache is populated (needed for snapshot_indices)
        if not _system_cache:
            _populate_system_cache(db)

        # --- freshest data timestamp ---
        latest_collection = (
            db.query(func.max(DataCollectionLog.timestamp)).scalar()
        )

        # --- system aggregate ---
        sys_snap = (
            db.query(SystemSnapshot)
            .order_by(SystemSnapshot.timestamp.desc())
            .first()
        )

        # --- node state string ---
        node_snap = (
            db.query(NodeSnapshot)
            .order_by(NodeSnapshot.timestamp.desc())
            .first()
        )
        state_string = node_snap.snapshot_data if node_snap else ""

        # State counts — only for compute nodes (use their snapshot indices)
        compute_indices = _system_cache.get("snapshot_indices", [])
        state_counts: dict[str, int] = {}
        for si in compute_indices:
            if si < len(state_string):
                ch = state_string[si]
                label = STATE_CHAR_LABELS.get(ch, "unknown")
                state_counts[label] = state_counts.get(label, 0) + 1

        # --- node name → snapshot_index lookup (compute nodes only) ---
        node_map: dict[str, int] = {
            n.name: n.snapshot_index
            for n in db.query(Node.name, Node.snapshot_index)
            .filter(Node.name.like('x%'))
            .all()
        }

        # --- running jobs ---
        running_rows = db.query(Job).filter(Job.state == JobState.RUNNING).all()
        running_jobs = []
        for job in running_rows:
            elapsed = 0
            remaining = 0
            wall_secs = _parse_walltime(job.walltime)
            if job.start_time:
                start_utc = job.start_time
                if start_utc.tzinfo is None:
                    start_utc = start_utc.replace(tzinfo=timezone.utc)
                elapsed = int((now - start_utc).total_seconds())
                if wall_secs is not None:
                    remaining = max(0, wall_secs - elapsed)

            exec_names = _parse_execution_nodes(job.execution_node)
            node_indices = [node_map[n] for n in exec_names if n in node_map]

            queue_time = job.queue_time_seconds
            if queue_time is None and job.start_time and job.submit_time:
                st = job.start_time
                su = job.submit_time
                if st.tzinfo is None:
                    st = st.replace(tzinfo=timezone.utc)
                if su.tzinfo is None:
                    su = su.replace(tzinfo=timezone.utc)
                queue_time = int((st - su).total_seconds())

            running_jobs.append({
                "job_id": _short_job_id(job.job_id),
                "full_job_id": job.job_id,
                "name": job.job_name or "",
                "owner": job.owner or "",
                "project": job.project or "",
                "allocation_type": job.allocation_type or "",
                "queue": job.queue or "",
                "nodes": job.nodes or len(exec_names) or 1,
                "walltime": job.walltime or "",
                "elapsed_seconds": elapsed,
                "remaining_seconds": remaining,
                "queue_time_seconds": queue_time or 0,
                "node_indices": node_indices,
            })

        # --- queued jobs (full detail for table) ---
        queued_rows = db.query(Job).filter(Job.state == JobState.QUEUED).all()
        queued_jobs = []
        for job in queued_rows:
            queue_time = 0
            if job.submit_time:
                su = job.submit_time
                if su.tzinfo is None:
                    su = su.replace(tzinfo=timezone.utc)
                queue_time = int((now - su).total_seconds())

            # Extract score from raw PBS data (eligible_time based)
            score = _extract_job_score(job)

            queued_jobs.append({
                "job_id": _short_job_id(job.job_id),
                "full_job_id": job.job_id,
                "name": job.job_name or "",
                "owner": job.owner or "",
                "project": job.project or "",
                "allocation_type": job.allocation_type or "",
                "queue": job.queue or "",
                "nodes": job.nodes or 1,
                "walltime": job.walltime or "",
                "queue_time_seconds": queue_time,
                "score": score,
            })

        # --- held jobs count ---
        held_count = db.query(func.count(Job.job_id)).filter(Job.state == JobState.HELD).scalar() or 0

        # --- queue counts for queue status bars ---
        queued_by_q: dict[str, int] = {}
        for j in queued_jobs:
            q = j["queue"]
            queued_by_q[q] = queued_by_q.get(q, 0) + 1
        held_by_q = dict(
            db.query(Job.queue, func.count(Job.job_id))
            .filter(Job.state == JobState.HELD)
            .group_by(Job.queue)
            .all()
        )

        # --- queue status from current job state ---
        # Use actual job counts rather than potentially-stale queue snapshots
        running_by_q = dict(
            db.query(Job.queue, func.count(Job.job_id))
            .filter(Job.state == JobState.RUNNING)
            .group_by(Job.queue)
            .all()
        )
        all_queue_names = set(running_by_q) | set(queued_by_q) | set(held_by_q)

        queues = []
        for qname in all_queue_names:
            if not qname:
                continue
            r = running_by_q.get(qname, 0)
            q = queued_by_q.get(qname, 0)
            h = held_by_q.get(qname, 0)
            total = r + q + h
            if total == 0:
                continue
            queues.append({
                "name": qname,
                "running": r,
                "queued": q,
                "held": h,
                "total": total,
                "runningPct": round(r / total * 100, 1),
                "queuedPct": round(q / total * 100, 1),
                "heldPct": round(h / total * 100, 1),
            })

        # Use the freshest timestamp available
        best_ts = latest_collection or (
            sys_snap.timestamp if sys_snap else
            node_snap.timestamp if node_snap else None
        )

        return {
            "timestamp": best_ts.isoformat() if best_ts else None,
            "system": {
                "running_jobs": sys_snap.running_jobs if sys_snap else len(running_jobs),
                "queued_jobs": sys_snap.queued_jobs if sys_snap else sum(queued_by_q.values()),
                "held_jobs": sys_snap.held_jobs if sys_snap else sum(held_by_q.values()),
                "utilization_percent": round(sys_snap.system_utilization_percent or 0, 1) if sys_snap else 0,
                "total_nodes": sys_snap.total_nodes if sys_snap else 0,
                "available_nodes": sys_snap.available_nodes if sys_snap else 0,
            },
            "state_string": state_string,
            "state_counts": state_counts,
            "jobs": {
                "running": running_jobs,
                "queued": queued_jobs,
            },
            "queues": queues,
        }

    @app.get("/api/jobs/{job_id}")
    def api_job_detail(job_id: str, db: Session = Depends(get_db)):
        # Try exact match first, then with short id
        job = db.query(Job).filter(Job.job_id == job_id).first()
        if not job:
            job = db.query(Job).filter(Job.job_id.like(f"{job_id}.%")).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {
            "job_id": job.job_id,
            "job_name": job.job_name,
            "owner": job.owner,
            "project": job.project,
            "state": job.state.value if job.state else None,
            "queue": job.queue,
            "nodes": job.nodes,
            "walltime": job.walltime,
            "submit_time": job.submit_time.isoformat() if job.submit_time else None,
            "start_time": job.start_time.isoformat() if job.start_time else None,
            "end_time": job.end_time.isoformat() if job.end_time else None,
            "execution_node": job.execution_node,
            "total_cores": job.total_cores,
            "actual_runtime_seconds": job.actual_runtime_seconds,
            "queue_time_seconds": job.queue_time_seconds,
        }

    # ---- static files ----
    # Serve index.html at root, everything else from /static
    @app.get("/")
    async def serve_index():
        return FileResponse(STATIC_DIR / "index.html")

    app.mount("/css", StaticFiles(directory=str(STATIC_DIR / "css")), name="css")
    app.mount("/js", StaticFiles(directory=str(STATIC_DIR / "js")), name="js")

    return app


# Allow `python -m pbs_monitor.web.server` for quick testing
if __name__ == "__main__":
    import uvicorn
    app = create_app()
    uvicorn.run(app, host="127.0.0.1", port=8080)
