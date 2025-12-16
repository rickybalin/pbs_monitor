# Database Guide

PBS Monitor persists PBS data for historical analysis and richer queries.

## Features

- SQLite for development; PostgreSQL for production
- Automatic schema creation and migration
- Connection pooling and concurrent access support

## Commands

```bash
# Initialize database
pbs-monitor database init

# Status and validation
pbs-monitor database status
pbs-monitor database validate

# Backup / Restore (SQLite only)
pbs-monitor database backup [path]
pbs-monitor database restore <path>

# Cleanup old data
pbs-monitor database cleanup --job-history-days 365 --snapshot-days 90

# Migrate schema
pbs-monitor database migrate
```

## On-demand collection

```bash
# Collect data while running commands
pbs-monitor status --collect
pbs-monitor jobs --collect
pbs-monitor nodes --collect
pbs-monitor queues --collect
```

## Completed jobs and history

```bash
# View historical jobs
pbs-monitor history

# User filter and lookback window
pbs-monitor history -u username --days 30

# Include recent PBS completed jobs
pbs-monitor history --include-pbs-history

# Sort and limit
pbs-monitor history -s F --sort runtime --reverse --limit 50
```

## Schema overview

- jobs: current/final job state (one per job)
- job_history: every job state change
- reservations & reservation_history: reservation lifecycle tracking
- queues, nodes: configuration and properties (nodes include a `snapshot_index` for compact snapshots)
- queue_snapshots: queue stats over time
- node_snapshots: single string per collection representing all node states (one character per node slot)
- system_snapshots: overall cluster metrics
- data_collection_log: audit trail of collection events

Node snapshots are now stored as a compact string so each collection writes a single row regardless of cluster size. When a node does not report during a collection, its slot contains `0`.

## Migrating legacy databases

If you initialized a fresh database and want to backfill jobs/reservations from an older file, use the helper script:

```bash
python scripts/migrate_db.py \
  --source /path/to/pbs_monitor.db_2025-12-15 \
  --dest   /path/to/pbs_monitor.db
```

By default the script copies `jobs`, `job_history`, `reservations`, `reservation_history`, and `reservation_utilization`, truncating the destination tables before inserting. Use `--tables` to adjust the list or `--keep-existing` to merge without truncation. Node snapshots are intentionally skipped, since the new schema stores them in a different format.

