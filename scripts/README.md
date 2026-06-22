# Utility Scripts

Standalone helpers that complement the `pbs-monitor` CLI. They are not installed as console entry points — run them directly from this directory.

## End-user tools

### `job_summary`

Quick one-shot summary for one or more PBS jobs: state, queue, size, requested walltime, time spent in the queue, and any scheduler comment. Wraps `qstat -f` and pulls out just the fields most useful when checking on a submission.

Does **not** require the PBS Monitor database or daemon — only `qstat` in `PATH`.

```bash
# Single job
./job_summary 12345

# Multiple jobs (space- or comma-separated)
./job_summary 12345 12346
./job_summary 12345,12346
```

### `recent_finished_jobs.py`

Print all finished jobs from the last 24 hours as CSV, sourced from the PBS Monitor database. Useful for spreadsheet or pandas analysis.

Requires a populated PBS Monitor database (see the top-level [README](../README.md)).

```bash
./recent_finished_jobs.py              # to stdout
./recent_finished_jobs.py -o jobs.csv  # to file
./recent_finished_jobs.py -v           # verbose logging
```

## Maintenance tools

### `migrate_db_new_nodesnapshot.py`

Migrate selected tables (jobs, reservations, and their history) from an existing PBS Monitor database into a freshly initialized one with the newer compact node-snapshot schema.

```bash
python migrate_db_new_nodesnapshot.py \
    --source /path/to/pbs_monitor.db_old \
    --dest   /path/to/pbs_monitor.db
```

### `generate_cli_reference.py`

Regenerate `docs/user/cli_reference.md` from the live `pbs-monitor --help` output. Run after adding or changing CLI commands.

```bash
python generate_cli_reference.py
```
