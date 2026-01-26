# CLI Reference

Generated from `pbs-monitor --help` output.

## Global Options

```
pbs-monitor [-h] [-c CONFIG] [-v] [-q] [--log-file LOG_FILE]
            [--use-sample-data] [--max-width MAX_WIDTH] [--auto-width]
            [--no-expand] [--wrap]
            {status,jobs,nodes,queues,resv,history,analyze,score-formula,replay,config,database,daemon}
```

| Option | Description |
|--------|-------------|
| `-c, --config` | Configuration file path |
| `-v, --verbose` | Enable verbose logging |
| `-q, --quiet` | Suppress normal output |
| `--log-file` | Log file path |
| `--use-sample-data` | Use sample JSON data for testing |
| `--max-width` | Maximum table width |
| `--auto-width` | Auto-detect terminal width |
| `--no-expand` | Don't expand columns to fit content |
| `--wrap` | Enable word wrapping in table cells |

---

## status

Show PBS system status.

```
pbs-monitor status [-r] [--collect] [--queue-depth]
```

| Option | Description |
|--------|-------------|
| `-r, --refresh` | Force refresh of data |
| `--collect` | Persist data to database |
| `--queue-depth` | Show detailed queue depth breakdown by job size |

---

## jobs

Show job information.

```
pbs-monitor jobs [-u USER] [-p PROJECT] [-q QUEUE] [-s STATE] [-r]
                 [--columns COLUMNS] [--sort SORT] [--reverse] [--collect]
                 [-d] [--history] [--format {table,detailed,json}] [--show-raw]
                 [job_ids ...]
```

| Option | Description |
|--------|-------------|
| `-u, --user` | Filter by username |
| `-p, --project` | Filter by project name |
| `-q, --queue` | Filter by queue name |
| `-s, --state` | Filter by state: R, Q, H, W, T, E, S, C, F |
| `-r, --refresh` | Force refresh of data |
| `--columns` | Columns to display (comma-separated) |
| `--sort` | Sort by column (default: score) |
| `--reverse` | Reverse sort order |
| `--collect` | Persist data to database |
| `-d, --detailed` | Show detailed job info |
| `--history` | Include job history from database |
| `--format` | Output format: table, detailed, json |
| `--show-raw` | Show raw PBS attributes |

**Available columns:** job_id, name, owner, project, allocation, state, queue, nodes, ppn, walltime, walltime_actual, memory, submit_time, start_time, end_time, runtime, priority, cores, score, queue_time, exit_status, execution_node

---

## nodes

Show node information.

```
pbs-monitor nodes [-s STATE] [-r] [--columns COLUMNS] [-d] [--collect] [node_ids ...]
```

| Option | Description |
|--------|-------------|
| `-s, --state` | Filter by state: free, offline, down, busy, job-exclusive, etc. |
| `-r, --refresh` | Force refresh of data |
| `--columns` | Columns to display |
| `-d, --detailed` | Show detailed table format |
| `--collect` | Persist data to database |

---

## queues

Show queue information.

```
pbs-monitor queues [-r] [--columns COLUMNS] [--collect]
```

| Option | Description |
|--------|-------------|
| `-r, --refresh` | Force refresh of data |
| `--columns` | Columns to display |
| `--collect` | Persist data to database |

---

## history

Show historical job information from database.

```
pbs-monitor history [-u USER] [-p PROJECT] [-d DAYS] [-s STATE]
                    [--columns COLUMNS] [--sort SORT] [--reverse]
                    [--limit LIMIT] [--include-pbs-history]
```

| Option | Description |
|--------|-------------|
| `-u, --user` | Filter by username |
| `-p, --project` | Filter by project name |
| `-d, --days` | Days to look back (default: 30) |
| `-s, --state` | Filter by state: C, F, E, UNKNOWN_END, all |
| `--columns` | Columns to display |
| `--sort` | Sort column (default: submit_time) |
| `--reverse` | Reverse sort order |
| `--limit` | Max jobs to show (default: 100) |
| `--include-pbs-history` | Include recent jobs from qstat -x |

---

## resv

Reservation information and management.

### resv list

```
pbs-monitor resv list [-u USER] [-s STATE] [-r] [--collect] [--format {table,json}] [--columns COLUMNS]
```

### resv show

```
pbs-monitor resv show [--format {table,json,yaml}] [--show-nodes] [reservation_ids ...]
```

---

## analyze

Analytics and analysis commands.

### analyze run-now

Suggest a job shape you can run immediately without delaying queued jobs.

```
pbs-monitor analyze run-now [-b BUFFER_MINUTES] [--format {table,json}] [-r]
```

| Option | Description |
|--------|-------------|
| `-b, --buffer-minutes` | Safety buffer in minutes (default: 8) |
| `--format` | Output format |
| `-r, --refresh` | Force refresh |

### analyze run-score

Analyze job scores at queue→run transitions.

```
pbs-monitor analyze run-score [-d DAYS] [--format {table,csv}]
```

### analyze walltime-efficiency-by-user

Analyze walltime efficiency by user.

```
pbs-monitor analyze walltime-efficiency-by-user [-d DAYS] [-u USER] [--min-jobs MIN]
                                                [-q QUEUE] [--min-nodes N] [--max-nodes N]
                                                [--format {table,csv}]
```

### analyze walltime-efficiency-by-project

Analyze walltime efficiency by project.

```
pbs-monitor analyze walltime-efficiency-by-project [-d DAYS] [--format {table,csv}]
```

### analyze leaderboard

Show top users and projects by node-hours.

```
pbs-monitor analyze leaderboard [-d DAYS] [-w WEEKS] [-n TOP_N]
                                [--min-node-hours MIN] [--include-running]
                                [--include-queued] [--format {table,csv}]
```

| Option | Description |
|--------|-------------|
| `-d, --days` | Days to analyze (default: 30) |
| `-w, --weeks` | Weeks to analyze (shows weekly breakdown) |
| `-n, --top-n` | Top entries to show (default: 10) |
| `--include-running` | Include running jobs (default: True) |
| `--include-queued` | Include queued jobs |

### analyze usage-insights

Usage insights with derived metrics and plots.

```
pbs-monitor analyze usage-insights [-d DAYS] [-m MIN_QUEUE_NODE_HOURS] [-n TOP_N_QUEUES]
                                   [-R] [-a QUEUES...] [-x QUEUES...] [-o OUTPUT_DIR]
                                   [-P] [-f {table,csv}] [-t {H,D,W}]
```

| Option | Description |
|--------|-------------|
| `-d, --days` | Days to analyze (default: 30) |
| `-m` | Min node-hours per queue (default: 100) |
| `-n` | Limit to top-N queues |
| `-R, --incl-resv` | Include reservation queues |
| `-o, --output-dir` | Save plots to directory |
| `-P, --no-plots` | Skip plot generation |
| `-t, --ts-freq` | Time-series frequency: H, D, W |

### analyze time-comparison

Compare throughput and metrics between two time periods.

```
pbs-monitor analyze time-comparison --a-lower START --a-upper END
                                    --b-lower START --b-upper END
                                    [--group-by {queue,project,allocation_type}]
                                    [--output-dir DIR] [--format {table,csv}]
```

### analyze reservation-utilization

Analyze reservation utilization efficiency.

```
pbs-monitor analyze reservation-utilization [reservation_ids...] [--start-date DATE]
                                            [--end-date DATE] [--format {table,csv,json}]
                                            [--status STATUS] [-d DAYS]
```

### analyze reservation-trends

Analyze reservation utilization trends over time.

```
pbs-monitor analyze reservation-trends [-d DAYS] [-o OWNER] [-q QUEUE]
```

### analyze reservation-owner-ranking

Rank reservation owners by utilization efficiency.

```
pbs-monitor analyze reservation-owner-ranking [-d DAYS]
```

---

## score-formula

Display and explain the PBS job sort formula.

```
pbs-monitor score-formula [--raw] [--no-defaults] [-r] [--job-ids IDS...]
                          [--plot] [--output-dir DIR] [--nodes N] [--walltime HH:MM:SS]
                          [--project-priority N] [--sample-count N] [--max-time-hours N]
                          [--plot-grid] [--grid-nodes N...] [--grid-walltimes N...]
                          [--routing-queue QUEUE] [--log-scale]
```

| Option | Description |
|--------|-------------|
| `--raw` | Show only raw formula string |
| `--no-defaults` | Hide parameters table |
| `--plot` | Generate score evolution plots |
| `--plot-grid` | Generate grid plot of score components |
| `--output-dir` | Directory to save plots |
| `--sample-count` | Jobs to sample from queue (default: 6) |
| `--max-time-hours` | Max eligible time to plot (default: 48) |
| `--log-scale` | Use logarithmic y-axis |

---

## replay

Replay historical job timelines with visualization.

```
pbs-monitor replay [--start START] [--end END] [-u USER] [-q QUEUE] [-p PROJECT]
                   [--output-format {split-panel,text,timeline,waffle}]
                   [--step STEP] [--top-n N] [--live]
                   [--color-by {job,user,queue,project,allocation}]
                   [--grid-rows N] [--grid-cols N] [--small-job-threshold N]
                   [--output-dir DIR] [--frame-duration MS]
```

| Option | Description |
|--------|-------------|
| `--start` | Start time (ISO format or relative like '24h ago') |
| `--end` | End time (default: now) |
| `--output-format` | split-panel, text, timeline, or waffle |
| `--step` | Time step (e.g., '5m', '1h', default: 1h) |
| `--live` | Continuously update display |
| `--color-by` | Waffle chart coloring |
| `--output-dir` | Save waffle frames/GIF to directory |
| `--frame-duration` | GIF frame duration in ms (default: 1000) |

---

## config

Configuration management.

```
pbs-monitor config [--create] [--show]
```

| Option | Description |
|--------|-------------|
| `--create` | Create sample configuration file |
| `--show` | Show current configuration |

---

## database

Database management.

### database init

```
pbs-monitor database init [--force]
```

Initialize database. Use `--force` to drop existing tables.

### database status

```
pbs-monitor database status
```

Show database information and table counts.

### database validate

```
pbs-monitor database validate
```

Validate database schema.

### database backup

```
pbs-monitor database backup [backup_path]
```

Create database backup (SQLite only).

### database restore

```
pbs-monitor database restore <backup_path>
```

Restore database from backup.

### database cleanup

```
pbs-monitor database cleanup [--job-history-days N] [--snapshot-days N] [--force]
```

| Option | Description |
|--------|-------------|
| `--job-history-days` | Keep job history for N days (default: 365) |
| `--snapshot-days` | Keep snapshots for N days (default: 90) |
| `--force` | Skip confirmation prompt |

### database show

```
pbs-monitor database show -t TABLE [-a AFTER] [-b BEFORE] [-s START] [-n NUM_ROWS] [--format {table,csv}]
```

Show table data. Use `-a N` for last N rows, `-b N` for first N rows, or `-s START -n NUM` for a range.

---

## daemon

Background data collection daemon.

### daemon start

```
pbs-monitor daemon start [--foreground] [--pid-file PATH]
```

| Option | Description |
|--------|-------------|
| `--foreground, -f` | Run in foreground (don't detach) |
| `--pid-file` | PID file path (default: ~/.pbs_monitor_daemon.pid) |

### daemon stop

```
pbs-monitor daemon stop [--pid-file PATH]
```

### daemon status

```
pbs-monitor daemon status
```

Show daemon status and recent collection activity.
