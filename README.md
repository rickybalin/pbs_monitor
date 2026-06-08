# PBS Monitor

A CLI toolkit for monitoring PBS scheduler environments with historical data storage and analytics.

## Quick Start

### Installation

```bash
git clone https://github.com/jtchilders/pbs_monitor.git
cd pbs_monitor

# Create and activate a virtual environment (recommended)
python -m venv venv
source venv/bin/activate

# Install (dependencies are installed automatically)
pip install -e .
```

**Requirements:** Python 3.8+, PBS commands (`qstat`, `pbsnodes`) in PATH.

### Database Setup

**Option A: Start fresh**
```bash
pbs-monitor config --create   # Creates ~/.pbs_monitor.yaml
pbs-monitor database init      # Creates database (default: ~/.pbs_monitor.db)
```

**Note:** The default database location is `~/.pbs_monitor.db`. On large systems this file can grow significantly. Consider editing `~/.pbs_monitor.yaml` to place it on project space before running `database init`.

**Option B: Use an existing database**
```bash
pbs-monitor config --create
# Edit ~/.pbs_monitor.yaml to set database.url to your existing database path
```

### Basic Commands

```bash
pbs-monitor status              # System overview
pbs-monitor jobs                # Current jobs
pbs-monitor jobs -u myuser      # Filter by user
pbs-monitor nodes               # Node status
pbs-monitor queues              # Queue info
pbs-monitor history -d 7        # Completed jobs from last 7 days
```

## Commands

### Monitoring
| Command | Description |
|---------|-------------|
| `pbs-monitor status` | System status summary |
| `pbs-monitor jobs` | Job listing with filters (`-u`, `-p`, `-q`, `-s`) |
| `pbs-monitor nodes` | Node status and utilization |
| `pbs-monitor queues` | Queue information |
| `pbs-monitor history` | Historical completed jobs from database |
| `pbs-monitor resv` | Reservation listing and details |

### Analytics
| Command | Description |
|---------|-------------|
| `pbs-monitor analyze run-now` | Find job shapes that can start immediately |
| `pbs-monitor analyze run-score` | Analyze job scores at queue→run transitions |
| `pbs-monitor analyze walltime-efficiency-by-user` | Walltime efficiency per user |
| `pbs-monitor analyze walltime-efficiency-by-project` | Walltime efficiency per project |
| `pbs-monitor analyze reservation-utilization` | Reservation utilization analysis |
| `pbs-monitor analyze leaderboard` | Top users/projects by node-hours |
| `pbs-monitor analyze usage-insights` | Derived usage metrics and plots |
| `pbs-monitor analyze time-comparison` | Compare metrics across time windows |

### Visualization
| Command | Description |
|---------|-------------|
| `score-formula` | Display and plot PBS job sort formula |
| `replay` | Replay historical job timelines (terminal, waffle charts, GIFs) |

### Database & Daemon
| Command | Description |
|---------|-------------|
| `pbs-monitor database init` | Initialize database schema |
| `pbs-monitor database status` | Show database info and table counts |
| `pbs-monitor database backup` | Create backup (SQLite) |
| `pbs-monitor database cleanup` | Remove old data |
| `pbs-monitor daemon start` | Start background data collection |
| `pbs-monitor daemon stop` | Stop daemon |
| `pbs-monitor daemon status` | Check daemon status |

## Common Options

```bash
# Filters
-u, --user USER        # Filter by username
-p, --project PROJECT  # Filter by project
-q, --queue QUEUE      # Filter by queue
-s, --state STATE      # Filter by state (R, Q, H, etc.)
-d, --days N           # Look back N days

# Output
--format table|json|csv
--columns col1,col2,...
--sort COLUMN
--reverse

# Data collection
--collect              # Persist to database while viewing
--refresh              # Force data refresh
```

## Configuration

Config files are searched in order:
1. `~/.pbs_monitor.yaml`
2. `~/.config/pbs_monitor/config.yaml`
3. `/etc/pbs_monitor/config.yaml`

```yaml
database:
  url: "sqlite:///~/.pbs_monitor.db"

display:
  use_colors: true
  max_table_width: 120
```

Environment override: `PBS_MONITOR_DB_URL="sqlite:///path/to/db"`

## Background Collection

```bash
# Start daemon (background)
pbs-monitor daemon start

# Check status
pbs-monitor daemon status

# Stop
pbs-monitor daemon stop
```

The daemon periodically collects job, node, and queue data to the database.

## Generate Utilization Plots

Use `pbs-monitor analyze usage-insights` to generate utilization plots. These will be created under `./plots` in `.png` format.

```bash
# Analyze all data from last 30 days (default)
pbs-monitor analyze usage-insights

# Filter based on past number of days (7 in this case)
pbs-monitor analyze usage-insights -d 7

# Filter out undesired queues
pbs-monitor analyze usage-insights -x 

# See all options
pbs-monitor analyze usage-insights -h
```

## Documentation

See [docs/](docs/) for detailed documentation:
- [Quick Start](docs/user/quickstart.md) - Installation, setup, and usage
- [CLI Reference](docs/user/cli_reference.md) - Full command reference
- [Database Relocation](docs/user/database_relocation.md) - Moving your database

## License

MIT License - see LICENSE file.
