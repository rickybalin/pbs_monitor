# Database Relocation Guide

This guide documents how to safely relocate the PBS Monitor database file to a different location on the filesystem.

## Overview

PBS Monitor uses SQLite by default with the database stored at `~/.pbs_monitor.db`. You may want to relocate the database for reasons such as:

- Moving to a shared filesystem for multi-user access
- Moving to a location with more storage space
- Organizing data in a specific directory structure
- Performance considerations (different storage types)

## Current Database Location

PBS Monitor determines the database location in this priority order:

1. **Configuration file** (`database.url` setting)
2. **Environment variable** (`PBS_MONITOR_DB_URL`)
3. **Default location** (`~/.pbs_monitor.db`)

## Step-by-Step Relocation Process

### 1. Stop PBS Monitor Services

If you're running the PBS Monitor daemon, stop it first:

```bash
# Activate virtual environment
source /path/to/pbs_monitor/venv/bin/activate

# Stop daemon
pbs-monitor daemon stop
```

### 2. Create a Backup

Always create a backup before relocating:

```bash
# Create timestamped backup
pbs-monitor database backup

# Or specify custom backup location
pbs-monitor database backup /path/to/backup/pbs_monitor_backup.db
```

### 3. Choose Your Relocation Method

#### Method A: Configuration File (Recommended)

Create or edit a configuration file at one of these locations:
- `~/.pbs_monitor.yaml` (recommended)
- `~/.config/pbs_monitor/config.yaml`
- `/etc/pbs_monitor/config.yaml`

Add the database configuration:

```yaml
database:
  url: "sqlite:////absolute/path/to/new/location/pbs_monitor.db"
  # Keep other settings as needed
  pool_size: 5
  daemon_enabled: true
  auto_persist: false
```

#### Method B: Environment Variable

```bash
# Add to your .bashrc or .profile
export PBS_MONITOR_DB_URL="sqlite:////absolute/path/to/new/location/pbs_monitor.db"
```

### 4. Copy Database to New Location

```bash
# Ensure target directory exists
mkdir -p /new/location/

# Copy the database file
cp ~/.pbs_monitor.db /new/location/pbs_monitor.db
```

### 5. Verify Database Access

Test the connection to the new location:

```bash
# Check database status
pbs-monitor database status

# Validate schema and data
pbs-monitor database validate
```

### 6. Restart Services

If using daemon mode:

```bash
pbs-monitor daemon start
```

## Important Configuration Details

### SQLite URL Format

**Critical**: Use the correct number of slashes in SQLite URLs:

- ✅ **Correct**: `sqlite:////absolute/path/to/database.db` (4 slashes for absolute paths)
- ❌ **Incorrect**: `sqlite:///absolute/path/to/database.db` (3 slashes - treated as relative)

### Path Examples

```yaml
# Absolute path (note the 4 slashes)
url: "sqlite:////opt/pbs_monitor/data/pbs_monitor.db"

# Home directory (tilde expansion supported)
url: "sqlite:///~/.pbs_monitor.db"

# Shared filesystem example
url: "sqlite:////lus/eagle/projects/datascience/shared/pbs_monitor.db"
```

## Alternative: Backup/Restore Method

If you prefer using the built-in backup/restore functionality:

1. **Create backup**:
   ```bash
   pbs-monitor database backup /path/to/backup.db
   ```

2. **Update configuration** to point to new location

3. **Restore to new location**:
   ```bash
   pbs-monitor database restore /path/to/backup.db
   ```

## Troubleshooting

### Common Issues

#### "Unable to open database file" Error

**Cause**: Usually incorrect SQLite URL format or permissions issue.

**Solution**:
1. Check URL format - ensure 4 slashes for absolute paths
2. Verify file permissions and directory access
3. Confirm the file exists at the specified location

#### Permission Denied

**Cause**: PBS Monitor process doesn't have read/write access.

**Solution**:
1. Check file permissions: `ls -la /path/to/database.db`
2. Check directory permissions
3. Ensure the user running PBS Monitor owns the file or has appropriate permissions

#### Configuration Not Found

**Cause**: PBS Monitor isn't finding your configuration file.

**Solution**:
1. Verify configuration file location and name
2. Check YAML syntax is valid
3. Use absolute paths in configuration

### Verification Commands

```bash
# Test database connectivity
pbs-monitor database status

# Validate all tables and data
pbs-monitor database validate

# Show current configuration
python -c "
from pbs_monitor.config import Config
config = Config()
print('Database URL:', config.database.url)
"
```

## File Permissions and Security

### Recommended Permissions

```bash
# Database file
chmod 644 /path/to/pbs_monitor.db

# Directory
chmod 755 /path/to/database/directory/

# Configuration file
chmod 600 ~/.pbs_monitor.yaml
```

### Multi-User Considerations

For shared access:
- Use a shared filesystem location
- Set appropriate group permissions
- Consider using PostgreSQL for production multi-user setups

## Sample Complete Configuration

```yaml
# Complete configuration for relocated database
database:
  url: "sqlite:////lus/eagle/projects/datascience/shared/pbs_monitor.db"
  pool_size: 5
  daemon_enabled: true
  auto_persist: false
  job_collection_interval: 900      # 15 minutes
  node_collection_interval: 1800    # 30 minutes
  queue_collection_interval: 3600   # 60 minutes
  snapshot_interval: 1800           # 30 minutes
  job_history_days: 365             # Keep 1 year
  snapshot_retention_days: 90       # Keep 90 days

display:
  use_colors: true
  max_table_width: 120
  truncate_long_names: true

logging:
  level: INFO
  date_format: "%d-%m %H:%M"
  log_file: pbs_monitor-log.txt

pbs:
  command_timeout: 30
  job_refresh_interval: 600
  node_refresh_interval: 1200
  queue_refresh_interval: 600
```

## Production Considerations

### Storage Requirements

Monitor database growth over time:
- Job history: ~1-2 KB per job
- Node snapshots: Depends on collection frequency
- Queue snapshots: Usually minimal

### Backup Strategy

```bash
# Regular backups (consider adding to cron)
pbs-monitor database backup /backup/location/pbs_monitor_$(date +%Y%m%d).db

# Cleanup old data
pbs-monitor database cleanup --job-history-days 365 --snapshot-days 90
```

### Migration to PostgreSQL

For larger deployments, consider migrating to PostgreSQL:

```yaml
database:
  url: "postgresql://user:password@host:5432/pbs_monitor"
  pool_size: 10
  max_overflow: 20
```

See the main database documentation for PostgreSQL setup details.
