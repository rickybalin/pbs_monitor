# Job Replay Feature Plan

## Overview

Build a "replay" feature that reconstructs and visualizes the timeline of PBS jobs based on their timing metadata (`submit_time`, `start_time`, `end_time`, `eligible_time`). This approach uses the definitive timestamps from PBS rather than periodic JobHistory snapshots, providing accurate event timing without the complexity of snapshot interpolation.

**Display**: Split-panel view showing running jobs on one side and top-N queued jobs (ranked by score) on the other, with key metadata like node count, walltime, and score at the given timestamp.

## Goals

1. **Reconstruct job timelines** - Show when jobs were submitted, became eligible, started running, and completed
2. **Visualize scheduler decisions** - Show which queued jobs had the highest scores and why they were (or weren't) selected
3. **Support analysis use cases** - Understand queue behavior, scheduling patterns, and score dynamics

## Design Decisions

- **Timezone**: All display output uses **US Central Time** (America/Chicago)
- **Node-level detail**: Not included (systems range from hundreds to 10k+ nodes)
- **Comparison mode**: Not included
- **Granularity**: Coarse-grained visualization suitable for large-scale systems

## Data Source: Job Timing Fields

Using fields from `PBSJob` model (`pbs_monitor/models/job.py`) and `jobs` DB table:

| Field | Source | Description |
|-------|--------|-------------|
| `submit_time` | `qtime` from qstat | When job entered the queue |
| `eligible_time` | `eligible_time` from qstat | When job became eligible for scheduling (score accrual starts) |
| `start_time` | `stime` from qstat | When job began execution |
| `end_time` | `mtime` from qstat | When job completed/terminated |
| `queue_time_seconds` | Derived | `start_time - submit_time` |
| `actual_runtime_seconds` | Derived | `end_time - start_time` |
| `state` | qstat | Job state (Q, R, F, etc.) |
| `queue` | qstat | Queue name |
| `project` | qstat | Project/account |
| `nodes_used` | Resource_List | Node count |
| `walltime` | Resource_List | Requested walltime |
| `exit_status` | qstat | Exit code (for completed jobs) |

### Eligible Time vs Submit Time

`eligible_time` is distinct from `submit_time` and is critical for accurate score calculation:

- **submit_time** (`qtime`): When the job was submitted to PBS
- **eligible_time**: When the job became eligible for scheduling and started accruing priority score

These can differ when:
- Jobs are submitted to **routing queues** and cannot immediately be placed on an execution queue due to job limits
- Jobs have **dependencies** that haven't been satisfied yet
- Jobs are submitted with a **future start time** or hold

For replay, we use `eligible_time` to determine when a job should appear in the scored queue list, not `submit_time`.

### Why Job Details Over JobHistory

- **Precision**: `submit_time`, `start_time`, `end_time` are the actual PBS timestamps, not observation times
- **Simplicity**: No need to interpolate between snapshots or handle observation gaps
- **Completeness**: Every job has these fields (when applicable to its lifecycle)
- **Lower storage overhead**: No need to query/join JobHistory tables

### Limitation

- We only know discrete state transitions (submitted → eligible → running → finished)
- No intermediate states like priority changes during queued period (that would require JobHistory)
- Score is calculated at display time based on `eligible_time` and the replay timestamp
- For most replay use cases, this is sufficient

## Event Model

Each job produces up to 4 events:

```python
@dataclass
class JobEvent:
    timestamp: datetime
    event_type: Literal["submitted", "eligible", "started", "ended"]
    job_id: str
    job: PBSJob  # Full job details for context
```

Event generation logic:
```python
def job_to_events(job: PBSJob) -> List[JobEvent]:
    events = []
    if job.submit_time:
        events.append(JobEvent(job.submit_time, "submitted", job.job_id, job))
    if job.eligible_time and job.eligible_time != job.submit_time:
        events.append(JobEvent(job.eligible_time, "eligible", job.job_id, job))
    if job.start_time:
        events.append(JobEvent(job.start_time, "started", job.job_id, job))
    if job.end_time:
        events.append(JobEvent(job.end_time, "ended", job.job_id, job))
    return events
```

## Architecture

### New Module: `pbs_monitor/replay/`

```
pbs_monitor/replay/
├── __init__.py
├── event_stream.py      # Event generation and timeline building
├── state_tracker.py     # Tracks system state at any point in time
├── renderer.py          # Output formatters (text, plots, animation)
└── cli.py               # CLI command integration
```

### Component Details

#### 1. `event_stream.py` - Event Generation

```python
class JobEventStream:
    """Generates and manages a timeline of job events."""

    def __init__(self, jobs: List[PBSJob]):
        self.jobs = jobs
        self._events: List[JobEvent] = []

    def build_timeline(self) -> List[JobEvent]:
        """Convert jobs to events and sort by timestamp."""
        events = []
        for job in self.jobs:
            events.extend(job_to_events(job))
        return sorted(events, key=lambda e: e.timestamp)

    def get_events_in_range(self, start: datetime, end: datetime) -> List[JobEvent]:
        """Get events within a time window."""
        ...

    def get_time_bounds(self) -> Tuple[datetime, datetime]:
        """Return earliest and latest event timestamps."""
        ...
```

#### 2. `state_tracker.py` - System State Reconstruction

```python
@dataclass
class SystemState:
    """Represents cluster state at a point in time."""
    timestamp: datetime
    queued_jobs: Dict[str, QueuedJobInfo]   # job_id -> job info with score
    running_jobs: Dict[str, PBSJob]         # job_id -> job
    completed_jobs: Dict[str, PBSJob]       # job_id -> job (recently completed)

    @property
    def total_queued(self) -> int: ...

    @property
    def total_running(self) -> int: ...

    @property
    def running_nodes(self) -> int:
        """Sum of nodes used by running jobs."""
        ...

    def get_top_queued(self, n: int = 20) -> List[QueuedJobInfo]:
        """Return top N queued jobs sorted by score (highest first)."""
        ...

@dataclass
class QueuedJobInfo:
    """Queued job with calculated score at current replay timestamp."""
    job: PBSJob
    score: float                    # Calculated score at this timestamp
    eligible_seconds: int           # Seconds since eligible_time
    nodes: int                      # Requested node count
    walltime: str                   # Requested walltime (formatted)
    queue: str                      # Queue name
    user: str                       # Job owner
    project: str                    # Project/account

class StateTracker:
    """Reconstructs system state by replaying events."""

    def __init__(self, event_stream: JobEventStream, score_formula: Optional[str] = None):
        self.events = event_stream.build_timeline()
        self._state = SystemState(...)
        self._event_index = 0
        self._score_formula = score_formula  # PBS job_sort_formula for score calculation

    def advance_to(self, timestamp: datetime) -> SystemState:
        """Apply all events up to timestamp and return state with recalculated scores."""
        while self._event_index < len(self.events):
            event = self.events[self._event_index]
            if event.timestamp > timestamp:
                break
            self._apply_event(event)
            self._event_index += 1

        # Recalculate scores for all queued jobs at this timestamp
        self._recalculate_scores(timestamp)
        return self._state

    def _apply_event(self, event: JobEvent):
        """Update state based on event type."""
        if event.event_type == "submitted":
            # Job submitted but not yet eligible - track but don't score
            pass
        elif event.event_type == "eligible":
            # Job now eligible - add to queued with score tracking
            self._state.queued_jobs[event.job_id] = QueuedJobInfo(
                job=event.job, score=0, eligible_seconds=0, ...)
        elif event.event_type == "started":
            self._state.queued_jobs.pop(event.job_id, None)
            self._state.running_jobs[event.job_id] = event.job
        elif event.event_type == "ended":
            self._state.running_jobs.pop(event.job_id, None)
            self._state.completed_jobs[event.job_id] = event.job

    def _recalculate_scores(self, current_time: datetime):
        """Recalculate score for each queued job based on eligible_time and current_time."""
        for job_id, info in self._state.queued_jobs.items():
            eligible_time = info.job.eligible_time or info.job.submit_time
            if eligible_time:
                info.eligible_seconds = int((current_time - eligible_time).total_seconds())
                info.score = self._calculate_score(info.job, info.eligible_seconds)

    def _calculate_score(self, job: PBSJob, eligible_seconds: int) -> float:
        """Calculate job score using PBS formula."""
        # Uses same logic as PBSCommands.calculate_job_score()
        # Score typically based on: eligible_time, walltime, node count, fairshare
        ...

    def get_state_at(self, timestamp: datetime) -> SystemState:
        """Get state at arbitrary time (resets if going backwards)."""
        ...

    def iterate_states(self, step: timedelta) -> Iterator[SystemState]:
        """Yield states at regular intervals for animation."""
        ...
```

#### 3. `renderer.py` - Output Formats

```python
import pytz

CENTRAL_TZ = pytz.timezone('America/Chicago')

class SplitPanelRenderer:
    """
    Renders replay as a split-panel display:
    - Left panel: Running jobs
    - Right panel: Top-N queued jobs with scores
    """

    def __init__(self, top_n: int = 20):
        self.top_n = top_n

    def render_state(self, state: SystemState) -> str:
        """
        Render current state as split-panel text display.

        Example output:
        ================================================================================
        Replay: 2024-01-15 14:30:00 CST
        ================================================================================

        RUNNING JOBS (47 jobs, 2,340 nodes)          | TOP QUEUED JOBS (156 waiting)
        ---------------------------------------------|--------------------------------
        Job ID       User     Nodes  Walltime Queue  | Score    Job ID     Nodes  Wall
        12345.pbs    alice      128  04:00:00 prod   | 1847.2   12400.pbs    256  02:00
        12346.pbs    bob         64  02:00:00 prod   | 1523.8   12401.pbs    128  04:00
        12347.pbs    charlie    256  06:00:00 prod   |  987.4   12402.pbs     64  01:00
        ...                                          | ...
        """
        ...

    def _format_timestamp(self, dt: datetime) -> str:
        """Format timestamp in US Central time."""
        return dt.astimezone(CENTRAL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')

    def _format_running_panel(self, running_jobs: Dict[str, PBSJob]) -> List[str]:
        """Format the running jobs panel."""
        ...

    def _format_queued_panel(self, state: SystemState) -> List[str]:
        """Format the top-N queued jobs panel with scores."""
        top_queued = state.get_top_queued(self.top_n)
        lines = []
        for info in top_queued:
            lines.append(
                f"{info.score:>8.1f}   {info.job.job_id:<12} {info.nodes:>5}  {info.walltime}"
            )
        return lines


class TextRenderer:
    """Renders replay as text timeline/event log."""

    def render_event_log(self, events: List[JobEvent], limit: int = 100) -> str:
        """Format events as a text log with Central time."""
        ...

    def render_state_summary(self, state: SystemState) -> str:
        """Format current state as text summary."""
        ...


class PlotRenderer:
    """Renders replay as matplotlib visualizations (coarse-grained for large systems)."""

    def plot_utilization_over_time(self, tracker: StateTracker,
                                    start: datetime, end: datetime,
                                    step: timedelta, output_path: Path):
        """
        Line plot showing:
        - Total running jobs over time
        - Total queued jobs over time
        - Aggregate node utilization (percentage or absolute)

        Uses coarse binning suitable for systems with 100s to 10k+ nodes.
        """
        ...

    def plot_queue_depth_vs_score(self, tracker: StateTracker,
                                   start: datetime, end: datetime,
                                   step: timedelta, output_path: Path):
        """
        Show relationship between queue position and score over time.
        Helps visualize scheduler fairness and score evolution.
        """
        ...

    def animate_split_panel(self, tracker: StateTracker,
                            start: datetime, end: datetime,
                            step: timedelta, output_path: Path,
                            top_n: int = 20):
        """
        Generate animated split-panel visualization.
        Each frame shows running jobs + top-N queued at that timestamp.
        """
        ...
```

#### 4. `cli.py` - CLI Integration

New subcommand under `pbs-monitor`:

```
pbs-monitor replay [OPTIONS]

Options:
  --start TEXT          Start time (ISO format or relative like "24h ago")
  --end TEXT            End time (ISO format or "now")
  --user TEXT           Filter by user
  --queue TEXT          Filter by queue
  --project TEXT        Filter by project
  --output-format TEXT  Output format: split-panel, text, timeline, animation
  --output-dir PATH     Directory for generated files
  --step TEXT           Time step for stepping/animation (e.g., "5m", "1h")
  --top-n INT           Number of top queued jobs to show (default: 20)
  --live                Live mode: continuously update display
```

Examples:
```bash
# Split-panel view of last 24 hours (step through snapshots)
pbs-monitor replay --start "24h ago" --output-format split-panel --step 1h

# Live split-panel view (updates every 30s)
pbs-monitor replay --live --output-format split-panel

# Text event log for a specific day
pbs-monitor replay --start 2024-01-15 --end 2024-01-16 --output-format text

# Animated split-panel replay (generates GIF/MP4)
pbs-monitor replay --start "7d ago" --step 1h --output-format animation --top-n 30

# Filter to specific queue with more queued jobs shown
pbs-monitor replay --queue prod --start "48h ago" --output-format split-panel --top-n 50
```

## Implementation Phases

### Phase 1: Core Event Infrastructure
- [ ] Create `pbs_monitor/replay/` module structure
- [ ] Implement `JobEvent` dataclass with "eligible" event type
- [ ] Implement `job_to_events()` function handling submit/eligible/start/end
- [ ] Implement `JobEventStream` class
- [ ] Add unit tests for event generation

### Phase 2: State Tracking with Scores
- [ ] Implement `QueuedJobInfo` dataclass with score fields
- [ ] Implement `SystemState` dataclass with `get_top_queued(n)`
- [ ] Implement `StateTracker` with forward iteration
- [ ] Add score calculation using PBS formula (reuse `PBSCommands.calculate_job_score` logic)
- [ ] Handle eligible_time vs submit_time distinction
- [ ] Add `get_state_at()` for random access (reset + replay)
- [ ] Add `iterate_states()` generator for stepping/animations
- [ ] Unit tests for state reconstruction and score accuracy

### Phase 3: Split-Panel Display
- [ ] Implement `SplitPanelRenderer` with running/queued panels
- [ ] Add US Central timezone formatting
- [ ] Display top-N queued jobs with score, nodes, walltime
- [ ] Integrate with CLI as `pbs-monitor replay --output-format split-panel`
- [ ] Support time range and filtering options
- [ ] Add `--top-n` option for configurable queue depth

### Phase 4: Text Output
- [ ] Implement `TextRenderer` with event log format
- [ ] Add state summary formatting
- [ ] Integrate with CLI as `pbs-monitor replay --output-format text`

### Phase 5: Visualization (Coarse-Grained)
- [ ] Implement utilization over time plot (jobs + nodes)
- [ ] Implement queue depth vs score plot
- [ ] Ensure plots scale well for 100-10k+ node systems
- [ ] Save plots to configurable output directory

### Phase 6: Animation (Optional)
- [ ] Frame generation for split-panel state
- [ ] GIF/MP4 output using matplotlib animation or moviepy
- [ ] Progress bar for long renders
- [ ] Configurable step size

### Phase 7: Data Source Integration
- [ ] Query jobs from database via `JobRepository`
- [ ] Support live PBS query via `DataCollector.get_completed_jobs()`
- [ ] Merge database and live data for recent history
- [ ] Normalize all timestamps to US Central for display
- [ ] Parse and store `eligible_time` from qstat if not already captured

## Data Retrieval Strategy

```python
import pytz

CENTRAL_TZ = pytz.timezone('America/Chicago')

def get_jobs_for_replay(data_collector: DataCollector,
                        start_time: datetime,
                        end_time: datetime,
                        **filters) -> List[PBSJob]:
    """
    Get jobs that were active during the replay window.

    A job is included if any of these overlap with [start_time, end_time]:
    - submit_time to eligible_time (submitted but not yet eligible)
    - eligible_time to start_time (eligible/queued period)
    - start_time to end_time (running period)
    """
    # From database: completed jobs in range
    db_jobs = data_collector.get_completed_jobs(
        user=filters.get('user'),
        days=...,  # Calculate from start_time
        include_pbs_history=True
    )

    # Filter to jobs active in window
    active_jobs = []
    for job in db_jobs:
        job_start = job.submit_time or job.eligible_time or job.start_time
        job_end = job.end_time or datetime.now(CENTRAL_TZ)

        if job_start <= end_time and job_end >= start_time:
            active_jobs.append(job)

    return active_jobs


def calculate_score_at_time(job: PBSJob, timestamp: datetime,
                            score_formula: str) -> float:
    """
    Calculate job score at a specific timestamp.

    Uses eligible_time (not submit_time) as the baseline for score accrual.
    This handles routing queue delays correctly.
    """
    eligible_time = job.eligible_time or job.submit_time
    if not eligible_time or timestamp < eligible_time:
        return 0.0  # Job not yet eligible at this timestamp

    eligible_seconds = int((timestamp - eligible_time).total_seconds())

    # Apply PBS score formula
    # Typical formula uses: eligible_time, walltime, node count, fairshare
    # Reuse logic from PBSCommands.calculate_job_score()
    ...
```

## Design Decisions (Resolved)

| Question | Decision |
|----------|----------|
| Timezone | US Central Time (America/Chicago) for all display |
| Node-level detail | Not included - too variable across systems (100s to 10k+ nodes) |
| Comparison mode | Not included |
| Display layout | Split-panel: running jobs (left) + top-N queued with scores (right) |
| Score timing | Use `eligible_time` for score accrual start (handles routing queue delays) |
| Granularity | Coarse-grained, suitable for large-scale systems |

## Open Questions

1. **Large time ranges**: For multi-month replays, should we aggregate to reduce data volume?
2. **Export formats**: JSON export for external visualization tools?
3. **Score formula source**: Should we store historical score formulas, or assume current formula applies to past jobs?

## Related Files

- `pbs_monitor/models/job.py` - PBSJob dataclass with timing fields
- `pbs_monitor/data_collector.py` - Data retrieval methods
- `pbs_monitor/database/repositories.py` - Database queries
- `pbs_monitor/analytics/usage_insights.py` - Existing plotting patterns
- `pbs_monitor/cli/main.py` - CLI command registration

## Success Criteria

1. Can reconstruct accurate job states at any historical timestamp
2. Text output matches what `pbs-monitor jobs` would have shown at that time
3. Timeline visualizations clearly show job lifecycles and overlaps
4. Performance: Can process 10,000+ jobs in under 5 seconds
5. Memory efficient: Streaming/iterator-based for large datasets
