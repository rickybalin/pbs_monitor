"""
Renderers for PBS job replay output.

Provides different output formats for replay visualization:
- SplitPanelRenderer: Running jobs + top-N queued jobs with scores
- TextRenderer: Event log and state summaries
"""

import os
import shutil
from datetime import datetime
from typing import List, Optional, Tuple

try:
    import pytz
    CENTRAL_TZ = pytz.timezone('America/Chicago')
    HAS_PYTZ = True
except ImportError:
    from datetime import timezone
    CENTRAL_TZ = timezone.utc  # Fallback to UTC
    HAS_PYTZ = False

from pbs_monitor.models.job import PBSJob
from pbs_monitor.replay.event_stream import JobEvent
from pbs_monitor.replay.state_tracker import SystemState, QueuedJobInfo


def get_terminal_size() -> Tuple[int, int]:
    """
    Get the current terminal size.

    Returns:
        Tuple of (width, height) in characters
    """
    try:
        size = shutil.get_terminal_size(fallback=(120, 40))
        return (size.columns, size.lines)
    except Exception:
        return (120, 40)


def _format_timestamp_central(dt: datetime) -> str:
    """
    Format timestamp in US Central time.

    Args:
        dt: datetime to format

    Returns:
        Formatted string in Central time
    """
    if HAS_PYTZ:
        if dt.tzinfo is None:
            # Assume naive datetime is in local time, localize to Central
            central_dt = CENTRAL_TZ.localize(dt)
        else:
            central_dt = dt.astimezone(CENTRAL_TZ)
        return central_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
    else:
        # Fallback without pytz
        return dt.strftime('%Y-%m-%d %H:%M:%S')


def _format_duration(seconds: int) -> str:
    """
    Format duration in seconds to HH:MM:SS.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted duration string
    """
    if seconds < 0:
        return "N/A"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _truncate(s: str, length: int) -> str:
    """Truncate string to specified length with ellipsis."""
    if len(s) <= length:
        return s
    return s[:length - 1] + "…"


def _short_job_id(job_id: str) -> str:
    """Extract short job ID (number only) from full PBS job ID."""
    # "12345.aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov" -> "12345"
    return job_id.split('.')[0]


class SplitPanelRenderer:
    """
    Renders replay as a split-panel display:
    - Left panel: Running jobs
    - Right panel: Top-N queued jobs with scores

    Automatically adapts to terminal size for width and height.
    """

    # Fixed overhead lines: header (3) + panel headers (2) + column headers (2) + footer (4) + blank lines (2)
    OVERHEAD_LINES = 13

    def __init__(
        self,
        top_n: Optional[int] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
        auto_size: bool = True
    ):
        """
        Initialize the split panel renderer.

        Args:
            top_n: Number of jobs to show per panel. If None and auto_size=True,
                   calculated from terminal height.
            width: Total width of the display. If None and auto_size=True,
                   uses terminal width.
            height: Total height of the display. If None and auto_size=True,
                    uses terminal height.
            auto_size: If True, automatically detect terminal dimensions.
        """
        self.auto_size = auto_size
        self._fixed_top_n = top_n
        self._fixed_width = width
        self._fixed_height = height

    def _get_dimensions(self) -> Tuple[int, int, int]:
        """
        Get the current dimensions for rendering.

        Returns:
            Tuple of (width, height, max_jobs_per_panel)
        """
        if self.auto_size:
            term_width, term_height = get_terminal_size()
            width = self._fixed_width or term_width
            height = self._fixed_height or term_height
        else:
            width = self._fixed_width or 120
            height = self._fixed_height or 40

        # Calculate how many job lines we can fit
        available_lines = height - self.OVERHEAD_LINES
        max_jobs = max(available_lines, 5)  # Minimum 5 jobs

        # Override with fixed top_n if specified
        if self._fixed_top_n is not None:
            max_jobs = self._fixed_top_n

        return (width, height, max_jobs)

    def render_state(self, state: SystemState) -> str:
        """
        Render current state as split-panel text display.

        Args:
            state: SystemState to render

        Returns:
            Formatted string with running jobs on left, queued on right
        """
        # Get current dimensions
        width, height, max_jobs = self._get_dimensions()
        left_width = width // 2 - 1
        right_width = width - left_width - 3  # Account for separator

        lines = []

        # Header
        header = f"Replay: {_format_timestamp_central(state.timestamp)}"
        lines.append("=" * width)
        lines.append(header.center(width))
        lines.append("=" * width)
        lines.append("")

        # Panel headers
        running_header = f"RUNNING JOBS ({state.total_running} jobs, {state.running_nodes:,} nodes)"
        queued_header = f"TOP QUEUED JOBS ({state.total_queued} waiting)"

        lines.append(
            f"{running_header:<{left_width}} | {queued_header}"
        )
        lines.append("-" * left_width + " | " + "-" * right_width)

        # Column headers
        left_cols = f"{'Job ID':<10} {'User':<10} {'Nodes':>6} {'Walltime':<9} {'Queue':<8}"
        right_cols = f"{'Score':>9} {'Job ID':<10} {'User':<8} {'Nodes':>5} {'Wall':<6}"

        lines.append(
            f"{left_cols:<{left_width}} | {right_cols}"
        )
        lines.append("-" * left_width + " | " + "-" * right_width)

        # Get data for both panels
        running_lines = self._format_running_panel(state.running_jobs, max_jobs, left_width)
        queued_lines = self._format_queued_panel(state, max_jobs, right_width)

        # Combine panels side by side
        panel_lines = max(len(running_lines), len(queued_lines), 1)

        for i in range(panel_lines):
            left = running_lines[i] if i < len(running_lines) else ""
            right = queued_lines[i] if i < len(queued_lines) else ""
            lines.append(f"{left:<{left_width}} | {right}")

        # Footer with summary
        lines.append("")
        lines.append("=" * width)
        summary = (
            f"Running: {state.total_running} jobs ({state.running_nodes:,} nodes) | "
            f"Queued: {state.total_queued} jobs ({state.queued_nodes:,} nodes) | "
            f"Pending: {state.total_pending}"
        )
        lines.append(summary.center(width))
        lines.append("=" * width)

        return "\n".join(lines)

    def _format_running_panel(
        self,
        running_jobs: dict,
        max_jobs: int,
        panel_width: int
    ) -> List[str]:
        """
        Format the running jobs panel.

        Args:
            running_jobs: Dict of job_id -> PBSJob
            max_jobs: Maximum number of jobs to display
            panel_width: Width of the panel

        Returns:
            List of formatted lines
        """
        if not running_jobs:
            return ["  (no running jobs)"]

        lines = []
        # Sort by nodes descending (largest jobs first)
        sorted_jobs = sorted(
            running_jobs.values(),
            key=lambda j: j.nodes,
            reverse=True
        )

        for job in sorted_jobs[:max_jobs]:
            job_id = _truncate(_short_job_id(job.job_id), 10)
            user = _truncate(job.owner, 10)
            nodes = job.nodes
            walltime = job.walltime or "N/A"
            if len(walltime) > 9:
                walltime = walltime[:8]
            queue = _truncate(job.queue, 8)

            lines.append(
                f"{job_id:<10} {user:<10} {nodes:>6} {walltime:<9} {queue:<8}"
            )

        if len(running_jobs) > max_jobs:
            lines.append(f"  ... and {len(running_jobs) - max_jobs} more")

        return lines

    def _format_queued_panel(
        self,
        state: SystemState,
        max_jobs: int,
        panel_width: int
    ) -> List[str]:
        """
        Format the top-N queued jobs panel with scores.

        Args:
            state: Current system state
            max_jobs: Maximum number of jobs to display
            panel_width: Width of the panel

        Returns:
            List of formatted lines
        """
        top_queued = state.get_top_queued(max_jobs)

        if not top_queued:
            return ["  (no queued jobs)"]

        lines = []
        for info in top_queued:
            score = f"{info.score:>9.1f}"
            job_id = _truncate(_short_job_id(info.job_id), 10)
            user = _truncate(info.user, 8)
            nodes = info.nodes
            walltime = info.walltime or "N/A"
            # Shorten walltime for display
            if ":" in walltime:
                parts = walltime.split(":")
                if len(parts) >= 2:
                    walltime = f"{parts[0]}:{parts[1]}"
            if len(walltime) > 6:
                walltime = walltime[:5]

            lines.append(
                f"{score} {job_id:<10} {user:<8} {nodes:>5} {walltime:<6}"
            )

        if state.total_queued > max_jobs:
            lines.append(f"  ... and {state.total_queued - max_jobs} more in queue")

        return lines

    def render_compact(self, state: SystemState) -> str:
        """
        Render a compact one-line summary of the state.

        Args:
            state: SystemState to render

        Returns:
            Single-line summary string
        """
        return (
            f"[{_format_timestamp_central(state.timestamp)}] "
            f"Running: {state.total_running} ({state.running_nodes:,} nodes) | "
            f"Queued: {state.total_queued} ({state.queued_nodes:,} nodes)"
        )


class TextRenderer:
    """
    Renders replay as text timeline/event log.
    """

    def render_event_log(
        self,
        events: List[JobEvent],
        limit: int = 100
    ) -> str:
        """
        Format events as a text log with Central time.

        Args:
            events: List of events to render
            limit: Maximum number of events to show

        Returns:
            Formatted event log
        """
        lines = []
        lines.append("PBS Job Event Log")
        lines.append("=" * 80)
        lines.append("")
        lines.append(
            f"{'Timestamp':<25} {'Event':<10} {'Job ID':<12} "
            f"{'User':<10} {'Nodes':>6} {'Queue':<10}"
        )
        lines.append("-" * 80)

        for event in events[:limit]:
            timestamp = _format_timestamp_central(event.timestamp)
            event_type = event.event_type
            job_id = _short_job_id(event.job_id)
            user = _truncate(event.job.owner, 10)
            nodes = event.job.nodes
            queue = _truncate(event.job.queue, 10)

            lines.append(
                f"{timestamp:<25} {event_type:<10} {job_id:<12} "
                f"{user:<10} {nodes:>6} {queue:<10}"
            )

        if len(events) > limit:
            lines.append("")
            lines.append(f"... {len(events) - limit} more events not shown")

        lines.append("")
        lines.append(f"Total events: {len(events)}")

        return "\n".join(lines)

    def render_state_summary(self, state: SystemState) -> str:
        """
        Format current state as text summary.

        Args:
            state: SystemState to render

        Returns:
            Formatted state summary
        """
        lines = []
        lines.append(f"System State at {_format_timestamp_central(state.timestamp)}")
        lines.append("=" * 60)
        lines.append("")

        # Running jobs summary
        lines.append(f"Running Jobs: {state.total_running}")
        lines.append(f"  Total Nodes: {state.running_nodes:,}")

        if state.running_jobs:
            lines.append("  Top 5 by node count:")
            sorted_running = sorted(
                state.running_jobs.values(),
                key=lambda j: j.nodes,
                reverse=True
            )[:5]
            for job in sorted_running:
                lines.append(
                    f"    {_short_job_id(job.job_id)}: "
                    f"{job.nodes} nodes, {job.owner}, {job.queue}"
                )

        lines.append("")

        # Queued jobs summary
        lines.append(f"Queued Jobs: {state.total_queued}")
        lines.append(f"  Total Nodes Requested: {state.queued_nodes:,}")

        top_queued = state.get_top_queued(5)
        if top_queued:
            lines.append("  Top 5 by score:")
            for info in top_queued:
                wait_time = _format_duration(info.eligible_seconds)
                lines.append(
                    f"    {_short_job_id(info.job_id)}: "
                    f"score={info.score:.1f}, {info.nodes} nodes, "
                    f"wait={wait_time}, {info.user}"
                )

        lines.append("")

        # Pending jobs
        if state.total_pending > 0:
            lines.append(f"Pending Jobs (not yet eligible): {state.total_pending}")
            lines.append("")

        return "\n".join(lines)

    def render_timeline_summary(
        self,
        states: List[SystemState]
    ) -> str:
        """
        Render a summary timeline from multiple states.

        Args:
            states: List of SystemState objects at different times

        Returns:
            Formatted timeline summary
        """
        if not states:
            return "No states to display"

        lines = []
        lines.append("Timeline Summary")
        lines.append("=" * 80)
        lines.append("")
        lines.append(
            f"{'Timestamp':<25} {'Running':>10} {'Nodes':>10} "
            f"{'Queued':>10} {'Q-Nodes':>10}"
        )
        lines.append("-" * 80)

        for state in states:
            timestamp = _format_timestamp_central(state.timestamp)
            lines.append(
                f"{timestamp:<25} {state.total_running:>10} "
                f"{state.running_nodes:>10,} {state.total_queued:>10} "
                f"{state.queued_nodes:>10,}"
            )

        lines.append("")
        lines.append(f"Total snapshots: {len(states)}")

        return "\n".join(lines)
