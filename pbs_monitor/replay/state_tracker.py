"""
State tracking for PBS job replay.

Reconstructs system state (running jobs, queued jobs with scores)
at any point in time by replaying job events.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Iterator, List, Optional, Any
import copy

from pbs_monitor.models.job import PBSJob
from pbs_monitor.replay.event_stream import JobEvent, JobEventStream, _get_eligible_time


@dataclass
class QueuedJobInfo:
    """
    Queued job with calculated score at current replay timestamp.

    Tracks all the information needed to display a queued job
    in the replay view, including its dynamically calculated score.
    """

    job: PBSJob
    score: float = 0.0
    eligible_seconds: int = 0
    eligible_time: Optional[datetime] = None

    @property
    def nodes(self) -> int:
        """Requested node count."""
        return self.job.nodes

    @property
    def walltime(self) -> str:
        """Requested walltime (formatted)."""
        return self.job.walltime or "N/A"

    @property
    def queue(self) -> str:
        """Queue name."""
        return self.job.queue

    @property
    def user(self) -> str:
        """Job owner."""
        return self.job.owner

    @property
    def project(self) -> str:
        """Project/account."""
        return self.job.project or "N/A"

    @property
    def job_id(self) -> str:
        """Job ID."""
        return self.job.job_id

    @property
    def job_name(self) -> str:
        """Job name."""
        return self.job.job_name


@dataclass
class SystemState:
    """
    Represents cluster state at a point in time.

    Contains the current running jobs, queued jobs (with scores),
    and recently completed jobs.
    """

    timestamp: datetime
    queued_jobs: Dict[str, QueuedJobInfo] = field(default_factory=dict)
    running_jobs: Dict[str, PBSJob] = field(default_factory=dict)
    completed_jobs: Dict[str, PBSJob] = field(default_factory=dict)

    # Jobs that are submitted but not yet eligible
    pending_jobs: Dict[str, PBSJob] = field(default_factory=dict)

    @property
    def total_queued(self) -> int:
        """Total number of queued jobs."""
        return len(self.queued_jobs)

    @property
    def total_running(self) -> int:
        """Total number of running jobs."""
        return len(self.running_jobs)

    @property
    def total_pending(self) -> int:
        """Total number of pending (submitted but not eligible) jobs."""
        return len(self.pending_jobs)

    @property
    def running_nodes(self) -> int:
        """Sum of nodes used by running jobs."""
        return sum(job.nodes for job in self.running_jobs.values())

    @property
    def queued_nodes(self) -> int:
        """Sum of nodes requested by queued jobs."""
        return sum(info.nodes for info in self.queued_jobs.values())

    def get_top_queued(self, n: int = 20) -> List[QueuedJobInfo]:
        """
        Return top N queued jobs sorted by score (highest first).

        Args:
            n: Number of top jobs to return

        Returns:
            List of QueuedJobInfo sorted by score descending
        """
        sorted_jobs = sorted(
            self.queued_jobs.values(),
            key=lambda x: x.score,
            reverse=True
        )
        return sorted_jobs[:n]

    def copy(self) -> "SystemState":
        """Create a deep copy of the current state."""
        return SystemState(
            timestamp=self.timestamp,
            queued_jobs=copy.copy(self.queued_jobs),
            running_jobs=copy.copy(self.running_jobs),
            completed_jobs=copy.copy(self.completed_jobs),
            pending_jobs=copy.copy(self.pending_jobs),
        )


class ScoreCalculator:
    """
    Calculates job scores based on PBS formula.

    Reuses the scoring logic from PBSCommands but allows
    for score calculation at arbitrary timestamps.
    """

    def __init__(
        self,
        formula: Optional[str] = None,
        server_defaults: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the score calculator.

        Args:
            formula: PBS job_sort_formula string. If None, uses a default
                     FIFO-based formula.
            server_defaults: Server default values for score parameters
        """
        # Default formula if none provided
        # This is a simplified FIFO formula: older eligible jobs score higher
        self.formula = formula or "eligible_time"
        self.server_defaults = server_defaults or {
            "base_score": 0,
            "score_boost": 0,
            "enable_wfp": 0,
            "wfp_factor": 100000,
            "enable_backfill": 0,
            "backfill_max": 50,
            "backfill_factor": 84600,
            "enable_fifo": 1,
            "fifo_factor": 1800,
            "total_cpus": 1,
        }

    def calculate_score(
        self,
        job: PBSJob,
        current_time: datetime,
        eligible_time: Optional[datetime] = None
    ) -> float:
        """
        Calculate job score at a specific timestamp.

        Args:
            job: The job to calculate score for
            current_time: The timestamp at which to calculate the score
            eligible_time: When job became eligible (if not provided,
                          extracted from job or falls back to submit_time)

        Returns:
            Calculated score value
        """
        # Determine eligible time
        if eligible_time is None:
            eligible_time = _get_eligible_time(job) or job.submit_time

        if not eligible_time:
            return 0.0

        # Job not yet eligible at this timestamp
        if current_time < eligible_time:
            return 0.0

        eligible_seconds = int((current_time - eligible_time).total_seconds())

        # Build variables for formula evaluation
        resource_list = job.raw_attributes.get("Resource_List", {})

        variables = {
            "base_score": int(resource_list.get(
                "base_score", self.server_defaults.get("base_score", 0)
            )),
            "score_boost": int(resource_list.get(
                "score_boost", self.server_defaults.get("score_boost", 0)
            )),
            "enable_wfp": int(resource_list.get(
                "enable_wfp", self.server_defaults.get("enable_wfp", 0)
            )),
            "wfp_factor": int(resource_list.get(
                "wfp_factor", self.server_defaults.get("wfp_factor", 100000)
            )),
            "enable_backfill": int(resource_list.get(
                "enable_backfill", self.server_defaults.get("enable_backfill", 0)
            )),
            "backfill_max": int(resource_list.get(
                "backfill_max", self.server_defaults.get("backfill_max", 50)
            )),
            "backfill_factor": int(resource_list.get(
                "backfill_factor", self.server_defaults.get("backfill_factor", 84600)
            )),
            "enable_fifo": int(resource_list.get(
                "enable_fifo", self.server_defaults.get("enable_fifo", 1)
            )),
            "fifo_factor": int(resource_list.get(
                "fifo_factor", self.server_defaults.get("fifo_factor", 1800)
            )),
            "project_priority": int(resource_list.get("project_priority", 1)),
            "nodect": job.nodes,
            "total_cpus": int(resource_list.get(
                "total_cpus", self.server_defaults.get("total_cpus", 1)
            )),
            "walltime": self._parse_walltime_to_seconds(
                resource_list.get("walltime", job.walltime or "01:00:00")
            ),
            "eligible_time": eligible_seconds,
            "min": min,
            "max": max,
        }

        try:
            score = eval(self.formula, {"__builtins__": {}}, variables)
            return float(score)
        except Exception:
            # Fall back to simple eligible time score
            return float(eligible_seconds)

    @staticmethod
    def _parse_walltime_to_seconds(walltime_str: str) -> float:
        """Parse walltime string to seconds."""
        if not walltime_str:
            return 3600.0

        try:
            parts = walltime_str.split(':')
            if len(parts) == 3:
                hours, minutes, seconds = map(int, parts)
                return hours * 3600 + minutes * 60 + seconds
            elif len(parts) == 4:
                days, hours, minutes, seconds = map(int, parts)
                return days * 86400 + hours * 3600 + minutes * 60 + seconds
            else:
                return 3600.0
        except (ValueError, TypeError):
            return 3600.0


class StateTracker:
    """
    Reconstructs system state by replaying events.

    Maintains the current state and allows advancing through time
    to see what the system looked like at any point.
    """

    def __init__(
        self,
        event_stream: JobEventStream,
        score_formula: Optional[str] = None,
        server_defaults: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the state tracker.

        Args:
            event_stream: The event stream to replay
            score_formula: PBS job_sort_formula for score calculation
            server_defaults: Server default values for score parameters
        """
        self.event_stream = event_stream
        self.events = event_stream.build_timeline()
        self.score_calculator = ScoreCalculator(score_formula, server_defaults)

        # Initialize empty state
        self._initial_timestamp = (
            self.events[0].timestamp if self.events else datetime.now()
        )
        self._state = SystemState(timestamp=self._initial_timestamp)
        self._event_index = 0

    def reset(self) -> None:
        """Reset state tracker to initial state."""
        self._state = SystemState(timestamp=self._initial_timestamp)
        self._event_index = 0

    def advance_to(self, timestamp: datetime) -> SystemState:
        """
        Apply all events up to timestamp and return state with recalculated scores.

        If the requested timestamp is before the current state's timestamp,
        this will reset and replay from the beginning.

        Args:
            timestamp: Target timestamp to advance to

        Returns:
            SystemState at the given timestamp
        """
        # Need to go backwards? Reset first.
        if timestamp < self._state.timestamp:
            self.reset()

        # Apply events up to timestamp
        while self._event_index < len(self.events):
            event = self.events[self._event_index]
            if event.timestamp > timestamp:
                break
            self._apply_event(event)
            self._event_index += 1

        # Update state timestamp
        self._state.timestamp = timestamp

        # Recalculate scores for all queued jobs at this timestamp
        self._recalculate_scores(timestamp)

        return self._state

    def _apply_event(self, event: JobEvent) -> None:
        """
        Update state based on event type.

        Args:
            event: The event to apply
        """
        if event.event_type == "submitted":
            # Job submitted - check if eligible time is different
            eligible_time = _get_eligible_time(event.job)
            if eligible_time and eligible_time != event.job.submit_time:
                # Job is pending (submitted but not yet eligible)
                self._state.pending_jobs[event.job_id] = event.job
            else:
                # Job is immediately eligible
                self._state.queued_jobs[event.job_id] = QueuedJobInfo(
                    job=event.job,
                    eligible_time=event.job.submit_time
                )

        elif event.event_type == "eligible":
            # Job now eligible - move from pending to queued
            self._state.pending_jobs.pop(event.job_id, None)
            eligible_time = _get_eligible_time(event.job) or event.timestamp
            self._state.queued_jobs[event.job_id] = QueuedJobInfo(
                job=event.job,
                eligible_time=eligible_time
            )

        elif event.event_type == "started":
            # Job started running
            self._state.queued_jobs.pop(event.job_id, None)
            self._state.pending_jobs.pop(event.job_id, None)
            self._state.running_jobs[event.job_id] = event.job

        elif event.event_type == "ended":
            # Job completed
            self._state.running_jobs.pop(event.job_id, None)
            self._state.queued_jobs.pop(event.job_id, None)
            self._state.pending_jobs.pop(event.job_id, None)
            self._state.completed_jobs[event.job_id] = event.job

    def _recalculate_scores(self, current_time: datetime) -> None:
        """
        Recalculate score for each queued job based on eligible_time and current_time.

        Args:
            current_time: The timestamp to calculate scores at
        """
        for job_id, info in self._state.queued_jobs.items():
            eligible_time = info.eligible_time or info.job.submit_time
            if eligible_time:
                info.eligible_seconds = int(
                    (current_time - eligible_time).total_seconds()
                )
                info.score = self.score_calculator.calculate_score(
                    info.job,
                    current_time,
                    eligible_time
                )
            else:
                info.eligible_seconds = 0
                info.score = 0.0

    def get_state_at(self, timestamp: datetime) -> SystemState:
        """
        Get state at arbitrary time.

        This is an alias for advance_to that makes the API clearer
        when you just want to query state at a specific time.

        Args:
            timestamp: Target timestamp

        Returns:
            SystemState at the given timestamp
        """
        return self.advance_to(timestamp)

    def iterate_states(
        self,
        start: datetime,
        end: datetime,
        step: timedelta
    ) -> Iterator[SystemState]:
        """
        Yield states at regular intervals for animation/stepping.

        Args:
            start: Start timestamp
            end: End timestamp
            step: Time step between states

        Yields:
            SystemState at each step
        """
        # Reset to ensure we start fresh
        self.reset()

        current = start
        while current <= end:
            state = self.advance_to(current)
            # Return a copy so the caller can store states
            yield state.copy()
            current += step

    def get_current_state(self) -> SystemState:
        """Get the current state without advancing."""
        return self._state

    @property
    def time_bounds(self) -> Optional[tuple]:
        """
        Get the earliest and latest event timestamps.

        Returns:
            Tuple of (earliest, latest) or None if no events
        """
        return self.event_stream.get_time_bounds()
