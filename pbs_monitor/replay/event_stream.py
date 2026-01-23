"""
Event stream generation for PBS job replay.

Converts PBS jobs into a timeline of events (submitted, eligible, started, ended)
that can be replayed to reconstruct system state at any point in time.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Literal, Optional, Tuple

from pbs_monitor.models.job import PBSJob


EventType = Literal["submitted", "eligible", "started", "ended"]


@dataclass
class JobEvent:
    """Represents a single job lifecycle event."""

    timestamp: datetime
    event_type: EventType
    job_id: str
    job: PBSJob

    def __lt__(self, other: "JobEvent") -> bool:
        """Allow sorting events by timestamp."""
        return self.timestamp < other.timestamp

    def __repr__(self) -> str:
        return f"JobEvent({self.timestamp}, {self.event_type}, {self.job_id})"


def job_to_events(job: PBSJob) -> List[JobEvent]:
    """
    Convert a PBSJob into a list of lifecycle events.

    Each job can produce up to 4 events:
    - submitted: When job entered the queue (qtime/submit_time)
    - eligible: When job became eligible for scheduling (etime/eligible_time)
    - started: When job began execution (stime/start_time)
    - ended: When job completed/terminated (mtime/end_time)

    Args:
        job: The PBSJob to convert

    Returns:
        List of JobEvent objects, unsorted
    """
    events = []

    # Get eligible time from raw attributes if available
    eligible_time = _get_eligible_time(job)

    # Submit event
    if job.submit_time:
        events.append(JobEvent(
            timestamp=job.submit_time,
            event_type="submitted",
            job_id=job.job_id,
            job=job
        ))

    # Eligible event (only if different from submit time)
    if eligible_time and eligible_time != job.submit_time:
        events.append(JobEvent(
            timestamp=eligible_time,
            event_type="eligible",
            job_id=job.job_id,
            job=job
        ))
    elif job.submit_time and not eligible_time:
        # If no explicit eligible time, job is eligible at submit time
        # Don't create a separate event, the submitted event covers it
        pass

    # Started event
    if job.start_time:
        events.append(JobEvent(
            timestamp=job.start_time,
            event_type="started",
            job_id=job.job_id,
            job=job
        ))

    # Ended event
    if job.end_time:
        events.append(JobEvent(
            timestamp=job.end_time,
            event_type="ended",
            job_id=job.job_id,
            job=job
        ))

    return events


def _get_eligible_time(job: PBSJob) -> Optional[datetime]:
    """
    Extract eligible time from job's raw attributes.

    PBS provides 'etime' as the timestamp when the job became eligible
    for scheduling. This can differ from submit_time when:
    - Job is submitted to a routing queue with limits
    - Job has dependencies that need to be satisfied
    - Job was submitted with a future start time or hold

    Args:
        job: The PBSJob to extract eligible time from

    Returns:
        datetime of when job became eligible, or None
    """
    if not job.raw_attributes:
        return None

    etime_str = job.raw_attributes.get('etime')
    if not etime_str:
        return None

    return PBSJob._parse_pbs_time(etime_str)


class JobEventStream:
    """
    Generates and manages a timeline of job events.

    Converts a list of PBS jobs into a sorted stream of events
    that can be used to reconstruct system state at any point in time.
    """

    def __init__(self, jobs: List[PBSJob]):
        """
        Initialize the event stream from a list of jobs.

        Args:
            jobs: List of PBSJob objects to convert to events
        """
        self.jobs = jobs
        self._events: Optional[List[JobEvent]] = None

    def build_timeline(self) -> List[JobEvent]:
        """
        Convert all jobs to events and sort by timestamp.

        Returns:
            Sorted list of JobEvent objects (earliest first)
        """
        if self._events is not None:
            return self._events

        events = []
        for job in self.jobs:
            events.extend(job_to_events(job))

        # Sort by timestamp, then by event type priority
        # (submitted < eligible < started < ended for same timestamp)
        event_priority = {"submitted": 0, "eligible": 1, "started": 2, "ended": 3}
        self._events = sorted(
            events,
            key=lambda e: (e.timestamp, event_priority.get(e.event_type, 0))
        )

        return self._events

    def get_events_in_range(
        self,
        start: datetime,
        end: datetime
    ) -> List[JobEvent]:
        """
        Get events within a specific time window.

        Args:
            start: Start of time window (inclusive)
            end: End of time window (inclusive)

        Returns:
            List of events within the window, sorted by timestamp
        """
        timeline = self.build_timeline()
        return [
            event for event in timeline
            if start <= event.timestamp <= end
        ]

    def get_time_bounds(self) -> Optional[Tuple[datetime, datetime]]:
        """
        Return earliest and latest event timestamps.

        Returns:
            Tuple of (earliest, latest) timestamps, or None if no events
        """
        timeline = self.build_timeline()
        if not timeline:
            return None

        return (timeline[0].timestamp, timeline[-1].timestamp)

    def get_jobs_active_in_range(
        self,
        start: datetime,
        end: datetime
    ) -> List[PBSJob]:
        """
        Get jobs that were active (queued or running) during a time window.

        A job is considered active in the window if:
        - It was submitted before or during the window AND
        - It ended after the window started (or hasn't ended)

        Args:
            start: Start of time window
            end: End of time window

        Returns:
            List of jobs active during the window
        """
        active_jobs = []

        for job in self.jobs:
            # Determine job's active period
            job_start = job.submit_time
            if not job_start:
                # Skip jobs without submit time
                continue

            job_end = job.end_time or datetime.max

            # Check if job's active period overlaps with the window
            if job_start <= end and job_end >= start:
                active_jobs.append(job)

        return active_jobs

    def __len__(self) -> int:
        """Return number of events in the stream."""
        return len(self.build_timeline())

    def __iter__(self):
        """Iterate over events in chronological order."""
        return iter(self.build_timeline())
