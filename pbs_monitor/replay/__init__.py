"""
PBS Job Replay Module

Provides functionality to replay historical PBS job timelines,
showing running jobs and top-N queued jobs with scores at any
point in time.
"""

from pbs_monitor.replay.event_stream import JobEvent, JobEventStream, job_to_events
from pbs_monitor.replay.state_tracker import (
    QueuedJobInfo,
    SystemState,
    StateTracker,
)
from pbs_monitor.replay.renderer import (
    SplitPanelRenderer,
    TextRenderer,
)

# Waffle chart renderer is optional (requires matplotlib)
try:
    from pbs_monitor.replay.waffle_renderer import WaffleChartRenderer
    _HAS_WAFFLE = True
except ImportError:
    _HAS_WAFFLE = False

__all__ = [
    "JobEvent",
    "JobEventStream",
    "job_to_events",
    "QueuedJobInfo",
    "SystemState",
    "StateTracker",
    "SplitPanelRenderer",
    "TextRenderer",
]

if _HAS_WAFFLE:
    __all__.append("WaffleChartRenderer")
