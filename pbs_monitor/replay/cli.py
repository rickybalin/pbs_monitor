"""
CLI command for PBS job replay.

Provides the 'replay' subcommand for visualizing historical job timelines.
"""

import sys
from datetime import datetime, timedelta
from typing import Optional, List
import time

try:
    import pytz
    CENTRAL_TZ = pytz.timezone('America/Chicago')
    HAS_PYTZ = True
except ImportError:
    CENTRAL_TZ = None
    HAS_PYTZ = False

from pbs_monitor.models.job import PBSJob
from pbs_monitor.replay.event_stream import JobEventStream
from pbs_monitor.replay.state_tracker import StateTracker
from pbs_monitor.replay.renderer import SplitPanelRenderer, TextRenderer

# Waffle chart renderer is optional (requires matplotlib)
try:
    from pbs_monitor.replay.waffle_renderer import WaffleChartRenderer
    HAS_WAFFLE = True
except ImportError:
    HAS_WAFFLE = False


def parse_time_arg(time_str: str, reference: Optional[datetime] = None) -> datetime:
    """
    Parse a time argument which can be:
    - ISO format: 2024-01-15T14:30:00
    - Date only: 2024-01-15
    - Relative: "24h ago", "7d ago", "30m ago"
    - "now"

    Args:
        time_str: Time string to parse
        reference: Reference time for relative times (default: now)

    Returns:
        Parsed datetime
    """
    if reference is None:
        reference = datetime.now()

    time_str = time_str.strip().lower()

    if time_str == "now":
        return reference

    # Handle relative times
    if time_str.endswith(" ago") or time_str.endswith("ago"):
        time_str = time_str.replace(" ago", "").replace("ago", "")

        # Parse number and unit
        unit = time_str[-1]
        try:
            value = int(time_str[:-1])
        except ValueError:
            raise ValueError(f"Invalid relative time: {time_str}")

        if unit == 'm':
            delta = timedelta(minutes=value)
        elif unit == 'h':
            delta = timedelta(hours=value)
        elif unit == 'd':
            delta = timedelta(days=value)
        elif unit == 'w':
            delta = timedelta(weeks=value)
        else:
            raise ValueError(f"Unknown time unit: {unit}")

        return reference - delta

    # Try ISO format
    for fmt in [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]:
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            continue

    raise ValueError(f"Could not parse time: {time_str}")


def parse_step_arg(step_str: str) -> timedelta:
    """
    Parse a step argument like "5m", "1h", "30s".

    Args:
        step_str: Step string to parse

    Returns:
        timedelta for the step
    """
    step_str = step_str.strip().lower()

    unit = step_str[-1]
    try:
        value = int(step_str[:-1])
    except ValueError:
        raise ValueError(f"Invalid step: {step_str}")

    if unit == 's':
        return timedelta(seconds=value)
    elif unit == 'm':
        return timedelta(minutes=value)
    elif unit == 'h':
        return timedelta(hours=value)
    elif unit == 'd':
        return timedelta(days=value)
    else:
        raise ValueError(f"Unknown time unit: {unit}")


class ReplayCommand:
    """
    Command handler for the 'replay' subcommand.
    """

    def __init__(self, collector, config):
        """
        Initialize the replay command.

        Args:
            collector: DataCollector instance
            config: Configuration object
        """
        self.collector = collector
        self.config = config

    def execute(self, args) -> int:
        """
        Execute the replay command.

        Args:
            args: Parsed command-line arguments

        Returns:
            Exit code (0 for success)
        """
        try:
            # Parse time arguments
            now = datetime.now()
            start_time = parse_time_arg(args.start, now) if args.start else now - timedelta(hours=24)
            end_time = parse_time_arg(args.end, now) if args.end else now

            # Validate time range
            if start_time >= end_time:
                print("Error: Start time must be before end time", file=sys.stderr)
                return 1

            # Parse step if provided
            step = parse_step_arg(args.step) if args.step else timedelta(hours=1)

            # Get jobs for the replay window
            jobs = self._get_jobs_for_replay(
                start_time,
                end_time,
                user=args.user,
                queue=args.queue,
                project=args.project
            )

            if not jobs:
                print("No jobs found for the specified time range and filters.")
                return 0

            print(f"Found {len(jobs)} jobs for replay from {start_time} to {end_time}")

            # Get score formula from server if available
            score_formula = None
            server_defaults = None
            total_nodes = None
            try:
                score_formula = self.collector.pbs_commands.get_job_sort_formula()
                server_data = self.collector.get_cached_server_data()
                if server_data:
                    server_info = server_data.get("Server", {})
                    for server_name, server_details in server_info.items():
                        server_defaults = server_details.get("resources_default", {})
                        # Get total_cpus which represents total nodes
                        if server_defaults:
                            total_nodes = server_defaults.get("total_cpus")
                            if total_nodes:
                                try:
                                    total_nodes = int(total_nodes)
                                except (ValueError, TypeError):
                                    total_nodes = None
                        break
            except Exception:
                pass  # Use defaults if we can't get the formula

            # Build event stream and state tracker
            event_stream = JobEventStream(jobs)
            tracker = StateTracker(
                event_stream,
                score_formula=score_formula,
                server_defaults=server_defaults
            )

            # Execute based on output format
            output_format = args.output_format or "split-panel"

            if output_format == "split-panel":
                # Use None for top_n to enable auto-sizing, unless explicitly set
                top_n = args.top_n if args.top_n else None
                return self._run_split_panel(
                    tracker, start_time, end_time, step,
                    top_n=top_n,
                    live=getattr(args, 'live', False)
                )
            elif output_format == "text":
                return self._run_text_output(
                    event_stream, tracker, start_time, end_time
                )
            elif output_format == "timeline":
                return self._run_timeline_output(
                    tracker, start_time, end_time, step
                )
            elif output_format == "waffle":
                return self._run_waffle_output(
                    tracker, start_time, end_time, step,
                    total_nodes=total_nodes,
                    color_by=getattr(args, 'color_by', 'queue'),
                    grid_rows=getattr(args, 'grid_rows', 144),
                    grid_cols=getattr(args, 'grid_cols', 256),
                    small_job_threshold=getattr(args, 'small_job_threshold', None),
                    output_dir=getattr(args, 'output_dir', '.'),
                    frame_duration=getattr(args, 'frame_duration', 1000)
                )
            else:
                print(f"Unknown output format: {output_format}", file=sys.stderr)
                return 1

        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
        except Exception as e:
            print(f"Error during replay: {e}", file=sys.stderr)
            if hasattr(args, 'verbose') and args.verbose:
                import traceback
                traceback.print_exc()
            return 1

    def _get_jobs_for_replay(
        self,
        start_time: datetime,
        end_time: datetime,
        user: Optional[str] = None,
        queue: Optional[str] = None,
        project: Optional[str] = None
    ) -> List[PBSJob]:
        """
        Get jobs that were active during the replay window.

        Args:
            start_time: Start of replay window
            end_time: End of replay window
            user: Optional user filter
            queue: Optional queue filter
            project: Optional project filter

        Returns:
            List of PBSJob objects active during the window
        """
        # Calculate days to look back
        days_back = (datetime.now() - start_time).days + 1

        # Get completed jobs from database/PBS
        all_jobs = []

        # Try to get historical jobs
        try:
            completed_jobs = self.collector.get_completed_jobs(
                user=user,
                days=max(days_back, 7),
                include_pbs_history=True
            )
            all_jobs.extend(completed_jobs)
        except Exception as e:
            print(f"Warning: Could not retrieve completed jobs: {e}", file=sys.stderr)

        # Also get current jobs
        try:
            current_jobs = self.collector.get_jobs(
                user=user,
                queue=queue,
                project=project,
                force_refresh=True
            )
            # Add current jobs that aren't already in the list
            current_ids = {j.job_id for j in all_jobs}
            for job in current_jobs:
                if job.job_id not in current_ids:
                    all_jobs.append(job)
        except Exception as e:
            print(f"Warning: Could not retrieve current jobs: {e}", file=sys.stderr)

        # Filter to jobs active in the window
        active_jobs = []
        for job in all_jobs:
            # Apply filters
            if queue and job.queue.lower() != queue.lower():
                continue
            if project and job.project and project.lower() not in job.project.lower():
                continue

            # Check if job was active during the window
            job_start = job.submit_time
            if not job_start:
                continue

            job_end = job.end_time or datetime.now()

            # Job is active if its period overlaps with the window
            if job_start <= end_time and job_end >= start_time:
                active_jobs.append(job)

        return active_jobs

    def _run_split_panel(
        self,
        tracker: StateTracker,
        start_time: datetime,
        end_time: datetime,
        step: timedelta,
        top_n: Optional[int] = None,
        live: bool = False
    ) -> int:
        """
        Run split-panel display mode.

        Args:
            tracker: StateTracker instance
            start_time: Start time
            end_time: End time
            step: Time step between frames
            top_n: Number of jobs to show per panel. If None, auto-sizes to terminal.
            live: If True, continuously update

        Returns:
            Exit code
        """
        # Create renderer with auto-sizing if top_n not specified
        renderer = SplitPanelRenderer(top_n=top_n, auto_size=True)

        if live:
            # Live mode - continuously update
            try:
                while True:
                    now = datetime.now()
                    state = tracker.get_state_at(now)
                    output = renderer.render_state(state)

                    # Clear screen and print
                    print("\033[2J\033[H", end="")  # ANSI clear screen
                    print(output)
                    print(f"\nLive mode - updating every 30s. Press Ctrl+C to exit.")

                    time.sleep(30)
            except KeyboardInterrupt:
                print("\nExiting live mode.")
                return 0
        else:
            # Step through time
            states = list(tracker.iterate_states(start_time, end_time, step))

            if not states:
                print("No states to display for the given time range.")
                return 0

            # Interactive stepping
            current_idx = 0
            while True:
                state = states[current_idx]
                output = renderer.render_state(state)

                # Clear screen
                print("\033[2J\033[H", end="")
                print(output)
                print(f"\nStep {current_idx + 1}/{len(states)}")
                print("[n]ext, [p]rev, [f]irst, [l]ast, [q]uit, or enter step number: ", end="")

                try:
                    cmd = input().strip().lower()
                except EOFError:
                    break

                if cmd == 'q' or cmd == 'quit':
                    break
                elif cmd == 'n' or cmd == 'next' or cmd == '':
                    current_idx = min(current_idx + 1, len(states) - 1)
                elif cmd == 'p' or cmd == 'prev':
                    current_idx = max(current_idx - 1, 0)
                elif cmd == 'f' or cmd == 'first':
                    current_idx = 0
                elif cmd == 'l' or cmd == 'last':
                    current_idx = len(states) - 1
                elif cmd.isdigit():
                    idx = int(cmd) - 1
                    if 0 <= idx < len(states):
                        current_idx = idx
                    else:
                        print(f"Invalid step number. Valid range: 1-{len(states)}")
                        time.sleep(1)

        return 0

    def _run_text_output(
        self,
        event_stream: JobEventStream,
        tracker: StateTracker,
        start_time: datetime,
        end_time: datetime
    ) -> int:
        """
        Run text output mode (event log).

        Args:
            event_stream: JobEventStream instance
            tracker: StateTracker instance
            start_time: Start time
            end_time: End time

        Returns:
            Exit code
        """
        renderer = TextRenderer()

        # Get events in range
        events = event_stream.get_events_in_range(start_time, end_time)

        # Print event log
        print(renderer.render_event_log(events))
        print()

        # Print final state summary
        final_state = tracker.get_state_at(end_time)
        print(renderer.render_state_summary(final_state))

        return 0

    def _run_timeline_output(
        self,
        tracker: StateTracker,
        start_time: datetime,
        end_time: datetime,
        step: timedelta
    ) -> int:
        """
        Run timeline summary output mode.

        Args:
            tracker: StateTracker instance
            start_time: Start time
            end_time: End time
            step: Time step

        Returns:
            Exit code
        """
        renderer = TextRenderer()

        # Collect states at each step
        states = list(tracker.iterate_states(start_time, end_time, step))

        # Print timeline summary
        print(renderer.render_timeline_summary(states))

        return 0

    def _run_waffle_output(
        self,
        tracker: StateTracker,
        start_time: datetime,
        end_time: datetime,
        step: timedelta,
        total_nodes: Optional[int] = None,
        color_by: str = 'queue',
        grid_rows: int = 144,
        grid_cols: int = 256,
        small_job_threshold: Optional[int] = None,
        output_dir: str = '.',
        frame_duration: int = 1000
    ) -> int:
        """
        Run waffle chart output mode.

        Args:
            tracker: StateTracker instance
            start_time: Start time
            end_time: End time
            step: Time step between frames
            total_nodes: Total nodes in the system (from PBS)
            color_by: How to color jobs (job, user, queue, project, allocation)
            grid_rows: Number of rows in the waffle grid
            grid_cols: Number of columns in the waffle grid
            small_job_threshold: Node threshold for bundling small jobs
            output_dir: Output directory for frames and GIF
            frame_duration: Duration of each frame in GIF (milliseconds)

        Returns:
            Exit code
        """
        if not HAS_WAFFLE:
            print("Error: matplotlib is required for waffle chart output.", file=sys.stderr)
            print("Install it with: pip install matplotlib", file=sys.stderr)
            return 1

        # Validate total_nodes
        if total_nodes is None or total_nodes <= 0:
            # Try to estimate from the maximum running nodes we see
            print("Warning: Could not get total_nodes from PBS server.", file=sys.stderr)
            print("Estimating from job data...", file=sys.stderr)

            # Find max running nodes across all states
            max_nodes = 0
            for state in tracker.iterate_states(start_time, end_time, step):
                max_nodes = max(max_nodes, state.running_nodes)

            if max_nodes > 0:
                # Add 20% buffer for estimation
                total_nodes = int(max_nodes * 1.2)
                print(f"Estimated total_nodes: {total_nodes}", file=sys.stderr)
            else:
                print("Error: Could not determine total node count.", file=sys.stderr)
                print("Please ensure PBS server is accessible.", file=sys.stderr)
                return 1

            # Reset tracker for actual rendering
            tracker.reset()

        print(f"Generating waffle chart animation...")
        print(f"  Total nodes: {total_nodes:,}")
        print(f"  Grid: {grid_rows} x {grid_cols} ({grid_rows * grid_cols:,} cells)")
        print(f"  Color by: {color_by}")
        print(f"  Output directory: {output_dir}")

        # Create renderer
        renderer = WaffleChartRenderer(
            total_nodes=total_nodes,
            grid_rows=grid_rows,
            grid_cols=grid_cols,
            color_by=color_by,
            small_job_threshold=small_job_threshold,
            output_dir=output_dir,
            frame_duration=frame_duration
        )

        # Show effective grid info
        print(f"  Nodes per cell: {renderer.nodes_per_cell:.2f}")
        print(f"  Effective cells: {renderer.effective_cells:,} (of {renderer.total_cells:,})")
        print(f"  Small job threshold: {renderer.small_job_threshold} nodes")
        print()

        # Generate frames
        frame_num = 0
        states = list(tracker.iterate_states(start_time, end_time, step))
        total_frames = len(states)

        print(f"Rendering {total_frames} frames...")

        for state in states:
            frame_path = renderer.render_frame(state, frame_num)
            frame_num += 1

            # Progress indicator
            progress = frame_num / total_frames * 100
            print(f"\r  Frame {frame_num}/{total_frames} ({progress:.0f}%)", end="", flush=True)

        print()  # Newline after progress

        if frame_num == 0:
            print("No frames generated - no states in the given time range.")
            return 0

        print(f"Saved {frame_num} frames to {renderer.frames_dir}/")

        # Create GIF
        print("Creating animated GIF...")
        gif_path = renderer.create_gif(start_time)

        if gif_path:
            print(f"GIF saved to: {gif_path}")
        else:
            print("Warning: Could not create GIF. Frames are available as PNGs.")

        return 0
