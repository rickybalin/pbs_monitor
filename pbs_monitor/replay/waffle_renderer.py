"""
Waffle chart renderer for PBS job replay visualization.

Provides matplotlib-based waffle chart visualization showing node utilization
as a grid where each cell represents N nodes, colored by job/user/queue/project/allocation.
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
import math

try:
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend for file output
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from matplotlib.colors import to_rgba
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

try:
    import pytz
    CENTRAL_TZ = pytz.timezone('America/Chicago')
    HAS_PYTZ = True
except ImportError:
    CENTRAL_TZ = None
    HAS_PYTZ = False

from pbs_monitor.models.job import PBSJob
from pbs_monitor.replay.state_tracker import SystemState


# Default color palette for distinct categories
DEFAULT_COLORS = [
    '#1f77b4',  # blue
    '#ff7f0e',  # orange
    '#2ca02c',  # green
    '#d62728',  # red
    '#9467bd',  # purple
    '#8c564b',  # brown
    '#e377c2',  # pink
    '#7f7f7f',  # gray
    '#bcbd22',  # olive
    '#17becf',  # cyan
    '#aec7e8',  # light blue
    '#ffbb78',  # light orange
    '#98df8a',  # light green
    '#ff9896',  # light red
    '#c5b0d5',  # light purple
]

FREE_COLOR = '#e0e0e0'  # Light gray for unused nodes
SMALL_JOBS_COLOR = '#a0a0a0'  # Darker gray for bundled small jobs


def _format_timestamp_central(dt: datetime) -> str:
    """Format timestamp in US Central time."""
    if HAS_PYTZ and CENTRAL_TZ:
        if dt.tzinfo is None:
            central_dt = CENTRAL_TZ.localize(dt)
        else:
            central_dt = dt.astimezone(CENTRAL_TZ)
        return central_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
    else:
        return dt.strftime('%Y-%m-%d %H:%M:%S')


def _format_timestamp_filename(dt: datetime) -> str:
    """Format timestamp for use in filenames."""
    if HAS_PYTZ and CENTRAL_TZ:
        if dt.tzinfo is None:
            central_dt = CENTRAL_TZ.localize(dt)
        else:
            central_dt = dt.astimezone(CENTRAL_TZ)
        return central_dt.strftime('%Y%m%d_%H%M%S')
    else:
        return dt.strftime('%Y%m%d_%H%M%S')


class WaffleChartRenderer:
    """
    Renders node utilization as a waffle chart (grid visualization).

    Each cell in the grid represents a fixed number of nodes. Running jobs
    are shown as colored cells, with unused nodes shown in gray.
    """

    def __init__(
        self,
        total_nodes: int,
        grid_rows: int = 72,
        grid_cols: int = 144,
        color_by: str = 'queue',
        small_job_threshold: Optional[int] = None,
        output_dir: str = '.',
        frame_duration: int = 1000,
    ):
        """
        Initialize the waffle chart renderer.

        Args:
            total_nodes: Total number of nodes in the system (from PBS)
            grid_rows: Number of rows in the grid (default: 72 for 2:1 ratio)
            grid_cols: Number of columns in the grid (default: 144 for 2:1 ratio)
            color_by: How to color jobs - 'job', 'user', 'queue', 'project', or 'allocation'
            small_job_threshold: Nodes threshold for bundling small jobs (default: nodes_per_cell)
            output_dir: Base directory for output files
            frame_duration: Duration of each frame in GIF (milliseconds)
        """
        if not HAS_MATPLOTLIB:
            raise ImportError("matplotlib is required for waffle chart visualization")

        self.total_nodes = total_nodes
        self.grid_rows = grid_rows
        self.grid_cols = grid_cols
        self.total_cells = grid_rows * grid_cols

        # Calculate nodes per cell - each cell represents some number of nodes
        # The grid represents the TOTAL capacity, so we scale to fit
        self.nodes_per_cell = total_nodes / self.total_cells

        # For display purposes, round to a sensible value
        if self.nodes_per_cell < 1:
            # More cells than nodes - we'll only use a portion of the grid
            # Recalculate: use exactly as many cells as we have nodes
            self.nodes_per_cell = 1.0
            self.effective_cells = total_nodes
        else:
            self.effective_cells = self.total_cells

        self.color_by = color_by
        self.small_job_threshold = small_job_threshold if small_job_threshold is not None else max(1, int(math.ceil(self.nodes_per_cell)))
        self.output_dir = Path(output_dir)
        self.frame_duration = frame_duration
        self.frame_duration = frame_duration

        # Create frames directory
        self.frames_dir = self.output_dir / 'replay_frames'
        self.frames_dir.mkdir(parents=True, exist_ok=True)

        # Color assignment cache
        self._color_cache: Dict[str, str] = {}
        self._color_index = 0

        # Track generated frames for GIF creation
        self._frame_paths: List[Path] = []

    def _get_color_key(self, job: PBSJob) -> str:
        """Get the key used for coloring based on color_by mode."""
        if self.color_by == 'job':
            return job.job_id
        elif self.color_by == 'user':
            return job.owner or 'unknown'
        elif self.color_by == 'queue':
            return job.queue or 'unknown'
        elif self.color_by == 'project':
            return job.project or 'unknown'
        elif self.color_by == 'allocation':
            return job.allocation_type or 'unknown'
        else:
            return job.job_id

    def _assign_color(self, key: str) -> str:
        """Assign a color to a key, reusing cached colors."""
        if key not in self._color_cache:
            color = DEFAULT_COLORS[self._color_index % len(DEFAULT_COLORS)]
            self._color_cache[key] = color
            self._color_index += 1
        return self._color_cache[key]

    def _group_jobs_by_color_key(self, running_jobs: Dict[str, PBSJob]) -> Dict[str, List[PBSJob]]:
        """Group jobs by their color key."""
        groups: Dict[str, List[PBSJob]] = defaultdict(list)
        for job in running_jobs.values():
            key = self._get_color_key(job)
            groups[key].append(job)
        return dict(groups)

    def _bundle_small_jobs(self, running_jobs: Dict[str, PBSJob]) -> Tuple[List[PBSJob], List[PBSJob]]:
        """
        Separate jobs into regular and small jobs for bundling.

        Returns:
            Tuple of (regular_jobs, small_jobs)
        """
        regular = []
        small = []
        for job in running_jobs.values():
            if job.nodes <= self.small_job_threshold:
                small.append(job)
            else:
                regular.append(job)
        return regular, small

    def _calculate_cells_for_nodes(self, nodes: int) -> int:
        """Calculate number of cells for a given node count."""
        # Scale nodes to cells based on the ratio
        cells = round(nodes / self.nodes_per_cell)
        return max(1, cells)  # At least 1 cell per job/group

    def _calculate_total_used_cells(self, running_nodes: int) -> int:
        """Calculate total cells that should be used for running nodes."""
        return round(running_nodes / self.nodes_per_cell)

    def _calculate_free_cells(self, running_nodes: int) -> int:
        """Calculate cells that should be shown as free."""
        used_cells = self._calculate_total_used_cells(running_nodes)
        return self.effective_cells - used_cells

    def _build_cell_assignments(
        self,
        state: SystemState
    ) -> Tuple[List[Tuple[str, str]], Dict[str, int], int]:
        """
        Build cell assignments for the waffle chart.

        Returns:
            Tuple of:
            - List of (color_key, color) for each cell
            - Dict of color_key -> total_nodes for legend
            - Total small jobs node count (if bundling)
        """
        assignments = []
        legend_data: Dict[str, int] = defaultdict(int)
        small_jobs_nodes = 0

        running_jobs = state.running_jobs

        if self.color_by == 'job':
            # Bundle small jobs together
            regular_jobs, small_jobs = self._bundle_small_jobs(running_jobs)

            # Sort regular jobs by node count (largest first)
            regular_jobs.sort(key=lambda j: j.nodes, reverse=True)

            # Assign cells to regular jobs
            for job in regular_jobs:
                key = self._get_color_key(job)
                color = self._assign_color(key)
                cells = self._calculate_cells_for_nodes(job.nodes)
                assignments.extend([(key, color)] * cells)
                legend_data[key] += job.nodes

            # Bundle small jobs
            if small_jobs:
                small_jobs_nodes = sum(j.nodes for j in small_jobs)
                cells = self._calculate_cells_for_nodes(small_jobs_nodes)
                assignments.extend([('.small_jobs', SMALL_JOBS_COLOR)] * cells)
        else:
            # Group by color key (user, queue, project, allocation)
            groups = self._group_jobs_by_color_key(running_jobs)

            # Sort groups by total nodes (largest first)
            sorted_groups = sorted(
                groups.items(),
                key=lambda x: sum(j.nodes for j in x[1]),
                reverse=True
            )

            for key, jobs in sorted_groups:
                color = self._assign_color(key)
                total_nodes = sum(j.nodes for j in jobs)
                cells = self._calculate_cells_for_nodes(total_nodes)
                assignments.extend([(key, color)] * cells)
                legend_data[key] = total_nodes

        return assignments, dict(legend_data), small_jobs_nodes

    def render_frame(
        self,
        state: SystemState,
        frame_number: int
    ) -> Path:
        """
        Render a single frame of the waffle chart.

        Args:
            state: SystemState to render
            frame_number: Frame number for filename

        Returns:
            Path to the generated PNG file
        """
        # Build cell assignments
        assignments, legend_data, small_jobs_nodes = self._build_cell_assignments(state)

        # Create figure with 16:9 aspect ratio
        fig_width = 16
        fig_height = 9
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))

        # Create grid data - start with a "not used" color for cells beyond effective_cells
        NOT_USED_COLOR = '#f5f5f5'  # Very light gray for unused grid cells
        grid = [[NOT_USED_COLOR for _ in range(self.grid_cols)] for _ in range(self.grid_rows)]

        # Calculate how many cells represent the total system capacity
        # and how many are free (not running any jobs)
        running_nodes = state.running_nodes
        free_cells = self._calculate_free_cells(running_nodes)

        # Fill grid: first the job assignments, then free cells, then unused grid space
        cell_idx = 0
        total_to_fill = len(assignments) + free_cells

        for row in range(self.grid_rows):
            for col in range(self.grid_cols):
                if cell_idx < len(assignments):
                    # Job cells
                    _, color = assignments[cell_idx]
                    grid[row][col] = color
                elif cell_idx < total_to_fill:
                    # Free node cells
                    grid[row][col] = FREE_COLOR
                # else: leave as NOT_USED_COLOR (grid cells beyond system capacity)
                cell_idx += 1

        # Convert to RGBA for imshow
        rgba_grid = [[[0.0, 0.0, 0.0, 1.0] for _ in range(self.grid_cols)] for _ in range(self.grid_rows)]
        for row in range(self.grid_rows):
            for col in range(self.grid_cols):
                rgba_grid[row][col] = list(to_rgba(grid[row][col]))

        # Display the grid
        ax.imshow(rgba_grid, aspect='auto')
        ax.set_xticks([])
        ax.set_yticks([])

        # Calculate statistics
        running_nodes = state.running_nodes
        free_nodes = self.total_nodes - running_nodes
        utilization = (running_nodes / self.total_nodes * 100) if self.total_nodes > 0 else 0

        # Title with timestamp and stats
        timestamp_str = _format_timestamp_central(state.timestamp)
        title = (
            f"Node Utilization - {timestamp_str}\n"
            f"Running: {running_nodes:,} / {self.total_nodes:,} nodes ({utilization:.1f}%) | "
            f"Jobs: {state.total_running} | Colored by: {self.color_by}"
        )
        ax.set_title(title, fontsize=12, fontweight='bold')

        # Build legend
        legend_handles = []

        # Determine if we show all items or top 5
        show_all = self.color_by in ['queue', 'allocation']

        # Sort legend items by node count
        sorted_legend = sorted(legend_data.items(), key=lambda x: x[1], reverse=True)

        if not show_all and len(sorted_legend) > 5:
            display_items = sorted_legend[:5]
            other_nodes = sum(nodes for _, nodes in sorted_legend[5:])
            if other_nodes > 0:
                display_items.append(('(other)', other_nodes))
        else:
            display_items = sorted_legend

        for key, nodes in display_items:
            if key == '(other)':
                color = '#808080'
            else:
                color = self._color_cache.get(key, '#808080')
            patch = mpatches.Patch(color=color, label=f'{key} ({nodes:,} nodes)')
            legend_handles.append(patch)

        # Add small jobs to legend if applicable
        if small_jobs_nodes > 0:
            patch = mpatches.Patch(
                color=SMALL_JOBS_COLOR,
                label=f'small jobs ({small_jobs_nodes:,} nodes)'
            )
            legend_handles.append(patch)

        # Add free nodes to legend
        free_patch = mpatches.Patch(color=FREE_COLOR, label=f'free ({free_nodes:,} nodes)')
        legend_handles.append(free_patch)

        # Position legend outside the plot
        ax.legend(
            handles=legend_handles,
            loc='upper left',
            bbox_to_anchor=(1.02, 1),
            fontsize=9,
            framealpha=0.9
        )

        # Adjust layout to make room for legend
        plt.tight_layout()
        fig.subplots_adjust(right=0.82)

        # Save frame
        frame_path = self.frames_dir / f'frame_{frame_number:05d}.png'
        fig.savefig(frame_path, dpi=100, bbox_inches='tight', facecolor='white')
        plt.close(fig)

        self._frame_paths.append(frame_path)
        return frame_path

    def create_gif(self, start_time: datetime) -> Optional[Path]:
        """
        Create an animated GIF from all rendered frames.

        Args:
            start_time: Start time for filename timestamp

        Returns:
            Path to the generated GIF file, or None if no frames
        """
        if not self._frame_paths:
            return None

        try:
            from PIL import Image
        except ImportError:
            print("Warning: PIL/Pillow is required for GIF creation. Frames saved as PNGs.")
            return None

        # Load all frames
        frames = []
        for frame_path in self._frame_paths:
            img = Image.open(frame_path)
            frames.append(img.copy())
            img.close()

        if not frames:
            return None

        # Generate filename with timestamp
        timestamp_str = _format_timestamp_filename(start_time)
        gif_path = self.output_dir / f'replay_{timestamp_str}.gif'

        # Save as GIF
        frames[0].save(
            gif_path,
            save_all=True,
            append_images=frames[1:],
            duration=self.frame_duration,
            loop=0  # Loop forever
        )

        # Clean up frame images
        for img in frames:
            img.close()

        return gif_path

    def clear_frames(self):
        """Clear the list of rendered frames (but don't delete files)."""
        self._frame_paths = []
        self._color_cache = {}
        self._color_index = 0

    def get_frame_count(self) -> int:
        """Get the number of frames rendered."""
        return len(self._frame_paths)
