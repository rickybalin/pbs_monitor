"""
Score Trajectory Analyzer for PBS Monitor Analytics

For a given set of job IDs, plots each job's score-over-time alongside the
mean and ±1 std band of run-time scores from finished jobs with the same
node/walltime shape and queue, plus a projected start time based on the
mean queue time of those comparable jobs.

This helps diagnose jobs whose priority is accruing slowly because their
shape is poorly supported by the queue policy.
"""

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging
import os

import pandas as pd
import numpy as np
from sqlalchemy import and_
from sqlalchemy.orm import Session

try:
   import matplotlib.pyplot as plt
   import matplotlib.dates as mdates
   import seaborn as sns
except Exception:  # pragma: no cover - plotting optional
   plt = None
   mdates = None
   sns = None

from ..database.repositories import RepositoryFactory
from ..database.models import JobHistory, Job, JobState
from ..pbs_commands import PBSCommands
from .run_score import RunScoreAnalyzer


class ScoreTrajectoryAnalyzer:
   """Analyzer for per-job score trajectories vs. comparable historical jobs."""

   def __init__(self, repository_factory: Optional[RepositoryFactory] = None):
      self.repo_factory = repository_factory or RepositoryFactory()
      self.pbs_commands = PBSCommands()
      self.logger = logging.getLogger(__name__)
      # Reuse bins from RunScoreAnalyzer so we don't drift
      self._run_score = RunScoreAnalyzer(repository_factory=self.repo_factory)

   def analyze_jobs(self, job_ids: List[str], days: int = 30) -> List[Dict[str, Any]]:
      """
      Analyze score trajectory for one or more jobs.

      Args:
         job_ids: List of full job IDs
         days: Historical window (days) for comparable-job statistics

      Returns:
         List of per-job result dicts with keys:
            job_id, queue, nodes, walltime, node_bin, walltime_bin,
            submit_time, current_score, trajectory (list of (datetime, float)),
            n_comparable, mean_run_score, std_run_score,
            mean_queue_time_hours, std_queue_time_hours,
            projected_start (datetime or None), error (str or None)
      """
      cutoff_date = datetime.now() - timedelta(days=days)

      # Fetch server data once for score recomputation of comparable jobs
      server_data, server_defaults = self._fetch_server_context()

      results: List[Dict[str, Any]] = []
      with self.repo_factory.get_job_repository().get_session() as session:
         for job_id in job_ids:
            try:
               results.append(
                  self._analyze_single_job(session, job_id, cutoff_date,
                                          server_data, server_defaults)
               )
            except Exception as e:
               self.logger.error(f"Failed to analyze job {job_id}: {e}")
               results.append({
                  'job_id': job_id,
                  'error': str(e),
                  'trajectory': [],
                  'n_comparable': 0,
               })
      return results

   def _analyze_single_job(self, session: Session, job_id: str,
                           cutoff_date: datetime,
                           server_data: Optional[Dict[str, Any]],
                           server_defaults: Optional[Dict[str, Any]]) -> Dict[str, Any]:
      job = session.query(Job).filter(Job.job_id == job_id).first()
      if not job:
         return {
            'job_id': job_id,
            'error': 'Job not found in database',
            'trajectory': [],
            'n_comparable': 0,
         }

      walltime_hours = self._run_score._parse_walltime_to_hours(job.walltime)
      node_bin = self._run_score._categorize_by_nodes(job.nodes or 0)
      walltime_bin = self._run_score._categorize_by_walltime(walltime_hours)

      # Score trajectory from job_history
      trajectory = self._get_trajectory(session, job_id)

      current_score = trajectory[-1][1] if trajectory else None

      # Comparable finished jobs: same queue + same shape bin
      comparable_stats = self._get_comparable_stats(
         session, cutoff_date, job.queue, node_bin, walltime_bin,
         server_data, server_defaults
      )

      projected_start = None
      if (comparable_stats['n_comparable'] > 0
            and comparable_stats['mean_queue_time_hours'] is not None
            and job.submit_time):
         projected_start = job.submit_time + timedelta(
            hours=float(comparable_stats['mean_queue_time_hours'])
         )

      return {
         'job_id': job.job_id,
         'queue': job.queue,
         'nodes': job.nodes,
         'walltime': job.walltime,
         'walltime_hours': walltime_hours,
         'node_bin': node_bin,
         'walltime_bin': walltime_bin,
         'submit_time': job.submit_time,
         'state': job.state.value if job.state else None,
         'current_score': current_score,
         'trajectory': trajectory,
         'projected_start': projected_start,
         'error': None,
         **comparable_stats,
      }

   def _get_trajectory(self, session: Session, job_id: str) -> List[Tuple[datetime, float]]:
      """Return ordered list of (timestamp, score) from job_history."""
      rows = session.query(JobHistory).filter(
         and_(
            JobHistory.job_id == job_id,
            JobHistory.score.isnot(None),
         )
      ).order_by(JobHistory.timestamp).all()
      return [(r.timestamp, float(r.score)) for r in rows if r.timestamp is not None]

   def _get_comparable_stats(self, session: Session, cutoff_date: datetime,
                             queue: Optional[str], node_bin: str, walltime_bin: str,
                             server_data: Optional[Dict[str, Any]],
                             server_defaults: Optional[Dict[str, Any]]) -> Dict[str, Any]:
      """
      Compute run-score and queue-time stats for finished jobs in the same
      queue and the same node/walltime shape bin.
      """
      empty = {
         'n_comparable': 0,
         'mean_run_score': None,
         'std_run_score': None,
         'mean_queue_time_hours': None,
         'std_queue_time_hours': None,
      }
      if queue is None:
         return empty

      finished = session.query(Job).filter(
         and_(
            Job.queue == queue,
            Job.state == JobState.FINISHED,
            Job.end_time >= cutoff_date,
            Job.nodes.isnot(None),
            Job.walltime.isnot(None),
            Job.start_time.isnot(None),
            Job.submit_time.isnot(None),
         )
      ).all()

      run_scores: List[float] = []
      queue_times: List[float] = []
      for fj in finished:
         try:
            fj_nodes = fj.nodes or 0
            fj_wall = self._run_score._parse_walltime_to_hours(fj.walltime)
            if (self._run_score._categorize_by_nodes(fj_nodes) != node_bin
                  or self._run_score._categorize_by_walltime(fj_wall) != walltime_bin):
               continue

            # Q→R score: prefer recomputation from raw_pbs_data, fall back
            # to the score recorded in job_history at/near start_time.
            score = None
            if (server_data is not None and isinstance(fj.raw_pbs_data, dict)):
               try:
                  score = self.pbs_commands.calculate_job_score(
                     fj.raw_pbs_data, server_defaults, server_data
                  )
               except Exception:
                  score = None
            if score is None:
               score = self._find_start_score(session, fj)
            if score is None:
               continue

            qt_hours = (fj.start_time - fj.submit_time).total_seconds() / 3600.0
            if qt_hours < 0:
               continue

            run_scores.append(float(score))
            queue_times.append(float(qt_hours))
         except Exception:
            continue

      if not run_scores:
         return empty

      run_arr = np.asarray(run_scores, dtype=float)
      qt_arr = np.asarray(queue_times, dtype=float)
      return {
         'n_comparable': len(run_scores),
         'mean_run_score': float(run_arr.mean()),
         'std_run_score': float(run_arr.std(ddof=0)),
         'mean_queue_time_hours': float(qt_arr.mean()),
         'std_queue_time_hours': float(qt_arr.std(ddof=0)),
      }

   def _find_start_score(self, session: Session, job: Job) -> Optional[float]:
      """Latest score at/before start_time, falling back to first after."""
      if not job.start_time:
         return None
      hist = session.query(JobHistory).filter(
         JobHistory.job_id == job.job_id,
         JobHistory.timestamp <= job.start_time,
         JobHistory.score.isnot(None),
      ).order_by(JobHistory.timestamp.desc()).first()
      if hist and hist.score is not None:
         return float(hist.score)
      hist2 = session.query(JobHistory).filter(
         JobHistory.job_id == job.job_id,
         JobHistory.timestamp > job.start_time,
         JobHistory.score.isnot(None),
      ).order_by(JobHistory.timestamp.asc()).first()
      if hist2 and hist2.score is not None:
         return float(hist2.score)
      return None

   def _fetch_server_context(self) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
      """Fetch current server data and defaults for score recomputation."""
      try:
         server_data = self.pbs_commands.qstat_server()
         server_defaults: Optional[Dict[str, Any]] = None
         for _, details in server_data.get("Server", {}).items():
            server_defaults = details.get("resources_default", {})
            break
         return server_data, server_defaults
      except Exception as e:
         self.logger.warning(f"Could not fetch PBS server data: {e}")
         return None, None

   def generate_plot(self, results: List[Dict[str, Any]],
                     save_path: Optional[str] = None,
                     dpi: int = 120) -> Optional[str]:
      """
      Render one subplot per job into a single figure and save.

      Returns the saved file path, or None if plotting is unavailable or
      no plottable data exists.
      """
      if plt is None or sns is None:
         self.logger.warning("Plotting libraries not available (matplotlib/seaborn)")
         return None

      plottable = [r for r in results if r.get('trajectory')]
      if not plottable:
         self.logger.warning("No score history available for any of the supplied jobs")
         return None

      sns.set_context('talk')
      sns.set_style('whitegrid')

      n = len(plottable)
      fig, axes = plt.subplots(n, 1, figsize=(12, max(4, 4 * n)), squeeze=False)
      for ax, res in zip(axes[:, 0], plottable):
         self._render_subplot(ax, res)

      fig.tight_layout()

      if save_path:
         os.makedirs(os.path.dirname(save_path) or '.', exist_ok=True)
         fig.savefig(save_path, dpi=dpi, bbox_inches='tight')
         plt.close(fig)
         return save_path

      plt.close(fig)
      return None

   def _render_subplot(self, ax, res: Dict[str, Any]) -> None:
      times = [t for (t, _) in res['trajectory']]
      scores = [s for (_, s) in res['trajectory']]
      ax.step(times, scores, where='post', linewidth=2, label='Score')

      # Comparable band + mean line
      if res.get('n_comparable', 0) > 0 and res.get('mean_run_score') is not None:
         mean = float(res['mean_run_score'])
         std = float(res.get('std_run_score') or 0.0)
         ax.axhspan(mean - std, mean + std, color='gray', alpha=0.18,
                    label=f"Comparable Q→R score: {mean:.0f} ± {std:.0f} (n={res['n_comparable']})")
         ax.axhline(mean, color='gray', linestyle='--', linewidth=1.5)

      # Projected start
      if res.get('projected_start') is not None:
         ax.axvline(res['projected_start'], color='tab:red', linestyle='--',
                    linewidth=1.5,
                    label=f"Projected start: {res['projected_start'].strftime('%Y-%m-%d %H:%M')}")

      shape = f"{res.get('nodes', '?')}n × {res.get('walltime', '?')}"
      bin_lbl = f"[{res.get('node_bin', '?')} nodes, {res.get('walltime_bin', '?')}]"
      title = f"{res['job_id']} — queue={res.get('queue', '?')}, {shape}  {bin_lbl}"
      ax.set_title(title, fontsize=12)
      ax.set_xlabel("Time")
      ax.set_ylabel("Score")
      if mdates is not None:
         ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
         for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_horizontalalignment('right')
      ax.legend(loc='best', fontsize=9)

   def to_summary_dataframe(self, results: List[Dict[str, Any]]) -> pd.DataFrame:
      """Flatten per-job results to a one-row-per-job summary DataFrame."""
      rows = []
      for r in results:
         rows.append({
            'job_id': r.get('job_id'),
            'queue': r.get('queue'),
            'nodes': r.get('nodes'),
            'walltime': r.get('walltime'),
            'node_bin': r.get('node_bin'),
            'walltime_bin': r.get('walltime_bin'),
            'state': r.get('state'),
            'current_score': r.get('current_score'),
            'mean_run_score': r.get('mean_run_score'),
            'std_run_score': r.get('std_run_score'),
            'mean_queue_time_hours': r.get('mean_queue_time_hours'),
            'std_queue_time_hours': r.get('std_queue_time_hours'),
            'n_comparable': r.get('n_comparable', 0),
            'submit_time': r.get('submit_time'),
            'projected_start': r.get('projected_start'),
            'error': r.get('error'),
         })
      return pd.DataFrame(rows)

   def get_analysis_summary(self, job_ids: List[str], days: int = 30) -> Dict[str, Any]:
      """Return summary metadata about an analysis run."""
      return {
         'analysis_period_days': days,
         'cutoff_date': datetime.now() - timedelta(days=days),
         'n_jobs_requested': len(job_ids),
         'job_ids': list(job_ids),
      }
