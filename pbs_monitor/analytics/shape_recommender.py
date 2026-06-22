"""
Shape Recommender Analyzer for PBS Monitor Analytics

For a single job, suggests alternative shapes (node count × walltime) that
would historically have a shorter mean queue time in the same queue.
Recommendations fall in three categories:

- BUNDLE: same walltime, more nodes (integer multiplier of the current
  node count) — e.g. for users who have many small jobs that could be
  bundled into one larger job.
- CHANGE_WALLTIME: same node count, different walltime bin — e.g. for
  users who could checkpoint more or less frequently.
- NO_CHANGE: no alternative meets the improvement threshold.

This analyzer focuses on recommendation only; it does not produce plots.
"""

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging

import numpy as np
from sqlalchemy import and_
from sqlalchemy.orm import Session

from ..database.repositories import RepositoryFactory
from ..database.models import Job, JobState, Queue
from .run_score import RunScoreAnalyzer


class ShapeRecommenderAnalyzer:
   """Suggest a better shape (bundle or walltime change) for a single job."""

   def __init__(self, repository_factory: Optional[RepositoryFactory] = None):
      self.repo_factory = repository_factory or RepositoryFactory()
      self.logger = logging.getLogger(__name__)
      # Reuse bins / categorization / walltime parsing
      self._run_score = RunScoreAnalyzer(repository_factory=self.repo_factory)

   def recommend_for_job(
      self,
      job_id: str,
      days: int = 30,
      min_samples: int = 5,
      top_n: int = 3,
   ) -> Dict[str, Any]:
      """
      Compute shape recommendations for a single job.

      Args:
         job_id: Full PBS job ID
         days: Historical window in days for comparable-job statistics
         min_samples: Minimum jobs in a candidate bin to consider it
         top_n: Maximum number of recommendations to return

      Returns:
         Result dict with target job metadata, current bin stats,
         a list of `recommendations`, an `overall` category, and any
         `error` message.
      """
      cutoff = datetime.now() - timedelta(days=days)
      empty_current = {'mean_wait_hours': None, 'std_wait_hours': None, 'n': 0}

      with self.repo_factory.get_job_repository().get_session() as session:
         job = session.query(Job).filter(Job.job_id == job_id).first()
         if not job:
            return {
               'job_id': job_id,
               'error': 'Job not found in database',
               'current_stats': empty_current,
               'recommendations': [],
               'overall': 'NO_CHANGE',
            }
         if not (job.nodes and job.walltime and job.queue):
            return {
               'job_id': job_id,
               'queue': job.queue,
               'nodes': job.nodes,
               'walltime': job.walltime,
               'error': 'Job is missing queue/nodes/walltime — cannot classify shape',
               'current_stats': empty_current,
               'recommendations': [],
               'overall': 'NO_CHANGE',
            }

         walltime_hours = self._run_score._parse_walltime_to_hours(job.walltime)
         node_bin = self._run_score._categorize_by_nodes(job.nodes)
         walltime_bin = self._run_score._categorize_by_walltime(walltime_hours)

         bin_stats = self._aggregate_bin_stats(session, job.queue, cutoff)
         current_stats_t = bin_stats.get((node_bin, walltime_bin))
         current_stats = current_stats_t or empty_current

         queue_limits = self._get_queue_limits(job.queue)

         candidates: List[Dict[str, Any]] = []
         candidates.extend(self._build_bundle_candidates(
            job, node_bin, walltime_bin, walltime_hours, bin_stats, queue_limits
         ))
         candidates.extend(self._build_walltime_candidates(
            job, node_bin, walltime_bin, bin_stats, queue_limits
         ))

         # Filter by min_samples
         candidates = [c for c in candidates if c['n'] >= min_samples]

         # Filter against current
         kept = self._filter_better_than_current(candidates, current_stats)

         # Sort ascending mean wait, take top_n
         kept.sort(key=lambda c: c['mean_wait_hours'])
         kept = kept[:max(0, int(top_n))]

         overall = self._overall_category(kept)

         return {
            'job_id': job.job_id,
            'queue': job.queue,
            'nodes': job.nodes,
            'walltime': job.walltime,
            'walltime_hours': walltime_hours,
            'node_bin': node_bin,
            'walltime_bin': walltime_bin,
            'current_stats': current_stats,
            'recommendations': kept,
            'overall': overall,
            'min_samples': min_samples,
            'days': days,
            'error': None,
         }

   def _aggregate_bin_stats(
      self, session: Session, queue: str, cutoff: datetime
   ) -> Dict[Tuple[str, str], Dict[str, float]]:
      """
      Compute mean/std/N of wait_hours per (node_bin, walltime_bin) for
      finished jobs in `queue` since `cutoff`.
      """
      finished = session.query(Job).filter(
         and_(
            Job.queue == queue,
            Job.state == JobState.FINISHED,
            Job.end_time >= cutoff,
            Job.nodes.isnot(None),
            Job.walltime.isnot(None),
            Job.start_time.isnot(None),
            Job.submit_time.isnot(None),
         )
      ).all()

      buckets: Dict[Tuple[str, str], List[float]] = {}
      for fj in finished:
         try:
            nb = self._run_score._categorize_by_nodes(fj.nodes or 0)
            wb = self._run_score._categorize_by_walltime(
               self._run_score._parse_walltime_to_hours(fj.walltime)
            )
            wait_h = (fj.start_time - fj.submit_time).total_seconds() / 3600.0
            if wait_h < 0:
               continue
            buckets.setdefault((nb, wb), []).append(float(wait_h))
         except Exception:
            continue

      stats: Dict[Tuple[str, str], Dict[str, float]] = {}
      for key, vals in buckets.items():
         arr = np.asarray(vals, dtype=float)
         stats[key] = {
            'mean_wait_hours': float(arr.mean()),
            'std_wait_hours': float(arr.std(ddof=0)),
            'n': int(arr.size),
         }
      return stats

   def _build_bundle_candidates(
      self,
      job: Job,
      node_bin: str,
      walltime_bin: str,
      walltime_hours: float,
      bin_stats: Dict[Tuple[str, str], Dict[str, float]],
      queue_limits: Dict[str, Any],
   ) -> List[Dict[str, Any]]:
      """BUNDLE: same walltime bin, larger node bin. Use integer multipliers."""
      out: List[Dict[str, Any]] = []
      node_bins = self._run_score.node_bins
      bin_index = {label: i for i, (_, _, label) in enumerate(node_bins)}
      cur_idx = bin_index.get(node_bin, -1)
      if cur_idx < 0:
         return out

      max_nodes_q = queue_limits.get('max_nodes')

      for i in range(cur_idx + 1, len(node_bins)):
         nb_label = node_bins[i][2]
         key = (nb_label, walltime_bin)
         if key not in bin_stats:
            continue
         suggested = self._suggest_nodes_in_bin(job.nodes, node_bins[i])
         if suggested is None:
            continue
         if max_nodes_q is not None and suggested > max_nodes_q:
            self.logger.debug(
               f"Skipping bundle candidate nodes={suggested} > max_nodes={max_nodes_q} in queue {job.queue}"
            )
            continue
         stats = bin_stats[key]
         out.append({
            'category': 'BUNDLE',
            'node_bin': nb_label,
            'walltime_bin': walltime_bin,
            'suggested_nodes': int(suggested),
            'suggested_walltime': job.walltime,
            **stats,
         })
      return out

   def _build_walltime_candidates(
      self,
      job: Job,
      node_bin: str,
      walltime_bin: str,
      bin_stats: Dict[Tuple[str, str], Dict[str, float]],
      queue_limits: Dict[str, Any],
   ) -> List[Dict[str, Any]]:
      """CHANGE_WALLTIME: same node bin, different walltime bin."""
      out: List[Dict[str, Any]] = []
      walltime_bins = self._run_score.walltime_bins
      max_walltime_hours_q = self._parse_max_walltime_hours(queue_limits.get('max_walltime'))

      for (wb_min, wb_max, wb_label) in walltime_bins:
         if wb_label == walltime_bin:
            continue
         key = (node_bin, wb_label)
         if key not in bin_stats:
            continue
         suggested_hours, suggested_str = self._suggest_walltime_in_bin(
            wb_min, wb_max, wb_label, max_walltime_hours_q
         )
         stats = bin_stats[key]
         out.append({
            'category': 'CHANGE_WALLTIME',
            'node_bin': node_bin,
            'walltime_bin': wb_label,
            'suggested_nodes': int(job.nodes),
            'suggested_walltime': suggested_str,
            **stats,
         })
      return out

   def _suggest_nodes_in_bin(
      self, current_nodes: int, node_bin_tuple: Tuple[int, float, str]
   ) -> Optional[int]:
      """Smallest integer multiplier k >= 2 such that current_nodes * k lands in the bin."""
      min_n, max_n, _ = node_bin_tuple
      if current_nodes <= 0:
         return None
      k_min = max(2, int(np.ceil(min_n / current_nodes)))
      candidate = current_nodes * k_min
      if max_n != float('inf') and candidate > max_n:
         return None
      return candidate

   def _suggest_walltime_in_bin(
      self,
      wb_min: float,
      wb_max: float,
      wb_label: str,
      max_walltime_hours_q: Optional[float],
   ) -> Tuple[Optional[float], str]:
      """Midpoint of the walltime bin (in hours), clamped to queue max; open-ended bin reports label only."""
      if wb_max == float('inf'):
         return None, wb_label
      mid = (wb_min + wb_max) / 2.0
      if max_walltime_hours_q is not None:
         mid = min(mid, max_walltime_hours_q)
      return mid, self._format_walltime_from_hours(mid)

   def _format_walltime_from_hours(self, hours: float) -> str:
      total_seconds = int(round(hours * 3600))
      h = total_seconds // 3600
      m = (total_seconds % 3600) // 60
      s = total_seconds % 60
      return f"{h:02d}:{m:02d}:{s:02d}"

   def _parse_max_walltime_hours(self, max_walltime: Optional[str]) -> Optional[float]:
      if not max_walltime:
         return None
      try:
         return self._run_score._parse_walltime_to_hours(max_walltime)
      except Exception:
         return None

   def _get_queue_limits(self, queue_name: str) -> Dict[str, Any]:
      """Return {'max_nodes': int|None, 'max_walltime': str|None} for the queue."""
      limits: Dict[str, Any] = {'max_nodes': None, 'max_walltime': None}
      try:
         q = self.repo_factory.get_queue_repository().get_queue_by_name(queue_name)
         if q is not None:
            limits['max_nodes'] = getattr(q, 'max_nodes', None)
            limits['max_walltime'] = getattr(q, 'max_walltime', None)
      except Exception as e:
         self.logger.debug(f"Could not fetch queue limits for {queue_name}: {e}")
      return limits

   def _filter_better_than_current(
      self,
      candidates: List[Dict[str, Any]],
      current_stats: Dict[str, Any],
   ) -> List[Dict[str, Any]]:
      """
      Keep only candidates meaningfully better than the current shape.
      With current stats: mean_alt < mean_current - std_current.
      Without current stats: keep all (n>=min_samples already applied).
      `improvement_hours` = mean_current - mean_alt (NaN if no current).
      """
      cur_mean = current_stats.get('mean_wait_hours')
      cur_std = current_stats.get('std_wait_hours') or 0.0
      threshold = (cur_mean - cur_std) if cur_mean is not None else None

      kept: List[Dict[str, Any]] = []
      for c in candidates:
         improvement = (cur_mean - c['mean_wait_hours']) if cur_mean is not None else float('nan')
         c['improvement_hours'] = improvement
         if threshold is None or c['mean_wait_hours'] < threshold:
            kept.append(c)
      return kept

   def _overall_category(self, kept: List[Dict[str, Any]]) -> str:
      if not kept:
         return 'NO_CHANGE'
      top_cat = kept[0]['category']
      cats = {c['category'] for c in kept}
      if len(cats) > 1:
         return 'MIXED'
      return top_cat
