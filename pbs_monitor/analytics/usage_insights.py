"""
Usage Insights analytics and plotting

Milestone 1 implements:
- Derived metrics DataFrame for jobs in a time window
- Initial plots:
  - Score at start vs wait time (by queue)
  - Score at start vs requested node-hours (by queue)
  - Start-score distribution by queue
  - ECDF of wait time by queue

Outputs can be saved to disk and/or returned to callers (e.g., notebooks).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

import logging
import math
import os
import subprocess
import shutil
import re
from functools import partial

import numpy as np
import pandas as pd

try:
   import matplotlib.pyplot as plt
   import seaborn as sns
except Exception:  # pragma: no cover - plotting is optional for headless testing
   plt = None
   sns = None

from sqlalchemy.orm import Session
from sqlalchemy import and_, func, or_

from ..database.repositories import RepositoryFactory
from ..database.models import Job, JobHistory, JobState
from ..data_collector import DataCollector
from ..models.job import PBSJob, JobState as PBSJobState
from ..pbs_commands import PBSCommands
from ..utils.json_helpers import load_json_safe
import matplotlib.ticker as mticker


_LOGGER = logging.getLogger(__name__)


@dataclass
class QueueFilter:
   days: int = 30
   min_queue_node_hours: float = 100.0
   top_n_queues: Optional[int] = None
   allowlist_queues: Optional[List[str]] = None
   ignore_queues: Optional[List[str]] = None
   include_reservations: bool = False
   reservation_queue_regex: str = r'^[MRS]\d+$'


   reservation_queue_regex: str = r'^[MRS]\d+$'


class UsageInsights:
   """Compute usage insight metrics and generate plots."""

   def compare_time_periods(
      self,
      a_start: datetime, a_end: datetime,
      b_start: datetime, b_end: datetime,
      group_by: str = 'queue',
      save_dir: Optional[str] = None
   ) -> Tuple[Dict[str, pd.DataFrame], Dict[str, str]]:
      """
      Compare job throughput and metrics between two time periods (A and B).
      
      Returns:
          metrics: Dict containing summary DataFrames
          plots: Dict mapping plot names to file paths
      """
      outputs = {}
      metrics = {}
      
      # 1. Fetch data for both periods
      df_a = self._fetch_period_data(a_start, a_end)
      df_b = self._fetch_period_data(b_start, b_end)
      
      df_a['period'] = 'A'
      df_b['period'] = 'B'
      
      if df_a.empty:
         raise ValueError(f"Period A ({a_start} to {a_end}) has no data.")
      if df_b.empty:
         raise ValueError(f"Period B ({b_start} to {b_end}) has no data.")
      
      combined = pd.concat([df_a, df_b], ignore_index=True)
      
      os.makedirs(save_dir, exist_ok=True) if save_dir else None
      
      # Helper for plotting settings
      if plt and sns:
           sns.set_context('talk')
           sns.set_style('whitegrid')
      
      # 2. Compute Aggregate Throughput (Total Node-Hours)
      total_a = df_a['requested_node_hours'].sum()
      total_b = df_b['requested_node_hours'].sum()
      
      # 3. Grouped Comparison
      # Pivot table: Index=Category, Columns=Period, Values=NodeHours
      if group_by not in combined.columns:
          group_by = 'queue' # fallback
          
      grouped = combined.groupby([group_by, 'period'])['requested_node_hours'].sum().reset_index()
      
      # Create summary table for return
      summary_pivot = grouped.pivot(index=group_by, columns='period', values='requested_node_hours').fillna(0.0)
      
      # Ensure A and B columns exist even if one period had no data
      for col in ['A', 'B']:
         if col not in summary_pivot.columns:
            summary_pivot[col] = 0.0

      summary_pivot['diff'] = summary_pivot['B'] - summary_pivot['A']
      
      # Avoid division by zero: replace 0 in denominator with 1 (or NaN to signify undefined)
      # We use copy to avoid setting on copy warning
      denom = summary_pivot['A'].replace(0.0, np.nan)
      summary_pivot['pct_change'] = (summary_pivot['diff'] / denom) * 100.0
      summary_pivot['pct_change'] = summary_pivot['pct_change'].fillna(0.0) # or leave as NaN? Let's say 0 change if start is 0

      # Reset index to make group_by a column
      metrics['summary_table'] = summary_pivot.reset_index()

      # 4. Top Users & Projects Analysis
      
      def get_top_entities(df, group_cols, value_col='requested_node_hours', top_n=10):
         if df.empty:
            return pd.DataFrame()
         return (df.groupby(group_cols)[value_col]
                  .sum()
                  .reset_index()
                  .sort_values(value_col, ascending=False)
                  .head(top_n))

      # Top Users (Overall)
      metrics['top_users_A'] = get_top_entities(df_a, ['owner'])
      metrics['top_users_B'] = get_top_entities(df_b, ['owner'])
      
      # Top Projects (Overall)
      metrics['top_projects_A'] = get_top_entities(df_a, ['project'])
      metrics['top_projects_B'] = get_top_entities(df_b, ['project'])
      
      # Top Users by Queue
      metrics['top_users_by_queue_A'] = get_top_entities(df_a, ['queue', 'owner'])
      metrics['top_users_by_queue_B'] = get_top_entities(df_b, ['queue', 'owner'])
      
      # Top Projects by Queue
      metrics['top_projects_by_queue_A'] = get_top_entities(df_a, ['queue', 'project'])
      metrics['top_projects_by_queue_B'] = get_top_entities(df_b, ['queue', 'project'])

      
      # --- PLOTTING ---
      if plt is None or sns is None:
         return metrics, outputs

      # Plot 1: Total Throughput Comparison
      try:
         fig, ax = plt.subplots(figsize=(8, 6))
         sns.barplot(x=['Period A', 'Period B'], y=[total_a, total_b], ax=ax, palette=['royalblue', 'darkorange'])
         ax.set_title(f'Total Throughput (Node-Hours)\nPeriod A: {a_start.date()} to {a_end.date()}\nPeriod B: {b_start.date()} to {b_end.date()}')
         ax.set_ylabel('Node-Hours')
         
         # Add labels
         for i, v in enumerate([total_a, total_b]):
            ax.text(i, v, f'{int(v):,}', ha='center', va='bottom')
            
         if save_dir:
            pth = os.path.join(save_dir, 'total_throughput_comparison.png')
            fig.savefig(pth, bbox_inches='tight', dpi=120)
            outputs['total_throughput'] = pth
         plt.close(fig)
      except Exception as e:
         self.logger.warning(f"Plot total_throughput failed: {e}")

      # Plot 2: Grouped Breakdown
      try:
         # Filter top N categories to avoid clutter if too many
         top_cats = combined.groupby(group_by)['requested_node_hours'].sum().nlargest(15).index
         plot_df = grouped[grouped[group_by].isin(top_cats)].copy()
         
         fig, ax = plt.subplots(figsize=(12, 8))
         sns.barplot(data=plot_df, x=group_by, y='requested_node_hours', hue='period', 
                     palette={'A': 'royalblue', 'B': 'darkorange'}, ax=ax)
         ax.set_title(f'Throughput by {group_by}')
         ax.set_ylabel('Requested Node-Hours')
         plt.xticks(rotation=45, ha='right')
         ax.legend(title='Period')
         
         if save_dir:
            pth = os.path.join(save_dir, f'throughput_by_{group_by}.png')
            fig.savefig(pth, bbox_inches='tight', dpi=120)
            outputs['throughput_breakdown'] = pth
         plt.close(fig)
      except Exception as e:
         self.logger.warning(f"Plot throughput_by_{group_by} failed: {e}")

      # Plot 3: Weighted Histogram of Wait Time (Node-Hours)
      try:
         fig, ax = plt.subplots(figsize=(12, 6))
         
         # We want to bin wait_time_hours, weighted by requested_node_hours
         # Use common bins for A and B
         # Log scale often makes sense for wait times, but let's stick to linear or log-x bins based on data range.
         # For simplicity: Use log-spaced bins if max wait > 0
         
         max_wait = combined['wait_time_hours'].max()
         if max_wait and max_wait > 0:
             # Create log bins
             bins = np.logspace(np.log10(0.01), np.log10(max_wait), 50)
             # Handle 0 wait times? replace 0 with small epsilon for log plotting
             
             # Histogram for A
             waits_a = (df_a['wait_time_hours'].fillna(0) + 0.01).astype(float)
             weights_a = df_a['requested_node_hours'].fillna(0).astype(float)
             hist_a, _ = np.histogram(waits_a, bins=bins, weights=weights_a)
             
             # Histogram for B
             waits_b = (df_b['wait_time_hours'].fillna(0) + 0.01).astype(float)
             weights_b = df_b['requested_node_hours'].fillna(0).astype(float)
             hist_b, _ = np.histogram(waits_b, bins=bins, weights=weights_b)
             
             # Centers for plotting
             centers = (bins[:-1] + bins[1:]) / 2
             
             # Step plot
             ax.plot(centers, hist_a, label='Period A', color='royalblue', linewidth=2)
             ax.fill_between(centers, 0, hist_a, color='royalblue', alpha=0.3)
             
             ax.plot(centers, hist_b, label='Period B', color='darkorange', linewidth=2)
             ax.fill_between(centers, 0, hist_b, color='darkorange', alpha=0.3)
             
             ax.set_xscale('log')
             ax.set_xlabel('Wait Time (Hours) [Log Scale]')
             ax.set_ylabel('Total Node-Hours (Weighted Count)')
             ax.set_title('Wait Time Distribution (Weighted by Node-Hours)')
             ax.legend()
             
             if save_dir:
                pth = os.path.join(save_dir, 'wait_time_weighted_dist.png')
                fig.savefig(pth, bbox_inches='tight', dpi=120)
                outputs['wait_time_dist'] = pth
             plt.close(fig)
      except Exception as e:
         self.logger.warning(f"Plot wait_time_weighted_dist failed: {e}")

      return metrics, outputs

   def _fetch_period_data(self, start: datetime, end: datetime) -> pd.DataFrame:
       """Helper to fetch and process jobs for a specific period."""
       with self.repo_factory.get_job_repository().get_session() as session:
          # Query jobs started in range
          jobs = session.query(Job).filter(
             and_(
                Job.start_time >= start,
                Job.start_time <= end,
                Job.nodes.isnot(None),
                Job.walltime.isnot(None)
             )
          ).all()
          
          if not jobs:
             return pd.DataFrame(columns=[
                'job_id', 'queue', 'project', 'allocation_type', 
                'state', 'requested_node_hours', 'wait_time_hours'
             ])

          records = []
          for job in jobs:
             try:
                # Basic parsing
                wall_h = self._parse_walltime_to_hours(job.walltime)
                node_h = (job.nodes or 0) * wall_h
                wait_h = self._compute_wait_hours(job.submit_time, job.start_time)
                
                records.append({
                   'job_id': job.job_id,
                   'owner': job.owner,
                   'queue': job.queue,
                   'project': job.project,
                   'allocation_type': getattr(job, 'allocation_type', 'unknown'),
                   'state': str(job.state),
                   'requested_node_hours': float(node_h),
                   'wait_time_hours': float(wait_h)
                })
             except Exception:
                continue
                
          return pd.DataFrame.from_records(records)
   def __init__(self, repository_factory: Optional[RepositoryFactory] = None, data_collector: Optional[DataCollector] = None):
      self.repo_factory = repository_factory or RepositoryFactory()
      self.data_collector = data_collector
      self.logger = logging.getLogger(__name__)

   # --------- Frequency normalization helper ---------
   def _normalize_freq(self, freq: str) -> str:
      """
      Normalize frequency strings to avoid pandas FutureWarnings.
      Convert deprecated uppercase frequencies to lowercase.
      """
      freq_map = {
         'H': 'h',   # Hourly
         'D': 'D',   # Daily (already correct)
         'W': 'W',   # Weekly (already correct)
         'M': 'M',   # Monthly (already correct)
         'Y': 'Y',   # Yearly (already correct)
      }
      return freq_map.get(freq, freq)

   # --------- Public API ---------
   def build_job_metrics(
      self,
      queue_filter: QueueFilter,
   ) -> pd.DataFrame:
      """
      Build a DataFrame of job-level derived metrics for jobs that:
      1) Started within the window
      2) Are currently queued

      Columns include:
      - job_id, owner, project, queue, allocation_type
      - nodes, walltime_hours
      - submit_time, start_time, end_time
      - wait_time_hours, run_time_hours, requested_node_hours
      - start_score
      - start_score_quantile (within queue over the window)
      - state (job state)
      """
      with self.repo_factory.get_job_repository().get_session() as session:
         cutoff_start = datetime.now() - timedelta(days=queue_filter.days)

         # Get both started and queued jobs
         started_jobs = self._query_started_jobs(session, cutoff_start)
         queued_jobs = self._get_live_queued_jobs()
         jobs = started_jobs + queued_jobs

         if not jobs:
            return pd.DataFrame(columns=[
               'job_id', 'owner', 'project', 'queue', 'allocation_type', 'nodes', 'walltime_hours',
               'submit_time', 'start_time', 'end_time', 'wait_time_hours',
               'run_time_hours', 'requested_node_hours', 'start_score',
               'start_score_quantile', 'state'
            ])

         # Build raw records with derived metrics
         # --- OPTIMIZATION START ---
         stats = {
            'total_jobs': len(jobs),
            'scores_calculated': 0,
            'scores_from_db': 0,
            'scores_missing': 0
         }
         
         # Initialize PBSCommands for on-the-fly calculation
         pbs_cmds = PBSCommands()
         server_data = None
         server_defaults = None
         try:
            # Try to get server data for formula/defaults
            # This might fail if PBS is not reachable, but we should try
            if pbs_cmds.test_connection():
               server_data = pbs_cmds.qstat_server()
               # Extract server defaults
               server_info = server_data.get("Server", {})
               for _, details in server_info.items():
                  server_defaults = details.get("resources_default", {})
                  break
         except Exception as e:
            self.logger.warning(f"Failed to fetch PBS server data for score calculation: {e}")

         # Dictionary to store resolved scores
         job_start_scores: Dict[str, float] = {} # job_id -> score
         jobs_needing_db: List[Job] = []

         for job in jobs:
            score_found = False
            # Try calculation first
            if server_data and getattr(job, 'raw_pbs_data', None):
               try:
                  # Ensure raw_pbs_data is a dict
                  raw_data = job.raw_pbs_data
                  if isinstance(raw_data, dict):
                     # We need eligible_time and Resource_List
                     # Check if calculation is possible
                     calc_score = pbs_cmds.calculate_job_score(
                        raw_data, 
                        server_defaults=server_defaults, 
                        server_data=server_data
                     )
                     if calc_score is not None:
                        job_start_scores[job.job_id] = calc_score
                        stats['scores_calculated'] += 1
                        score_found = True
               except Exception as e:
                  # Fallback to DB if calculation crashes
                  pass
            
            if not score_found:
               jobs_needing_db.append(job)

         # Fallback: Batch DB query for remaining jobs
         if jobs_needing_db:
            job_ids = [j.job_id for j in jobs_needing_db]
            
            # Chunking to avoid SQL variable limits
            chunk_size = 500
            history_map = {}
            
            for i in range(0, len(job_ids), chunk_size):
                chunk_ids = job_ids[i:i + chunk_size]
                try:
                    history_records = session.query(
                        JobHistory.job_id, JobHistory.timestamp, JobHistory.score
                    ).filter(
                        JobHistory.job_id.in_(chunk_ids),
                        JobHistory.score.isnot(None)
                    ).all()
                    
                    for jid, ts, sc in history_records:
                        if jid not in history_map:
                            history_map[jid] = []
                        history_map[jid].append((ts, sc))
                except Exception as e:
                    self.logger.error(f"Batch history lookup failed for chunk {i}: {e}")

            # Resolve scores for each job
            for job in jobs_needing_db:
               hist_list = history_map.get(job.job_id, [])
               if not hist_list:
                  stats['scores_missing'] += 1
                  continue
                  
               # We want last score <= start_time
               start_time = job.start_time
               if not start_time:
                  stats['scores_missing'] += 1
                  continue
                  
               # Sort entries by timestamp
               hist_list.sort(key=lambda x: x[0])
               
               best_score = None
               # Look for last entry <= start_time
               # This linear scan is fast enough for small history lists per job
               candidates = [h for h in hist_list if h[0] <= start_time]
               if candidates:
                  best_score = float(candidates[-1][1])
               else:
                  # Fallback: first entry > start_time (nearest future)
                  future_candidates = [h for h in hist_list if h[0] > start_time]
                  if future_candidates:
                     best_score = float(future_candidates[0][1])
               
               if best_score is not None:
                  job_start_scores[job.job_id] = best_score
                  stats['scores_from_db'] += 1
               else:
                  stats['scores_missing'] += 1

         # Report stats
         self.logger.info(
               f"Score stats: Total={stats['total_jobs']}, "
               f"Calculated={stats['scores_calculated']}, "
               f"DB-Retrieved={stats['scores_from_db']}, "
               f"Missing={stats['scores_missing']}"
         )
         # Print to console for user visibility
         print(f"Usage Insights Score Stats: Calculated {stats['scores_calculated']} | DB-Lookup {stats['scores_from_db']} | Missing {stats['scores_missing']} (Total {stats['total_jobs']})")

         records: List[Dict[str, object]] = []
         for job in jobs:

            try:
               walltime_hours = self._parse_walltime_to_hours(job.walltime)
               wait_h = self._compute_wait_hours(job.submit_time, job.start_time)
               run_h = self._compute_run_hours(job.start_time, job.end_time)
               start_score = job_start_scores.get(job.job_id) # Using pre-calculated/fetched score
               requested_node_hours = (job.nodes or 0) * walltime_hours
               records.append({
                  'job_id': job.job_id,
                  'owner': job.owner,
                  'project': job.project,
                  'queue': job.queue,
                  'allocation_type': getattr(job, 'allocation_type', None),
                  'nodes': int(job.nodes or 0),
                  'walltime_hours': float(walltime_hours),
                  'submit_time': job.submit_time,
                  'start_time': job.start_time,
                  'end_time': job.end_time,
                  'wait_time_hours': float(wait_h),
                  'run_time_hours': float(run_h) if run_h is not None else np.nan,
                  'requested_node_hours': float(requested_node_hours),
                  'start_score': float(start_score) if start_score is not None else np.nan,
                  'state': str(job.state) if job.state else None,
               })
            except Exception as e:  # robust to malformed rows
               self.logger.debug(f"Skipping job {getattr(job, 'job_id', '?')}: {e}")
               continue

         if not records:
            return pd.DataFrame()

         df = pd.DataFrame.from_records(records)

         # Queue filtering based on node-hours
         df = self._filter_queues(df, queue_filter)

         # Add start_score_quantile within queue for the window
         df['start_score_quantile'] = (
            df.groupby('queue')['start_score']
              .transform(lambda s: s.rank(pct=True, method='average'))
         )

         # Slowdown: (wait + run)/max(run, eps). Use eps=1 minute in hours for stability
         eps_hours = 1.0 / 60.0
         if 'run_time_hours' in df.columns:
            denom = df['run_time_hours'].fillna(eps_hours).clip(lower=eps_hours)
            df['slowdown'] = (df['wait_time_hours'].fillna(0) + df['run_time_hours'].fillna(0)) / denom
         else:
            df['slowdown'] = np.nan

         return df

   def generate_plots(
      self,
      df: pd.DataFrame,
      save_dir: Optional[str] = None,
      dpi: int = 120
   ) -> Dict[str, str]:
      """
      Generate milestone-1 plots. Returns mapping of plot name to saved file path when saved.
      If save_dir is None or plotting backends unavailable, returns an empty dict.
      """
      outputs: Dict[str, str] = {}
      if df.empty:
         self.logger.warning("No data to plot - dataframe is empty")
         return outputs
      if plt is None or sns is None:
         self.logger.warning("Plotting libraries not available - matplotlib or seaborn import failed")
         return outputs

      os.makedirs(save_dir, exist_ok=True) if save_dir else None

      # Common aesthetics
      sns.set_context('talk')
      sns.set_style('whitegrid')

      # Build a consistent palette for queues across plots
      try:
         queues = sorted(df['queue'].dropna().astype(str).unique().tolist())
      except Exception:
         queues = []
      queue_palette = self._build_queue_palette(queues)

      # 1) Score at start vs wait time (by queue)
      try:
         g = sns.FacetGrid(df, col='queue', col_wrap=3, sharex=False, sharey=False)
         g.map_dataframe(partial(self._hex_or_scatter, palette=queue_palette), 'wait_time_hours', 'start_score')
         g.set_axis_labels('Wait time (hours, log)', 'Score at start')
         for ax in g.axes.ravel():
            ax.set_xscale('log')
         g.fig.suptitle('Score at start vs Wait time (by queue)', y=1.02)
         if save_dir:
            pth = os.path.join(save_dir, 'score_vs_wait_by_queue.png')
            g.fig.savefig(pth, bbox_inches='tight', dpi=dpi)
            outputs['score_vs_wait_by_queue'] = pth
         plt.close(g.fig)
      except Exception as e:
         self.logger.debug(f"Plot score_vs_wait_by_queue failed: {e}")

      # 2) Score at start vs requested node-hours (by queue)
      try:
         g = sns.FacetGrid(df, col='queue', col_wrap=3, sharex=False, sharey=False)
         g.map_dataframe(partial(self._hex_or_scatter, palette=queue_palette), 'requested_node_hours', 'start_score')
         g.set_axis_labels('Requested node-hours (log)', 'Score at start')
         for ax in g.axes.ravel():
            ax.set_xscale('log')
         g.fig.suptitle('Score at start vs Requested node-hours (by queue)', y=1.02)
         if save_dir:
            pth = os.path.join(save_dir, 'score_vs_node_hours_by_queue.png')
            g.fig.savefig(pth, bbox_inches='tight', dpi=dpi)
            outputs['score_vs_node_hours_by_queue'] = pth
         plt.close(g.fig)
      except Exception as e:
         self.logger.debug(f"Plot score_vs_node_hours_by_queue failed: {e}")

      # 3) ECDF of wait time by queue
      try:
         fig, ax = plt.subplots(figsize=(14, 6))
         for q, sub in df.groupby('queue'):
            x = np.sort(sub['wait_time_hours'].dropna().values)
            if x.size == 0:
               continue
            y = np.arange(1, x.size + 1) / x.size
            ax.step(x, y, where='post', label=str(q), color=queue_palette.get(str(q)))
         ax.set_xscale('log')
         ax.set_xlabel('Wait time (hours, log)')
         ax.set_ylabel('ECDF')
         ax.set_title('ECDF of wait time by queue')
         ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0, fontsize='small', title='Queue', frameon=False)
         if save_dir:
            pth = os.path.join(save_dir, 'ecdf_wait_by_queue.png')
            fig.savefig(pth, bbox_inches='tight', dpi=dpi)
            outputs['ecdf_wait_by_queue'] = pth
         plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot ecdf_wait_by_queue failed: {e}")

      # 4b) ECDF of wait time by allocation type
      try:
         fig, ax = plt.subplots(figsize=(14, 6))
         # Filter out null allocation types and group by allocation type
         df_alloc = df.dropna(subset=['allocation_type'])
         if not df_alloc.empty:
            # Build consistent palette for allocation types
            alloc_types = sorted(df_alloc['allocation_type'].astype(str).unique().tolist())
            alloc_palette = self._build_allocation_palette(alloc_types)
            
            for alloc_type, sub in df_alloc.groupby('allocation_type'):
               x = np.sort(sub['wait_time_hours'].dropna().values)
               if x.size == 0:
                  continue
               y = np.arange(1, x.size + 1) / x.size
               ax.step(x, y, where='post', label=str(alloc_type), color=alloc_palette.get(str(alloc_type)))
            ax.set_xscale('log')
            ax.set_xlabel('Wait time (hours, log)')
            ax.set_ylabel('ECDF')
            ax.set_title('ECDF of wait time by allocation type')
            ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0, fontsize='small', title='Allocation Type', frameon=False)
            if save_dir:
               pth = os.path.join(save_dir, 'ecdf_wait_by_allocation_type.png')
               fig.savefig(pth, bbox_inches='tight', dpi=dpi)
               outputs['ecdf_wait_by_allocation_type'] = pth
         plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot ecdf_wait_by_allocation_type failed: {e}")

      # 5) Requested vs Used node-hours hexbin
      try:
         # Calculate used_node_hours for each job (nodes * run_time_hours)
         df_efficiency = df.dropna(subset=['run_time_hours', 'nodes', 'requested_node_hours']).copy()
         df_efficiency['used_node_hours'] = df_efficiency['nodes'] * df_efficiency['run_time_hours']
         # Filter to jobs with positive values for log scale
         df_efficiency = df_efficiency[
            (df_efficiency['requested_node_hours'] > 0) &
            (df_efficiency['used_node_hours'] > 0)
         ]

         if not df_efficiency.empty and len(df_efficiency) >= 5:
            fig, ax = plt.subplots(figsize=(10, 8))

            # Use log scale for both axes
            x = df_efficiency['requested_node_hours'].values
            y = df_efficiency['used_node_hours'].values

            # Hexbin with log scale
            hb = ax.hexbin(x, y, gridsize=40, mincnt=1, xscale='log', yscale='log', cmap='viridis')
            cb = plt.colorbar(hb, ax=ax)
            cb.set_label('Number of jobs')

            # Add y=x reference line (perfect efficiency)
            min_val = min(x.min(), y.min())
            max_val = max(x.max(), y.max())
            ax.plot([min_val, max_val], [min_val, max_val], 'r--', linewidth=2, label='Perfect efficiency (y=x)', alpha=0.7)

            ax.set_xlabel('Requested node-hours (log)')
            ax.set_ylabel('Used node-hours (log)')
            ax.set_title('Requested vs Used Node-Hours\n(points below line = underutilization)')
            ax.legend(loc='upper left')

            if save_dir:
               pth = os.path.join(save_dir, 'requested_vs_used_node_hours.png')
               fig.savefig(pth, bbox_inches='tight', dpi=dpi)
               outputs['requested_vs_used_node_hours'] = pth
            plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot requested_vs_used_node_hours failed: {e}")

      # 6) Efficiency ratio distribution (used/requested)
      try:
         df_efficiency = df.dropna(subset=['run_time_hours', 'nodes', 'requested_node_hours']).copy()
         df_efficiency['used_node_hours'] = df_efficiency['nodes'] * df_efficiency['run_time_hours']
         # Filter to jobs with positive requested hours
         df_efficiency = df_efficiency[df_efficiency['requested_node_hours'] > 0]
         df_efficiency['efficiency'] = df_efficiency['used_node_hours'] / df_efficiency['requested_node_hours']

         # Export efficiency outliers (>100%) to CSV for investigation
         if save_dir:
            outliers = df_efficiency[df_efficiency['efficiency'] > 1.0].copy()
            if not outliers.empty:
               # Convert walltime and runtime to minutes for readability
               outliers['walltime_minutes'] = outliers['walltime_hours'] * 60
               outliers['run_time_minutes'] = outliers['run_time_hours'] * 60
               # Select columns for investigation
               outlier_cols = [
                  'job_id', 'owner', 'project', 'queue',
                  'nodes', 'walltime_minutes', 'requested_node_hours',
                  'submit_time', 'start_time', 'end_time',
                  'run_time_minutes', 'used_node_hours', 'efficiency'
               ]
               # Only include columns that exist
               outlier_cols = [c for c in outlier_cols if c in outliers.columns]
               outliers_export = outliers[outlier_cols].sort_values('efficiency', ascending=False)
               csv_path = os.path.join(save_dir, 'efficiency_outliers.csv')
               outliers_export.to_csv(csv_path, index=False)
               outputs['efficiency_outliers_csv'] = csv_path
               self.logger.info(f"Exported {len(outliers_export)} efficiency outliers (>100%) to {csv_path}")

         # Cap efficiency at reasonable bounds (some jobs may have data issues)
         df_efficiency['efficiency'] = df_efficiency['efficiency'].clip(0, 2)

         if not df_efficiency.empty and len(df_efficiency) >= 5:
            fig, axes = plt.subplots(1, 2, figsize=(14, 5))

            # Left: Histogram
            ax1 = axes[0]
            ax1.hist(df_efficiency['efficiency'], bins=50, edgecolor='black', alpha=0.7)
            ax1.axvline(x=1.0, color='r', linestyle='--', linewidth=2, label='100% efficiency')
            ax1.axvline(x=df_efficiency['efficiency'].median(), color='orange', linestyle='-', linewidth=2, label=f'Median: {df_efficiency["efficiency"].median():.1%}')
            ax1.set_xlabel('Efficiency (used / requested)')
            ax1.set_ylabel('Number of jobs')
            ax1.set_title('Job Efficiency Distribution')
            ax1.legend()

            # Right: ECDF by queue
            ax2 = axes[1]
            for q, sub in df_efficiency.groupby('queue'):
               eff_vals = np.sort(sub['efficiency'].dropna().values)
               if eff_vals.size == 0:
                  continue
               ecdf_y = np.arange(1, eff_vals.size + 1) / eff_vals.size
               ax2.step(eff_vals, ecdf_y, where='post', label=str(q), color=queue_palette.get(str(q)))
            ax2.axvline(x=1.0, color='r', linestyle='--', linewidth=1, alpha=0.5)
            ax2.set_xlabel('Efficiency (used / requested)')
            ax2.set_ylabel('ECDF')
            ax2.set_title('Efficiency ECDF by Queue')
            ax2.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0, fontsize='small', title='Queue', frameon=False)

            plt.tight_layout()
            if save_dir:
               pth = os.path.join(save_dir, 'efficiency_distribution.png')
               fig.savefig(pth, bbox_inches='tight', dpi=dpi)
               outputs['efficiency_distribution'] = pth
            plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot efficiency_distribution failed: {e}")

      return outputs

   def generate_plots_extended(
      self,
      df: pd.DataFrame,
      days: int = 30,
      save_dir: Optional[str] = None,
      dpi: int = 120,
      ts_freq: str = 'D'
   ) -> Dict[str, str]:
      """
      Generate advanced plot suite:
      - Backlog over time (node-hours queued) by queue
      - Active nodes over time by queue (stacked area)

      Returns mapping of plot name to saved file path when saved.
      """
      outputs: Dict[str, str] = {}
      if df.empty:
         self.logger.warning("No data to plot - dataframe is empty")
         return outputs
      if plt is None or sns is None:
         self.logger.warning("Plotting libraries not available - matplotlib or seaborn import failed")
         return outputs

      try:
         if not pd.api.types.is_datetime64_any_dtype(df['submit_time']):
            df['submit_time'] = pd.to_datetime(df['submit_time'], errors='coerce')
         if not pd.api.types.is_datetime64_any_dtype(df['start_time']):
            df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
         if 'end_time' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['end_time']):
            df['end_time'] = pd.to_datetime(df['end_time'], errors='coerce')
      except Exception:
         pass

      window_start = pd.Timestamp.now(tz=None) - pd.Timedelta(days=int(days))

      os.makedirs(save_dir, exist_ok=True) if save_dir else None

      sns.set_context('talk')
      sns.set_style('whitegrid')

      # Consistent palette across all extended plots
      try:
         queues = sorted(df['queue'].dropna().astype(str).unique().tolist())
      except Exception:
         queues = []
      queue_palette = self._build_queue_palette(queues)

      # ---- Queue depth over time (machine-hours queued) by queue ----
      try:
         bl_df = self._compute_backlog_timeseries(df, window_start, freq=ts_freq)
         self.logger.debug(f"Backlog timeseries: {bl_df}")
         self.logger.info(f"queues included in backlog timeseries: {bl_df['queue'].unique()}")
         if not bl_df.empty:
            pivot = bl_df.pivot_table(index='timestamp', columns='queue', values='machine_hours', aggfunc='sum').fillna(0.0)
            # Build full timeline to include zero-usage bins
            now_ts = pd.Timestamp.now(tz=None)
            start_bin = window_start.to_period(self._normalize_freq(ts_freq)).to_timestamp()
            end_bin = now_ts.to_period(self._normalize_freq(ts_freq)).to_timestamp()
            full_idx = pd.date_range(start=start_bin, end=end_bin, freq=self._normalize_freq(ts_freq))
            pivot = pivot.reindex(full_idx, fill_value=0.0)

            fig, ax = plt.subplots(figsize=(14, 6))
            color_order = [queue_palette.get(str(c)) for c in pivot.columns]
            ax.stackplot(pivot.index, *(pivot[c] for c in pivot.columns), labels=pivot.columns, colors=color_order, linewidth=0)
            ax.set_title(f'Queue depth over time (machine-hours queued per {ts_freq})')
            ax.set_xlabel('')  # Remove x-axis title
            ax.set_ylabel('Machine-hours queued')
            # Format x-axis dates - be more explicit to override pandas defaults
            import matplotlib.dates as mdates
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
            # Force the formatter to be applied
            fig.autofmt_xdate(rotation=45)
            ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0, fontsize='small', title='Queue', frameon=False)
            if save_dir:
               pth = os.path.join(save_dir, f'queue_depth_machine_hours_per_{ts_freq}.png')
               fig.savefig(pth, bbox_inches='tight', dpi=dpi)
               outputs['queue_depth_machine_hours'] = pth
            plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot backlog_node_hours failed: {e}")

      # ---- Queue depth over time (machine-hours queued) by allocation type ----
      try:
         bl_df_alloc = self._compute_backlog_timeseries_by_allocation(df, window_start, freq=ts_freq)
         self.logger.debug(f"Backlog timeseries by allocation: {bl_df_alloc}")
         if not bl_df_alloc.empty:
            pivot = bl_df_alloc.pivot_table(index='timestamp', columns='allocation_type', values='machine_hours', aggfunc='sum').fillna(0.0)
            # Build full timeline to include zero-usage bins
            now_ts = pd.Timestamp.now(tz=None)
            start_bin = window_start.to_period(self._normalize_freq(ts_freq)).to_timestamp()
            end_bin = now_ts.to_period(self._normalize_freq(ts_freq)).to_timestamp()
            full_idx = pd.date_range(start=start_bin, end=end_bin, freq=self._normalize_freq(ts_freq))
            pivot = pivot.reindex(full_idx, fill_value=0.0)

            fig, ax = plt.subplots(figsize=(14, 6))
            # Build consistent palette for allocation types
            alloc_types = sorted(pivot.columns.astype(str).tolist())
            alloc_palette = self._build_allocation_palette(alloc_types)
            color_order = [alloc_palette.get(str(c)) for c in pivot.columns]
            ax.stackplot(pivot.index, *(pivot[c] for c in pivot.columns), labels=pivot.columns, colors=color_order, linewidth=0)
            ax.set_title(f'Queue depth over time by allocation type (machine-hours queued per {ts_freq})')
            ax.set_xlabel('')  # Remove x-axis title
            ax.set_ylabel('Machine-hours queued')
            
            # Format x-axis dates - be more explicit to override pandas defaults
            import matplotlib.dates as mdates
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
            # Force the formatter to be applied
            fig.autofmt_xdate(rotation=45)

            ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0, fontsize='small', title='Allocation Type', frameon=False)
            if save_dir:
               pth = os.path.join(save_dir, f'queue_depth_machine_hours_by_allocation_per_{ts_freq}.png')
               fig.savefig(pth, bbox_inches='tight', dpi=dpi)
               outputs['queue_depth_machine_hours_by_allocation'] = pth
            plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot queue_depth_machine_hours_by_allocation failed: {e}")

      # ---- Current wait time distribution by queue ----
      try:
         self.logger.debug("Computing current wait bins...")
         wait_bins = self._compute_current_wait_bins(df)
         self.logger.debug(f"Wait bins result: {len(wait_bins)} rows")
         if not wait_bins.empty:
            # Aggregate across all queues to get total count per wait bin
            total_by_bin = wait_bins.groupby('wait_bin', observed=True)['count'].sum().reset_index()
            
            # Ensure all bins are present (even with 0 count)
            all_bins = ['<1hr', '1-6hrs', '6-12hrs', '12-24hrs', 
                       '1-2days', '2-7days', '7-14days', '2-3weeks', '3-5weeks', '>1month']
            total_by_bin = total_by_bin.set_index('wait_bin').reindex(all_bins, fill_value=0).reset_index()

            # Plot simple bar chart
            fig, ax = plt.subplots(figsize=(12, 6))
            bars = ax.bar(total_by_bin['wait_bin'], total_by_bin['count'], width=0.8)
            
            # Add value labels on top of bars
            for bar in bars:
               height = bar.get_height()
               if height > 0:
                  ax.text(bar.get_x() + bar.get_width()/2., height,
                         f'{int(height)}',
                         ha='center', va='bottom')
            
            ax.set_title('Current wait time distribution of queued jobs')
            ax.set_xlabel('Wait time')
            ax.set_ylabel('Number of jobs')
            plt.xticks(rotation=45)
            if save_dir:
               pth = os.path.join(save_dir, 'current_wait_distribution.png')
               fig.savefig(pth, bbox_inches='tight', dpi=dpi)
               outputs['current_wait_distribution'] = pth
            plt.close(fig)
         else:
            self.logger.warning("No data to plot - current_wait_distribution is empty")
      except Exception as e:
         self.logger.debug(f"Plot current_wait_distribution failed: {e}")

      # # ---- Active nodes over time by queue ----
      # try:
      #    an_df = self._compute_active_nodes_timeseries(df, window_start, freq=ts_freq)
      #    if not an_df.empty:
      #       pivot = an_df.pivot_table(index='timestamp', columns='queue', values='nodes', aggfunc='sum').fillna(0.0)
      #       fig, ax = plt.subplots(figsize=(14, 6))
      #       color_order = [queue_palette.get(str(c)) for c in pivot.columns]
      #       pivot.plot.area(ax=ax, color=color_order)
      #       ax.set_title(f'Active nodes over time by queue (per {ts_freq})')
      #       ax.set_xlabel('Time')
      #       ax.set_ylabel('Active nodes')
      #       ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0, fontsize='small', title='Queue', frameon=False)
      #       if save_dir:
      #          pth = os.path.join(save_dir, f'active_nodes_per_{ts_freq}.png')
      #          fig.savefig(pth, bbox_inches='tight', dpi=dpi)
      #          outputs['active_nodes'] = pth
      #       plt.close(fig)
      # except Exception as e:
      #    self.logger.debug(f"Plot active_nodes failed: {e}")

      # ---- Utilization: percent of capacity used per period ----
      try:
         total_nodes = self._detect_total_cluster_nodes()
         if total_nodes and int(total_nodes) > 0:
            used_df = self._compute_used_node_hours_timeseries(df, window_start, freq=ts_freq)
            if not used_df.empty:
               # Build full timeline to include zero-usage bins
               now_ts = pd.Timestamp.now(tz=None)
               start_bin = window_start.to_period(self._normalize_freq(ts_freq)).to_timestamp()
               end_bin = now_ts.to_period(self._normalize_freq(ts_freq)).to_timestamp()
               full_idx = pd.date_range(start=start_bin, end=end_bin, freq=self._normalize_freq(ts_freq))
               used_series = used_df.set_index('timestamp')['used_node_hours'].reindex(full_idx, fill_value=0.0)

               # Compute capacity hours per bin
               offset = pd.tseries.frequencies.to_offset(self._normalize_freq(ts_freq))
               cap_hours = []
               for t in used_series.index:
                  candidate_next = t + offset
                  # Clip last bin to now
                  next_t = min(candidate_next, now_ts)
                  hours = max(0.0, (next_t - t).total_seconds() / 3600.0)
                  cap_hours.append(hours)
               cap_hours = pd.Series(cap_hours, index=used_series.index)
               capacity_node_hours = cap_hours.astype(float) * float(int(total_nodes))
               eps = 1e-9
               utilization_pct = (used_series.astype(float) / capacity_node_hours.clip(lower=eps)) * 100.0

               # Compute reservation utilization (excluded capacity)
               res_df = self._compute_reserved_node_hours_timeseries(window_start, freq=ts_freq)
               res_series = pd.Series(0.0, index=full_idx)
               if not res_df.empty:
                  res_series = res_df.set_index('timestamp')['reserved_node_hours'].reindex(full_idx, fill_value=0.0)
               
               reservation_pct = (res_series.astype(float) / capacity_node_hours.clip(lower=eps)) * 100.0

               fig, ax = plt.subplots(figsize=(14, 6))
               ax.plot(utilization_pct.index, utilization_pct.values, label='Job Utilization')
               ax.plot(reservation_pct.index, reservation_pct.values, label='Reserved (Excluded)', linestyle='--', color='salmon')
               
               ax.set_title(f'Utilization over time (% of capacity used per {ts_freq})')
               ax.set_xlabel('')  # Remove x-axis title
               ax.set_ylabel('Utilization (%)')
               ax.set_ylim(0, 100)
               ax.legend(loc='upper right')
               
               # Format x-axis dates and rotate labels
               import matplotlib.dates as mdates
               ax.xaxis.set_major_locator(mdates.AutoDateLocator())
               ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
               plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
               # Force the formatter to be applied
               fig.autofmt_xdate(rotation=45)
               if save_dir:
                  pth = os.path.join(save_dir, f'utilization_percent_per_{ts_freq}.png')
                  fig.savefig(pth, bbox_inches='tight', dpi=dpi)
                  outputs['utilization_percent'] = pth
               plt.close(fig)

            # ---- Grouped Utilization Plots ----
            # 1) By Queue (excluding reservations)
            if save_dir:
               q_pth = os.path.join(save_dir, f'utilization_percent_by_queue_per_{ts_freq}.png')
               saved_q = self._plot_grouped_utilization(
                  df=df,
                  group_col='queue',
                  total_nodes=total_nodes,
                  window_start=window_start,
                  freq=ts_freq,
                  palette=queue_palette,
                  title=f'Utilization by Queue (% of capacity used per {ts_freq})',
                  save_path=q_pth,
                  dpi=dpi
               )
               if saved_q:
                  outputs['utilization_percent_by_queue'] = saved_q

            # 2) By Allocation Type (excluding reservations)
            if save_dir:
               try:
                  alloc_types = sorted(df['allocation_type'].dropna().astype(str).unique().tolist())
               except Exception:
                  alloc_types = []
               alloc_palette = self._build_allocation_palette(alloc_types)
               
               alloc_pth = os.path.join(save_dir, f'utilization_percent_by_allocation_type_per_{ts_freq}.png')
               saved_alloc = self._plot_grouped_utilization(
                  df=df.dropna(subset=['allocation_type']),
                  group_col='allocation_type',
                  total_nodes=total_nodes,
                  window_start=window_start,
                  freq=ts_freq,
                  palette=alloc_palette,
                  title=f'Utilization by Allocation Type (% of capacity used per {ts_freq})',
                  save_path=alloc_pth,
                  dpi=dpi
               )
               if saved_alloc:
                  outputs['utilization_percent_by_allocation_type'] = saved_alloc
      except Exception as e:
         self.logger.debug(f"Plot utilization_percent failed: {e}")

      # Per-user plots removed (user feedback indicated low value)

      return outputs

   # --------- Internals ---------
   def _query_started_jobs(self, session: Session, cutoff_start: datetime) -> List[Job]:
      """Jobs that started within the window with required fields."""
      jobs = session.query(Job).filter(
         and_(
            Job.start_time.isnot(None),
            Job.submit_time.isnot(None),
            Job.nodes.isnot(None),
            Job.walltime.isnot(None),
            Job.start_time >= cutoff_start,
         )
      ).all()
      return jobs

   def _get_live_queued_jobs(self) -> List[Job]:
      """Get currently queued jobs from live PBS system."""
      try:
         # Get live PBS data
         if self.data_collector is None:
            self.data_collector = DataCollector()
         
         live_jobs = self.data_collector.get_jobs(force_refresh=True)
         
         # Filter for queued jobs and convert to database Job objects
         queued_jobs = []
         for pbs_job in live_jobs:
            if pbs_job.state == PBSJobState.QUEUED:
               db_job = self._convert_pbs_job_to_db_job(pbs_job)
               if db_job:
                  queued_jobs.append(db_job)
         
         self.logger.debug(f"Found {len(queued_jobs)} live queued jobs")
         return queued_jobs
         
      except Exception as e:
         self.logger.warning(f"Failed to get live queued jobs: {e}")
         # Fallback to database queued jobs (will include stale data)
         return self._query_queued_jobs_from_db()
   
   def _query_queued_jobs_from_db(self) -> List[Job]:
      """Fallback: Get queued jobs from database (may include stale data)."""
      with self.repo_factory.get_job_repository().get_session() as session:
         jobs = session.query(Job).filter(
            and_(
               Job.submit_time.isnot(None),
               Job.nodes.isnot(None),
               Job.walltime.isnot(None),
               Job.start_time.is_(None),
               Job.state == JobState.QUEUED
            )
         ).all()
         self.logger.debug(f"Found {len(jobs)} queued jobs in database (fallback)")
         return jobs
   
   def _convert_pbs_job_to_db_job(self, pbs_job: PBSJob) -> Optional[Job]:
      """Convert a PBSJob to a database Job object for analytics."""
      try:
         # Create a Job-like object that has the required attributes
         # We don't actually save this to the database, just use it for analytics
         class MockJob:
            def __init__(self):
               self.job_id = pbs_job.job_id
               self.owner = pbs_job.owner
               self.project = pbs_job.project
               self.queue = pbs_job.queue
               self.allocation_type = pbs_job.allocation_type
               self.nodes = pbs_job.nodes
               self.walltime = pbs_job.walltime
               self.submit_time = pbs_job.submit_time
               self.start_time = pbs_job.start_time
               self.end_time = pbs_job.end_time
               # Pass through raw attributes for calculation
               self.raw_pbs_data = pbs_job.raw_attributes
               # Convert PBSJobState to JobState
               if pbs_job.state == PBSJobState.QUEUED:
                  self.state = JobState.QUEUED
               elif pbs_job.state == PBSJobState.RUNNING:
                  self.state = JobState.RUNNING
               elif pbs_job.state == PBSJobState.HELD:
                  self.state = JobState.HELD
               else:
                  self.state = JobState.QUEUED  # Default for unknown states
         
         return MockJob()
      except Exception as e:
         self.logger.debug(f"Failed to convert PBSJob {pbs_job.job_id}: {e}")
         return None

   def _find_start_score(self, session: Session, job: Job) -> Optional[float]:
      """
      Find score at start from `job_history` by taking the last recorded score
      at or before `start_time`. If none, try the first score after start.
      """
      if not job.start_time:
         return None

      hist = session.query(JobHistory).filter(
         JobHistory.job_id == job.job_id,
         JobHistory.timestamp <= job.start_time,
         JobHistory.score.isnot(None)
      ).order_by(JobHistory.timestamp.desc()).first()
      if hist and hist.score is not None:
         return float(hist.score)

      # Fallback: nearest after start
      hist2 = session.query(JobHistory).filter(
         JobHistory.job_id == job.job_id,
         JobHistory.timestamp > job.start_time,
         JobHistory.score.isnot(None)
      ).order_by(JobHistory.timestamp.asc()).first()
      if hist2 and hist2.score is not None:
         return float(hist2.score)
      return None

   def _parse_walltime_to_hours(self, walltime: Optional[str]) -> float:
      if not walltime:
         return 1.0
      try:
         parts = [int(x) for x in str(walltime).split(':')]
         if len(parts) == 3:
            h, m, s = parts
            return float(h) + m / 60.0 + s / 3600.0
         if len(parts) == 4:
            d, h, m, s = parts
            return float(d * 24 + h) + m / 60.0 + s / 3600.0
      except Exception:
         pass
      return 1.0

   def _compute_wait_hours(self, submit_time: Optional[datetime], start_time: Optional[datetime]) -> float:
      if not submit_time or not start_time:
         return float('nan')
      try:
         return max(0.0, (start_time - submit_time).total_seconds() / 3600.0)
      except Exception:
         return float('nan')

   def _compute_run_hours(self, start_time: Optional[datetime], end_time: Optional[datetime]) -> Optional[float]:
      if not start_time or not end_time:
         return None
      try:
         v = (end_time - start_time).total_seconds() / 3600.0
         return max(0.0, v)
      except Exception:
         return None

   def _filter_queues(self, df: pd.DataFrame, qf: QueueFilter) -> pd.DataFrame:
      if df.empty:
         return df
      # Exclude queues explicitly ignored
      if getattr(qf, 'ignore_queues', None):
         df = df[~df['queue'].isin(set(qf.ignore_queues))].copy()
      # Exclude reservation queues by default unless allowlisted or inclusion requested
      try:
         pattern = re.compile(getattr(qf, 'reservation_queue_regex', r'^[MRS]\\d+$'))
      except Exception:
         pattern = re.compile(r'^[MRS]\\d+$')
      allowlist_set = set(getattr(qf, 'allowlist_queues', []) or [])
      if not getattr(qf, 'include_reservations', False):
         is_resv = df['queue'].astype(str).map(lambda q: bool(pattern.match(q)))
         df = df[(~is_resv) | (df['queue'].isin(allowlist_set))].copy()
      # Compute per-queue total requested node-hours in window
      per_q = (
         df.groupby('queue')['requested_node_hours']
           .sum()
           .sort_values(ascending=False)
      )

      # Determine inclusion set
      include = set(per_q[per_q >= float(qf.min_queue_node_hours)].index.tolist())
      if qf.allowlist_queues:
         include.update(qf.allowlist_queues)
      if qf.top_n_queues is not None and qf.top_n_queues > 0:
         top = per_q.head(qf.top_n_queues).index.tolist()
         include.update(top)

      if include:
         out = df[df['queue'].isin(sorted(include))].copy()
         return out
      return df

   def _hex_or_scatter(self, data: pd.DataFrame, x: str, y: str, color=None, **kwargs) -> None:
      ax = plt.gca()
      x_values = data[x].values
      y_values = data[y].values
      try:
         # If a palette was provided, prefer a scatter with the designated queue color for consistency
         palette = kwargs.pop('palette', None)
         qname = None
         try:
            uq = data['queue'].dropna().astype(str).unique()
            qname = uq[0] if len(uq) > 0 else None
         except Exception:
            qname = None
         chosen_color = None
         if palette and qname is not None:
            chosen_color = palette.get(str(qname))

         if chosen_color is not None:
            ax.scatter(x_values, y_values, s=8, alpha=0.6, color=chosen_color)
            return

         hb = ax.hexbin(x_values, y_values, gridsize=30, mincnt=1, xscale='linear', cmap='viridis')
         cb = plt.colorbar(hb, ax=ax)
         cb.set_label('Count')
      except Exception:
         ax.scatter(x_values, y_values, s=8, alpha=0.6, color=color)

   def _build_queue_palette(self, queues: List[str]) -> Dict[str, str]:
      """Return a deterministic mapping from queue name to color."""
      palette: Dict[str, str] = {}
      if not queues:
         return palette
      try:
         base_colors = sns.color_palette('tab20', n_colors=max(3, len(queues)))
      except Exception:
         # Fallback basic colors if seaborn unavailable
         base_colors = [
            (0.121, 0.466, 0.705), (1.0, 0.498, 0.054), (0.172, 0.627, 0.172),
            (0.839, 0.153, 0.157), (0.580, 0.404, 0.741), (0.549, 0.337, 0.294),
            (0.890, 0.467, 0.761), (0.498, 0.498, 0.498), (0.737, 0.741, 0.133),
            (0.090, 0.745, 0.811)
         ]
         # Repeat if necessary
         if len(base_colors) < len(queues):
            k = int(math.ceil(len(queues) / float(len(base_colors))))
            base_colors = (base_colors * k)[:len(queues)]
      for idx, q in enumerate(queues):
         color = base_colors[idx % len(base_colors)]
         try:
            color = sns.utils.hex_color(color) if hasattr(sns.utils, 'hex_color') else color
         except Exception:
            pass
         palette[str(q)] = color
      return palette

   def _build_allocation_palette(self, alloc_types: List[str]) -> Dict[str, str]:
      """Return a deterministic mapping from allocation type to color."""
      palette: Dict[str, str] = {}
      if not alloc_types:
         return palette
      try:
         base_colors = sns.color_palette('Set2', n_colors=max(3, len(alloc_types)))
      except Exception:
         # Fallback basic colors if seaborn unavailable
         base_colors = [
            (0.400, 0.760, 0.647), (0.988, 0.553, 0.384), (0.553, 0.627, 0.796),
            (0.906, 0.541, 0.765), (0.651, 0.847, 0.329), (1.0, 0.851, 0.184),
            (0.898, 0.769, 0.580), (0.702, 0.702, 0.702)
         ]
         # Repeat if necessary
         if len(base_colors) < len(alloc_types):
            k = int(math.ceil(len(alloc_types) / float(len(base_colors))))
            base_colors = (base_colors * k)[:len(alloc_types)]
      for idx, alloc_type in enumerate(alloc_types):
         color = base_colors[idx % len(base_colors)]
         try:
            color = sns.utils.hex_color(color) if hasattr(sns.utils, 'hex_color') else color
         except Exception:
            pass
         palette[str(alloc_type)] = color
      return palette

   # --------- Time series helpers for Milestone 2 ---------
   def _compute_throughput_timeseries(self, df: pd.DataFrame, window_start: pd.Timestamp, freq: str = 'D') -> pd.DataFrame:
      """Aggregate requested node-hours started per period by queue."""
      if df.empty:
         return pd.DataFrame(columns=['timestamp', 'queue', 'node_hours'])
      dfx = df.dropna(subset=['start_time']).copy()
      dfx = dfx[dfx['start_time'] >= window_start]
      if dfx.empty:
         return pd.DataFrame(columns=['timestamp', 'queue', 'node_hours'])
      norm_freq = self._normalize_freq(freq)
      dfx['timestamp'] = dfx['start_time'].dt.to_period(norm_freq).dt.to_timestamp()
      out = (
         dfx.groupby(['timestamp', 'queue'])['requested_node_hours']
            .sum()
            .reset_index(name='node_hours')
            .sort_values('timestamp')
      )
      return out

   def _compute_backlog_timeseries(self, df: pd.DataFrame, window_start: pd.Timestamp, freq: str = 'D') -> pd.DataFrame:
      """
      Calculate machine-hours queued over time by queue.
      
      Machine-hours = (sum of queued node-hours) / total_system_nodes
      This gives the fraction of the total system that would be consumed.
      
      For each time bin:
      1. Find all jobs in QUEUED state during that time period
      2. Sum their requested_node_hours (nodes × walltime_hours) 
      3. Divide by total system nodes to get machine-hours fraction
      
      Returns columns: ['timestamp', 'queue', 'machine_hours']
      """
      if df.empty:
         return pd.DataFrame(columns=['timestamp', 'queue', 'machine_hours'])

      # Get total nodes from PBS for machine-hours calculation
      total_nodes = self._get_total_nodes_from_pbs()
      
      if total_nodes and total_nodes > 0:
         # Use actual node count for machine-hours calculation
         self.logger.debug(f"Using machine-hours calculation with {total_nodes} total nodes")
      else:
         # Fallback: try the old detection method
         self.logger.debug("PBS node count unavailable, trying fallback detection")
         total_nodes = self._detect_total_cluster_nodes()
         if not total_nodes or total_nodes <= 0:
            self.logger.warning("Could not determine total cluster nodes, using fallback of 1000")
            total_nodes = 1000  # Fallback value
      
      # Generate timeline bins for the analysis window
      now = pd.Timestamp.now(tz=None)
      start_bin = window_start.to_period(self._normalize_freq(freq)).to_timestamp()
      end_bin = now.to_period(self._normalize_freq(freq)).to_timestamp()
      timeline = pd.date_range(start=start_bin, end=end_bin, freq=self._normalize_freq(freq))
      
      rows: List[Tuple[pd.Timestamp, str, float]] = []
      
      # For each time bin, calculate machine-hours
      for t in timeline:
         next_t_candidates = pd.date_range(start=t, periods=2, freq=self._normalize_freq(freq))
         next_t = next_t_candidates[-1] if len(next_t_candidates) == 2 else (t + pd.Timedelta(hours=24))
         
         # Group queued node-hours by queue for this time bin
         queue_node_hours = {}
         
         # Find jobs that were queued during this time period [t, next_t)
         for _, row in df.iterrows():
            try:
               sub = row.get('submit_time')
               st = row.get('start_time') 
               state = row.get('state')
               q = row.get('queue')
               req_node_hours = row.get('requested_node_hours', 0.0)
               
               if pd.isna(sub) or req_node_hours <= 0:
                  continue
               
               # Convert to timestamps for comparison
               sub_ts = pd.Timestamp(sub)
               st_ts = pd.Timestamp(st) if not pd.isna(st) else now
               
               # Job is queued during [t, next_t) if:
               # 1. Job was submitted before next_t, AND
               # 2. Job was still queued after t (either not started yet, or started after t)
               # 3. Job is in QUEUED state (for current jobs) OR started after t (for historical jobs)
               is_queued_in_bin = (sub_ts < next_t and st_ts >= t and 
                                 (pd.isna(st) or st_ts >= t))
               # self.logger.debug(f"Job {row.get('job_id')} is queued in bin: {is_queued_in_bin} {sub_ts} {st_ts} {row.get('nodes')} {row.get('walltime_hours')} {req_node_hours}")
               if is_queued_in_bin:
                  queue_name = str(q) if q else 'unknown'
                  if queue_name not in queue_node_hours:
                     queue_node_hours[queue_name] = 0.0
                  queue_node_hours[queue_name] += float(req_node_hours)
               
            except Exception:
               continue
         
         # Convert to machine-hours and add to results
         for queue_name, total_queued_node_hours in queue_node_hours.items():
            # Machine-hours: queued node-hours / total system nodes
            # This gives the fraction of the system that would be consumed
            machine_hours = total_queued_node_hours / total_nodes
            rows.append((t, queue_name, machine_hours))
      if not rows:
         return pd.DataFrame(columns=['timestamp', 'queue', 'machine_hours'])
      out = pd.DataFrame(rows, columns=['timestamp', 'queue', 'machine_hours'])
      out = (
         out.groupby(['timestamp', 'queue'])['machine_hours']
            .sum()
            .reset_index()
            .sort_values('timestamp')
      )
      return out

   def _compute_backlog_timeseries_by_allocation(self, df: pd.DataFrame, window_start: pd.Timestamp, freq: str = 'D') -> pd.DataFrame:
      """
      Calculate machine-hours queued over time by allocation type.
      
      Machine-hours = (sum of queued node-hours) / total_system_nodes
      This gives the fraction of the total system that would be consumed.
      
      Similar to _compute_backlog_timeseries but groups by allocation_type instead of queue.
      
      Returns columns: ['timestamp', 'allocation_type', 'machine_hours']
      """
      if df.empty:
         return pd.DataFrame(columns=['timestamp', 'allocation_type', 'machine_hours'])

      # Get total nodes from PBS for machine-hours calculation
      total_nodes = self._get_total_nodes_from_pbs()
      
      if total_nodes and total_nodes > 0:
         # Use actual node count for machine-hours calculation
         self.logger.debug(f"Using machine-hours calculation with {total_nodes} total nodes")
      else:
         # Fallback: try the old detection method
         self.logger.debug("PBS node count unavailable, trying fallback detection")
         total_nodes = self._detect_total_cluster_nodes()
         if not total_nodes or total_nodes <= 0:
            self.logger.warning("Could not determine total cluster nodes, using fallback of 1000")
            total_nodes = 1000  # Fallback value
      
      # Generate timeline bins for the analysis window
      now = pd.Timestamp.now(tz=None)
      start_bin = window_start.to_period(self._normalize_freq(freq)).to_timestamp()
      end_bin = now.to_period(self._normalize_freq(freq)).to_timestamp()
      timeline = pd.date_range(start=start_bin, end=end_bin, freq=self._normalize_freq(freq))
      
      rows: List[Tuple[pd.Timestamp, str, float]] = []
      
      # For each time bin, calculate machine-hours
      for t in timeline:
         next_t_candidates = pd.date_range(start=t, periods=2, freq=self._normalize_freq(freq))
         next_t = next_t_candidates[-1] if len(next_t_candidates) == 2 else (t + pd.Timedelta(hours=24))
         
         # Group queued node-hours by allocation type for this time bin
         alloc_node_hours = {}
         
         # Find jobs that were queued during this time period [t, next_t)
         for _, row in df.iterrows():
            try:
               sub = row.get('submit_time')
               st = row.get('start_time') 
               state = row.get('state')
               alloc_type = row.get('allocation_type')
               req_node_hours = row.get('requested_node_hours', 0.0)
               
               if pd.isna(sub) or req_node_hours <= 0 or pd.isna(alloc_type):
                  continue
               
               # Convert to timestamps for comparison
               sub_ts = pd.Timestamp(sub)
               st_ts = pd.Timestamp(st) if not pd.isna(st) else now
               
               # Job is queued during [t, next_t) if:
               # 1. Job was submitted before next_t, AND
               # 2. Job was still queued after t (either not started yet, or started after t)
               # 3. Job is in QUEUED state (for current jobs) OR started after t (for historical jobs)
               is_queued_in_bin = (sub_ts < next_t and st_ts >= t and 
                                 (pd.isna(st) or st_ts >= t))
               
               if is_queued_in_bin:
                  alloc_name = str(alloc_type)
                  if alloc_name not in alloc_node_hours:
                     alloc_node_hours[alloc_name] = 0.0
                  alloc_node_hours[alloc_name] += float(req_node_hours)
               
            except Exception:
               continue
         
         # Convert to machine-hours and add to results
         for alloc_name, total_queued_node_hours in alloc_node_hours.items():
            # Machine-hours: queued node-hours / total system nodes
            # This gives the fraction of the system that would be consumed
            machine_hours = total_queued_node_hours / total_nodes
            rows.append((t, alloc_name, machine_hours))
      
      if not rows:
         return pd.DataFrame(columns=['timestamp', 'allocation_type', 'machine_hours'])
      
      out = pd.DataFrame(rows, columns=['timestamp', 'allocation_type', 'machine_hours'])
      out = (
         out.groupby(['timestamp', 'allocation_type'])['machine_hours']
            .sum()
            .reset_index()
            .sort_values('timestamp')
      )
      return out

   def _compute_active_nodes_timeseries(self, df: pd.DataFrame, window_start: pd.Timestamp, freq: str = 'D') -> pd.DataFrame:
      """Sum active nodes per timestamp by queue based on job run intervals."""
      if df.empty:
         return pd.DataFrame(columns=['timestamp', 'queue', 'nodes'])

      rows: List[Tuple[pd.Timestamp, str, int]] = []
      for _, row in df.iterrows():
         try:
            st = row.get('start_time')
            en = row.get('end_time')
            q = row.get('queue')
            nodes = int(row.get('nodes') or 0)
            if pd.isna(st) or pd.isna(en) or nodes <= 0:
               continue
            if en <= window_start:
               continue
            start_bin = max(pd.Timestamp(st).to_period(self._normalize_freq(freq)).to_timestamp(), window_start)
            end_bin = pd.Timestamp(en).to_period(self._normalize_freq(freq)).to_timestamp()
            if end_bin < start_bin:
               end_bin = start_bin
            timeline = pd.date_range(start=start_bin, end=end_bin, freq=self._normalize_freq(freq))
            if len(timeline) == 0:
               timeline = pd.DatetimeIndex([start_bin])
            for t in timeline:
               rows.append((t, str(q), nodes))
         except Exception:
            continue

      if not rows:
         return pd.DataFrame(columns=['timestamp', 'queue', 'nodes'])
      out = pd.DataFrame(rows, columns=['timestamp', 'queue', 'nodes'])
      out = (
         out.groupby(['timestamp', 'queue'])['nodes']
            .sum()
            .reset_index()
            .sort_values('timestamp')
      )
      return out

   def _compute_used_node_hours_timeseries(self, df: pd.DataFrame, window_start: pd.Timestamp, freq: str = 'D') -> pd.DataFrame:
      """
      Aggregate actual used node-hours per period across all queues.
      Computes sum over jobs of nodes × overlap_hours between job [start,end) and each period [t, next_t).
      Returns columns: ['timestamp', 'used_node_hours']
      """
      if df.empty:
         return pd.DataFrame(columns=['timestamp', 'used_node_hours'])

      rows: List[Tuple[pd.Timestamp, float]] = []
      for _, row in df.iterrows():
         try:
            st = row.get('start_time')
            en = row.get('end_time')
            nodes = int(row.get('nodes') or 0)
            if pd.isna(st) or pd.isna(en) or nodes <= 0:
               continue
            if en <= window_start:
               continue
            start_bin = max(pd.Timestamp(st).to_period(self._normalize_freq(freq)).to_timestamp(), window_start)
            end_bin = pd.Timestamp(en).to_period(self._normalize_freq(freq)).to_timestamp()
            if end_bin < start_bin:
               end_bin = start_bin
            timeline = pd.date_range(start=start_bin, end=end_bin, freq=self._normalize_freq(freq))
            if len(timeline) == 0:
               timeline = pd.DatetimeIndex([start_bin])
            for t in timeline:
               next_t_candidates = pd.date_range(start=t, periods=2, freq=self._normalize_freq(freq))
               next_t = next_t_candidates[-1] if len(next_t_candidates) == 2 else (t + pd.Timedelta(hours=24))
               # overlap within [t, next_t)
               seg_start = max(pd.Timestamp(st), t)
               seg_end = min(pd.Timestamp(en), next_t)
               hours = max(0.0, (seg_end - seg_start).total_seconds() / 3600.0)
               if hours > 0.0:
                  rows.append((t, float(nodes) * float(hours)))
         except Exception:
            continue

      if not rows:
         return pd.DataFrame(columns=['timestamp', 'used_node_hours'])
      out = pd.DataFrame(rows, columns=['timestamp', 'used_node_hours'])
      out = (
         out.groupby(['timestamp'])['used_node_hours']
            .sum()
            .reset_index()
            .sort_values('timestamp')
      )
      return out

   def _compute_used_node_hours_by_queue_timeseries(self, df: pd.DataFrame, window_start: pd.Timestamp, freq: str = 'D') -> pd.DataFrame:
      """
      Aggregate actual used node-hours per period by queue.
      Computes sum over jobs of nodes × overlap_hours between job [start,end) and each period [t, next_t).
      Returns columns: ['timestamp', 'queue', 'used_node_hours']
      """
      if df.empty:
         return pd.DataFrame(columns=['timestamp', 'queue', 'used_node_hours'])

      rows: List[Tuple[pd.Timestamp, str, float]] = []
      for _, row in df.iterrows():
         try:
            st = row.get('start_time')
            en = row.get('end_time')
            queue = row.get('queue')
            nodes = int(row.get('nodes') or 0)
            if pd.isna(st) or pd.isna(en) or nodes <= 0 or pd.isna(queue):
               continue
            if en <= window_start:
               continue
            start_bin = max(pd.Timestamp(st).to_period(self._normalize_freq(freq)).to_timestamp(), window_start)
            end_bin = pd.Timestamp(en).to_period(self._normalize_freq(freq)).to_timestamp()
            if end_bin < start_bin:
               end_bin = start_bin
            timeline = pd.date_range(start=start_bin, end=end_bin, freq=self._normalize_freq(freq))
            if len(timeline) == 0:
               timeline = pd.DatetimeIndex([start_bin])
            for t in timeline:
               next_t_candidates = pd.date_range(start=t, periods=2, freq=self._normalize_freq(freq))
               next_t = next_t_candidates[-1] if len(next_t_candidates) == 2 else (t + pd.Timedelta(hours=24))
               # overlap within [t, next_t)
               seg_start = max(pd.Timestamp(st), t)
               seg_end = min(pd.Timestamp(en), next_t)
               hours = max(0.0, (seg_end - seg_start).total_seconds() / 3600.0)
               if hours > 0.0:
                  rows.append((t, str(queue), float(nodes) * float(hours)))
         except Exception:
            continue

      if not rows:
         return pd.DataFrame(columns=['timestamp', 'queue', 'used_node_hours'])
      out = pd.DataFrame(rows, columns=['timestamp', 'queue', 'used_node_hours'])
      out = (
         out.groupby(['timestamp', 'queue'])['used_node_hours']
            .sum()
            .reset_index()
            .sort_values('timestamp')
      )
      return out

   def _compute_used_node_hours_by_allocation_timeseries(self, df: pd.DataFrame, window_start: pd.Timestamp, freq: str = 'D') -> pd.DataFrame:
      """
      Aggregate actual used node-hours per period by allocation type.
      Computes sum over jobs of nodes × overlap_hours between job [start,end) and each period [t, next_t).
      Returns columns: ['timestamp', 'allocation_type', 'used_node_hours']
      """
      if df.empty:
         return pd.DataFrame(columns=['timestamp', 'allocation_type', 'used_node_hours'])

      rows: List[Tuple[pd.Timestamp, str, float]] = []
      for _, row in df.iterrows():
         try:
            st = row.get('start_time')
            en = row.get('end_time')
            allocation_type = row.get('allocation_type')
            nodes = int(row.get('nodes') or 0)
            if pd.isna(st) or pd.isna(en) or nodes <= 0 or pd.isna(allocation_type):
               continue
            if en <= window_start:
               continue
            start_bin = max(pd.Timestamp(st).to_period(self._normalize_freq(freq)).to_timestamp(), window_start)
            end_bin = pd.Timestamp(en).to_period(self._normalize_freq(freq)).to_timestamp()
            if end_bin < start_bin:
               end_bin = start_bin
            timeline = pd.date_range(start=start_bin, end=end_bin, freq=self._normalize_freq(freq))
            if len(timeline) == 0:
               timeline = pd.DatetimeIndex([start_bin])
            for t in timeline:
               next_t_candidates = pd.date_range(start=t, periods=2, freq=self._normalize_freq(freq))
               next_t = next_t_candidates[-1] if len(next_t_candidates) == 2 else (t + pd.Timedelta(hours=24))
               # overlap within [t, next_t)
               seg_start = max(pd.Timestamp(st), t)
               seg_end = min(pd.Timestamp(en), next_t)
               hours = max(0.0, (seg_end - seg_start).total_seconds() / 3600.0)
               if hours > 0.0:
                  rows.append((t, str(allocation_type), float(nodes) * float(hours)))
         except Exception:
            continue

      if not rows:
         return pd.DataFrame(columns=['timestamp', 'allocation_type', 'used_node_hours'])
      out = pd.DataFrame(rows, columns=['timestamp', 'allocation_type', 'used_node_hours'])
      out = (
         out.groupby(['timestamp', 'allocation_type'])['used_node_hours']
            .sum()
            .reset_index()
            .sort_values('timestamp')
      )
      return out

   def _compute_reserved_node_hours_timeseries(self, window_start: pd.Timestamp, freq: str = 'D') -> pd.DataFrame:
      """
      Aggregate reserved node-hours per period across all reservations.
      Computes sum over reservations of nodes × overlap_hours between reservation [start,end) and each period [t, next_t).
      Returns columns: ['timestamp', 'reserved_node_hours']
      """
      # Ensure Timestamp
      window_start = pd.Timestamp(window_start)
      
      # Fetch reservations overlapping the window
      with self.repo_factory.get_reservation_repository().get_session() as session:
          # We need all reservations that might overlap with window_start onwards
          # Overlap if: res.end_time > window_start
          # Note: We rely on the repository to provide a reasonable subset or filter in memory
          # Ideally, we would add a method to ReservationRepository to query by overlap, 
          # but for now let's fetch historical ones and filter.
          # Assuming historical_reservations(days=X) covers the window.
          # Calculate days needed from window_start to now
          days_needed = (datetime.now() - window_start).days + 2
          reservations = self.repo_factory.get_reservation_repository().get_historical_reservations(days=days_needed)
          
      if not reservations:
         return pd.DataFrame(columns=['timestamp', 'reserved_node_hours'])

      rows: List[Tuple[pd.Timestamp, float]] = []
      
      # Use same timeline generation as other methods
      norm_freq = self._normalize_freq(freq)
      now = pd.Timestamp.now(tz=None)
      
      # We need to iterate over time bins covering the window
      start_bin = window_start.to_period(norm_freq).to_timestamp()
      end_bin = now.to_period(norm_freq).to_timestamp()
      timeline = pd.date_range(start=start_bin, end=end_bin, freq=norm_freq)
      if len(timeline) == 0:
         timeline = pd.DatetimeIndex([start_bin])
         
      for t in timeline:
         next_t_candidates = pd.date_range(start=t, periods=2, freq=norm_freq)
         next_t = next_t_candidates[-1] if len(next_t_candidates) == 2 else (t + pd.Timedelta(hours=24))
         
         total_reserved_hours = 0.0
         
         for res in reservations:
            try:
               st = res.start_time
               en = res.end_time
               nodes = int(res.nodes or 0)
               
               if pd.isna(st) or pd.isna(en) or nodes <= 0:
                  continue
               
               # Check overlap
               st_ts = pd.Timestamp(st)
               en_ts = pd.Timestamp(en)
               
               # overlap within [t, next_t)
               seg_start = max(st_ts, t)
               seg_end = min(en_ts, next_t)
               
               hours = max(0.0, (seg_end - seg_start).total_seconds() / 3600.0)
               if hours > 0.0:
                  total_reserved_hours += float(nodes) * float(hours)
            except Exception:
               continue
               
         if total_reserved_hours > 0.0:
            rows.append((t, total_reserved_hours))

      if not rows:
         return pd.DataFrame(columns=['timestamp', 'reserved_node_hours'])
         
      out = pd.DataFrame(rows, columns=['timestamp', 'reserved_node_hours'])
      out = (
         out.groupby(['timestamp'])['reserved_node_hours']
            .sum()
            .reset_index()
            .sort_values('timestamp')
      )
      return out

   def _plot_grouped_utilization(
      self,
      df: pd.DataFrame,
      group_col: str,
      total_nodes: int,
      window_start: pd.Timestamp,
      freq: str,
      palette: Dict[str, str],
      title: str,
      save_path: Optional[str] = None,
      dpi: int = 120
   ) -> Optional[str]:
      """Helper to compute and plot grouped utilization."""
      if df.empty:
         return None
      
      # Compute used node-hours by group
      if group_col == 'queue':
         used_df = self._compute_used_node_hours_by_queue_timeseries(df, window_start, freq)
      elif group_col == 'allocation_type':
         used_df = self._compute_used_node_hours_by_allocation_timeseries(df, window_start, freq)
      else:
         return None

      if used_df.empty:
         return None
      
      # Build full timeline
      now_ts = pd.Timestamp.now(tz=None)
      start_bin = window_start.to_period(self._normalize_freq(freq)).to_timestamp()
      end_bin = now_ts.to_period(self._normalize_freq(freq)).to_timestamp()
      full_idx = pd.date_range(start=start_bin, end=end_bin, freq=self._normalize_freq(freq))
      
      # Pivot to matrix form: index=timestamp, columns=group
      pivot = used_df.pivot_table(index='timestamp', columns=group_col, values='used_node_hours', aggfunc='sum').reindex(full_idx, fill_value=0.0).fillna(0.0)

      # Determine capacity per bin for normalization
      # Capacity = total_nodes * hours_in_bin
      offset = pd.tseries.frequencies.to_offset(self._normalize_freq(freq))
      cap_factors = []
      for t in pivot.index:
         candidate_next = t + offset
         next_t = min(candidate_next, now_ts)
         hours = max(0.0, (next_t - t).total_seconds() / 3600.0)
         cap_factors.append(hours)
      cap_factors_series = pd.Series(cap_factors, index=pivot.index)
      capacity_node_hours = cap_factors_series.astype(float) * float(int(total_nodes))
      
      # Normalize to 0-100%
      eps = 1e-9
      pct_df = pivot.divide(capacity_node_hours.clip(lower=eps), axis=0) * 100.0
      
      # Plot
      fig, ax = plt.subplots(figsize=(14, 6))
      color_order = [palette.get(str(c)) for c in pct_df.columns]
      ax.stackplot(pct_df.index, *(pct_df[c] for c in pct_df.columns), labels=pct_df.columns, colors=color_order, linewidth=0)
      
      ax.set_title(title)
      ax.set_xlabel('')
      ax.set_ylabel('Utilization (%)')
      ax.set_ylim(0, 100)
      
      # Format x-axis
      import matplotlib.dates as mdates
      ax.xaxis.set_major_locator(mdates.AutoDateLocator())
      ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
      plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
      
      # Add legend
      ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0, fontsize='small', title=group_col.replace('_', ' ').title(), frameon=False)
      
      fig.autofmt_xdate(rotation=45)
      
      saved_path = None
      if save_path:
         fig.savefig(save_path, bbox_inches='tight', dpi=dpi)
         saved_path = save_path
      plt.close(fig)
      return saved_path

   def _compute_current_wait_bins(self, df: pd.DataFrame) -> pd.DataFrame:
      """
      Compute wait time bins for currently queued jobs.
      Returns DataFrame with columns: ['queue', 'wait_bin', 'count']
      """
      if df.empty:
         self.logger.debug("Input DataFrame is empty")
         return pd.DataFrame(columns=['queue', 'wait_bin', 'count'])

      # Debug: Log the DataFrame info
      self.logger.debug(f"Input DataFrame has {len(df)} rows")
      if 'state' in df.columns:
         state_counts = df['state'].value_counts()
         self.logger.debug(f"State distribution: {state_counts.to_dict()}")
      else:
         self.logger.debug("No 'state' column in DataFrame")

      # Define bin edges in hours
      bins = [0, 1, 6, 12, 24, 48, 24*7, 24*14, 24*21, 24*35, float('inf')]
      labels = [
         '<1hr', '1-6hrs', '6-12hrs', '12-24hrs',
         '1-2days', '2-7days', '7-14days', '2-3weeks', '3-5weeks', '>1month'
      ]

      # Get currently queued jobs - only those in QUEUED state
      now = pd.Timestamp.now(tz=None)
      
      # More flexible filtering to debug what's happening
      if 'state' not in df.columns:
         self.logger.warning("No 'state' column found in DataFrame")
         return pd.DataFrame(columns=['queue', 'wait_bin', 'count'])
      
      # Check various conditions separately
      has_submit = df['submit_time'].notna()
      # JobState.QUEUED has value "Q", so check for both
      is_queued = df['state'].astype(str).isin(['Q', 'QUEUED', 'JobState.QUEUED'])
      no_start = df['start_time'].isna()
      
      self.logger.debug(f"Jobs with submit_time: {has_submit.sum()}")
      self.logger.debug(f"Jobs in QUEUED state: {is_queued.sum()}")
      self.logger.debug(f"Jobs without start_time: {no_start.sum()}")
      self.logger.debug(f"Unique state values: {df['state'].unique()}")
      
      queued = df[has_submit & is_queued & no_start].copy()
      
      self.logger.debug(f"Final queued jobs after filtering: {len(queued)}")
      
      if queued.empty:
         self.logger.warning("No queued jobs found after filtering")
         return pd.DataFrame(columns=['queue', 'wait_bin', 'count'])

      # Compute current wait time in hours
      queued['wait_hours'] = (now - queued['submit_time']).dt.total_seconds() / 3600.0
      
      # Bin the wait times
      queued['wait_bin'] = pd.cut(
         queued['wait_hours'],
         bins=bins,
         labels=labels,
         right=False
      )
      
      # Group by queue and wait bin
      counts = (
         queued.groupby(['queue', 'wait_bin'], observed=True)
         .size()
         .reset_index(name='count')
         .sort_values(['queue', 'wait_bin'])
      )
      
      return counts

   def _get_total_nodes_from_pbs(self) -> Optional[int]:
      """
      Get the total number of nodes from PBS using pbsnodes JSON output.
      
      Returns the count of nodes from the PBS system, or None if unavailable.
      """
      try:
         # Use PBSCommands to get raw node data as JSON
         pbs_commands = PBSCommands(use_sample_data=False)
         
         # Get the raw JSON data directly
         import subprocess
         result = subprocess.run(
            ['pbsnodes', '-a', '-F', 'json'],
            check=True,
            capture_output=True,
            text=True,
            timeout=30
         )
         
         if result.returncode == 0:
            # import json
            node_data = load_json_safe(result.stdout, "usage_insights_pbsnodes")
            total_nodes = len(node_data.get('nodes', {}))
            
            if total_nodes > 0:
               self.logger.debug(f"Found {total_nodes} total nodes from pbsnodes")
               return total_nodes
            else:
               self.logger.warning("No nodes found in pbsnodes output")
               return None
         else:
            self.logger.warning("pbsnodes command failed")
            return None
            
      except subprocess.TimeoutExpired:
         self.logger.warning("pbsnodes command timed out")
         return None
      except json.JSONDecodeError as e:
         self.logger.warning(f"Failed to parse pbsnodes JSON output: {e}")
         return None
      except Exception as e:
         self.logger.warning(f"Failed to get node count from PBS: {e}")
         return None

   def _detect_total_cluster_nodes(self) -> Optional[int]:
      """
      Detect total number of cluster nodes.
      
      Tries multiple approaches in order:
      1. pbsnodes -a -F dsv (PBS command, preferred)
      2. pbsnodes -a (PBS command, fallback)
      3. Database node count (fallback if PBS unavailable)
      
      Returns None if all detection methods fail.
      """
      # First try PBS commands
      try:
         if shutil.which('pbsnodes') is not None:
            # Preferred: one line per node
            try:
               result = subprocess.run(
                  ['pbsnodes', '-a', '-F', 'dsv'],
                  check=True,
                  capture_output=True,
                  text=True,
                  timeout=15
               )
               count = sum(1 for line in (result.stdout or '').splitlines() if line.strip())
               if count > 0:
                  self.logger.debug(f"Detected {count} nodes via pbsnodes -a -F dsv")
                  return int(count)
            except Exception:
               pass
            
            # Fallback: generic output; approximate by counting non-empty lines that look like node records
            try:
               result2 = subprocess.run(
                  ['pbsnodes', '-a'],
                  check=True,
                  capture_output=True,
                  text=True,
                  timeout=15
               )
               # Heuristic: count lines that start new node sections, commonly like: 'Node: <name>'
               lines = (result2.stdout or '').splitlines()
               count2 = sum(1 for line in lines if line.strip().lower().startswith('node:'))
               if count2 > 0:
                  self.logger.debug(f"Detected {count2} nodes via pbsnodes -a heuristic")
                  return int(count2)
               # As last resort, count blocks separated by blank lines
               blocks = [b for b in (result2.stdout or '').split('\n\n') if b.strip()]
               if blocks:
                  count3 = len(blocks)
                  self.logger.debug(f"Detected {count3} nodes via pbsnodes -a block counting")
                  return int(count3)
            except Exception:
               pass
      except Exception:
         pass
      
      # If PBS commands fail, try database as fallback
      try:
         node_repo = self.repo_factory.get_node_repository()
         nodes = node_repo.get_all_nodes()
         if nodes:
            # Only count active nodes
            active_nodes = [n for n in nodes if getattr(n, 'is_active', True)]
            count = len(active_nodes)
            if count > 0:
               self.logger.debug(f"Detected {count} nodes from database")
               return int(count)
      except Exception as e:
         self.logger.debug(f"Failed to get node count from database: {e}")
      
      self.logger.warning("Could not determine total cluster nodes using any method")
      return None

   # --------- Run Score Analysis for Production Queues ---------

   def generate_run_score_plots(
      self,
      days: int = 30,
      save_dir: Optional[str] = None,
      dpi: int = 120,
      queue_filter: Optional[QueueFilter] = None
   ) -> Dict[str, str]:
      """
      Generate plots analyzing job scores at run time for production queue jobs.

      Focuses on jobs routed through 'prod' queue to tiny/small/medium/large queues.
      Creates three visualizations:
      1. 2D Heatmap: median score/eligible_time by node count × walltime bins
      2. Ridge plot: score distributions by job shape
      3. Quantile regression: score vs eligible time trajectories per shape

      Args:
          days: Number of days to look back
          save_dir: Directory to save plots (optional)
          dpi: Plot resolution
          queue_filter: Optional QueueFilter to control which queues are included

      Returns:
          Dict mapping plot names to file paths
      """
      outputs: Dict[str, str] = {}

      if plt is None or sns is None:
         self.logger.warning("Plotting libraries not available")
         return outputs

      # Fetch run score data for production queues
      df = self._fetch_prod_run_score_data(days, queue_filter=queue_filter)

      if df.empty:
         self.logger.warning("No production queue job data found for run score analysis")
         return outputs

      self.logger.info(f"Analyzing run scores for {len(df)} production queue jobs")

      if save_dir:
         os.makedirs(save_dir, exist_ok=True)

      sns.set_context('talk')
      sns.set_style('whitegrid')

      # Generate each plot type
      try:
         path = self._plot_run_score_heatmap(df, save_dir, dpi)
         if path:
            outputs['run_score_heatmap'] = path
      except Exception as e:
         self.logger.warning(f"Failed to generate run score heatmap: {e}")

      try:
         path = self._plot_run_score_ridge(df, save_dir, dpi)
         if path:
            outputs['run_score_ridge'] = path
      except Exception as e:
         self.logger.warning(f"Failed to generate run score ridge plot: {e}")

      try:
         path = self._plot_run_score_quantiles(df, save_dir, dpi)
         if path:
            outputs['run_score_quantiles'] = path
      except Exception as e:
         self.logger.warning(f"Failed to generate run score quantile plot: {e}")

      return outputs

   def _fetch_prod_run_score_data(self, days: int = 30, queue_filter: Optional[QueueFilter] = None) -> pd.DataFrame:
      """
      Fetch job data from production queues with score and eligible_time at run time.

      By default, fetches from tiny/small/medium/large queues, but respects
      queue_filter settings (allowlist, ignore_queues) when provided.

      Returns DataFrame with columns:
      - job_id, queue, nodes, walltime_hours
      - eligible_time_hours: time job was eligible before running
      - run_score: score at time job started running
      - node_bin, walltime_bin, job_shape: categorization columns
      """
      from ..pbs_commands import PBSCommands
      import re

      # Default production queues
      prod_queues = ['tiny', 'small', 'medium', 'large']

      # Apply queue filter if provided
      if queue_filter:
         # If allowlist is specified, use only those queues (intersected with prod_queues)
         if queue_filter.allowlist_queues:
            prod_queues = [q for q in queue_filter.allowlist_queues if q in prod_queues] or queue_filter.allowlist_queues

         # Remove ignored queues
         if queue_filter.ignore_queues:
            prod_queues = [q for q in prod_queues if q not in queue_filter.ignore_queues]

         # Filter out reservation queues unless explicitly included
         if not queue_filter.include_reservations and queue_filter.reservation_queue_regex:
            resv_pattern = re.compile(queue_filter.reservation_queue_regex)
            prod_queues = [q for q in prod_queues if not resv_pattern.match(q)]

      if not prod_queues:
         self.logger.warning("No queues remaining after applying filters")
         return pd.DataFrame()

      cutoff = datetime.now() - timedelta(days=days)

      records = []

      with self.repo_factory.get_job_repository().get_session() as session:
         from ..database.models import Job, JobState

         jobs = session.query(Job).filter(
            and_(
               Job.queue.in_(prod_queues),
               Job.end_time >= cutoff,
               Job.state == JobState.FINISHED,
               Job.nodes.isnot(None),
               Job.walltime.isnot(None),
               Job.raw_pbs_data.isnot(None)
            )
         ).all()

         if not jobs:
            return pd.DataFrame()

         # Get server data for score calculation
         pbs_cmds = PBSCommands()
         server_data = None
         server_defaults = {}
         try:
            server_data = pbs_cmds.qstat_server()
            server_info = server_data.get("Server", {})
            for _, details in server_info.items():
               server_defaults = details.get("resources_default", {})
               break
         except Exception as e:
            self.logger.warning(f"Failed to get PBS server data: {e}")

         for job in jobs:
            try:
               if not isinstance(job.raw_pbs_data, dict):
                  continue

               # Skip jobs with score_boost > 0 to avoid biasing results
               resource_list = job.raw_pbs_data.get('Resource_List', {})
               score_boost = int(resource_list.get('score_boost', 0))
               if score_boost > 0:
                  continue

               # Parse walltime
               walltime_hours = self._parse_walltime_to_hours(job.walltime)

               # Parse eligible_time from raw PBS data
               eligible_time_str = job.raw_pbs_data.get('eligible_time')
               if not eligible_time_str:
                  continue

               eligible_time_secs = pbs_cmds._parse_eligible_time_to_seconds(eligible_time_str)
               eligible_time_hours = eligible_time_secs / 3600.0

               # Calculate score at run time
               run_score = None
               if server_data:
                  try:
                     run_score = pbs_cmds.calculate_job_score(
                        job.raw_pbs_data,
                        server_defaults=server_defaults,
                        server_data=server_data
                     )
                  except Exception:
                     pass

               # Fallback to DB history if calculation failed
               if run_score is None:
                  run_score = self._find_start_score(session, job)

               if run_score is None:
                  continue

               records.append({
                  'job_id': job.job_id,
                  'queue': job.queue,
                  'nodes': job.nodes,
                  'walltime_hours': walltime_hours,
                  'eligible_time_hours': eligible_time_hours,
                  'run_score': run_score,
               })

            except Exception as e:
               self.logger.debug(f"Skipping job {job.job_id}: {e}")
               continue

      if not records:
         return pd.DataFrame()

      df = pd.DataFrame.from_records(records)

      # Add binning columns
      df['node_bin'] = df['nodes'].apply(self._categorize_nodes)
      df['walltime_bin'] = df['walltime_hours'].apply(self._categorize_walltime)
      df['job_shape'] = df.apply(self._categorize_job_shape, axis=1)

      self.logger.info(f"Fetched {len(df)} jobs with run score data")

      return df

   def _categorize_nodes(self, nodes: int) -> str:
      """
      Categorize node count into bins aligned with Aurora queue structure.
      Each queue range is split into 2 bins for more granularity.
      Labels show the lower bound (upper bound is next bin's lower bound - 1).

      Queue boundaries (from PBS qstat -Q):
      - tiny: 1-512 nodes
      - small: 513-1024 nodes
      - medium: 1025-2048 nodes
      - large: 2048+ nodes (min 2048, but overlaps with medium max)
      """
      # Tiny queue: 1-512, split into 1-256, 257-512
      if nodes <= 256:
         return '1 (tiny)'
      elif nodes <= 512:
         return '257 (tiny)'
      # Small queue: 513-1024, split into 513-768, 769-1024
      elif nodes <= 768:
         return '513 (small)'
      elif nodes <= 1024:
         return '769 (small)'
      # Medium queue: 1025-2048, split into 1025-1536, 1537-2048
      elif nodes <= 1536:
         return '1025 (medium)'
      elif nodes <= 2048:
         return '1537 (medium)'
      # Large queue: 2048+, split into 2049-4096, 4097+
      elif nodes <= 4096:
         return '2049 (large)'
      else:
         return '4097 (large)'

   def _categorize_walltime(self, hours: float) -> str:
      """
      Categorize walltime into 3-hour bins (8 bins total for 24h max).

      Queue max walltimes (from PBS qstat -Q):
      - tiny: 6h max
      - small: 12h max
      - medium: 18h max
      - large: 24h max
      """
      if hours <= 3:
         return '0-3h'
      elif hours <= 6:
         return '3-6h'
      elif hours <= 9:
         return '6-9h'
      elif hours <= 12:
         return '9-12h'
      elif hours <= 15:
         return '12-15h'
      elif hours <= 18:
         return '15-18h'
      elif hours <= 21:
         return '18-21h'
      else:
         return '21-24h'

   def _categorize_job_shape(self, row: pd.Series) -> str:
      """
      Categorize job into a shape based on node count and walltime.
      Aligned with Aurora queue structure for meaningful analysis.
      """
      nodes = row['nodes']
      wt = row['walltime_hours']

      # Node-based primary category aligned with queue boundaries
      if nodes <= 512:
         node_cat = 'Tiny'
      elif nodes <= 1024:
         node_cat = 'Small'
      elif nodes <= 2048:
         node_cat = 'Medium'
      else:
         node_cat = 'Large'

      # Walltime modifier based on queue limits
      if node_cat == 'Tiny':
         # Tiny queue max is 6h
         if wt <= 2:
            return 'Tiny/Short (≤2h)'
         elif wt <= 6:
            return 'Tiny/Full (2-6h)'
         else:
            return 'Tiny/Overflow (>6h)'  # Should be rare
      elif node_cat == 'Small':
         # Small queue max is 12h
         if wt <= 6:
            return 'Small/Short (≤6h)'
         else:
            return 'Small/Full (6-12h)'
      elif node_cat == 'Medium':
         # Medium queue max is 18h
         if wt <= 12:
            return 'Medium/Short (≤12h)'
         else:
            return 'Medium/Full (12-18h)'
      else:
         # Large queue max is 24h
         if wt <= 18:
            return 'Large/Short (≤18h)'
         else:
            return 'Large/Full (18-24h)'

   def _plot_run_score_heatmap(
      self,
      df: pd.DataFrame,
      save_dir: Optional[str],
      dpi: int
   ) -> Optional[str]:
      """
      Plot 1: 2D Heatmap showing median eligible time and score by node × walltime bins.
      """
      # Create pivot tables for median values
      # Node bins: 2 per queue for granularity, labeled by lower bound
      node_order = [
         '1 (tiny)', '257 (tiny)',
         '513 (small)', '769 (small)',
         '1025 (medium)', '1537 (medium)',
         '2049 (large)', '4097 (large)'
      ]
      # Walltime bins: 3-hour intervals (8 bins total)
      wt_order = [
         '0-3h', '3-6h', '6-9h', '9-12h',
         '12-15h', '15-18h', '18-21h', '21-24h'
      ]

      # Filter to only bins that exist
      node_order = [n for n in node_order if n in df['node_bin'].values]
      wt_order = [w for w in wt_order if w in df['walltime_bin'].values]

      if not node_order or not wt_order:
         return None

      # Pivot for eligible time
      pivot_elig = df.pivot_table(
         index='node_bin',
         columns='walltime_bin',
         values='eligible_time_hours',
         aggfunc='median'
      ).reindex(index=node_order, columns=wt_order)

      # Pivot for run score
      pivot_score = df.pivot_table(
         index='node_bin',
         columns='walltime_bin',
         values='run_score',
         aggfunc='median'
      ).reindex(index=node_order, columns=wt_order)

      # Pivot for job count
      pivot_count = df.pivot_table(
         index='node_bin',
         columns='walltime_bin',
         values='job_id',
         aggfunc='count'
      ).reindex(index=node_order, columns=wt_order).fillna(0).astype(int)

      # Create figure with two heatmaps side by side
      fig, axes = plt.subplots(1, 2, figsize=(16, 7))

      # Left: Eligible time heatmap
      sns.heatmap(
         pivot_elig,
         ax=axes[0],
         annot=True,
         fmt='.1f',
         cmap='YlOrRd',
         cbar_kws={'label': 'Hours'}
      )
      axes[0].set_title('Median Eligible Time at Run (hours)')
      axes[0].set_xlabel('Walltime')
      axes[0].set_ylabel('Node Count')

      # Right: Run score heatmap
      sns.heatmap(
         pivot_score,
         ax=axes[1],
         annot=True,
         fmt='.0f',
         cmap='YlGnBu',
         cbar_kws={'label': 'Score'}
      )
      axes[1].set_title('Median Score at Run')
      axes[1].set_xlabel('Walltime')
      axes[1].set_ylabel('Node Count')

      # Add job counts as text annotation
      fig.suptitle('Production Queue Jobs: Score & Wait Time by Job Shape', fontsize=14, y=1.02)

      plt.tight_layout()

      if save_dir:
         path = os.path.join(save_dir, 'run_score_heatmap.png')
         fig.savefig(path, bbox_inches='tight', dpi=dpi)
         plt.close(fig)
         return path

      plt.close(fig)
      return None

   def _plot_run_score_ridge(
      self,
      df: pd.DataFrame,
      save_dir: Optional[str],
      dpi: int
   ) -> Optional[str]:
      """
      Plot 2: Ridge/Joy plot showing score distributions by job shape.
      """
      # Define shape order for consistent display (aligned with queue structure)
      shape_order = [
         'Tiny/Short (≤2h)', 'Tiny/Full (2-6h)', 'Tiny/Overflow (>6h)',
         'Small/Short (≤6h)', 'Small/Full (6-12h)',
         'Medium/Short (≤12h)', 'Medium/Full (12-18h)',
         'Large/Short (≤18h)', 'Large/Full (18-24h)'
      ]

      # Filter to shapes with data
      available_shapes = df['job_shape'].unique()
      shape_order = [s for s in shape_order if s in available_shapes]

      if len(shape_order) < 2:
         return None

      # Create ridge plot
      n_shapes = len(shape_order)
      fig, axes = plt.subplots(n_shapes, 1, figsize=(12, 2 * n_shapes), sharex=True)

      if n_shapes == 1:
         axes = [axes]

      # Color palette
      colors = sns.color_palette('husl', n_colors=n_shapes)

      # Global x-axis limits based on data
      x_min = df['run_score'].quantile(0.01)
      x_max = df['run_score'].quantile(0.99)

      for idx, (shape, ax) in enumerate(zip(shape_order, axes)):
         shape_data = df[df['job_shape'] == shape]['run_score'].dropna()

         if len(shape_data) < 5:
            ax.set_visible(False)
            continue

         # Plot KDE
         try:
            sns.kdeplot(
               data=shape_data,
               ax=ax,
               fill=True,
               color=colors[idx],
               alpha=0.7,
               linewidth=1.5
            )
         except Exception:
            # Fallback to histogram if KDE fails
            ax.hist(shape_data, bins=30, color=colors[idx], alpha=0.7, density=True)

         # Add median line
         median_score = shape_data.median()
         ax.axvline(median_score, color='black', linestyle='--', linewidth=1.5, alpha=0.8)

         # Label
         ax.set_ylabel('')
         ax.set_yticks([])
         ax.text(
            0.02, 0.5, f"{shape}\n(n={len(shape_data)}, med={median_score:.0f})",
            transform=ax.transAxes,
            fontsize=10,
            verticalalignment='center'
         )
         ax.set_xlim(x_min, x_max)

         # Only show x-axis on bottom plot
         if idx < n_shapes - 1:
            ax.set_xlabel('')

      axes[-1].set_xlabel('Score at Run Time')
      fig.suptitle('Run Score Distributions by Job Shape (Production Queues)', fontsize=14, y=1.01)

      plt.tight_layout()

      if save_dir:
         path = os.path.join(save_dir, 'run_score_ridge.png')
         fig.savefig(path, bbox_inches='tight', dpi=dpi)
         plt.close(fig)
         return path

      plt.close(fig)
      return None

   def _plot_run_score_quantiles(
      self,
      df: pd.DataFrame,
      save_dir: Optional[str],
      dpi: int
   ) -> Optional[str]:
      """
      Plot 3: Quantile regression lines showing score vs eligible time per job shape.
      Shows median with 25th-75th percentile bands.
      """
      shape_order = [
         'Tiny/Short (≤2h)', 'Tiny/Full (2-6h)', 'Tiny/Overflow (>6h)',
         'Small/Short (≤6h)', 'Small/Full (6-12h)',
         'Medium/Short (≤12h)', 'Medium/Full (12-18h)',
         'Large/Short (≤18h)', 'Large/Full (18-24h)'
      ]

      # Filter to shapes with enough data
      available_shapes = df['job_shape'].unique()
      shape_order = [s for s in shape_order if s in available_shapes]

      if not shape_order:
         return None

      # Create figure
      fig, ax = plt.subplots(figsize=(14, 8))

      # Color palette
      colors = sns.color_palette('husl', n_colors=len(shape_order))

      # Define eligible time bins for computing quantiles
      max_elig = df['eligible_time_hours'].quantile(0.95)
      elig_bins = np.linspace(0, max_elig, 20)
      elig_centers = (elig_bins[:-1] + elig_bins[1:]) / 2

      for idx, shape in enumerate(shape_order):
         shape_df = df[df['job_shape'] == shape]

         if len(shape_df) < 20:
            continue

         # Bin eligible time and compute quantiles
         shape_df = shape_df.copy()
         shape_df['elig_bin'] = pd.cut(
            shape_df['eligible_time_hours'],
            bins=elig_bins,
            labels=elig_centers
         )

         # Compute quantiles per bin
         quantiles = shape_df.groupby('elig_bin', observed=True)['run_score'].agg(
            ['median', lambda x: x.quantile(0.25), lambda x: x.quantile(0.75), 'count']
         )
         quantiles.columns = ['median', 'q25', 'q75', 'count']
         quantiles = quantiles[quantiles['count'] >= 3]  # Need at least 3 points per bin

         if len(quantiles) < 3:
            continue

         x = quantiles.index.astype(float)

         # Plot median line
         ax.plot(
            x, quantiles['median'],
            color=colors[idx],
            linewidth=2,
            label=f"{shape} (n={len(shape_df)})"
         )

         # Plot confidence band
         ax.fill_between(
            x,
            quantiles['q25'],
            quantiles['q75'],
            color=colors[idx],
            alpha=0.2
         )

      ax.set_xlabel('Eligible Time (hours)', fontsize=12)
      ax.set_ylabel('Score at Run Time', fontsize=12)
      ax.set_title('Score vs Eligible Time by Job Shape (Production Queues)\nLines show median, bands show 25th-75th percentile', fontsize=14)
      ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), fontsize=9)
      ax.grid(True, alpha=0.3)

      # Set reasonable axis limits
      ax.set_xlim(0, max_elig)

      plt.tight_layout()

      if save_dir:
         path = os.path.join(save_dir, 'run_score_quantiles.png')
         fig.savefig(path, bbox_inches='tight', dpi=dpi)
         plt.close(fig)
         return path

      plt.close(fig)
      return None

