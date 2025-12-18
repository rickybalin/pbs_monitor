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

      # 3) Start-score distribution by queue (violin + box)
      try:
         fig, ax = plt.subplots(figsize=(10, 6))
         sns.violinplot(data=df, x='queue', y='start_score', hue='queue', inner=None, ax=ax, cut=0, palette=queue_palette, legend=False)
         sns.boxplot(data=df, x='queue', y='start_score', ax=ax, width=0.25, showcaps=True, boxprops={'facecolor':'none'})
         ax.set_title('Start-score distribution by queue')
         ax.set_xlabel('Queue')
         ax.set_ylabel('Score at start')
         fig.autofmt_xdate(rotation=30)
         if save_dir:
            pth = os.path.join(save_dir, 'start_score_distribution_by_queue.png')
            fig.savefig(pth, bbox_inches='tight', dpi=dpi)
            outputs['start_score_distribution_by_queue'] = pth
         plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot start_score_distribution_by_queue failed: {e}")

      # 4) ECDF of wait time by queue
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
         if not bl_df.empty:
            pivot = bl_df.pivot_table(index='timestamp', columns='queue', values='machine_hours', aggfunc='sum').fillna(0.0)
            fig, ax = plt.subplots(figsize=(14, 6))
            color_order = [queue_palette.get(str(c)) for c in pivot.columns]
            pivot.plot.area(ax=ax, color=color_order)
            ax.set_title(f'Queue depth over time (machine-hours queued per {ts_freq})')
            ax.set_xlabel('')  # Remove x-axis title
            ax.set_ylabel('Machine-hours queued')
            # Format x-axis dates - be more explicit to override pandas defaults
            import matplotlib.dates as mdates
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(pivot.index) // 10)))
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
            fig, ax = plt.subplots(figsize=(14, 6))
            # Build consistent palette for allocation types
            alloc_types = sorted(pivot.columns.astype(str).tolist())
            alloc_palette = self._build_allocation_palette(alloc_types)
            color_order = [alloc_palette.get(str(c)) for c in pivot.columns]
            pivot.plot.area(ax=ax, color=color_order)
            ax.set_title(f'Queue depth over time by allocation type (machine-hours queued per {ts_freq})')
            ax.set_xlabel('')  # Remove x-axis title
            ax.set_ylabel('Machine-hours queued')
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
               ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
               ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(utilization_pct.index) // 10)))
               plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
               # Force the formatter to be applied
               fig.autofmt_xdate(rotation=45)
               if save_dir:
                  pth = os.path.join(save_dir, f'utilization_percent_per_{ts_freq}.png')
                  fig.savefig(pth, bbox_inches='tight', dpi=dpi)
                  outputs['utilization_percent'] = pth
               plt.close(fig)
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
            import json
            node_data = json.loads(result.stdout)
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
