"""
Leaderboard Analytics for PBS Monitor

Provides leaderboard functionality to show top users and projects by node-hours
over various time periods (days or weeks).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta
import logging

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc

from ..database.repositories import RepositoryFactory
from ..database.models import Job, JobState
from ..data_collector import DataCollector
from ..models.job import PBSJob, JobState as PBSJobState


_LOGGER = logging.getLogger(__name__)


@dataclass
class LeaderboardConfig:
    """Configuration for leaderboard analysis"""
    days: Optional[int] = None  # For daily analysis
    weeks: Optional[int] = None  # For weekly analysis
    top_n: int = 10  # Number of top entries to show
    min_node_hours: float = 1.0  # Minimum node-hours to be included
    include_running: bool = True  # Include currently running jobs
    include_queued: bool = False  # Include queued jobs (with estimates)


class LeaderboardAnalyzer:
    """Analyze top users and projects by node-hours over time periods"""

    def __init__(self, repository_factory: Optional[RepositoryFactory] = None, data_collector: Optional[DataCollector] = None):
        self.repo_factory = repository_factory or RepositoryFactory()
        self.data_collector = data_collector
        self.logger = logging.getLogger(__name__)

    def analyze_daily_leaderboard(self, config: LeaderboardConfig) -> Dict[str, pd.DataFrame]:
        """
        Analyze top users and projects by node-hours for the last D days.
        
        Returns dict with keys:
        - 'users': DataFrame with top users
        - 'projects': DataFrame with top projects
        """
        if config.days is None:
            raise ValueError("days must be specified for daily leaderboard")
        
        cutoff_date = datetime.now() - timedelta(days=config.days)
        
        with self.repo_factory.get_job_repository().get_session() as session:
            # Get jobs from the time period
            jobs = self._get_jobs_in_period(session, cutoff_date, None, config)
            
            if not jobs:
                return {
                    'users': pd.DataFrame(columns=['rank', 'user', 'total_node_hours', 'total_jobs', 'avg_nodes', 'avg_walltime_hours']),
                    'projects': pd.DataFrame(columns=['rank', 'project', 'total_node_hours', 'total_jobs', 'avg_nodes', 'avg_walltime_hours'])
                }
            
            # Calculate aggregations
            users_df = self._aggregate_by_user(jobs, config.top_n)
            projects_df = self._aggregate_by_project(jobs, config.top_n)
            
            return {
                'users': users_df,
                'projects': projects_df
            }

    def analyze_weekly_leaderboard(self, config: LeaderboardConfig) -> Dict[str, pd.DataFrame]:
        """
        Analyze top users and projects by node-hours for each week in the last W weeks.
        
        Returns dict with keys:
        - 'users_by_week': DataFrame with top users per week
        - 'projects_by_week': DataFrame with top projects per week
        """
        if config.weeks is None:
            raise ValueError("weeks must be specified for weekly leaderboard")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(weeks=config.weeks)
        
        weekly_results = {
            'users_by_week': [],
            'projects_by_week': []
        }
        
        # Analyze each week
        current_week_end = end_date
        for week_num in range(config.weeks):
            week_start = current_week_end - timedelta(weeks=1)
            
            with self.repo_factory.get_job_repository().get_session() as session:
                jobs = self._get_jobs_in_period(session, week_start, current_week_end, config)
                
                if jobs:
                    week_label = f"Week {week_num + 1} ({week_start.strftime('%m/%d')} - {current_week_end.strftime('%m/%d')})"
                    
                    # Users for this week
                    users_df = self._aggregate_by_user(jobs, config.top_n)
                    if not users_df.empty:
                        users_df['week'] = week_label
                        weekly_results['users_by_week'].append(users_df)
                    
                    # Projects for this week
                    projects_df = self._aggregate_by_project(jobs, config.top_n)
                    if not projects_df.empty:
                        projects_df['week'] = week_label
                        weekly_results['projects_by_week'].append(projects_df)
            
            current_week_end = week_start
        
        # Combine all weeks
        result = {}
        if weekly_results['users_by_week']:
            result['users_by_week'] = pd.concat(weekly_results['users_by_week'], ignore_index=True)
        else:
            result['users_by_week'] = pd.DataFrame(columns=['week', 'rank', 'user', 'total_node_hours', 'total_jobs', 'avg_nodes', 'avg_walltime_hours'])
        
        if weekly_results['projects_by_week']:
            result['projects_by_week'] = pd.concat(weekly_results['projects_by_week'], ignore_index=True)
        else:
            result['projects_by_week'] = pd.DataFrame(columns=['week', 'rank', 'project', 'total_node_hours', 'total_jobs', 'avg_nodes', 'avg_walltime_hours'])
        
        return result

    def _get_jobs_in_period(self, session: Session, start_date: datetime, end_date: Optional[datetime], config: LeaderboardConfig) -> List[Job]:
        """Get jobs that were active during the specified period"""
        
        # Base query for completed jobs
        query_conditions = [
            Job.nodes.isnot(None),
            Job.walltime.isnot(None)
        ]
        
        if end_date:
            # For weekly analysis: jobs that started within the week
            query_conditions.extend([
                Job.start_time.isnot(None),
                Job.start_time >= start_date,
                Job.start_time < end_date
            ])
        else:
            # For daily analysis: jobs that started within the days
            query_conditions.extend([
                Job.start_time.isnot(None),
                Job.start_time >= start_date
            ])
        
        # Get completed and running jobs from database
        db_jobs = session.query(Job).filter(and_(*query_conditions)).all()
        
        jobs = list(db_jobs)
        
        # Add currently running jobs if enabled
        if config.include_running and end_date is None:  # Only for daily analysis
            try:
                live_jobs = self._get_live_running_jobs()
                # Filter live jobs to those that started in our time window
                for live_job in live_jobs:
                    if (live_job.start_time and live_job.start_time >= start_date and
                        live_job.nodes and live_job.walltime):
                        jobs.append(live_job)
            except Exception as e:
                self.logger.warning(f"Failed to get live running jobs: {e}")
        
        # Add queued jobs if enabled (estimate node-hours using walltime)
        if config.include_queued and end_date is None:  # Only for daily analysis
            try:
                queued_jobs = self._get_live_queued_jobs()
                for queued_job in queued_jobs:
                    if (queued_job.submit_time and queued_job.submit_time >= start_date and
                        queued_job.nodes and queued_job.walltime):
                        jobs.append(queued_job)
            except Exception as e:
                self.logger.warning(f"Failed to get live queued jobs: {e}")
        
        return jobs

    def _get_live_running_jobs(self) -> List[Job]:
        """Get currently running jobs from live PBS system"""
        try:
            if self.data_collector is None:
                self.data_collector = DataCollector()
            
            live_jobs = self.data_collector.get_jobs(force_refresh=True)
            
            running_jobs = []
            for pbs_job in live_jobs:
                if pbs_job.state == PBSJobState.RUNNING:
                    db_job = self._convert_pbs_job_to_db_job(pbs_job)
                    if db_job:
                        running_jobs.append(db_job)
            
            self.logger.debug(f"Found {len(running_jobs)} live running jobs")
            return running_jobs
            
        except Exception as e:
            self.logger.warning(f"Failed to get live running jobs: {e}")
            return []

    def _get_live_queued_jobs(self) -> List[Job]:
        """Get currently queued jobs from live PBS system"""
        try:
            if self.data_collector is None:
                self.data_collector = DataCollector()
            
            live_jobs = self.data_collector.get_jobs(force_refresh=True)
            
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
            return []

    def _convert_pbs_job_to_db_job(self, pbs_job: PBSJob) -> Optional[Job]:
        """Convert a PBSJob to a database Job object for analytics"""
        try:
            # Create a Job-like object for analytics (not saved to database)
            class MockJob:
                def __init__(self):
                    self.job_id = pbs_job.job_id
                    self.owner = pbs_job.owner
                    self.project = pbs_job.project
                    self.queue = pbs_job.queue
                    self.nodes = pbs_job.nodes
                    self.walltime = pbs_job.walltime
                    self.submit_time = pbs_job.submit_time
                    self.start_time = pbs_job.start_time
                    self.end_time = pbs_job.end_time
                    # Convert PBSJobState to JobState
                    if pbs_job.state == PBSJobState.QUEUED:
                        self.state = JobState.QUEUED
                    elif pbs_job.state == PBSJobState.RUNNING:
                        self.state = JobState.RUNNING
                    elif pbs_job.state == PBSJobState.HELD:
                        self.state = JobState.HELD
                    else:
                        self.state = JobState.QUEUED
            
            return MockJob()
        except Exception as e:
            self.logger.debug(f"Failed to convert PBSJob {pbs_job.job_id}: {e}")
            return None

    def _aggregate_by_user(self, jobs: List[Job], top_n: int) -> pd.DataFrame:
        """Aggregate jobs by user and calculate metrics"""
        user_stats = {}
        
        for job in jobs:
            if not job.owner:
                continue
                
            node_hours = self._calculate_node_hours(job)
            if node_hours <= 0:
                continue
                
            walltime_hours = self._parse_walltime_to_hours(job.walltime)
            
            if job.owner not in user_stats:
                user_stats[job.owner] = {
                    'total_node_hours': 0.0,
                    'total_jobs': 0,
                    'total_nodes': 0,
                    'total_walltime_hours': 0.0
                }
            
            stats = user_stats[job.owner]
            stats['total_node_hours'] += node_hours
            stats['total_jobs'] += 1
            stats['total_nodes'] += job.nodes or 0
            stats['total_walltime_hours'] += walltime_hours
        
        # Convert to DataFrame and rank
        data = []
        for user, stats in user_stats.items():
            avg_nodes = stats['total_nodes'] / stats['total_jobs'] if stats['total_jobs'] > 0 else 0
            avg_walltime = stats['total_walltime_hours'] / stats['total_jobs'] if stats['total_jobs'] > 0 else 0
            
            data.append({
                'user': user,
                'total_node_hours': round(stats['total_node_hours'], 2),
                'total_jobs': stats['total_jobs'],
                'avg_nodes': round(avg_nodes, 1),
                'avg_walltime_hours': round(avg_walltime, 2)
            })
        
        if not data:
            return pd.DataFrame(columns=['rank', 'user', 'total_node_hours', 'total_jobs', 'avg_nodes', 'avg_walltime_hours'])
        
        df = pd.DataFrame(data)
        df = df.sort_values('total_node_hours', ascending=False).head(top_n)
        df['rank'] = range(1, len(df) + 1)
        
        # Reorder columns
        df = df[['rank', 'user', 'total_node_hours', 'total_jobs', 'avg_nodes', 'avg_walltime_hours']]
        return df.reset_index(drop=True)

    def _aggregate_by_project(self, jobs: List[Job], top_n: int) -> pd.DataFrame:
        """Aggregate jobs by project and calculate metrics"""
        project_stats = {}
        
        for job in jobs:
            project = job.project or 'unknown'
                
            node_hours = self._calculate_node_hours(job)
            if node_hours <= 0:
                continue
                
            walltime_hours = self._parse_walltime_to_hours(job.walltime)
            
            if project not in project_stats:
                project_stats[project] = {
                    'total_node_hours': 0.0,
                    'total_jobs': 0,
                    'total_nodes': 0,
                    'total_walltime_hours': 0.0
                }
            
            stats = project_stats[project]
            stats['total_node_hours'] += node_hours
            stats['total_jobs'] += 1
            stats['total_nodes'] += job.nodes or 0
            stats['total_walltime_hours'] += walltime_hours
        
        # Convert to DataFrame and rank
        data = []
        for project, stats in project_stats.items():
            avg_nodes = stats['total_nodes'] / stats['total_jobs'] if stats['total_jobs'] > 0 else 0
            avg_walltime = stats['total_walltime_hours'] / stats['total_jobs'] if stats['total_jobs'] > 0 else 0
            
            data.append({
                'project': project,
                'total_node_hours': round(stats['total_node_hours'], 2),
                'total_jobs': stats['total_jobs'],
                'avg_nodes': round(avg_nodes, 1),
                'avg_walltime_hours': round(avg_walltime, 2)
            })
        
        if not data:
            return pd.DataFrame(columns=['rank', 'project', 'total_node_hours', 'total_jobs', 'avg_nodes', 'avg_walltime_hours'])
        
        df = pd.DataFrame(data)
        df = df.sort_values('total_node_hours', ascending=False).head(top_n)
        df['rank'] = range(1, len(df) + 1)
        
        # Reorder columns
        df = df[['rank', 'project', 'total_node_hours', 'total_jobs', 'avg_nodes', 'avg_walltime_hours']]
        return df.reset_index(drop=True)

    def _calculate_node_hours(self, job: Job) -> float:
        """Calculate node-hours for a job"""
        if not job.nodes or not job.walltime:
            return 0.0
        
        # For completed jobs, use actual runtime if available
        if job.start_time and job.end_time:
            runtime_hours = (job.end_time - job.start_time).total_seconds() / 3600.0
            return job.nodes * runtime_hours
        
        # For running/queued jobs, use walltime estimate
        walltime_hours = self._parse_walltime_to_hours(job.walltime)
        return job.nodes * walltime_hours

    def _parse_walltime_to_hours(self, walltime: Optional[str]) -> float:
        """Parse PBS walltime string to hours"""
        if not walltime:
            return 0.0
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
        return 0.0
