#!/usr/bin/env python3
"""
Recent Finished Jobs Report

This script prints all finished jobs from the last 24 hours from the PBS Monitor database.
Output is formatted as CSV for easy analysis with spreadsheets or pandas.
"""

import argparse
import csv
import sys
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add the parent directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pbs_monitor.config import Config
from pbs_monitor.database.connection import get_db_session
from pbs_monitor.database.models import Job, JobState
from pbs_monitor.utils.logging_setup import create_pbs_logger

# Configure logging with DD-MM HH:MM format as per workspace rules
logger = create_pbs_logger(__name__)

def setup_logging(verbose: bool = False):
   """Set up logging configuration"""
   level = logging.DEBUG if verbose else logging.INFO
   logger.setLevel(level)

def get_finished_jobs_last_24h(config: Config = None) -> list:
   """
   Query finished jobs from the last 24 hours
   
   Args:
      config: Configuration object (optional)
      
   Returns:
      List of Job objects that are finished and from last 24 hours
   """
   # Calculate 24 hours ago
   now = datetime.now(timezone.utc)
   cutoff_time = now - timedelta(hours=24)
   
   logger.info(f"Querying finished jobs since {cutoff_time.strftime('%d-%m %H:%M')}")
   
   finished_states = [JobState.COMPLETED, JobState.FINISHED, JobState.UNKNOWN_END]
   
   with get_db_session(config) as session:
      # Query jobs that:
      # 1. Are in finished states
      # 2. Have end_time within last 24 hours OR last_updated within last 24 hours (for jobs that may not have end_time)
      jobs = session.query(Job).filter(
         Job.state.in_(finished_states),
         (Job.end_time >= cutoff_time) | 
         ((Job.end_time.is_(None)) & (Job.last_updated >= cutoff_time))
      ).order_by(Job.end_time.desc().nullslast(), Job.last_updated.desc()).all()
      
      # Expunge to avoid detached instance issues
      session.expunge_all()
      
      logger.info(f"Found {len(jobs)} finished jobs from last 24 hours")
      return jobs

def format_job_as_dict(job: Job) -> dict:
   """
   Convert Job object to dictionary with all relevant fields
   
   Args:
      job: Job database model instance
      
   Returns:
      Dictionary with job data
   """
   return {
      'job_id': job.job_id,
      'job_name': job.job_name,
      'owner': job.owner,
      'project': job.project,
      'allocation_type': job.allocation_type,
      'state': job.state.value if job.state else None,
      'queue': job.queue,
      'nodes': job.nodes,
      'ppn': job.ppn,
      'walltime': job.walltime,
      'memory': job.memory,
      'submit_time': job.submit_time.strftime('%Y-%m-%d %H:%M:%S') if job.submit_time else None,
      'start_time': job.start_time.strftime('%Y-%m-%d %H:%M:%S') if job.start_time else None,
      'end_time': job.end_time.strftime('%Y-%m-%d %H:%M:%S') if job.end_time else None,
      'priority': job.priority,
      'exit_status': job.exit_status,
      'execution_node': job.execution_node,
      'total_cores': job.total_cores,
      'actual_runtime_seconds': job.actual_runtime_seconds,
      'queue_time_seconds': job.queue_time_seconds,
      'first_seen': job.first_seen.strftime('%Y-%m-%d %H:%M:%S') if job.first_seen else None,
      'last_updated': job.last_updated.strftime('%Y-%m-%d %H:%M:%S') if job.last_updated else None,
      'final_state_recorded': job.final_state_recorded
   }

def print_jobs_csv(jobs: list, output_file=None):
   """
   Print jobs as CSV format
   
   Args:
      jobs: List of Job objects
      output_file: Optional file path to write to (default: stdout)
   """
   if not jobs:
      logger.warning("No finished jobs found in the last 24 hours")
      return
   
   # Convert jobs to dictionaries
   job_dicts = [format_job_as_dict(job) for job in jobs]
   
   # Get all field names from the first job
   fieldnames = list(job_dicts[0].keys())
   
   # Write CSV
   output = open(output_file, 'w', newline='') if output_file else sys.stdout
   
   try:
      writer = csv.DictWriter(output, fieldnames=fieldnames)
      writer.writeheader()
      writer.writerows(job_dicts)
      
      if output_file:
         logger.info(f"CSV output written to {output_file}")
      else:
         logger.info(f"CSV output printed to stdout ({len(jobs)} jobs)")
         
   finally:
      if output_file and output != sys.stdout:
         output.close()

def main():
   """Main function"""
   parser = argparse.ArgumentParser(
      description="Print finished jobs from the last 24 hours as CSV",
      formatter_class=argparse.RawDescriptionHelpFormatter,
      epilog="""
Examples:
   %(prog)s                    # Print to stdout
   %(prog)s -o jobs.csv        # Save to file
   %(prog)s -v                 # Verbose logging
      """)
   
   parser.add_argument('-o', '--output', 
                      help='Output CSV file (default: stdout)')
   parser.add_argument('-v', '--verbose', 
                      action='store_true',
                      help='Enable verbose logging')
   parser.add_argument('--config-file',
                      help='Path to configuration file')
   
   args = parser.parse_args()
   
   # Set up logging
   setup_logging(args.verbose)
   
   try:
      # Load configuration
      config = None
      if args.config_file:
         config = Config.from_file(args.config_file)
         logger.debug(f"Loaded configuration from {args.config_file}")
      else:
         config = Config()
         logger.debug("Using default configuration")
      
      # Get finished jobs
      jobs = get_finished_jobs_last_24h(config)
      
      # Print as CSV
      print_jobs_csv(jobs, args.output)
      
      return 0
      
   except Exception as e:
      logger.error(f"Error: {str(e)}")
      if args.verbose:
         logger.exception("Full traceback:")
      return 1

if __name__ == "__main__":
   sys.exit(main())
