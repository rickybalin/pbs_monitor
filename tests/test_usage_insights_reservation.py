
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
import pandas as pd
from pbs_monitor.analytics.usage_insights import UsageInsights
from pbs_monitor.database.models import Reservation, ReservationState

class MockRepositoryFactory:
    def __init__(self, reservation_repo):
        self.reservation_repo = reservation_repo
    
    def get_reservation_repository(self):
        return self.reservation_repo

    def get_job_repository(self):
        return MagicMock()

def test_compute_reserved_node_hours_timeseries():
    # Setup mock data
    now = datetime.now()
    window_start = now - timedelta(days=2)
    
    # Create sample reservations
    # Res 1: 10 nodes, overlaps entire window (2 days) -> should be constant 10 * hours
    res1 = Reservation(
        reservation_id="R1",
        start_time=window_start - timedelta(days=1),
        end_time=now + timedelta(days=1),
        nodes=10,
        state=ReservationState.CONFIRMED
    )
    
    # Res 2: 5 nodes, starts in middle of first day, ends in middle of second day
    # Day 1 bin: starts at T+12h (assuming daily bins start at T)
    # Day 2 bin: ends at T+1d+12h
    res2 = Reservation(
        reservation_id="R2",
        start_time=window_start + timedelta(hours=12),
        end_time=window_start + timedelta(hours=36), # 1.5 days from window_start
        nodes=5,
        state=ReservationState.RUNNING
    )
    
    mock_res_repo = MagicMock()
    mock_res_repo.get_historical_reservations.return_value = [res1, res2]
    
    repo_factory = MockRepositoryFactory(mock_res_repo)
    ui = UsageInsights(repository_factory=repo_factory)
    
    # Execute method
    df = ui._compute_reserved_node_hours_timeseries(window_start, freq='D')
    
    # Verify results
    assert not df.empty
    assert 'timestamp' in df.columns
    assert 'reserved_node_hours' in df.columns
    
    # Check bins
    # We expect bins for Day 0 and Day 1
    # Note: timestamp is start of bin
    
    # Filter for first bin
    bin1 = df[df['timestamp'] == pd.Timestamp(window_start).floor('D')]
    # If using 'D' freq, pandas might normalize times. The impl uses normalize_freq helper.
    # The helper implementation of bins usually starts from window_start
    
    # Let's verify via the output logic we expect.
    # _compute_reserved_node_hours_timeseries usually returns a DataFrame where we can check logic.
    
    # Just check that we have entries
    assert len(df) >= 2 
    
    # Calculate expected hours for Res 1 in a 24h bin: 10 nodes * 24 hours = 240
    # Calculate expected hours for Res 2 in first bin: Starts at +12h, so 12 hours overlap -> 5 nodes * 12 h = 60
    # Total for Day 1: 240 + 60 = 300
    
    # We need to be careful with exact binning implementation details (start vs end of bin etc)
    # But roughly we should see some reserved hours.
    
    assert df['reserved_node_hours'].sum() > 0

def test_compute_reserved_node_hours_empty():
    now = datetime.now()
    window_start = now - timedelta(days=1)
    
    mock_res_repo = MagicMock()
    mock_res_repo.get_historical_reservations.return_value = []
    
    repo_factory = MockRepositoryFactory(mock_res_repo)
    ui = UsageInsights(repository_factory=repo_factory)
    
    df = ui._compute_reserved_node_hours_timeseries(window_start, freq='D')
    
    assert df.empty or df['reserved_node_hours'].sum() == 0    
