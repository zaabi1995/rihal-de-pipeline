"""
Tests for the analytics load step.
Focus: idempotency - running twice should produce the same result, not duplicates.
"""
import sys
import os
import pytest
from unittest.mock import patch, MagicMock, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from load_analytics import load_analytics_data


class TestLoadIdempotency:
    """Verify the load step is idempotent."""

    @patch('load_analytics.get_db_connection')
    def test_truncates_before_insert(self, mock_db_conn):
        """Load should TRUNCATE analytics table before INSERT to prevent duplicates."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5
        mock_cursor.fetchone.return_value = (5, 100.00, 10)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        load_analytics_data()

        # Get all SQL executed
        all_calls = mock_cursor.execute.call_args_list
        all_sql = [str(c) for c in all_calls]
        sql_text = ' '.join(all_sql)

        # Verify TRUNCATE comes before INSERT
        assert 'TRUNCATE' in sql_text
        assert 'INSERT INTO analytics.shipping_spend_by_tier' in sql_text

        # Find positions - TRUNCATE must come before INSERT
        truncate_idx = None
        insert_idx = None
        for i, c in enumerate(all_calls):
            sql_str = str(c)
            if 'TRUNCATE' in sql_str:
                truncate_idx = i
            if 'INSERT INTO analytics.shipping_spend_by_tier' in sql_str:
                insert_idx = i

        assert truncate_idx is not None, "TRUNCATE not found"
        assert insert_idx is not None, "INSERT not found"
        assert truncate_idx < insert_idx, "TRUNCATE must happen before INSERT"

    @patch('load_analytics.get_db_connection')
    def test_no_drop_table(self, mock_db_conn):
        """Load should NOT use DROP TABLE - uses TRUNCATE instead."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3
        mock_cursor.fetchone.return_value = (3, 50.00, 6)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        load_analytics_data()

        all_sql = ' '.join(str(c) for c in mock_cursor.execute.call_args_list)
        assert 'DROP TABLE' not in all_sql

    @patch('load_analytics.get_db_connection')
    def test_idempotency_simulation(self, mock_db_conn):
        """
        Simulate running load twice.
        Because TRUNCATE runs first each time, the second run should
        produce the same state - not double the rows.
        """
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5
        mock_cursor.fetchone.return_value = (5, 200.00, 15)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        # Run 1
        load_analytics_data()
        first_run_calls = len(mock_cursor.execute.call_args_list)

        # Run 2
        load_analytics_data()
        second_run_calls = len(mock_cursor.execute.call_args_list) - first_run_calls

        # Both runs should execute the same number of statements
        assert first_run_calls == second_run_calls

        # Both runs should TRUNCATE
        truncate_count = sum(
            1 for c in mock_cursor.execute.call_args_list
            if 'TRUNCATE' in str(c)
        )
        assert truncate_count == 2  # Once per run

    @patch('load_analytics.get_db_connection')
    def test_verification_query(self, mock_db_conn):
        """Load should verify results by querying back counts after insert."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 4
        mock_cursor.fetchone.return_value = (4, 150.00, 12)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        load_analytics_data()

        # Should have a SELECT COUNT(*) or SUM after the INSERT
        all_sql = [str(c) for c in mock_cursor.execute.call_args_list]
        verification_queries = [
            s for s in all_sql
            if 'SELECT' in s and 'COUNT' in s and 'analytics' in s
        ]
        assert len(verification_queries) >= 1

    @patch('load_analytics.get_db_connection')
    def test_rollback_on_error(self, mock_db_conn):
        """Database error during load should trigger rollback."""
        mock_cursor = MagicMock()
        # CREATE TABLE succeeds, TRUNCATE succeeds, INSERT fails
        mock_cursor.execute.side_effect = [None, None, Exception("Disk full")]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        with pytest.raises(Exception, match="Disk full"):
            load_analytics_data()

        mock_conn.rollback.assert_called_once()

    @patch('load_analytics.get_db_connection')
    def test_commits_on_success(self, mock_db_conn):
        """Successful load should commit the transaction."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3
        mock_cursor.fetchone.return_value = (3, 100.00, 8)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        load_analytics_data()

        mock_conn.commit.assert_called_once()
