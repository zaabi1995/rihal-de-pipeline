"""
Tests for the transform step.
Since the transform runs SQL inside PostgreSQL, we test the logic by
verifying the SQL contains the right filters and by unit-testing
the data quality concepts with Python equivalents.
"""
import sys
import os
import pytest
from unittest.mock import patch, MagicMock, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from transform_data import transform_shipment_data


class TestTransformDataQuality:
    """Test that the transform SQL applies correct data quality filters."""

    @patch('transform_data.get_db_connection')
    def test_transform_sql_filters_null_customer(self, mock_db_conn):
        """Transform SQL should exclude rows where customer_id IS NULL."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        transform_shipment_data()

        # Find the CREATE TABLE AS query
        create_calls = [
            str(c) for c in mock_cursor.execute.call_args_list
            if 'CREATE TABLE staging.shipments_with_tiers' in str(c)
        ]
        assert len(create_calls) == 1
        sql = create_calls[0]

        assert 'customer_id IS NOT NULL' in sql

    @patch('transform_data.get_db_connection')
    def test_transform_sql_filters_non_positive_cost(self, mock_db_conn):
        """Transform SQL should exclude rows with shipping_cost <= 0."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        transform_shipment_data()

        create_calls = [
            str(c) for c in mock_cursor.execute.call_args_list
            if 'CREATE TABLE staging.shipments_with_tiers' in str(c)
        ]
        sql = create_calls[0]

        assert 'shipping_cost > 0' in sql

    @patch('transform_data.get_db_connection')
    def test_transform_sql_filters_cancelled(self, mock_db_conn):
        """Transform SQL should exclude cancelled shipments."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        transform_shipment_data()

        create_calls = [
            str(c) for c in mock_cursor.execute.call_args_list
            if 'CREATE TABLE staging.shipments_with_tiers' in str(c)
        ]
        sql = create_calls[0]

        assert "cancelled" in sql

    @patch('transform_data.get_db_connection')
    def test_transform_sql_deduplicates(self, mock_db_conn):
        """Transform SQL should use DISTINCT ON to deduplicate shipments."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        transform_shipment_data()

        create_calls = [
            str(c) for c in mock_cursor.execute.call_args_list
            if 'CREATE TABLE staging.shipments_with_tiers' in str(c)
        ]
        sql = create_calls[0]

        assert 'DISTINCT ON (shipment_id)' in sql

    @patch('transform_data.get_db_connection')
    def test_transform_sql_scd_join(self, mock_db_conn):
        """Transform SQL should use SCD-aware join with tier_updated_date."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        transform_shipment_data()

        create_calls = [
            str(c) for c in mock_cursor.execute.call_args_list
            if 'CREATE TABLE staging.shipments_with_tiers' in str(c)
        ]
        sql = create_calls[0]

        assert 'tier_updated_date' in sql
        assert 'LATERAL' in sql

    @patch('transform_data.get_db_connection')
    def test_transform_handles_unknown_tier(self, mock_db_conn):
        """Transform SQL should COALESCE null tiers to 'Unknown'."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        transform_shipment_data()

        create_calls = [
            str(c) for c in mock_cursor.execute.call_args_list
            if 'CREATE TABLE staging.shipments_with_tiers' in str(c)
        ]
        sql = create_calls[0]

        assert "COALESCE" in sql
        assert "'Unknown'" in sql

    @patch('transform_data.get_db_connection')
    def test_transform_logs_quality_stats(self, mock_db_conn):
        """Transform should query data quality counts before transforming."""
        mock_cursor = MagicMock()
        # Return values for: total_raw, null_customers, bad_costs, cancelled, dup_ids, final_count
        mock_cursor.fetchone.side_effect = [(21,), (1,), (2,), (1,), (1,), (15,)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        transform_shipment_data()

        # Should have queried for counts
        count_queries = [
            c for c in mock_cursor.execute.call_args_list
            if 'COUNT(*)' in str(c)
        ]
        # At least 5 count queries: total, null, bad cost, cancelled, dups, final
        assert len(count_queries) >= 5

    @patch('transform_data.get_db_connection')
    def test_transform_rollback_on_error(self, mock_db_conn):
        """Database errors should trigger rollback."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Connection lost")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        with pytest.raises(Exception, match="Connection lost"):
            transform_shipment_data()

        mock_conn.rollback.assert_called_once()


class TestDataQualityLogic:
    """
    Unit tests for the data quality filtering logic itself.
    These test the rules in pure Python (no SQL), validating the concepts
    that the SQL implements.
    """

    def test_filter_null_customer_id(self, sample_shipments):
        """Shipments with null customer_id should be filtered out."""
        filtered = [s for s in sample_shipments if s['customer_id'] is not None]
        null_count = sum(1 for s in sample_shipments if s['customer_id'] is None)

        assert null_count == 1  # SHP014
        assert all(s['customer_id'] is not None for s in filtered)

    def test_filter_negative_cost(self, sample_shipments):
        """Shipments with negative shipping_cost should be filtered out."""
        negative = [s for s in sample_shipments if s['shipping_cost'] < 0]
        assert len(negative) == 1  # SHP012
        assert negative[0]['shipment_id'] == 'SHP012'

    def test_filter_zero_cost(self, sample_shipments):
        """Shipments with zero shipping_cost should be filtered out."""
        zero = [s for s in sample_shipments if s['shipping_cost'] == 0]
        assert len(zero) == 1  # SHP013
        assert zero[0]['shipment_id'] == 'SHP013'

    def test_filter_cancelled(self, sample_shipments):
        """Cancelled shipments should be filtered out."""
        cancelled = [s for s in sample_shipments if s['status'] == 'cancelled']
        assert len(cancelled) == 1  # SHP017
        assert cancelled[0]['shipment_id'] == 'SHP017'

    def test_dedup_keeps_one(self, sample_shipments):
        """Duplicate shipment_ids should be deduped to one row."""
        seen = {}
        for s in sample_shipments:
            seen[s['shipment_id']] = s  # last-write-wins
        deduped = list(seen.values())

        shp002 = [s for s in deduped if s['shipment_id'] == 'SHP002']
        assert len(shp002) == 1
        # Last-write-wins means cost = 47.00
        assert shp002[0]['shipping_cost'] == 47.00

    def test_all_filters_combined(self, sample_shipments):
        """Apply all filters together — should produce the expected clean set."""
        # Dedup (last-write-wins)
        seen = {}
        for s in sample_shipments:
            seen[s['shipment_id']] = s
        deduped = list(seen.values())

        # Apply quality filters
        clean = [
            s for s in deduped
            if s['customer_id'] is not None
            and s['shipping_cost'] > 0
            and s['status'] != 'cancelled'
        ]

        # Expected: SHP001, SHP002 (deduped), SHP011 (unknown cust), minus SHP012/13/14/17
        expected_ids = {'SHP001', 'SHP002', 'SHP011'}
        actual_ids = {s['shipment_id'] for s in clean}
        assert actual_ids == expected_ids
