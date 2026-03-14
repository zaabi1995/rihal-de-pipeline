"""
Tests for shipment extraction: retry logic, parameterized queries, error handling.
"""
import sys
import os
import pytest
from unittest.mock import patch, MagicMock
from requests.exceptions import ConnectionError, Timeout

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from extract_shipments import fetch_shipments_with_retry, extract_shipments_from_api


class TestFetchShipmentsWithRetry:
    """Test the API fetch function with retry logic."""

    @patch('extract_shipments.requests.get')
    @patch('extract_shipments.time.sleep')  # Don't actually sleep in tests
    def test_successful_fetch_on_first_try(self, mock_sleep, mock_get):
        """API returns 200 on first attempt - no retries needed."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{"shipment_id": "SHP001", "customer_id": "CUST001",
                       "shipping_cost": 25.50, "shipment_date": "2024-01-15",
                       "status": "delivered"}],
            "count": 1
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = fetch_shipments_with_retry()

        assert len(result) == 1
        assert result[0]['shipment_id'] == 'SHP001'
        mock_sleep.assert_not_called()

    @patch('extract_shipments.requests.get')
    @patch('extract_shipments.time.sleep')
    def test_retry_on_connection_error(self, mock_sleep, mock_get):
        """API fails twice then succeeds - retries should recover."""
        good_response = MagicMock()
        good_response.status_code = 200
        good_response.json.return_value = {"data": [], "count": 0}
        good_response.raise_for_status = MagicMock()

        mock_get.side_effect = [
            ConnectionError("Connection refused"),
            ConnectionError("Connection refused"),
            good_response
        ]

        result = fetch_shipments_with_retry()

        assert result == []
        assert mock_get.call_count == 3
        assert mock_sleep.call_count == 2  # slept between retry 1->2 and 2->3

    @patch('extract_shipments.requests.get')
    @patch('extract_shipments.time.sleep')
    def test_exhausted_retries_raises(self, mock_sleep, mock_get):
        """All 3 attempts fail - should raise the exception."""
        mock_get.side_effect = Timeout("Request timed out")

        with pytest.raises(Timeout):
            fetch_shipments_with_retry()

        assert mock_get.call_count == 3

    @patch('extract_shipments.requests.get')
    @patch('extract_shipments.time.sleep')
    def test_http_500_triggers_retry(self, mock_sleep, mock_get):
        """API returns 500 - raise_for_status should trigger retry."""
        from requests.exceptions import HTTPError

        bad_response = MagicMock()
        bad_response.raise_for_status.side_effect = HTTPError("500 Server Error")

        good_response = MagicMock()
        good_response.status_code = 200
        good_response.json.return_value = {"data": [], "count": 0}
        good_response.raise_for_status = MagicMock()

        mock_get.side_effect = [bad_response, good_response]

        result = fetch_shipments_with_retry()
        assert result == []
        assert mock_get.call_count == 2


class TestExtractShipments:
    """Test the full extraction function."""

    @patch('extract_shipments.get_db_connection')
    @patch('extract_shipments.fetch_shipments_with_retry')
    def test_uses_parameterized_queries(self, mock_fetch, mock_db_conn):
        """Verify INSERT uses %s parameters, not f-string interpolation."""
        mock_fetch.return_value = [
            {"shipment_id": "SHP001", "customer_id": "CUST001",
             "shipping_cost": 25.50, "shipment_date": "2024-01-15",
             "status": "delivered"}
        ]

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        extract_shipments_from_api()

        # Find the INSERT call
        insert_calls = [
            call for call in mock_cursor.execute.call_args_list
            if 'INSERT INTO staging.shipments' in str(call)
        ]
        assert len(insert_calls) == 1

        # Verify it used parameterized query (second arg is the params tuple)
        sql, params = insert_calls[0][0]
        assert '%s' in sql
        assert params == ('SHP001', 'CUST001', 25.50, '2024-01-15', 'delivered')

        # Verify no f-string patterns (values directly in SQL)
        assert "SHP001" not in sql
        assert "CUST001" not in sql

    @patch('extract_shipments.get_db_connection')
    @patch('extract_shipments.fetch_shipments_with_retry')
    def test_handles_null_customer_id(self, mock_fetch, mock_db_conn):
        """Null customer_id should be passed as None parameter, not crash."""
        mock_fetch.return_value = [
            {"shipment_id": "SHP014", "customer_id": None,
             "shipping_cost": 30.00, "shipment_date": "2024-02-28",
             "status": "delivered"}
        ]

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        extract_shipments_from_api()

        insert_calls = [
            call for call in mock_cursor.execute.call_args_list
            if 'INSERT INTO staging.shipments' in str(call)
        ]
        assert len(insert_calls) == 1
        _, params = insert_calls[0][0]
        assert params[1] is None  # customer_id is None

    @patch('extract_shipments.get_db_connection')
    @patch('extract_shipments.fetch_shipments_with_retry')
    def test_truncates_before_insert(self, mock_fetch, mock_db_conn):
        """Should TRUNCATE staging table before inserting, not DROP."""
        mock_fetch.return_value = []

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        extract_shipments_from_api()

        all_sql = [str(call) for call in mock_cursor.execute.call_args_list]
        sql_text = ' '.join(all_sql)

        assert 'TRUNCATE' in sql_text
        assert 'DROP TABLE' not in sql_text

    @patch('extract_shipments.get_db_connection')
    @patch('extract_shipments.fetch_shipments_with_retry')
    def test_rollback_on_error(self, mock_fetch, mock_db_conn):
        """Database error should trigger rollback, not leave partial data."""
        mock_fetch.return_value = [
            {"shipment_id": "SHP001", "customer_id": "CUST001",
             "shipping_cost": 25.50, "shipment_date": "2024-01-15",
             "status": "delivered"}
        ]

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = [None, None, Exception("DB error")]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        with pytest.raises(Exception):
            extract_shipments_from_api()

        mock_conn.rollback.assert_called_once()
