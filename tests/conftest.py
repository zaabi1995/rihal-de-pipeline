"""
Test fixtures and shared utilities for pipeline tests.
Uses an in-memory approach — mocks psycopg2 and requests so tests
run without Docker or a real database.
"""
import sys
import os
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

# Make scripts importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))


class MockCursor:
    """
    A mock database cursor that tracks executed SQL and simulates
    basic database operations for testing.
    """
    def __init__(self):
        self.executed = []
        self.rowcount = 0
        self._fetchone_results = []
        self._fetchall_results = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        # Count INSERT statements for rowcount
        if sql.strip().upper().startswith('INSERT'):
            self.rowcount += 1

    def fetchone(self):
        if self._fetchone_results:
            return self._fetchone_results.pop(0)
        return (0,)

    def fetchall(self):
        if self._fetchall_results:
            return self._fetchall_results.pop(0)
        return []

    def close(self):
        pass

    def set_fetchone_results(self, results):
        """Queue up results for fetchone() calls."""
        self._fetchone_results = list(results)


class MockConnection:
    """Mock database connection."""
    def __init__(self):
        self.cursor_obj = MockCursor()
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


@pytest.fixture
def mock_db():
    """Provide a mock database connection and cursor."""
    conn = MockConnection()
    return conn


@pytest.fixture
def sample_shipments():
    """Sample API response data with various edge cases."""
    return [
        {"shipment_id": "SHP001", "customer_id": "CUST001", "shipping_cost": 25.50, "shipment_date": "2024-01-15", "status": "delivered"},
        {"shipment_id": "SHP002", "customer_id": "CUST002", "shipping_cost": 45.00, "shipment_date": "2024-01-16", "status": "delivered"},
        # Duplicate SHP002 with different cost
        {"shipment_id": "SHP002", "customer_id": "CUST002", "shipping_cost": 47.00, "shipment_date": "2024-01-16", "status": "delivered"},
        # Negative cost
        {"shipment_id": "SHP012", "customer_id": "CUST002", "shipping_cost": -5.00, "shipment_date": "2024-02-22", "status": "delivered"},
        # Zero cost
        {"shipment_id": "SHP013", "customer_id": "CUST004", "shipping_cost": 0.00, "shipment_date": "2024-02-25", "status": "delivered"},
        # Null customer
        {"shipment_id": "SHP014", "customer_id": None, "shipping_cost": 30.00, "shipment_date": "2024-02-28", "status": "delivered"},
        # Cancelled
        {"shipment_id": "SHP017", "customer_id": "CUST005", "shipping_cost": 50.00, "shipment_date": "2024-03-10", "status": "cancelled"},
        # Unknown customer
        {"shipment_id": "SHP011", "customer_id": "CUST999", "shipping_cost": 18.50, "shipment_date": "2024-02-20", "status": "delivered"},
    ]


@pytest.fixture
def sample_api_response(sample_shipments):
    """Mock API JSON response."""
    return {
        "data": sample_shipments,
        "count": len(sample_shipments),
        "timestamp": datetime.now().isoformat()
    }
