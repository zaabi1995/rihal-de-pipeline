"""
Mock Shipment API
Simulates an external shipment tracking system
"""
from flask import Flask, jsonify, request
from datetime import datetime, timedelta
import random

app = Flask(__name__)

# Static mock data
SHIPMENTS = [
    # Normal shipments
    {"shipment_id": "SHP001", "customer_id": "CUST001", "shipping_cost": 25.50, "shipment_date": "2024-01-15", "status": "delivered"},
    {"shipment_id": "SHP002", "customer_id": "CUST002", "shipping_cost": 45.00, "shipment_date": "2024-01-16", "status": "delivered"},
    {"shipment_id": "SHP003", "customer_id": "CUST003", "shipping_cost": 15.75, "shipment_date": "2024-01-20", "status": "delivered"},
    {"shipment_id": "SHP004", "customer_id": "CUST001", "shipping_cost": 30.00, "shipment_date": "2024-01-25", "status": "delivered"},
    {"shipment_id": "SHP005", "customer_id": "CUST004", "shipping_cost": 55.25, "shipment_date": "2024-02-01", "status": "delivered"},
    
    {"shipment_id": "SHP002", "customer_id": "CUST002", "shipping_cost": 47.00, "shipment_date": "2024-01-16", "status": "delivered"},
    
    # February shipments
    {"shipment_id": "SHP006", "customer_id": "CUST002", "shipping_cost": 35.50, "shipment_date": "2024-02-05", "status": "delivered"},
    {"shipment_id": "SHP007", "customer_id": "CUST005", "shipping_cost": 20.00, "shipment_date": "2024-02-10", "status": "in_transit"},
    {"shipment_id": "SHP008", "customer_id": "CUST001", "shipping_cost": 28.75, "shipment_date": "2024-02-12", "status": "delivered"},
    {"shipment_id": "SHP009", "customer_id": "CUST003", "shipping_cost": 42.00, "shipment_date": "2024-02-15", "status": "delivered"},
    {"shipment_id": "SHP010", "customer_id": "CUST006", "shipping_cost": 65.00, "shipment_date": "2024-02-18", "status": "delivered"},
    
    {"shipment_id": "SHP011", "customer_id": "CUST999", "shipping_cost": 18.50, "shipment_date": "2024-02-20", "status": "delivered"},
    {"shipment_id": "SHP012", "customer_id": "CUST002", "shipping_cost": -5.00, "shipment_date": "2024-02-22", "status": "delivered"},
    {"shipment_id": "SHP013", "customer_id": "CUST004", "shipping_cost": 0.00, "shipment_date": "2024-02-25", "status": "delivered"},
    {"shipment_id": "SHP014", "customer_id": None, "shipping_cost": 30.00, "shipment_date": "2024-02-28", "status": "delivered"},
    
    # March shipments
    {"shipment_id": "SHP015", "customer_id": "CUST001", "shipping_cost": 22.50, "shipment_date": "2024-03-01", "status": "delivered"},
    {"shipment_id": "SHP016", "customer_id": "CUST003", "shipping_cost": 38.00, "shipment_date": "2024-03-05", "status": "delivered"},
    {"shipment_id": "SHP017", "customer_id": "CUST005", "shipping_cost": 50.00, "shipment_date": "2024-03-10", "status": "cancelled"},
    {"shipment_id": "SHP018", "customer_id": "CUST002", "shipping_cost": 33.75, "shipment_date": "2024-03-15", "status": "delivered"},
    {"shipment_id": "SHP019", "customer_id": "CUST006", "shipping_cost": 44.50, "shipment_date": "2024-03-20", "status": "delivered"},
    {"shipment_id": "SHP020", "customer_id": "CUST004", "shipping_cost": 27.00, "shipment_date": "2024-03-25", "status": "delivered"},
]

# Counter for request tracking
request_counter = 0

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200

@app.route('/api/shipments', methods=['GET'])
def get_shipments():
    """
    Get shipment data
    Query params:
    - start_date: filter shipments from this date (YYYY-MM-DD)
    - end_date: filter shipments until this date (YYYY-MM-DD)
    """
    global request_counter
    request_counter += 1
    
    # Simulate occasional API errors
    if request_counter % 10 == 0:
        return jsonify({"error": "Internal server error", "message": "Database connection timeout"}), 500
    
    # Simulate network latency
    if request_counter % 7 == 0:
        import time
        time.sleep(5)
    
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    filtered_shipments = SHIPMENTS.copy()
    
    # Filter by date if provided
    if start_date:
        filtered_shipments = [s for s in filtered_shipments if s['shipment_date'] >= start_date]
    if end_date:
        filtered_shipments = [s for s in filtered_shipments if s['shipment_date'] <= end_date]
    
    return jsonify({
        "data": filtered_shipments,
        "count": len(filtered_shipments),
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route('/api/shipments/<shipment_id>', methods=['GET'])
def get_shipment(shipment_id):
    """Get a single shipment by ID"""
    shipment = next((s for s in SHIPMENTS if s['shipment_id'] == shipment_id), None)
    if shipment:
        return jsonify(shipment), 200
    return jsonify({"error": "Shipment not found"}), 404

if __name__ == '__main__':
    print("Starting Mock Shipment API on port 8000...")
    app.run(host='0.0.0.0', port=8000, debug=True)