import requests
import json

BASE_URL = "http://127.0.0.1:5000"

def test_health():
    print("Testing /health endpoint...")
    response = requests.get(f"{BASE_URL}/health")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}\n")

def test_orders():
    print("Testing /orders endpoint...")
    response = requests.get(f"{BASE_URL}/orders?user_id=1")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}\n")

def test_daily_revenue():
    print("Testing /metrics/daily-revenue endpoint...")
    response = requests.get(f"{BASE_URL}/metrics/daily-revenue?from=2024-01-01&to=2024-12-31")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}\n")

def test_top_products():
    print("Testing /products/top endpoint...")
    response = requests.get(f"{BASE_URL}/products/top?days=365&limit=5")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}\n")

def test_ingest_events():
    print("Testing /ingest/events endpoint...")
    events = [
        {
            "user_id": 1,
            "event_type": "page_view",
            "product_id": 5,
            "event_timestamp": "2024-10-15T10:30:00"
        },
        {
            "user_id": 2,
            "event_type": "add_to_cart",
            "product_id": 3,
            "event_timestamp": "2024-10-15T10:35:00"
        }
    ]
    response = requests.post(f"{BASE_URL}/ingest/events", json=events)
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}\n")

if __name__ == "__main__":
    test_health()
    test_orders()
    test_daily_revenue()
    test_top_products()
    test_ingest_events()