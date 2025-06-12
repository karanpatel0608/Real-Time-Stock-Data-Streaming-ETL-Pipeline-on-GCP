import json
import random
import time
import uuid
from google.cloud import pubsub_v1

# --- GCP Configuration ---
PROJECT_ID = "orbital-bee-462505-v9"  # CHANGE THIS
TOPIC_ID = "stock_data"      # The same topic as before

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# --- Expanded Mock Data Universe ---
stocks = {
    "INFY":       {"price": 1600.00, "volatility": 0.02, "sector": "IT"},
    "TCS":        {"price": 3850.00, "volatility": 0.015, "sector": "IT"},
    "HDFCBANK":   {"price": 1500.00, "volatility": 0.025, "sector": "Banking"},
    "ICICIBANK":  {"price": 1100.00, "volatility": 0.03, "sector": "Banking"},
    "RELIANCE":   {"price": 2950.00, "volatility": 0.02, "sector": "Conglomerate"},
    "TATAMOTORS": {"price": 980.00,  "volatility": 0.04, "sector": "Auto"},
    "HINDUNILVR": {"price": 2350.00, "volatility": 0.01, "sector": "FMCG"},
    "BHARTIARTL": {"price": 1400.00, "volatility": 0.028, "sector": "Telecom"}
}
client_ids = [f"C{1000 + i}" for i in range(20)]
client_locations = ["Mumbai", "Delhi", "Bengaluru", "Chennai", "Kolkata", "Hyderabad", "Pune"]
broker_ids = [101, 102, 103, 104, 105]
market_sessions = ["REGULAR", "REGULAR", "REGULAR", "PRE-MARKET", "POST-MARKET"] # Weighted towards regular

def generate_messy_trade():
    """Generates a single, richly detailed, and potentially messy stock trade record."""
    
    symbol = random.choice(list(stocks.keys()))
    stock_details = stocks[symbol]
    
    change_percent = random.normalvariate(0, stock_details['volatility'])
    new_price = stock_details['price'] * (1 + change_percent)
    stock_details['price'] = max(new_price, 0)

    record = {
        # Core Trade Info
        "trade_id": str(uuid.uuid4()),
        "ticker": symbol,
        "price": round(stock_details['price'], 2),
        "trade_volume": random.randint(10, 5000),
        "trade_type": random.choice(["BUY", "SELL"]),
        "order_type": random.choice(["MARKET", "LIMIT"]),
        
        # Client Info
        "client_id": random.choice(client_ids),
        "client_category": random.choice(["RETAIL", "INSTITUTIONAL"]),
        "client_location": random.choice(client_locations),

        # Execution Info
        "broker_id": random.choice(broker_ids),
        "execution_latency_ms": max(0, int(random.normalvariate(50, 15))), # Latency in ms
        "trade_status": "SUCCEED",

        # Contextual Info
        "market_session": random.choice(market_sessions),
        "sector": stock_details["sector"],
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%S.00Z', time.gmtime())
    }

    # --- INTRODUCE EVEN MORE DATA QUALITY ISSUES ---
    mess_type = random.randint(1, 20)

    if mess_type == 1: record["price"] = None
    elif mess_type == 2: record["price"] = f"Rs. {record.get('price', 0):.2f}"
    elif mess_type == 3: record["ticker"] = symbol.lower()
    elif mess_type == 4: del record["ticker"]
    elif mess_type == 6: record["trade_volume"] = random.choice([0, None, -100])
    elif mess_type == 7: record["trade_type"] = "Buy" # Inconsistent case
    elif mess_type == 8: del record["order_type"]
    elif mess_type == 9: record["broker_id"] = f"BRK-{record['broker_id']}"
    # --- New Messiness ---
    elif mess_type == 10: # Flag a failed trade
        record["trade_status"] = "FAIL"
        record["price"] = 0 # Failed trades often have no price
        record["trade_volume"] = 0
    elif mess_type == 11: # Bad latency value
        record["execution_latency_ms"] = random.choice([-50, 9999])
    elif mess_type == 12: # Inconsistent client category
        record["client_category"] = "Retail" 
    elif mess_type == 13: # Mismatch sector
        record["sector"] = "Technology" if record["sector"] == "IT" else "Services"
    elif mess_type == 14: # Missing location
        del record["client_location"]

    return json.dumps(record).encode("utf-8")

if __name__ == "__main__":
    print(f"Publishing enterprise-grade Indian stock data to {topic_path}...")
    print("Press Ctrl+C to stop.")
    while True:
        try:
            message_payload = generate_messy_trade()
            print(f"--> Sending: {message_payload.decode('utf-8')}")
            future = publisher.publish(topic_path, message_payload)
            future.result() 
            time.sleep(random.uniform(0.5, 2.0))
        except Exception as e:
            print(f"An error occurred: {e}")
