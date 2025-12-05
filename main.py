import requests
import json
import pandas as pd
import os
from datetime import datetime

# --- Configuration ---
# Fetch top 50 markets by volume (High relevance)
API_URL = "https://api.polymarket.com/markets?limit=50&order=volume:desc&closed=false"
HISTORY_FILE = "data_history.json"
OUTPUT_FILE = "graph_data.json"
MIN_CORRELATION = 0.5  # Connection threshold (0.5 ~ 0.7 recommended)

def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            return json.load(f)
    return {}

def fetch_current_prices():
    print("Fetching data from Polymarket...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        markets = response.json()
        
        data_snapshot = {}
        current_time = datetime.now().isoformat()
        
        for market in markets:
            m_id = market.get("id")
            question = market.get("question")
            tokens = market.get("tokens", [])
            
            # Use the first token price (usually 'Yes')
            if tokens:
                price = float(tokens[0].get("price", 0))
                data_snapshot[m_id] = {
                    "title": question,
                    "price": price,
                    "timestamp": current_time
                }
        return data_snapshot
    except Exception as e:
        print(f"Error fetching data: {e}")
        return {}

def update_history(history, snapshot):
    for m_id, data in snapshot.items():
        if m_id not in history:
            history[m_id] = {"title": data["title"], "prices": []}
        
        history[m_id]["prices"].append({
            "t": data["timestamp"],
            "p": data["price"]
        })
        
        # Keep last 336 data points (approx. 7 days if run every 30 mins)
        if len(history[m_id]["prices"]) > 336: 
            history[m_id]["prices"] = history[m_id]["prices"][-336:]
            
    return history

def calculate_correlation(history):
    print("Calculating correlations...")
    data_dict = {}
    
    # Create Pandas Series for each market
    for m_id, content in history.items():
        if len(content["prices"]) < 5: # Need minimal data to correlate
            continue
        prices = [entry["p"] for entry in content["prices"]]
        data_dict[m_id] = pd.Series(prices)

    if not data_dict:
        return [], []

    df = pd.DataFrame(data_dict)
    # Fill missing values to avoid errors
    df = df.fillna(method='ffill').fillna(method='bfill')
    
    corr_matrix = df.corr()
    
    nodes = []
    links = []
    
    # 1. Create Nodes
    for m_id in corr_matrix.columns:
        nodes.append({
            "id": m_id,
            "label": history[m_id]["title"], 
            "val": 1 
        })
        
    # 2. Create Links (Edges)
    cols = corr_matrix.columns
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            col1 = cols[i]
            col2 = cols[j]
            val = corr_matrix.iloc[i, j]
            
            # Correlation Logic: Positive or Negative strong correlation
            if abs(val) >= MIN_CORRELATION:
                links.append({
                    "source": col1,
                    "target": col2,
                    "value": round(val, 2)
                })
                
    return nodes, links

def main():
    history = load_history()
    snapshot = fetch_current_prices()
    
    if snapshot:
        updated_history = update_history(history, snapshot)
        
        # Save History
        with open(HISTORY_FILE, "w") as f:
            json.dump(updated_history, f)
            
        # Generate Graph Data
        nodes, links = calculate_correlation(updated_history)
        
        graph_data = {
            "nodes": nodes,
            "links": links,
            "last_updated": datetime.now().isoformat()
        }
        
        with open(OUTPUT_FILE, "w") as f:
            json.dump(graph_data, f)
            
        print(f"✅ Update Complete. Nodes: {len(nodes)}, Links: {len(links)}")
    else:
        print("⚠️ No data fetched.")

if __name__ == "__main__":
    main()
