import requests
import json
import pandas as pd
import os
from datetime import datetime
import time
import hmac
import hashlib

# --- Configuration ---
API_KEY = os.environ.get("POLYMARKET_API_KEY", None)
SECRET = os.environ.get("POLYMARKET_SECRET", None)
PASSPHRASE = os.environ.get("POLYMARKET_PASSPHRASE", None)

# CLOB REST endpoint (정식 가격/오더북 제공)
CLOB_URL = "https://clob.polymarket.com/markets"

HISTORY_FILE = "data_history.json"
OUTPUT_FILE = "graph_data.json"
MIN_CORRELATION = 0.5


# ---------------------------
# Load History
# ---------------------------
def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}


# ---------------------------
# Signature 생성 함수 (CLOB 전용)
# ---------------------------
def clob_headers(method, endpoint, body=""):
    timestamp = str(int(time.time() * 1000))
    prehash = timestamp + method + endpoint + body

    signature = hmac.new(
        SECRET.encode(),
        prehash.encode(),
        hashlib.sha256
    ).hexdigest()

    return {
        "POLY-API-KEY": API_KEY,
        "POLY-PASSPHRASE": PASSPHRASE,
        "POLY-SIGNATURE": signature,
        "POLY-TIMESTAMP": timestamp,
    }


# ---------------------------
# Fetch Prices (CLOB)
# ---------------------------
def fetch_current_prices():
    print("Fetching market data from CLOB API...")

    try:
        method = "GET"
        endpoint = "/markets"

        headers = clob_headers(method, endpoint)

        response = requests.get(CLOB_URL, headers=headers)
        response.raise_for_status()

        markets = response.json()
        print(f"DEBUG: Received {len(markets)} markets from CLOB")

        snapshot = {}
        now = datetime.now().isoformat()

        for m in markets:
            m_id = m.get("id")
            title = m.get("question", "No Title")

            tokens = m.get("tokens", [])
            if not tokens:
                continue

            # 가격은 bid → ask → price 순으로 사용
            price = (
                tokens[0].get("bestBid")
                or tokens[0].get("bestAsk")
                or tokens[0].get("price")
            )

            if price is None:
                continue

            try:
                price = float(price)
            except:
                continue

            snapshot[m_id] = {
                "title": title,
                "price": price,
                "timestamp": now
            }

        print(f"DEBUG: Snapshot contains {len(snapshot)} markets.")
        return snapshot

    except Exception as e:
        print("❌ Error:", e)
        return {}


# ---------------------------
# Update History
# ---------------------------
def update_history(history, snapshot):
    for m_id, data in snapshot.items():
        if m_id not in history:
            history[m_id] = {"title": data["title"], "prices": []}

        history[m_id]["prices"].append({
            "t": data["timestamp"],
            "p": data["price"]
        })

        if len(history[m_id]["prices"]) > 336:
            history[m_id]["prices"] = history[m_id]["prices"][-336:]

    return history


# ---------------------------
# Correlation
# ---------------------------
def calculate_correlation(history):
    print("Calculating correlations...")
    data_dict = {}

    for m_id, content in history.items():
        if len(content["prices"]) < 5:
            continue

        prices = [entry["p"] for entry in content["prices"]]
        data_dict[m_id] = pd.Series(prices)

    if not data_dict:
        return [], []

    df = pd.DataFrame(data_dict)
    df = df.fillna(method='ffill').fillna(method='bfill')
    df = df.dropna(axis=1, how='all')

    if df.empty:
        return [], []

    corr_matrix = df.corr()

    nodes = []
    links = []

    for m_id in corr_matrix.columns:
        nodes.append({
            "id": m_id,
            "label": history[m_id]["title"],
            "val": 1
        })

    cols = corr_matrix.columns
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            col1 = cols[i]
            col2 = cols[j]
            val = corr_matrix.iloc[i, j]

            if pd.isna(val):
                continue

            if abs(val) >= MIN_CORRELATION:
                links.append({
                    "source": col1,
                    "target": col2,
                    "value": round(val, 2)
                })

    return nodes, links


# ---------------------------
# Main
# ---------------------------
def main():
    history = load_history()
    snapshot = fetch_current_prices()

    if snapshot:
        updated_history = update_history(history, snapshot)

        with open(HISTORY_FILE, "w") as f:
            json.dump(updated_history, f, indent=4)

        nodes, links = calculate_correlation(updated_history)

        graph_data = {
            "nodes": nodes,
            "links": links,
            "last_updated": datetime.now().isoformat()
        }

        with open(OUTPUT_FILE, "w") as f:
            json.dump(graph_data, f, indent=4)

        print(f"✅ Update Complete. Nodes: {len(nodes)}, Links: {len(links)}")
    else:
        print("⚠️ No data fetched.")


if __name__ == "__main__":
    main()
