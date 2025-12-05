import requests
import json
import pandas as pd
import os
from datetime import datetime

# =========================
# CONFIGURATION
# =========================
CLOB_URL = "https://clob.polymarket.com/clob/markets"

HISTORY_FILE = "data_history.json"
OUTPUT_FILE = "graph_data.json"
MIN_CORRELATION = 0.5


# =========================
# LOAD HISTORY
# =========================
def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}


# =========================
# FETCH CURRENT PRICES
# =========================
def fetch_current_prices():
    print("Fetching market data from CLOB API...")

    try:
        params = {"limit": 200}
        response = requests.get(CLOB_URL, params=params)
        response.raise_for_status()

        raw = response.json()
        markets = raw.get("markets", [])

        print(f"DEBUG: Received {len(markets)} markets from CLOB")

        data_snapshot = {}
        current_time = datetime.now().isoformat()

        for market in markets:
            if not isinstance(market, dict):
                continue

            m_id = market.get("id")
            title = market.get("question", f"Market {m_id}")

            outcomes = market.get("outcomes", [])
            if not outcomes:
                continue

            # CLOB uses bestBid / bestAsk instead of "price"
            try:
                best_bid = outcomes[0].get("bestBid")
                best_ask = outcomes[0].get("bestAsk")

                if best_bid is None or best_ask is None:
                    continue

                price = (float(best_bid) + float(best_ask)) / 2
            except:
                continue

            # volume field handling
            volume = market.get("volume") or market.get("24hVolume") or 0
            try:
                if float(volume) <= 0:
                    continue
            except:
                continue

            data_snapshot[m_id] = {
                "title": title,
                "price": price,
                "timestamp": current_time
            }

        print(f"DEBUG: Snapshot processed {len(data_snapshot)} markets")
        return data_snapshot

    except Exception as e:
        print(f"❌ Error while fetching CLOB data: {e}")
        return {}


# =========================
# UPDATE HISTORY
# =========================
def update_history(history, snapshot):
    for m_id, data in snapshot.items():
        if m_id not in history:
            history[m_id] = {"title": data["title"], "prices": []}

        history[m_id]["prices"].append({
            "t": data["timestamp"],
            "p": data["price"]
        })

        # Keep only last 336 data points (7 days if 30 min interval)
        history[m_id]["prices"] = history[m_id]["prices"][-336:]

    return history


# =========================
# CORRELATION
# =========================
def calculate_correlation(history):
    print("Calculating correlations...")

    data_dict = {}

    # Convert to Pandas series
    for m_id, content in history.items():
        if len(content["prices"]) < 5:
            continue
        prices = [entry["p"] for entry in content["prices"]]
        data_dict[m_id] = pd.Series(prices)

    if not data_dict:
        return [], []

    df = pd.DataFrame(data_dict)

    # Fill missing data safely
    df = df.fillna(method='ffill').fillna(method='bfill')
    df = df.dropna(axis=1, how='all')

    if df.empty:
        return [], []

    corr = df.corr()

    nodes = []
    links = []

    # Build nodes
    for m_id in corr.columns:
        nodes.append({
            "id": m_id,
            "label": history[m_id]["title"],
            "val": 1
        })

    # Build edges
    cols = corr.columns
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            a = cols[i]
            b = cols[j]
            val = corr.iloc[i, j]

            if pd.isna(val):
                continue

            if abs(val) >= MIN_CORRELATION:
                links.append({
                    "source": a,
                    "target": b,
                    "value": round(val, 2)
                })

    return nodes, links


# =========================
# MAIN
# =========================
def main():
    history = load_history()
    snapshot = fetch_current_prices()

    if snapshot:
        updated = update_history(history, snapshot)

        # Save history
        with open(HISTORY_FILE, "w") as f:
            json.dump(updated, f)

        # Build graph data
        nodes, links = calculate_correlation(updated)
        graph_data = {
            "nodes": nodes,
            "links": links,
            "last_updated": datetime.now().isoformat()
        }

        with open(OUTPUT_FILE, "w") as f:
            json.dump(graph_data, f)

        print(f"✅ Update Complete — Nodes: {len(nodes)}, Links: {len(links)}")

    else:
        print("⚠️ No data fetched.")


if __name__ == "__main__":
    main()
