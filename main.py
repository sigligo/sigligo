import requests
import json
import pandas as pd
import os
from datetime import datetime

# =========================
# CONFIGURATION
# =========================
API_URL = "https://prod.api.polymarket.com/markets"

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
            except:
                return {}
    return {}


# =========================
# FETCH DATA
# =========================
def fetch_current_prices():
    print("Fetching market data from Polymarket Data API...")

    try:
        response = requests.get(API_URL)
        response.raise_for_status()

        markets = response.json()
        print(f"DEBUG: Received {len(markets)} markets")

        snapshot = {}
        now = datetime.now().isoformat()

        for m in markets:
            m_id = m.get("id")
            title = m.get("question", f"Market {m_id}")

            outcomes = m.get("outcomes", [])
            if not outcomes:
                continue

            # outcomes[n].price 존재
            try:
                price = float(outcomes[0].get("price"))
            except:
                continue

            # volume 필터
            volume = m.get("volume", 0)
            if volume is None or float(volume) <= 0:
                continue

            snapshot[m_id] = {
                "title": title,
                "price": price,
                "timestamp": now
            }

        print(f"DEBUG: Snapshot processed {len(snapshot)} markets")
        return snapshot

    except Exception as e:
        print(f"❌ Error fetching data: {e}")
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

        # Keep last week data
        history[m_id]["prices"] = history[m_id]["prices"][-336:]

    return history


# =========================
# CALCULATE CORRELATION
# =========================
def calculate_correlation(history):
    print("Calculating correlations...")

    data_dict = {}
    for m_id, content in history.items():
        if len(content["prices"]) < 5:
            continue
        data_dict[m_id] = pd.Series([p["p"] for p in content["prices"]])

    if not data_dict:
        return [], []

    df = pd.DataFrame(data_dict)
    df = df.fillna(method="ffill").fillna(method="bfill")
    df = df.dropna(axis=1, how="all")

    if df.empty:
        return [], []

    corr = df.corr()

    nodes = []
    links = []

    # nodes
    for col in corr.columns:
        nodes.append({
            "id": col,
            "label": history[col]["title"],
            "val": 1
        })

    # edges
    cols = corr.columns
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            a, b = cols[i], cols[j]
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

    if not snapshot:
        print("⚠️ No data fetched.")
        return

    updated = update_history(history, snapshot)

    with open(HISTORY_FILE, "w") as f:
        json.dump(updated, f)

    nodes, links = calculate_correlation(updated)
    graph_data = {
        "nodes": nodes,
        "links": links,
        "last_updated": datetime.now().isoformat()
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(graph_data, f)

    print(f"✅ Update complete — Nodes: {len(nodes)}, Links: {len(links)}")


if __name__ == "__main__":
    main()
