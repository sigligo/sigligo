import requests
import json
import pandas as pd
import os
from datetime import datetime

# =========================
# CONFIGURATION
# =========================
# [수정] 공식 문서 기반의 정확한 CLOB API 엔드포인트
CLOB_URL = "https://clob.polymarket.com/markets"

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
        # [핵심] next_cursor를 사용하여 여러 페이지의 데이터를 가져옵니다.
        # 일단 최대 500개(100개씩 5번)까지만 가져오도록 설정하여 속도와 안정성을 잡습니다.
        all_markets = []
        next_cursor = ""
        
        for _ in range(5): # 최대 5페이지 (약 500개 시장)
            params = {
                "limit": 100,
                "active": "true", # 활성 시장만 요청
                "closed": "false"
            }
            if next_cursor:
                params["next_cursor"] = next_cursor

            response = requests.get(CLOB_URL, params=params)
            response.raise_for_status()

            data = response.json()
            
            # 응답 구조 확인 (data가 리스트인지, 딕셔너리인지)
            # CLOB API는 보통 {"data": [...], "next_cursor": "..."} 또는 리스트 반환
            # 여기서는 리스트라고 가정하거나 cursor를 확인합니다.
            
            if isinstance(data, list):
                # 리스트로 바로 반환되는 경우 (단일 페이지일 가능성 높음)
                all_markets.extend(data)
                break
            elif isinstance(data, dict):
                # data 키 안에 시장 목록이 있는 경우
                markets_batch = data.get("data", [])
                all_markets.extend(markets_batch)
                next_cursor = data.get("next_cursor")
                
                if not next_cursor or next_cursor == "rte": # rte는 끝을 의미할 수 있음
                    break
            else:
                break

        print(f"DEBUG: Received {len(all_markets)} raw markets from CLOB")

        data_snapshot = {}
        current_time = datetime.now().isoformat()

        for market in all_markets:
            if not isinstance(market, dict):
                continue

            m_id = market.get("condition_id") # CLOB에서는 condition_id를 ID로 많이 씁니다. 없으면 id 사용
            if not m_id:
                m_id = market.get("id")

            title = market.get("question", f"Market {m_id}")
            
            # 가격 산출 로직 (Bid/Ask 중간값)
            # CLOB API 응답 구조에 따라 rewards/tokens 등의 위치가 다를 수 있으나,
            # 기본적으로 제공된 코드의 로직을 따릅니다.
            tokens = market.get("tokens", [])
            price = None
            
            # 1. tokens 구조에서 가격 찾기
            if tokens and isinstance(tokens, list):
                # bestBid/bestAsk가 tokens 안에 있는 경우
                best_bid = tokens[0].get("bestBid") if len(tokens) > 0 else None
                best_ask = tokens[0].get("bestAsk") if len(tokens) > 0 else None
                
                # 혹은 market root 레벨에 있을 수도 있음 (API 버전에 따라 다름)
                if not best_bid:
                    best_bid = market.get("best_bid")
                if not best_ask:
                    best_ask = market.get("best_ask")

                if best_bid and best_ask:
                     try:
                        price = (float(best_bid) + float(best_ask)) / 2
                     except:
                        pass
            
            # 2. 만약 위에서 못 구했으면 last_trade_price 확인
            if price is None:
                last_price = market.get("last_trade_price")
                if last_price:
                    try:
                        price = float(last_price)
                    except:
                        pass

            # 가격을 여전히 못 구했으면 건너뜀
            if price is None:
                continue

            # 거래량 필터 (비활성 시장 제외)
            # volume, 24h_volume 등 다양한 키를 확인
            volume = 0
            for key in ["volume", "volume_24h", "24hVolume"]:
                if market.get(key):
                    try:
                        volume = float(market.get(key))
                        break
                    except:
                        continue
            
            if volume <= 0:
                continue

            data_snapshot[m_id] = {
                "title": title,
                "price": price,
                "timestamp": current_time
            }

        print(f"DEBUG: Snapshot processed {len(data_snapshot)} valid markets")
        return data_snapshot

    except Exception as e:
        print(f"❌ Error while fetching CLOB data: {e}")
        # 오류가 나도 빈 딕셔너리 리턴하여 봇이 죽지 않게 함
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

        # Keep only last 336 data points
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
            json.dump(updated, f, indent=4)

        # Build graph data
        nodes, links = calculate_correlation(updated)
        graph_data = {
            "nodes": nodes,
            "links": links,
            "last_updated": datetime.now().isoformat()
        }

        with open(OUTPUT_FILE, "w") as f:
            json.dump(graph_data, f, indent=4)

        print(f"✅ Update Complete — Nodes: {len(nodes)}, Links: {len(links)}")

    else:
        print("⚠️ No data fetched.")


if __name__ == "__main__":
    main()
