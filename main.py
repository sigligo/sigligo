import requests
import json
import pandas as pd
import os
from datetime import datetime

# --- Configuration ---
# GitHub Secret에서 API 키를 환경 변수로 가져옵니다.
API_KEY = os.environ.get("POLYMARKET_API_KEY", None)
SECRET = os.environ.get("POLYMARKET_SECRET", None) 
PASSPHRASE = os.environ.get("POLYMARKET_PASSPHRASE", None)

# [수정됨] DNS 오류로 인해 v2 주소 대신, 접속 가능했던 gamma-api 주소로 복구합니다.
API_URL = "https://gamma-api.polymarket.com/markets?closed=false"

HISTORY_FILE = "data_history.json"
OUTPUT_FILE = "graph_data.json"
MIN_CORRELATION = 0.5  # Connection threshold (0.5 ~ 0.7 recommended)

def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}

def fetch_current_prices():
    print("Fetching data from Polymarket...")
    try:
        # API 키를 HTTP 헤더에 담아 전송합니다.
        headers = {}
        if API_KEY:
            # 현재 'X-API-KEY'로 인증을 시도합니다.
            headers = {"X-API-KEY": API_KEY} 
            
        # API 호출 시 헤더를 포함합니다.
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        
        # 최신 API는 markets가 list 안에 바로 들어있습니다.
        markets = response.json() 

        print(f"DEBUG: API returned {len(markets)} total markets.")
        
        data_snapshot = {}
        current_time = datetime.now().isoformat()
        
        for market in markets:
            
            m_id = market.get("id")
            question = market.get("question", f"Market ID: {m_id}")
            
            # 가격을 가져오지 못하면 기본값 0.5로 설정
            price = 0.5 
            
            tokens = market.get("tokens", [])
            
            # 가격 정보가 있다면 가져오고, 유효하지 않으면 기본값 0.5 유지 (혹시 모를 오류 방지)
            if tokens and tokens[0].get("price"):
                try:
                    price = float(tokens[0].get("price"))
                except (ValueError, TypeError):
                    # 가격 변환 오류가 발생하면 기본값 0.5를 사용하거나,
                    # 이 시장이 이상하다고 판단되면 건너뜁니다.
                    # 여기서는 안전하게 건너뛰지 않고 0.5로 설정하여 일단 저장합니다.
                    pass
            
            # 거래량 필터를 제거하여 20개 데이터를 모두 저장합니다.
            data_snapshot[m_id] = {
                "title": question,
                "price": price,
                "timestamp": current_time
            }

        print(f"DEBUG: Processed {len(data_snapshot)} markets into snapshot.")
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
    # Fill missing values to avoid errors (Forward fill then Backward fill)
    df = df.fillna(method='ffill').fillna(method='bfill')
    
    # NaN 값이 남는 경우 (시계열 전체가 NaN인 경우)를 대비해 안전하게 처리
    df = df.dropna(axis=1, how='all')

    # 데이터프레임에 유효한 열이 없으면 상관관계 계산을 건너뜕니다.
    if df.empty:
        return [], []

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
            # NaN (Not a Number) 값이 나오면 건너뜕니다.
            if pd.isna(val):
                continue

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
            json.dump(updated_history, f, indent=4) # 가독성을 위해 indent 추가
            
        # Generate Graph Data
        nodes, links = calculate_correlation(updated_history)
        
        graph_data = {
            "nodes": nodes,
            "links": links,
            "last_updated": datetime.now().isoformat()
        }
        
        with open(OUTPUT_FILE, "w") as f:
            json.dump(graph_data, f, indent=4) # 가독성을 위해 indent 추가
            
        print(f"✅ Update Complete. Nodes: {len(nodes)}, Links: {len(links)}")
    else:
        print("⚠️ No data fetched.")

if __name__ == "__main__":
    main()
