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

# 인증을 가정하고 가장 안정적인 공용 URL로 다시 시도합니다.
# 이 주소는 API 키가 없으면 404가 발생하거나 데이터를 반환하지 않을 수 있습니다.
API_URL = "https://api.polymarket.com/markets?closed=false"

HISTORY_FILE = "data_history.json"
OUTPUT_FILE = "graph_data.json"
MIN_CORRELATION = 0.5  # Connection threshold (0.5 ~ 0.7 recommended)

def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            # 파일이 비어있는 경우를 대비한 안전 장치
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}

def fetch_current_prices():
    print("Fetching data from Polymarket...")
    try:
        # [수정] API 키를 HTTP 헤더에 담아 전송합니다.
        headers = {}
        if API_KEY:
            # PolyMarket이 API Key를 'X-API-KEY' 헤더로 요구한다고 가정합니다.
            # (만약 에러가 나면 이 헤더 이름은 API 문서에 맞게 수정해야 합니다.)
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
            # 1. Active 시장만 필터링
            if market.get('market_type') != 'basic':
                continue
                
            m_id = market.get("id")
            question = market.get("question")
            
            # 2. 가격 정보는 'tokens' 키 안에 있습니다.
            tokens = market.get("tokens", [])
            
            # Use the first token price (usually 'Yes')
            if tokens:
                price_str = tokens[0].get("price", "0")
                price = float(price_str)
                
                # 3. 추가 필터링: 거래량이 0이 아닌 시장만 포함 (잡코인 제외)
                if float(market.get('volume', 0)) > 0:
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
            # NaN (Not a Number) 값이 나오면 건너뜁니다.
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
