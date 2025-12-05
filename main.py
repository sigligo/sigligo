# main.py — Robust Polymarket market collector & correlation builder (production-ready)
import os
import time
import json
import math
import hmac
import hashlib
import requests
import pandas as pd
from datetime import datetime

# ============= Configuration ============
# Primary (prefer) public production API (usually accessible from GH Actions)
PROD_API = "https://prod.api.polymarket.com/markets"
# Fallback CLOB endpoint (may require authentication)
CLOB_API = "https://clob.polymarket.com/markets"

HISTORY_FILE = "data_history.json"
OUTPUT_FILE = "graph_data.json"
MIN_CORRELATION = float(os.getenv("MIN_CORRELATION", 0.5))

# API credentials (for CLOB authenticated fallback)
API_KEY = os.getenv("POLYMARKET_API_KEY")
API_SECRET = os.getenv("POLYMARKET_SECRET")
API_PASSPHRASE = os.getenv("POLYMARKET_PASSPHRASE")

# Fetch settings
PAGE_SIZE = int(os.getenv("PAGE_SIZE", 200))
MAX_TOTAL = int(os.getenv("MAX_TOTAL", 800))  # max markets to collect
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
SLEEP_BETWEEN_PAGES = 0.25

# ============= Helpers ==================
def safe_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None

def sign_clob(method, path, body=""):
    """HMAC-SHA256 signature for CLOB REST API (if creds available)."""
    ts = str(int(time.time() * 1000))
    prehash = ts + method.upper() + path + (body or "")
    sig = hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).hexdigest()
    return {
        "POLY-API-KEY": API_KEY,
        "POLY-PASSPHRASE": API_PASSPHRASE,
        "POLY-SIGNATURE": sig,
        "POLY-TIMESTAMP": ts
    }

def request_with_retries(url, params=None, headers=None, timeout=REQUEST_TIMEOUT):
    """Simple retry wrapper with exponential backoff."""
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            # Try to parse JSON; if fails, raise
            return r.json()
        except Exception as e:
            attempt += 1
            backoff = 0.5 * (2 ** (attempt - 1))
            print(f"WARN: request error ({attempt}/{MAX_RETRIES}) for {url}: {e} — backing off {backoff}s")
            time.sleep(backoff)
    # final attempt without raising further (return None)
    print(f"ERROR: All retries failed for {url}")
    return None

def pick_volume(market):
    # try many possible fields
    for k in ("volume", "volume_24h", "24hVolume", "volume24h", "total_volume", "liquidity", "liquidity24h"):
        v = market.get(k)
        fv = safe_float(v)
        if fv is not None:
            return fv
    return 0.0

def extract_price_from_market(market):
    """Robust extraction of a representative 'price' (0..1) for correlations."""
    # 1) outcomes[] common pattern
    for outcomes_key in ("outcomes", "outcome", "outcomes_list", "results"):
        outcomes = market.get(outcomes_key)
        if isinstance(outcomes, list) and outcomes:
            # often outcomes[0] corresponds to 'Yes' or first outcome price
            # check many possible keys inside outcome entries
            for prefer_key in ("price", "probability", "last_price", "value", "p"):
                try:
                    v = outcomes[0].get(prefer_key)
                    fv = safe_float(v)
                    if fv is not None:
                        return fv
                except Exception:
                    continue
            # if bid/ask inside outcome
            try:
                bid = safe_float(outcomes[0].get("bestBid") or outcomes[0].get("bid"))
                ask = safe_float(outcomes[0].get("bestAsk") or outcomes[0].get("ask"))
                if bid is not None and ask is not None:
                    return (bid + ask) / 2.0
            except Exception:
                pass

    # 2) tokens list (some versions)
    tokens = market.get("tokens") or market.get("token")
    if isinstance(tokens, list) and tokens:
        t0 = tokens[0]
        if isinstance(t0, dict):
            for k in ("price", "bestBid", "bestAsk", "last_price", "last_trade_price"):
                fv = safe_float(t0.get(k))
                if fv is not None:
                    # if we got bid/ask, compute mid
                    if k in ("bestBid", "bestAsk"):
                        # attempt to combine
                        bid = safe_float(t0.get("bestBid") or t0.get("bid"))
                        ask = safe_float(t0.get("bestAsk") or t0.get("ask"))
                        if bid is not None and ask is not None:
                            return (bid + ask) / 2.0
                    return fv

    # 3) root-level keys
    for pk in ("price", "last_trade_price", "last_price", "mid_price", "market_price"):
        fv = safe_float(market.get(pk))
        if fv is not None:
            return fv

    # 4) fallback: inspect outcomes entries for any numeric field
    for k, v in market.items():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            for entry in v:
                for pk in ("price", "probability", "last_price", "value"):
                    fv = safe_float(entry.get(pk))
                    if fv is not None:
                        return fv

    return None

# ============= Data Fetching =================
def fetch_from_prod_api(max_total=MAX_TOTAL, page_size=PAGE_SIZE):
    """Fetch markets from production public API (no auth)."""
    collected = []
    offset = 0
    headers = {"User-Agent": "Mozilla/5.0"}
    while len(collected) < max_total:
        params = {"limit": page_size, "offset": offset, "closed": "false"}
        data = request_with_retries(PROD_API, params=params, headers=headers)
        if not data:
            break
        # normalise to list of market dicts
        batch = []
        if isinstance(data, list):
            batch = data
        elif isinstance(data, dict):
            # look for common container keys
            for k in ("markets", "data", "items", "results"):
                if k in data and isinstance(data[k], list):
                    batch = data[k]
                    break
            if not batch:
                # fallback: find first list-of-dict value
                for v in data.values():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        batch = v
                        break
        if not batch:
            break
        collected.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
        time.sleep(SLEEP_BETWEEN_PAGES)
    return collected[:max_total]

def fetch_from_clob_api(max_total=MAX_TOTAL, page_size=PAGE_SIZE):
    """Fetch markets from clob endpoint; use signature auth if creds supplied."""
    collected = []
    params = {"limit": page_size, "active": "true", "closed": "false"}
    next_cursor = None
    headers = {"User-Agent": "Mozilla/5.0"}
    # If credentials present, prepare signed headers per request
    use_auth = API_KEY and API_SECRET and API_PASSPHRASE
    pages = 0
    while len(collected) < max_total and pages < math.ceil(max_total / page_size):
        # apply cursor if provided
        req_params = dict(params)
        if next_cursor:
            req_params["next_cursor"] = next_cursor
        # sign if necessary
        req_headers = dict(headers)
        if use_auth:
            # path is likely "/markets" for clob; body empty for GET
            req_headers.update(sign_clob("GET", "/markets", ""))
        data = request_with_retries(CLOB_API, params=req_params, headers=req_headers)
        if not data:
            break
        # data often is {"data": [...], "next_cursor": "..."}
        batch = []
        if isinstance(data, dict):
            batch = data.get("data") or data.get("markets") or []
            next_cursor = data.get("next_cursor")
        elif isinstance(data, list):
            batch = data
            next_cursor = None
        else:
            break
        if not batch:
            break
        collected.extend(batch)
        pages += 1
        if not next_cursor:
            if len(batch) < page_size:
                break
        time.sleep(SLEEP_BETWEEN_PAGES)
    return collected[:max_total]

def fetch_current_prices(max_total=MAX_TOTAL, page_size=PAGE_SIZE):
    """Top-level: try prod public API first, fallback to clob (with auth if available)."""
    print("Fetching market data (prod API preferred) ...")
    markets = fetch_from_prod_api(max_total=max_total, page_size=page_size)
    source = "prod.api"
    if not markets:
        print("WARN: prod API returned no markets or failed; trying CLOB API (auth if available).")
        markets = fetch_from_clob_api(max_total=max_total, page_size=page_size)
        source = "clob.api"
    print(f"DEBUG: Received {len(markets)} raw markets from {source}")

    snapshot = {}
    now = datetime.now().isoformat()
    for m in markets:
        if not isinstance(m, dict):
            continue
        # robust id extraction
        m_id = m.get("id") or m.get("condition_id") or m.get("market_id") or m.get("uuid")
        if not m_id:
            continue
        # title extraction
        title = m.get("question") or m.get("title") or m.get("name") or f"Market {m_id}"
        # price
        price = extract_price_from_market(m)
        # ensure price is fraction (0..1) typically for prediction markets
        if price is None:
            continue
        # normalize price: sometimes API returns 0..100 percentages
        if price > 1.0 and price <= 100.0:
            price = price / 100.0
        # sanity check
        if not (0.0 < price < 1.0):
            continue
        # volume
        vol = pick_volume(m)
        if vol is None:
            vol = 0.0
        if vol <= 0:
            continue

        snapshot[str(m_id)] = {
            "title": title,
            "price": price,
            "timestamp": now,
            "volume": vol
        }
    print(f"DEBUG: Total snapshot markets collected: {len(snapshot)}")
    return snapshot

# ============= History & Correlation ==============
def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def update_history(history, snapshot):
    for m_id, data in snapshot.items():
        if m_id not in history:
            history[m_id] = {"title": data["title"], "prices": []}
        history[m_id]["prices"].append({"t": data["timestamp"], "p": data["price"]})
        # keep only last 336 points
        history[m_id]["prices"] = history[m_id]["prices"][-336:]
    return history

def calculate_correlation(history):
    print("Calculating correlations...")
    data_dict = {}
    for m_id, content in history.items():
        if len(content.get("prices", [])) < 5:
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
    for col in corr.columns:
        nodes.append({"id": col, "label": history[col]["title"], "val": 1})
    cols = corr.columns
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            a, b = cols[i], cols[j]
            val = corr.iloc[i, j]
            if pd.isna(val):
                continue
            if abs(val) >= MIN_CORRELATION:
                links.append({"source": a, "target": b, "value": round(val, 2)})
    return nodes, links

# ============= Main ==============
def main():
    history = load_history()
    snapshot = fetch_current_prices(max_total=MAX_TOTAL, page_size=PAGE_SIZE)
    if not snapshot:
        print("⚠️ No data fetched.")
        return
    updated = update_history(history, snapshot)
    # save history and graph data
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(updated, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"ERROR saving history: {e}")
    nodes, links = calculate_correlation(updated)
    graph_data = {"nodes": nodes, "links": links, "last_updated": datetime.now().isoformat()}
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(graph_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"ERROR saving graph: {e}")
    print(f"✅ Update complete — Nodes: {len(nodes)}, Links: {len(links)}")

if __name__ == "__main__":
    main()
