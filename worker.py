"""
FHCharts Worker for DigitalOcean App Platform with PostgreSQL
"""
import asyncio
import json
import time
import math
import os
import logging
from collections import defaultdict, deque
import websockets
from datetime import datetime, timedelta
import statistics
import numpy as np

# Import our PostgreSQL adapter
from db_postgres import get_db

# Configure logging for DigitalOcean
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SYMBOL = os.getenv('SYMBOL', 'xmrusdt')  # Allow symbol configuration via env var
TRADE_SIZES_WINDOW = 1000
trade_size_buffer = deque(maxlen=TRADE_SIZES_WINDOW)
STATIC_FLOOR = 10.0
TICK_SIZE = 0.01

# Configuration: timeframes in ms and corresponding price bin sizes
TF_CONFIG = {
    '1m':  {'ms': 1 * 60 * 1000,  'bin_size': 0.01},
    '3m':  {'ms': 3 * 60 * 1000,  'bin_size': 0.05},
    '5m':  {'ms': 5 * 60 * 1000,  'bin_size': 0.10},
    '15m': {'ms':15 * 60 * 1000,  'bin_size': 0.20},
    '30m': {'ms':30 * 60 * 1000,  'bin_size': 0.30},
    '1h':  {'ms':60 * 60 * 1000,  'bin_size': 0.50},
    '4h':  {'ms':4  * 60 * 60 * 1000,'bin_size': 1.00},
}

def is_large_order(qty):
    """Returns True if this trade quantity exceeds the dynamic threshold"""
    trade_size_buffer.append(qty)
    if len(trade_size_buffer) >= 100:
        mu = statistics.mean(trade_size_buffer)
        sigma = statistics.stdev(trade_size_buffer)
        threshold = mu + 2 * sigma
    else:
        threshold = STATIC_FLOOR
    return qty >= threshold

def classify_order_type(maker_flag: bool) -> str:
    """If maker_flag is True, this was a resting limit order; if False, it was a market (taker) order."""
    return 'limit' if maker_flag else 'market'

def floor_to_bin(price, bin_size, tick_size=0.01):
    """Floor price down to bin boundary (for market sells/bid liquidity)"""
    price_q = round(price / tick_size) * tick_size
    bin_p = math.floor(price_q / bin_size) * bin_size
    return round(bin_p, 2)

def ceil_to_bin(price, bin_size, tick_size=0.01):
    """Ceiling price up to bin boundary (for market buys/ask liquidity)"""
    price_q = round(price / tick_size) * tick_size
    bin_p = math.ceil(price_q / bin_size) * bin_size
    return round(bin_p, 2)

def floor_to_bin(price, bin_size, tick_size=0.01):
    """Floor price down to bin boundary (for market sells/bid liquidity)"""
    price_q = round(price / tick_size) * tick_size
    bin_p = math.floor(price_q / bin_size) * bin_size
    return round(bin_p, 2)

def ceil_to_bin(price, bin_size, tick_size=0.01):
    """Ceiling price up to bin boundary (for market buys/ask liquidity)"""
    price_q = round(price / tick_size) * tick_size
    bin_p = math.ceil(price_q / bin_size) * bin_size
    return round(bin_p, 2)

def should_reset_cvd(tf, prev_ts_ms, curr_ts_ms):
    prev_dt = datetime.utcfromtimestamp(prev_ts_ms / 1000)
    curr_dt = datetime.utcfromtimestamp(curr_ts_ms / 1000)

    if tf in ('1m', '3m'):
        prev_qtr = (prev_dt.minute // 15, prev_dt.hour, prev_dt.day)
        curr_qtr = (curr_dt.minute // 15, curr_dt.hour, curr_dt.day)
        return prev_qtr != curr_qtr
    elif tf in ('5m', '15m'):
        return prev_dt.date() != curr_dt.date()
    elif tf in ('30m', '1h'):
        prev_week = prev_dt.isocalendar()[1]
        curr_week = curr_dt.isocalendar()[1]
        return prev_week != curr_week
    elif tf == '4h':
        return False
    return False

def init_bucket(bin_size):
    return {
        'bucket': None,
        'open': None,
        'high': float('-inf'),
        'low': float('inf'),
        'close': None,
        'total_volume': 0.0,
        'buy_volume': 0.0,
        'sell_volume': 0.0,
        'buy_contracts': 0,
        'sell_contracts': 0,
        'delta': 0.0,
        'max_delta': float('-inf'),
        'min_delta': float('inf'),
        'CVD': 0.0,
        'large_order_count': 0,
        'market_volume': 0.0,
        'limit_volume': 0.0,
        'price_bins': defaultdict(lambda: {
            'buy_volume': 0.0,
            'sell_volume': 0.0,
            'buy_contracts': 0,
            'sell_contracts': 0
        }),
    }

def parse_bucket_from_db_row(row, bin_size):
    """Convert a database row back into a bucket structure"""
    bucket = init_bucket(bin_size)
    
    # Parse basic fields
    bucket['bucket'] = int(float(row['bucket'])) * 1000  # Convert back to milliseconds
    bucket['open'] = float(row['open']) if row['open'] else None
    bucket['high'] = float(row['high']) if row['high'] else float('-inf')
    bucket['low'] = float(row['low']) if row['low'] else float('inf')
    bucket['close'] = float(row['close']) if row['close'] else None
    bucket['total_volume'] = float(row['total_volume'])
    bucket['buy_volume'] = float(row['buy_volume'])
    bucket['sell_volume'] = float(row['sell_volume'])
    bucket['buy_contracts'] = int(row['buy_contracts'])
    bucket['sell_contracts'] = int(row['sell_contracts'])
    bucket['delta'] = float(row['delta'])
    bucket['max_delta'] = float(row['max_delta'])
    bucket['min_delta'] = float(row['min_delta'])
    bucket['CVD'] = float(row['cvd'])
    bucket['large_order_count'] = int(row['large_order_count'])
    
    # Parse price_levels JSON back to price_bins
    try:
        price_levels = json.loads(row['price_levels'])
        for price_str, data in price_levels.items():
            price = float(price_str)
            bucket['price_bins'][price] = {
                'buy_volume': data['buy_volume'],
                'sell_volume': data['sell_volume'],
                'buy_contracts': data['buy_contracts'],
                'sell_contracts': data['sell_contracts']
            }
    except:
        pass  # If parsing fails, keep empty price_bins
    
    return bucket

def initialize_state():
    """Initialize state with existing database data and resume incomplete buckets"""
    current_time_ms = int(time.time() * 1000)
    db = get_db()
    
    state = {}
    for tf, cfg in TF_CONFIG.items():
        ms = cfg['ms']
        bin_size = cfg['bin_size']
        
        # Check if we should resume the last bucket
        current_bucket_start = current_time_ms - (current_time_ms % ms)
        resume_bucket = None
        
        # Get the latest bucket from database
        latest_row = db.get_latest_bucket(SYMBOL, tf)
        
        if latest_row:
            last_bucket_timestamp = int(float(latest_row['bucket'])) * 1000  # Convert to ms
            
            # Check if current time falls within the last bucket's timeframe
            if last_bucket_timestamp == current_bucket_start:
                logger.info(f"Resuming incomplete {tf} bucket at {current_bucket_start}")
                resume_bucket = parse_bucket_from_db_row(latest_row, bin_size)
        
        # Initialize current bucket
        if resume_bucket:
            current_bucket = resume_bucket
        else:
            current_bucket = init_bucket(bin_size)
        
        state[tf] = {
            'current': current_bucket,
            'completed': []
        }
    
    return state

# Global state initialization
STATE = initialize_state()

# Track last CVD timestamp and global CVD per timeframe
last_cvd_ts = {tf: 0 for tf in TF_CONFIG}
global_cvd_state = {tf: 0.0 for tf in TF_CONFIG}

# Resume CVD state from existing data
db = get_db()
for tf in TF_CONFIG:
    current_bucket = STATE[tf]['current']
    if current_bucket['bucket'] is not None:
        # Resume from the CVD of the current bucket
        global_cvd_state[tf] = current_bucket['CVD']
        last_cvd_ts[tf] = current_bucket['bucket']
        logger.info(f"Resumed {tf} CVD at {current_bucket['CVD']}")
    else:
        # No current bucket, but we might have historical data
        latest_row = db.get_latest_bucket(SYMBOL, tf)
        if latest_row:
            global_cvd_state[tf] = float(latest_row['cvd'])
            last_cvd_ts[tf] = int(float(latest_row['bucket'])) * 1000
            logger.info(f"Resumed {tf} CVD from database at {global_cvd_state[tf]}")

def save_to_database(tf):
    """Save current bucket data to PostgreSQL database"""
    try:
        db = get_db()
        curr = STATE[tf]['current']
        
        if curr['bucket'] is not None:
            # Format the bucket data
            bin_size = TF_CONFIG[tf]['bin_size']
            bucket_data = format_row(curr, bin_size)
            
            # Save to database
            success = db.save_bucket_data(SYMBOL, tf, bucket_data)
            if not success:
                logger.error(f"Failed to save {tf} bucket to database")
        
        # Save any completed buckets
        for bucket in STATE[tf]['completed']:
            bucket_data = format_row(bucket, bin_size)
            db.save_bucket_data(SYMBOL, tf, bucket_data)
        
        # Clear completed buckets after saving
        STATE[tf]['completed'] = []
        
    except Exception as e:
        logger.error(f"Error saving to database for {tf}: {e}")

# [Include all the helper functions from original code - compute_poc, compute_imbalances, etc.]
def compute_poc(price_bins):
    max_vol = 0
    poc_price = None
    for price, data in price_bins.items():
        vol = data['buy_volume'] + data['sell_volume']
        if vol > max_vol:
            max_vol = vol
            poc_price = price
    return poc_price

def find_hvns_and_lvns(price_levels, hvn_frac=0.6, lvn_frac=0.2):
    prices = sorted(price_levels)
    vols = [price_levels[p] for p in prices]
    max_vol = max(vols)
    hvn_thresh = hvn_frac * max_vol
    lvn_thresh = lvn_frac * max_vol

    hvns, lvns = [], []
    for i, p in enumerate(prices):
        v = price_levels[p]
        if ((i == 0 or v >= price_levels[prices[i-1]]) and
            (i == len(prices)-1 or v >= price_levels[prices[i+1]])):
            if v == max_vol or v >= hvn_thresh:
                hvns.append(p)
        if v > 0 and ((i == 0 or v <= price_levels[prices[i-1]]) and
                      (i == len(prices)-1 or v <= price_levels[prices[i+1]])):
            if v <= lvn_thresh:
                lvns.append(p)

    return hvns[:3], lvns[:len(hvns)]

def compute_imbalances(price_levels_output):
    """
    price_levels_output: dict price → {buy_volume, sell_volume, …}
    Returns a dict: price → [ { ...curr_stats,
                                'imbalance_type': str,
                                'imbalance_strength': str }, … ]
    Calculates both same-level and diagonal imbalances.
    """
    MIN_VOL = 5.0  # minimum opposite-side volume to qualify
    R_NORM = 3.0   # ratio ≥ 3 → "normal"
    R_STRONG = 5.0 # ratio ≥ 5 → "strong"
    R_HEAVY = 7.0  # ratio ≥ 7 → "heavy"
    
    imbalance_map = {}
    
    # Convert string keys to float keys for computation if needed
    float_price_levels = {}
    for price_key, data in price_levels_output.items():
        price_float = float(price_key)
        float_price_levels[price_float] = data
    
    prices = sorted(float_price_levels.keys())

    # infer bin size from price spacing (default to 0 if only one level)
    if len(prices) > 1:
        bin_size = round(prices[1] - prices[0], 2)
    else:
        bin_size = 0.0

    for price in prices:
        curr   = float_price_levels[price]
        buy_v  = curr["buy_volume"]
        sell_v = curr["sell_volume"]

        # 1) same-level BUY imbalance
        if sell_v > MIN_VOL:
            ratio = buy_v / sell_v
            if ratio >= R_NORM:
                strength = ("heavy" if ratio >= R_HEAVY else
                            "strong" if ratio >= R_STRONG else
                            "normal")
                imbalance_map.setdefault(price, []).append({
                    **curr,
                    "imbalance_type":     "buy_same_level",
                    "imbalance_strength": strength
                })

        # 2) same-level SELL imbalance
        if buy_v > MIN_VOL:
            ratio = sell_v / buy_v
            if ratio >= R_NORM:
                strength = ("heavy" if ratio >= R_HEAVY else
                            "strong" if ratio >= R_STRONG else
                            "normal")
                imbalance_map.setdefault(price, []).append({
                    **curr,
                    "imbalance_type":     "sell_same_level",
                    "imbalance_strength": strength
                })

        # ───────────── diagonal imbalances ────────────────────────────────
        if bin_size > 0:

            # 3) BUY-diagonal  =  Ask(P) / Bid(P-bin)
            prev_price = round(price - bin_size, 2)
            prev_bid   = float_price_levels.get(prev_price, {}).get("sell_volume", 0.0)
            if prev_bid > MIN_VOL:
                ratio = buy_v / prev_bid           # ← Ask ÷ Bid
                if ratio >= R_NORM:
                    strength = ("heavy" if ratio >= R_HEAVY else
                                "strong" if ratio >= R_STRONG else
                                "normal")
                    imbalance_map.setdefault(price, []).append({
                        **curr,
                        "imbalance_type":     "buy_diagonal",
                        "imbalance_strength": strength
                    })

            # 4) SELL-diagonal =  Bid(P) / Ask(P+bin)
            next_price = round(price + bin_size, 2)
            next_ask   = float_price_levels.get(next_price, {}).get("buy_volume", 0.0)
            if next_ask > MIN_VOL:
                ratio = sell_v / next_ask          # ← Bid ÷ Ask
                if ratio >= R_NORM:
                    strength = ("heavy" if ratio >= R_HEAVY else
                                "strong" if ratio >= R_STRONG else
                                "normal")
                    imbalance_map.setdefault(price, []).append({
                        **curr,
                        "imbalance_type":     "sell_diagonal",
                        "imbalance_strength": strength
                    })

    return imbalance_map

def format_row(b, bin_size):
    """Format a bucket into database row format"""
    buy = b['buy_volume']
    sell = b['sell_volume']
    bs_ratio = buy / sell if sell else float('inf')

    # Convert bucket timestamp from ms to seconds
    bucket_sec = b['bucket'] // 1000 if b['bucket'] is not None else None

    # Create price_levels structure
    price_levels_output = {}
    for price, stats in b['price_bins'].items():
        price_levels_output[round(price, 2)] = {
            "buy_volume": round(stats["buy_volume"], 2),
            "sell_volume": round(stats["sell_volume"], 2),
            "buy_contracts": stats["buy_contracts"],
            "sell_contracts": stats["sell_contracts"]
        }

    # Compute HVNs/LVNs
    vol_map = {p: stats["buy_volume"] + stats["sell_volume"]
               for p, stats in price_levels_output.items()}
    hvns, lvns = find_hvns_and_lvns(vol_map)

    # Market/Limit ratio
    if b['limit_volume'] > 0:
        ml_ratio = b['market_volume'] / b['limit_volume']
    else:
        ml_ratio = None

    return {
        'bucket': bucket_sec,
        'total_volume': round(b['total_volume'], 2),
        'buy_volume': round(buy, 2),
        'sell_volume': round(sell, 2),
        'buy_contracts': b['buy_contracts'],
        'sell_contracts': b['sell_contracts'],
        'open': round(b['open'], 2) if b['open'] is not None else None,
        'high': round(b['high'], 2) if b['high'] != float('-inf') else None,
        'low': round(b['low'], 2) if b['low'] != float('inf') else None,
        'close': round(b['close'], 2) if b['close'] is not None else None,
        'delta': round(b['delta'], 2),
        'max_delta': round(b['max_delta'], 2),
        'min_delta': round(b['min_delta'], 2),
        'cvd': round(b['CVD'], 2),
        'large_order_count': b.get('large_order_count', 0),
        'market_limit_ratio': round(ml_ratio, 2) if ml_ratio is not None and ml_ratio != float('inf') else None,
        'buy_sell_ratio': round(bs_ratio, 2) if bs_ratio != float('inf') else 9999.99,
        'poc': round(compute_poc(b['price_bins']), 2) if compute_poc(b['price_bins']) is not None else None,
        'price_levels': json.dumps(price_levels_output),
        'hvns': json.dumps([round(p, 2) for p in hvns]),
        'lvns': json.dumps([round(p, 2) for p in lvns]),
        'imbalances': json.dumps(compute_imbalances(price_levels_output))
    }

def process_trade(trade):
    """Process aggTrade ticks"""
    price = float(trade['p'])
    qty = float(trade['q'])

    if price == 0.0 or qty == 0.0:
        return
    
    ts = trade['T']
    maker_flag = trade['m']  # True if buyer was maker (market sell), False if seller was maker (market buy)
    is_buyer_maker = maker_flag  # Same as PriceLadder.py naming convention
    order_type = classify_order_type(maker_flag)
    n_tr = trade['l'] - trade['f'] + 1

    for tf, cfg in TF_CONFIG.items():
        ms = cfg['ms']
        bin_size = cfg['bin_size']
        state = STATE[tf]
        curr = state['current']
        start = ts - (ts % ms)

        # Large order detection
        if is_large_order(qty):
            curr['large_order_count'] += 1

        if order_type == 'market':
            curr['market_volume'] += qty
        else:
            curr['limit_volume'] += qty

        # New bucket?
        if curr['bucket'] is None:
            curr['bucket'] = start
        elif start != curr['bucket']:
            # Save completed bucket to database
            save_to_database(tf)
            state['completed'].append(curr)
            curr = init_bucket(bin_size)
            curr['bucket'] = start
            state['current'] = curr

        # OHLC
        if curr['open'] is None:
            curr['open'] = price
        curr['close'] = price
        curr['high'] = max(curr['high'], price)
        curr['low'] = min(curr['low'], price)

        # Volume & contracts
        curr['total_volume'] += qty
        if is_buyer_maker:
            # buyer is maker = seller is taker = market sell = consumes bid liquidity
            curr['sell_volume'] += qty
            curr['sell_contracts'] += n_tr
            curr['delta'] -= qty
        else:
            # seller is maker = buyer is taker = market buy = consumes ask liquidity
            curr['buy_volume'] += qty
            curr['buy_contracts'] += n_tr
            curr['delta'] += qty

        # CVD reset logic
        if last_cvd_ts[tf] > 0 and should_reset_cvd(tf, last_cvd_ts[tf], ts):
            global_cvd_state[tf] = 0.0

        # Update global and current bucket CVD
        if is_buyer_maker:
            global_cvd_state[tf] -= qty  # market sell
        else:
            global_cvd_state[tf] += qty  # market buy

        curr['CVD'] = global_cvd_state[tf]
        last_cvd_ts[tf] = ts

        # CVD & extremes
        curr['max_delta'] = max(curr['max_delta'], curr['delta'])
        curr['min_delta'] = min(curr['min_delta'], curr['delta'])

        # Price binning using correct Binance aggregation method
        if is_buyer_maker:
            # Market sell (consumes bid liquidity) - use FLOOR
            bin_p = floor_to_bin(price, bin_size, TICK_SIZE)
            side_vol_key = 'sell_volume'
            side_ctr_key = 'sell_contracts'
        else:
            # Market buy (consumes ask liquidity) - use CEILING
            bin_p = ceil_to_bin(price, bin_size, TICK_SIZE)
            side_vol_key = 'buy_volume'
            side_ctr_key = 'buy_contracts'
        
        curr['price_bins'][bin_p][side_vol_key] += qty
        curr['price_bins'][bin_p][side_ctr_key] += n_tr

        # Save current state to database (real-time updates)
        save_to_database(tf)

async def main():
    """Main worker function"""
    uri = f"wss://fstream.binance.com/ws/{SYMBOL}@aggTrade"
    logger.info(f"Starting FHCharts worker for {SYMBOL}")
    
    while True:
        try:
            async with websockets.connect(
                uri,
                ping_interval=30,
                ping_timeout=20
            ) as ws:
                logger.info("🔗 Connected to Binance WebSocket")
                async for msg in ws:
                    process_trade(json.loads(msg))
        except websockets.exceptions.ConnectionClosedError as e:
            logger.warning(f"Connection lost ({e}); reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error: {e!r}")
            await asyncio.sleep(10)  # Wait longer for unexpected errors

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped")
    except Exception as e:
        logger.error(f"Worker failed: {e}")
    finally:
        # Cleanup database connection
        try:
            db = get_db()
            db.close()
        except:
            pass
