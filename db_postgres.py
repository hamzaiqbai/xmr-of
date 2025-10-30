"""Simple PostgreSQL adapter used by the FHCharts worker.

Provides a small synchronous helper that creates the `timeframe_data` table
and exposes get_latest_bucket / save_bucket_data / close methods. The worker
uses this in a simple blocking manner; this keeps the implementation
straightforward and compatible with DigitalOcean App Platform deployments.

Configuration (via environment variables):
  PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE

Note: the production deployment should provide these env vars securely.
"""
import os
import json
import logging
from typing import Optional, Dict, Any

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

_DB = None


class PostgresDB:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('PGHOST', 'localhost'),
            port=int(os.getenv('PGPORT', 5432)),
            user=os.getenv('PGUSER', 'postgres'),
            password=os.getenv('PGPASSWORD', ''),
            dbname=os.getenv('PGDATABASE', 'postgres')
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        self._ensure_table()

    def _ensure_table(self):
        # Create a public `timeframe_data` table to store HTF orderflow results
        create_sql = '''
        CREATE TABLE IF NOT EXISTS public.timeframe_data (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            bucket BIGINT NOT NULL,
            total_volume NUMERIC,
            buy_volume NUMERIC,
            sell_volume NUMERIC,
            buy_contracts INTEGER,
            sell_contracts INTEGER,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            delta NUMERIC,
            max_delta NUMERIC,
            min_delta NUMERIC,
            cvd NUMERIC,
            large_order_count INTEGER,
            market_limit_ratio NUMERIC,
            buy_sell_ratio NUMERIC,
            poc NUMERIC,
            price_levels JSONB,
            hvns JSONB,
            lvns JSONB,
            imbalances JSONB,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now(),
            UNIQUE(symbol, timeframe, bucket)
        );
        CREATE INDEX IF NOT EXISTS idx_timeframe_data_symbol_tf_bucket ON public.timeframe_data(symbol, timeframe, bucket);
        '''
        self.cur.execute(create_sql)

    def get_latest_bucket(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        try:
            sql = "SELECT * FROM public.timeframe_data WHERE symbol = %s AND timeframe = %s ORDER BY bucket DESC LIMIT 1"
            self.cur.execute(sql, (symbol, timeframe))
            row = self.cur.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"get_latest_bucket error: {e}")
            return None

    def save_bucket_data(self, symbol: str, timeframe: str, row: Dict[str, Any]) -> bool:
        """Insert or update a bucket row (upsert).

        Expects `row` keys that match keys produced by `format_row` in the worker
        (see worker_of.py). The `bucket` field is stored as seconds (int).
        """
        try:
            sql = '''
            INSERT INTO public.timeframe_data
            (symbol, timeframe, bucket, total_volume, buy_volume, sell_volume, buy_contracts, sell_contracts,
             open, high, low, close, delta, max_delta, min_delta, cvd, large_order_count,
             market_limit_ratio, buy_sell_ratio, poc, price_levels, hvns, lvns, imbalances, updated_at)
            VALUES
            (%(symbol)s, %(timeframe)s, %(bucket)s, %(total_volume)s, %(buy_volume)s, %(sell_volume)s, %(buy_contracts)s, %(sell_contracts)s,
             %(open)s, %(high)s, %(low)s, %(close)s, %(delta)s, %(max_delta)s, %(min_delta)s, %(cvd)s, %(large_order_count)s,
             %(market_limit_ratio)s, %(buy_sell_ratio)s, %(poc)s, %(price_levels)s::jsonb, %(hvns)s::jsonb, %(lvns)s::jsonb, %(imbalances)s::jsonb, now())
            ON CONFLICT (symbol, timeframe, bucket)
            DO UPDATE SET
               total_volume = EXCLUDED.total_volume,
               buy_volume = EXCLUDED.buy_volume,
               sell_volume = EXCLUDED.sell_volume,
               buy_contracts = EXCLUDED.buy_contracts,
               sell_contracts = EXCLUDED.sell_contracts,
               open = EXCLUDED.open,
               high = EXCLUDED.high,
               low = EXCLUDED.low,
               close = EXCLUDED.close,
               delta = EXCLUDED.delta,
               max_delta = EXCLUDED.max_delta,
               min_delta = EXCLUDED.min_delta,
               cvd = EXCLUDED.cvd,
               large_order_count = EXCLUDED.large_order_count,
               market_limit_ratio = EXCLUDED.market_limit_ratio,
               buy_sell_ratio = EXCLUDED.buy_sell_ratio,
               poc = EXCLUDED.poc,
               price_levels = EXCLUDED.price_levels,
               hvns = EXCLUDED.hvns,
               lvns = EXCLUDED.lvns,
               imbalances = EXCLUDED.imbalances,
               updated_at = now();
            '''

            payload = {
                'symbol': symbol,
                'timeframe': timeframe,
                'bucket': int(row.get('bucket')) if row.get('bucket') is not None else None,
                'total_volume': row.get('total_volume'),
                'buy_volume': row.get('buy_volume'),
                'sell_volume': row.get('sell_volume'),
                'buy_contracts': row.get('buy_contracts'),
                'sell_contracts': row.get('sell_contracts'),
                'open': row.get('open'),
                'high': row.get('high'),
                'low': row.get('low'),
                'close': row.get('close'),
                'delta': row.get('delta'),
                'max_delta': row.get('max_delta'),
                'min_delta': row.get('min_delta'),
                'cvd': row.get('cvd'),
                'large_order_count': row.get('large_order_count'),
                'market_limit_ratio': row.get('market_limit_ratio'),
                'buy_sell_ratio': row.get('buy_sell_ratio'),
                'poc': row.get('poc'),
                'price_levels': json.dumps(row.get('price_levels')) if not isinstance(row.get('price_levels'), str) else row.get('price_levels'),
                'hvns': json.dumps(row.get('hvns')) if not isinstance(row.get('hvns'), str) else row.get('hvns'),
                'lvns': json.dumps(row.get('lvns')) if not isinstance(row.get('lvns'), str) else row.get('lvns'),
                'imbalances': json.dumps(row.get('imbalances')) if not isinstance(row.get('imbalances'), str) else row.get('imbalances')
            }

            self.cur.execute(sql, payload)
            return True
        except Exception as e:
            logger.error(f"save_bucket_data error: {e}")
            try:
                self.conn.rollback()
            except Exception:
                pass
            return False

    def close(self):
        try:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
        except Exception:
            pass


def get_db() -> PostgresDB:
    global _DB
    if _DB is None:
        _DB = PostgresDB()
    return _DB
