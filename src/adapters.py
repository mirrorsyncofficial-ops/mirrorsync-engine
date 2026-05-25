import asyncio, sqlite3
from typing import Any, AsyncIterator, Dict, List, Optional
from mirrorsync import DatabaseAdapter

class SQLiteAdapter(DatabaseAdapter):
    def __init__(self, dsn: str):
        super().__init__(dsn)
        self._path = dsn.replace("sqlite:///", "").replace("sqlite://", "")
        self._conn = None

    async def connect(self):
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._connected = True

    async def disconnect(self):
        if self._conn:
            self._conn.close()
        self._connected = False

    async def list_tables(self):
        cur = self._conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [row[0] for row in cur.fetchall()]

    async def get_schema(self, table):
        cur = self._conn.execute(f"PRAGMA table_info({table})")
        return {row["name"]: row["type"] for row in cur.fetchall()}

    async def fetch_records(self, table, watermark=None, watermark_col="updated_at", batch_size=1000, offset=0):
        cur_offset = offset
        while True:
            if watermark:
                cur = self._conn.execute(f"SELECT * FROM {table} WHERE {watermark_col} > ? ORDER BY {watermark_col} ASC LIMIT ? OFFSET ?", (watermark, batch_size, cur_offset))
            else:
                cur = self._conn.execute(f"SELECT * FROM {table} LIMIT ? OFFSET ?", (batch_size, cur_offset))
            rows = [dict(row) for row in cur.fetchall()]
            if not rows:
                break
            yield rows
            cur_offset += len(rows)
            if len(rows) < batch_size:
                break
            await asyncio.sleep(0)

    async def upsert_records(self, table, records):
        if not records:
            return 0
        cols = list(records[0].keys())
        placeholders = ",".join("?" * len(cols))
        col_names = ",".join(cols)
        updates = ",".join(f"{c}=excluded.{c}" for c in cols)
        sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO UPDATE SET {updates}"
        with self._conn:
            self._conn.executemany(sql, [list(r.values()) for r in records])
        return len(records)

    async def delete_records(self, table, primary_keys):
        if not primary_keys:
            return 0
        placeholders = ",".join("?" * len(primary_keys))
        with self._conn:
            cur = self._conn.execute(f"DELETE FROM {table} WHERE id IN ({placeholders})", primary_keys)
        return cur.rowcount

    async def get_record_count(self, table, watermark=None):
        if watermark:
            cur = self._conn.execute(f"SELECT COUNT(*) FROM {table} WHERE updated_at > ?", (watermark,))
        else:
            cur = self._conn.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]

class PostgreSQLAdapter(DatabaseAdapter):
    def __init__(self, dsn: str):
        super().__init__(dsn)
        self._pool = None

    async def connect(self):
        try:
            import asyncpg
            self._pool = await asyncpg.create_pool(self.dsn, min_size=2, max_size=10)
            self._connected = True
        except ImportError:
            raise RuntimeError("PostgreSQL support requires: pip install asyncpg")

    async def disconnect(self):
        if self._pool:
            await self._pool.close()
        self._connected = False

    async def list_tables(self):
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
        return [r["tablename"] for r in rows]

    async def get_schema(self, table):
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name=$1 ORDER BY ordinal_position", table)
        return {r["column_name"]: r["data_type"] for r in rows}

    async def fetch_records(self, table, watermark=None, watermark_col="updated_at", batch_size=1000, offset=0):
        cur_offset = offset
        async with self._pool.acquire() as conn:
            while True:
                if watermark:
                    rows = await conn.fetch(f"SELECT * FROM {table} WHERE {watermark_col} > $1 ORDER BY {watermark_col} ASC LIMIT $2 OFFSET $3", watermark, batch_size, cur_offset)
                else:
                    rows = await conn.fetch(f"SELECT * FROM {table} LIMIT $1 OFFSET $2", batch_size, cur_offset)
                if not rows:
                    break
                yield [dict(r) for r in rows]
                cur_offset += len(rows)
                if len(rows) < batch_size:
                    break

    async def upsert_records(self, table, records):
        if not records:
            return 0
        cols = list(records[0].keys())
        col_names = ", ".join(cols)
        placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))
        updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c != "id")
        sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET {updates}"
        async with self._pool.acquire() as conn:
            await conn.executemany(sql, [list(r.values()) for r in records])
        return len(records)

    async def delete_records(self, table, primary_keys):
        if not primary_keys:
            return 0
        async with self._pool.acquire() as conn:
            result = await conn.execute(f"DELETE FROM {table} WHERE id=ANY($1::int[])", primary_keys)
        return int(result.split()[-1])

    async def get_record_count(self, table, watermark=None):
        async with self._pool.acquire() as conn:
            if watermark:
                row = await conn.fetchrow(f"SELECT COUNT(*) FROM {table} WHERE updated_at > $1", watermark)
            else:
                row = await conn.fetchrow(f"SELECT COUNT(*) FROM {table}")
        return row[0]

def create_adapter(dsn: str) -> DatabaseAdapter:
    dsn_lower = dsn.lower()
    if dsn_lower.startswith(("postgresql://", "postgres://")):
        return PostgreSQLAdapter(dsn)
    elif dsn_lower.startswith("sqlite"):
        return SQLiteAdapter(dsn)
    else:
        raise ValueError(f"Unknown DSN: {dsn}")
