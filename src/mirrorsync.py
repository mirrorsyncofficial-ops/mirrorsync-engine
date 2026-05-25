import asyncio, hashlib, json, logging, time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, AsyncIterator, Dict, List, Optional

__version__ = "1.0.0"
logger = logging.getLogger("mirrorsync")

class SyncMode(Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    REALTIME = "realtime"

class SyncDirection(Enum):
    SOURCE_TO_TARGET = "source_to_target"
    TARGET_TO_SOURCE = "target_to_source"
    BIDIRECTIONAL = "bidirectional"

class ConflictStrategy(Enum):
    SOURCE_WINS = "source_wins"
    TARGET_WINS = "target_wins"
    LATEST_WINS = "latest_wins"
    MANUAL = "manual"

class RecordStatus(Enum):
    SYNCED = "synced"
    PENDING = "pending"
    CONFLICT = "conflict"
    ERROR = "error"

@dataclass
class SyncConfig:
    name: str
    source_dsn: str
    target_dsn: str
    mode: SyncMode = SyncMode.INCREMENTAL
    direction: SyncDirection = SyncDirection.SOURCE_TO_TARGET
    conflict_strategy: ConflictStrategy = ConflictStrategy.LATEST_WINS
    batch_size: int = 1000
    max_retries: int = 3
    tables: Optional[List[str]] = None
    exclude_tables: List[str] = field(default_factory=list)
    column_mappings: Dict[str, Dict[str, str]] = field(default_factory=dict)
    transformations: Dict[str, Any] = field(default_factory=dict)
    watermark_column: str = "updated_at"
    checkpoint_interval: int = 100
    dry_run: bool = False

@dataclass
class SyncStats:
    job_id: str
    config_name: str
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None
    total_records: int = 0
    synced: int = 0
    errors: int = 0
    tables_processed: List[str] = field(default_factory=list)

    @property
    def duration_seconds(self):
        end = self.finished_at or datetime.now(timezone.utc)
        return (end - self.started_at).total_seconds()

    @property
    def records_per_second(self):
        d = self.duration_seconds
        return self.synced / d if d > 0 else 0.0

    @property
    def success_rate(self):
        total = self.synced + self.errors
        return (self.synced / total * 100) if total > 0 else 100.0

class DatabaseAdapter(ABC):
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._connected = False
    @abstractmethod
    async def connect(self): ...
    @abstractmethod
    async def disconnect(self): ...
    @abstractmethod
    async def list_tables(self): ...
    @abstractmethod
    async def get_schema(self, table): ...
    @abstractmethod
    async def fetch_records(self, table, watermark=None, watermark_col="updated_at", batch_size=1000, offset=0): ...
    @abstractmethod
    async def upsert_records(self, table, records): ...
    @abstractmethod
    async def delete_records(self, table, primary_keys): ...
    @abstractmethod
    async def get_record_count(self, table, watermark=None): ...
    async def __aenter__(self):
        await self.connect()
        return self
    async def __aexit__(self, *args):
        await self.disconnect()

class MirrorSync:
    def __init__(self, config, source, target):
        self.config = config
        self.source = source
        self.target = target
        self._job_id = f"sync_{config.name}_{int(time.time())}"
        self._stats = SyncStats(job_id=self._job_id, config_name=config.name)
        self._hooks: Dict[str, List] = {"pre_sync":[],"post_sync":[],"pre_table":[],"post_table":[],"on_error":[]}

    def on(self, event):
        def decorator(fn):
            self._hooks.setdefault(event, []).append(fn)
            return fn
        return decorator

    async def _fire(self, event, **kwargs):
        for hook in self._hooks.get(event, []):
            if asyncio.iscoroutinefunction(hook):
                await hook(**kwargs)
            else:
                hook(**kwargs)

    async def run(self):
        await self._fire("pre_sync", config=self.config, job_id=self._job_id)
        async with self.source, self.target:
            all_tables = await self.source.list_tables()
            tables = self.config.tables or all_tables
            tables = [t for t in tables if t not in self.config.exclude_tables]
            sem = asyncio.Semaphore(4)
            async def bounded(table):
                async with sem:
                    try:
                        await self._fire("pre_table", table=table, stats=self._stats)
                        count = 0
                        async for batch in self.source.fetch_records(table, batch_size=self.config.batch_size):
                            if not batch: break
                            if not self.config.dry_run:
                                written = await self.target.upsert_records(table, batch)
                                count += written
                                self._stats.synced += written
                            self._stats.total_records += len(batch)
                        if table not in self._stats.tables_processed:
                            self._stats.tables_processed.append(table)
                        await self._fire("post_table", table=table, count=count, stats=self._stats)
                    except Exception as e:
                        logger.error(f"[{table}] Error: {e}")
                        self._stats.errors += 1
                        await self._fire("on_error", table=table, error=e)
            await asyncio.gather(*[bounded(t) for t in tables])
        self._stats.finished_at = datetime.now(timezone.utc)
        await self._fire("post_sync", stats=self._stats)
        return self._stats
