import asyncio
import datetime
import logging
import os
from typing import List, Optional

import psycopg2
from psycopg2 import sql
from fastapi import FastAPI, Depends

from src.models import Event, AggregatorStats, ProcessedEvent

# KONFIGURASI LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# KONFIGURASI ENVIRONMENT
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://log_user:log_password@storage:5432/log_db")
MIN_CONSUMERS = int(os.getenv("MIN_CONSUMERS", "5")) 

class Aggregator:
    def __init__(self, db_url: str = DATABASE_URL):
        self.db_url = db_url
        self.queue = asyncio.Queue()
        
        self.received_count = 0
        self.unique_count = 0
        self.duplicate_count = 0
        self.start_time = datetime.datetime.now()
        
        try:
            self._init_db()
            logging.info(f"Aggregator initialized. Backend: PostgreSQL. Workers: {MIN_CONSUMERS}")
        except Exception as e:
            logging.critical(f"FATAL: Database initialization failed: {e}")
            raise

    def _get_db_connection(self):
        return psycopg2.connect(self.db_url)

    def _init_db(self):
        conn = self._get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                    event_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    PRIMARY KEY (event_id, topic)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS aggregator_stats (
                    key TEXT PRIMARY KEY,
                    value BIGINT DEFAULT 0
                );
                -- Inisialisasi awal jika tabel baru dibuat
                INSERT INTO aggregator_stats (key, value) 
                VALUES ('received', 0), ('unique_processed', 0), ('duplicate_dropped', 0)
                ON CONFLICT DO NOTHING;
            """)
            conn.commit()
            logging.info("PostgreSQL tables initialized.")
        except Exception as e:
            conn.rollback()
            raise
        finally:
            conn.close()

    async def add_event(self, event: Event):
        self.received_count += 1
        await self.queue.put(event)
        
    async def run_consumer(self, consumer_id: int):
        logging.info(f"Consumer #{consumer_id} started.")
        while True:
            event: Event = await self.queue.get()
            
            await asyncio.to_thread(self._process_event_atomic, event, consumer_id)
            
            self.queue.task_done()

    def _process_event_atomic(self, event: Event, consumer_id: int):
        conn = self._get_db_connection()
        try:
            cursor = conn.cursor()
            
            cursor.execute("UPDATE aggregator_stats SET value = value + 1 WHERE key = 'received'")

            query = sql.SQL("""
                INSERT INTO processed_events (event_id, topic, timestamp)
                VALUES (%s, %s, %s)
                ON CONFLICT (event_id, topic) DO NOTHING
                RETURNING event_id;
            """)
            cursor.execute(query, (event.event_id, event.topic, event.timestamp.isoformat()))
            is_unique = cursor.fetchone()

            if is_unique:
                cursor.execute("UPDATE aggregator_stats SET value = value + 1 WHERE key = 'unique_processed'")
                logging.info(f"CONSUMER #{consumer_id} PROCESSED: {event.event_id}")
            else:
                cursor.execute("UPDATE aggregator_stats SET value = value + 1 WHERE key = 'duplicate_dropped'")
                logging.warning(f"CONSUMER #{consumer_id} DUPLICATE: {event.event_id}")

            conn.commit() 
        except Exception as e:
            logging.error(f"Error: {e}")
            conn.rollback()
        finally:
            conn.close()

    def get_processed_events(self, topic: Optional[str] = None) -> List[ProcessedEvent]:
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        query = "SELECT event_id, topic, timestamp FROM processed_events"
        params = []
        
        if topic:
            query += " WHERE topic = %s"
            params.append(topic)
        
        try:
            cursor.execute(query, params)
            return [
                ProcessedEvent(
                    event_id=row[0],
                    topic=row[1],
                    timestamp=row[2].replace(tzinfo=datetime.timezone.utc) if row[2].tzinfo is None else row[2]
                ) for row in cursor.fetchall()
            ]
        except Exception as e:
            logging.error(f"Fetch events error: {e}")
            return []
        finally:
            conn.close()

    def get_stats(self) -> AggregatorStats:
        uptime = int((datetime.datetime.now() - self.start_time).total_seconds())
        conn = self._get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT key, value FROM aggregator_stats")
            db_stats = dict(cursor.fetchall())
            
            cursor.execute("SELECT DISTINCT topic FROM processed_events ORDER BY topic")
            topics = [row[0] for row in cursor.fetchall()]
            
            return AggregatorStats(
                received=db_stats.get('received', 0),
                unique_processed=db_stats.get('unique_processed', 0),
                duplicate_dropped=db_stats.get('duplicate_dropped', 0),
                topics=topics,
                uptime=uptime
            )
        finally:
            conn.close()

# FASTAPI APP SETUP
app = FastAPI(title="Idempotent Log Aggregator (PostgreSQL)")

def get_aggregator() -> Aggregator:
    global global_aggregator
    if 'global_aggregator' not in globals():
        db_url = os.getenv("DATABASE_URL", DATABASE_URL)
        global_aggregator = Aggregator(db_url=db_url)
    return global_aggregator

@app.on_event("startup")
async def startup_event():
    agg = get_aggregator()
    for i in range(MIN_CONSUMERS):
        asyncio.create_task(agg.run_consumer(i + 1))
    logging.info(f"Background workers started: {MIN_CONSUMERS}")

@app.post("/publish", status_code=202)
async def publish_event(event: Event, agg: Aggregator = Depends(get_aggregator)):
    await agg.add_event(event)
    return {"status": "accepted", "event_id": event.event_id}

@app.get("/events", response_model=List[ProcessedEvent])
async def get_events(topic: Optional[str] = None, agg: Aggregator = Depends(get_aggregator)):
    return await asyncio.to_thread(agg.get_processed_events, topic)

@app.get("/stats", response_model=AggregatorStats)
async def get_stats(agg: Aggregator = Depends(get_aggregator)):
    return await asyncio.to_thread(agg.get_stats)