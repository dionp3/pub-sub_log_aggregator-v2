# src/main.py
import asyncio
import datetime
import logging
from typing import List, Optional
import os
import psycopg2 
from psycopg2 import sql # Untuk membuat query SQL yang aman

from fastapi import FastAPI, Depends
from src.models import Event, AggregatorStats, ProcessedEvent

# Konfigurasi Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# Mengambil konfigurasi dari environment
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://log_user:log_password@storage:5432/log_db")
MIN_CONSUMERS = int(os.getenv("MIN_CONSUMERS", "5")) # T9: Jumlah worker concurrent

class Aggregator:
    def __init__(self, db_url: str = DATABASE_URL):
        self.db_url = db_url
        self.queue = asyncio.Queue()
        self.received_count = 0
        self.unique_count = 0
        self.duplicate_count = 0
        self.start_time = datetime.datetime.now()
        # Ensure initial connection works before starting
        try:
            self._init_db()
            logging.info(f"Aggregator initialized. DB: PostgreSQL. Workers: {MIN_CONSUMERS}")
        except Exception as e:
            logging.critical(f"FATAL: Could not initialize database connection: {e}")
            raise

    def _get_db_connection(self):
        """Membuka koneksi baru ke PostgreSQL."""
        # Menambahkan autocommit=False secara default untuk transaksi eksplisit
        return psycopg2.connect(self.db_url)

    def _init_db(self):
        """T4: Inisialisasi tabel dengan PRIMARY KEY komposit untuk Dedup atomik."""
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
            conn.commit()
            logging.info("PostgreSQL table 'processed_events' initialized.")
        except Exception as e:
            logging.error(f"Error during DB initialization: {e}")
            conn.rollback()
            raise # Re-raise error to stop startup if DB fails
        finally:
            conn.close()

    async def add_event(self, event: Event):
        self.received_count += 1
        await self.queue.put(event)
        
    async def run_consumer(self, consumer_id: int):
        """T9: Fungsi worker yang memproses queue secara konkuren."""
        logging.info(f"Consumer #{consumer_id} started.")
        while True:
            event: Event = await self.queue.get()
            
            # Menjalankan operasi DB yang blocking di thread terpisah (asyncio.to_thread)
            await asyncio.to_thread(self._process_event_atomic, event, consumer_id)
            
            self.queue.task_done()

    def _process_event_atomic(self, event: Event, consumer_id: int):
        """
        T8 & T9: Operasi Dedup Atomik dan Kontrol Konkurensi menggunakan ON CONFLICT.
        """
        conn = self._get_db_connection()
        try:
            # T8: Transaksi dimulai di sini
            cursor = conn.cursor()
            
            # T9: Idempotent Upsert Pattern
            query = sql.SQL("""
                INSERT INTO processed_events (event_id, topic, timestamp)
                VALUES (%s, %s, %s)
                ON CONFLICT (event_id, topic) DO NOTHING 
                RETURNING event_id;
            """)

            # Menggunakan isoformat() untuk timestamp yang benar
            cursor.execute(query, (event.event_id, event.topic, event.timestamp.isoformat()))
            
            inserted_id = cursor.fetchone()
            
            if inserted_id:
                self.unique_count += 1
                logging.info(f"CONSUMER #{consumer_id} PROCESSED: {event.topic} | {event.event_id}")
            else:
                self.duplicate_count += 1
                logging.warning(f"CONSUMER #{consumer_id} DUPLICATE DROPPED: {event.topic} | {event.event_id}")

            conn.commit() # T8: Commit transaksi
            
        except Exception as e:
            logging.error(f"Error processing event {event.event_id}: {e}")
            conn.rollback() # T8: Rollback jika ada kegagalan
        finally:
            conn.close()

    def get_processed_events(self, topic: Optional[str] = None) -> List[ProcessedEvent]:
        """Mengembalikan daftar event unik yang telah diproses, opsional difilter berdasarkan topik."""
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        query_parts = ["SELECT event_id, topic, timestamp FROM processed_events"]
        params = []
        
        if topic:
            query_parts.append("WHERE topic = %s")
            params.append(topic)
        
        query = " ".join(query_parts)
        
        try:
            cursor.execute(query, params)
            events = [
                ProcessedEvent(
                    event_id=row[0],
                    topic=row[1],
                    # PostgreSQL mengembalikan objek datetime, pastikan timezone diinterpretasi
                    timestamp=row[2].replace(tzinfo=datetime.timezone.utc) if row[2].tzinfo is None else row[2]
                ) for row in cursor.fetchall()
            ]
            return events
        except Exception as e:
            logging.error(f"Error fetching processed events: {e}")
            return []
        finally:
            conn.close()

    def get_stats(self) -> AggregatorStats:
        """Mengembalikan metrik operasional dan statistik deduplikasi."""
        uptime = int((datetime.datetime.now() - self.start_time).total_seconds())
        
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        topics = []
        try:
            # Dapatkan daftar topik unik yang ada di DB
            cursor.execute("SELECT DISTINCT topic FROM processed_events ORDER BY topic")
            topics = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error fetching topics for stats: {e}")
        finally:
            conn.close()
        
        return AggregatorStats(
            received=self.received_count,
            unique_processed=self.unique_count, # Counter in-memory
            duplicate_dropped=self.duplicate_count, # Counter in-memory
            topics=topics,
            uptime=uptime
        )


# ----------------------------------------
# FastAPI App Setup
# ----------------------------------------

app = FastAPI(title="Idempotent Log Aggregator (PostgreSQL Backend)")

def get_aggregator() -> Aggregator:
    """Dependency untuk mendapatkan instance Aggregator global."""
    # Selalu gunakan variabel lingkungan untuk koneksi
    db_url = os.getenv("DATABASE_URL", "postgresql://log_user:log_password@storage:5432/log_db")
    
    global global_aggregator
    if 'global_aggregator' not in globals():
        global_aggregator = Aggregator(db_url=db_url)
    return global_aggregator

global_aggregator = get_aggregator()

@app.on_event("startup")
async def startup_event():
    """Memulai worker consumer saat aplikasi startup."""
    agg = get_aggregator()
    # Memulai MIN_CONSUMERS worker untuk pengujian konkurensi (T9)
    for i in range(MIN_CONSUMERS):
        asyncio.create_task(agg.run_consumer(i + 1))
    logging.info(f"Started {MIN_CONSUMERS} concurrent consumer tasks.")

@app.post("/publish", status_code=202)
async def publish_event(event: Event, agg: Aggregator = Depends(get_aggregator)):
    """
    Endpoint POST untuk menerima event dari publisher.
    Status 202 (Accepted) menunjukkan event diterima dan sedang diproses secara asinkron.
    """
    await agg.add_event(event)
    return {"status": "accepted", "event_id": event.event_id}

@app.get("/events", response_model=List[ProcessedEvent])
async def get_events(topic: Optional[str] = None, agg: Aggregator = Depends(get_aggregator)):
    """
    Endpoint GET untuk mengembalikan daftar event unik yang telah diproses
    (diambil dari Deduplication Store/PostgreSQL).
    """
    return await asyncio.to_thread(agg.get_processed_events, topic) # Run DB fetch in a thread

@app.get("/stats", response_model=AggregatorStats)
async def get_stats(agg: Aggregator = Depends(get_aggregator)):
    """
    Endpoint GET untuk mengembalikan metrik operasional Aggregator.
    """
    return await asyncio.to_thread(agg.get_stats) # Run DB fetch in a thread