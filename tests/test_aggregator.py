import pytest
import asyncio
import uuid
import datetime
import psycopg2
import os
import pytest_asyncio
import logging
from httpx import AsyncClient, ASGITransport

# Impor komponen internal
import src.main
from src.main import app, Aggregator
from src.models import Event, EventPayload

# --- KONFIGURASI ---
TEST_DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://log_user:log_password@127.0.0.1:5432/log_db"
)

# Nonaktifkan logging berlebih agar output pytest bersih
logging.getLogger().setLevel(logging.ERROR)

# --- FIXTURES ---

@pytest_asyncio.fixture(autouse=True)
async def clean_database():
    """Memastikan skema tabel siap dan data bersih sebelum setiap test."""
    conn = psycopg2.connect(TEST_DATABASE_URL)
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        # Inisialisasi Tabel Utama
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                PRIMARY KEY (event_id, topic)
            );
        """)
        # Inisialisasi Tabel Statistik
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aggregator_stats (
                key TEXT PRIMARY KEY,
                value BIGINT DEFAULT 0
            );
            INSERT INTO aggregator_stats (key, value)
            VALUES ('received', 0), ('unique_processed', 0), ('duplicate_dropped', 0)
            ON CONFLICT DO NOTHING;
        """)
        
        # Bersihkan data untuk isolasi antar test
        cursor.execute("TRUNCATE TABLE processed_events RESTART IDENTITY CASCADE;")
        cursor.execute("UPDATE aggregator_stats SET value = 0;")
    finally:
        cursor.close()
        conn.close()
    yield

@pytest_asyncio.fixture
async def test_aggregator():
    """Menyediakan instance Aggregator dengan satu worker aktif."""
    agg = Aggregator(db_url=TEST_DATABASE_URL)
    task = asyncio.create_task(agg.run_consumer(1))
    await asyncio.sleep(0.2)
    yield agg
    task.cancel()

@pytest_asyncio.fixture
async def test_client(test_aggregator):
    """Menyediakan klien HTTP untuk FastAPI dengan dependency injection."""
    app.dependency_overrides[src.main.get_aggregator] = lambda: test_aggregator
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client
    app.dependency_overrides = {}

# --- HELPERS ---

def create_mock_event(event_id=None, topic="test.log"):
    """Membuat objek Event valid untuk keperluan testing."""
    return Event(
        topic=topic,
        event_id=event_id or str(uuid.uuid4()),
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        source="unit-test-service",
        payload=EventPayload(content="Test log message")
    )

# --- TEST CASES ---

@pytest.mark.asyncio
async def test_t1_deduplication_validity(test_client, test_aggregator):
    """Menguji apakah duplikat dengan ID yang sama dibuang (Idempotency)."""
    unique_id = str(uuid.uuid4())
    event = create_mock_event(event_id=unique_id)

    await test_client.post("/publish", json=event.model_dump(mode='json'))
    await test_client.post("/publish", json=event.model_dump(mode='json'))

    await test_aggregator.queue.join()

    # Pastikan hanya 1 yang tersimpan di database
    events = (await test_client.get("/events")).json()
    assert len(events) == 1
    
    # Verifikasi statistik
    stats = (await test_client.get("/stats")).json()
    assert stats['unique_processed'] == 1
    assert stats['duplicate_dropped'] == 1

@pytest.mark.asyncio
async def test_t2_persistence_after_restart():
    """Menguji apakah state tetap aman di database meski aggregator restart."""
    unique_id = str(uuid.uuid4())
    event = create_mock_event(event_id=unique_id)
    
    # Jalankan aggregator pertama, lalu matikan
    agg_initial = Aggregator(db_url=TEST_DATABASE_URL)
    task = asyncio.create_task(agg_initial.run_consumer(1))
    await agg_initial.add_event(event)
    await agg_initial.queue.join()
    assert agg_initial.get_stats().unique_processed == 1
    task.cancel()
    
    # Jalankan aggregator baru (restart), kirim duplikat
    restarted_agg = Aggregator(db_url=TEST_DATABASE_URL)
    task_new = asyncio.create_task(restarted_agg.run_consumer(1))
    await restarted_agg.add_event(event)
    await restarted_agg.queue.join()
    
    assert restarted_agg.get_stats().duplicate_dropped == 1
    task_new.cancel()

@pytest.mark.asyncio
async def test_t3_event_schema_validation(test_client):
    """Menguji validasi skema (Pydantic) saat field wajib hilang."""
    invalid_data = {"topic": "test", "source": "service-A"} # Tanpa event_id
    response = await test_client.post("/publish", json=invalid_data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_t4_get_stats_consistency(test_client, test_aggregator):
    """Memastikan konsistensi perhitungan statistik received vs unique."""
    await test_client.post("/publish", json=create_mock_event(topic="t.a").model_dump(mode='json'))
    await test_client.post("/publish", json=create_mock_event(topic="t.b").model_dump(mode='json'))
    await test_aggregator.queue.join()
    
    stats = (await test_client.get("/stats")).json()
    assert stats['received'] == 2
    assert set(stats['topics']) == {"t.a", "t.b"}

@pytest.mark.asyncio
async def test_t5_get_events_with_topic_filter(test_client, test_aggregator):
    """Menguji fitur filtering berdasarkan topik pada API."""
    await test_client.post("/publish", json=create_mock_event(topic="A").model_dump(mode='json'))
    await test_client.post("/publish", json=create_mock_event(topic="B").model_dump(mode='json'))
    await test_aggregator.queue.join()
    
    response = await test_client.get("/events?topic=A")
    data = response.json()
    assert len(data) == 1
    assert data[0]['topic'] == "A"

@pytest.mark.asyncio
async def test_t6_stress_small_batch(test_client, test_aggregator):
    """Menguji kecepatan pemrosesan 100 event unik."""
    NUM = 100
    start = datetime.datetime.now()
    for _ in range(NUM):
        await test_client.post("/publish", json=create_mock_event().model_dump(mode='json'))
    
    await test_aggregator.queue.join()
    duration = (datetime.datetime.now() - start).total_seconds()
    assert duration < 5.0

@pytest.mark.asyncio
async def test_t7_concurrency_race_condition(test_client, test_aggregator):
    """Menguji penanganan race condition saat banyak duplikat masuk serentak."""
    unique_id = str(uuid.uuid4())
    tasks = [
        test_client.post("/publish", json=create_mock_event(event_id=unique_id).model_dump(mode='json'))
        for _ in range(5)
    ]
    await asyncio.gather(*tasks)
    await test_aggregator.queue.join()
    
    stats = (await test_client.get("/stats")).json()
    assert stats['unique_processed'] == 1
    assert stats['duplicate_dropped'] == 4

@pytest.mark.asyncio
async def test_t8_concurrency_unique_events(test_client, test_aggregator):
    """Menguji apakah semua event unik masuk saat dikirim secara konkuren."""
    NUM = 10
    tasks = [
        test_client.post("/publish", json=create_mock_event().model_dump(mode='json'))
        for _ in range(NUM)
    ]
    await asyncio.gather(*tasks)
    await test_aggregator.queue.join()
    
    stats = (await test_client.get("/stats")).json()
    assert stats['unique_processed'] == NUM

@pytest.mark.asyncio
async def test_t9_composite_key_uniqueness(test_client, test_aggregator):
    """Menguji kunci komposit (ID sama diperbolehkan jika topik berbeda)."""
    shared_id = str(uuid.uuid4())
    await test_client.post("/publish", json=create_mock_event(event_id=shared_id, topic="T1").model_dump(mode='json'))
    await test_client.post("/publish", json=create_mock_event(event_id=shared_id, topic="T2").model_dump(mode='json'))
    
    await test_aggregator.queue.join()
    stats = (await test_client.get("/stats")).json()
    assert stats['unique_processed'] == 2

@pytest.mark.asyncio
async def test_t10_invalid_schema_timestamp(test_client):
    """Memastikan sistem menolak format timestamp yang tidak valid."""
    event = create_mock_event().model_dump(mode='json')
    event["timestamp"] = "invalid-date"
    response = await test_client.post("/publish", json=event)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_t11_get_events_empty_filter(test_client, test_aggregator):
    """Memastikan GET /events tanpa parameter mengembalikan semua data."""
    await test_client.post("/publish", json=create_mock_event(topic="X").model_dump(mode='json'))
    await test_client.post("/publish", json=create_mock_event(topic="Y").model_dump(mode='json'))
    await test_aggregator.queue.join()
    
    response = await test_client.get("/events")
    assert len(response.json()) == 2

@pytest.mark.asyncio
async def test_t12_stats_uptime_accuracy(test_client, test_aggregator):
    """Memastikan metrik uptime bertambah seiring berjalannya waktu."""
    t1 = (await test_client.get("/stats")).json()['uptime']
    await asyncio.sleep(2)
    t2 = (await test_client.get("/stats")).json()['uptime']
    assert (t2 - t1) >= 2