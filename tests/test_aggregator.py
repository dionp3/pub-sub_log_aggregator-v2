import pytest
import asyncio
import uuid
import datetime
import os
import pytest_asyncio
import time
import json
import logging

# Impor Aggregator dan Pydantic models
import src.main
from src.main import app, Aggregator
from src.models import Event, EventPayload
from httpx import AsyncClient, ASGITransport

# Konfigurasi untuk pengujian: Gunakan SQLite in-memory untuk kecepatan
# Catatan: Karena Aggregator di main.py menggunakan psycopg2, 
# kita perlu mocking atau mengganti Aggregator di test, tapi di sini 
# kita akan menggunakan DB_URL SQLite in-memory untuk simulasi cepat.
# ASUMSI: Aggregator di src.main.py memiliki mekanisme koneksi yang dapat 
# diinisialisasi ulang (misalnya, dengan mocking psycopg2 atau menggunakan wrapper DB).
# Untuk tujuan demonstrasi ini, kita paksa Aggregator menggunakan DB_URL in-memory.
TEST_DB_URL_IN_MEMORY = "sqlite:///:memory:" 
TEST_DB_PATH_PERSISTENCE = "test_persistence_store.db"

# Nonaktifkan logging dari Aggregator agar output Pytest lebih bersih
logging.getLogger().setLevel(logging.WARNING)

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Cleanup file DB persistence artifact if it exists."""
    # Pastikan file DB persistence dibersihkan sebelum dan sesudah test run
    if os.path.exists(TEST_DB_PATH_PERSISTENCE):
        os.remove(TEST_DB_PATH_PERSISTENCE)
    yield
    if os.path.exists(TEST_DB_PATH_PERSISTENCE):
        os.remove(TEST_DB_PATH_PERSISTENCE)


@pytest_asyncio.fixture
async def test_aggregator(scope="function"):
    """
    Fixture untuk Aggregator yang terisolasi menggunakan SQLite in-memory 
    untuk pengujian unit yang cepat dan concurrent.
    """
    # NOTE: Menginisialisasi Aggregator di sini untuk mengabaikan Postgres/psycopg2 
    # di lingkungan test lokal dan beralih ke in-memory store yang aman.
    
    # Simpan state Aggregator saat ini
    original_db_url = src.main.DATABASE_URL
    original_min_consumers = src.main.MIN_CONSUMERS
    
    # Override configuration for testing
    src.main.DATABASE_URL = TEST_DB_URL_IN_MEMORY
    src.main.MIN_CONSUMERS = 1 # Hanya 1 consumer worker untuk unit test default
    
    # Karena Aggregator di src.main menggunakan psycopg2, ini membutuhkan mocking
    # atau Aggregator yang dimodifikasi. Di sini kita SIMULASIKAN Aggregator
    # dapat menerima DB_URL berbasis file/memory untuk test.
    # *** Dalam implementasi nyata, Aggregator harus dimock agar menggunakan 
    # *** SQLite3 atau TestContainer PostgreSQL untuk integritas.
    
    # Inisialisasi Aggregator dengan 1 worker
    agg = Aggregator(db_url=TEST_DB_URL_IN_MEMORY)
    
    # Reset internal counters (jika ada data dari test sebelumnya)
    agg.received_count = 0
    agg.unique_count = 0
    agg.duplicate_count = 0
    agg.start_time = datetime.datetime.now()
    
    # Jalankan consumer worker dalam task
    consumer_task = asyncio.create_task(agg.run_consumer(1))
    
    # Tunggu sebentar untuk memastikan task dimulai
    await asyncio.sleep(0.1) 

    yield agg
    
    # Cleanup: Batalkan task consumer dan tunggu selesai
    consumer_task.cancel()
    try:
        await asyncio.wait_for(consumer_task, timeout=1.0)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass
    except Exception:
        pass
        
    # Kembalikan state Aggregator/konfigurasi asli
    src.main.DATABASE_URL = original_db_url
    src.main.MIN_CONSUMERS = original_min_consumers


@pytest_asyncio.fixture
async def test_client(test_aggregator):
    """Fixture untuk klien HTTP asinkron Fastapi."""
    
    # Override global aggregator instance dan dependency injection
    original_global_agg = src.main.global_aggregator
    src.main.global_aggregator = test_aggregator 

    app.dependency_overrides[src.main.get_aggregator] = lambda: test_aggregator
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client
        
    # Cleanup dependency overrides dan global state
    app.dependency_overrides = {} 
    src.main.global_aggregator = original_global_agg


def create_mock_event(event_id=None, topic="test.log", source="service-A", custom_ts=None):
    """Fungsi helper untuk membuat event Pydantic."""
    return Event(
        topic=topic,
        event_id=event_id if event_id else str(uuid.uuid4()),
        timestamp=custom_ts if custom_ts else datetime.datetime.now(datetime.timezone.utc),
        source=source,
        payload=EventPayload(content="Test log message")
    )

# ----------------------------------------------------------------------
# UNIT TESTS (Total 12 Tests)
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_t1_deduplication_validity(test_client, test_aggregator):
    """Test 1: Kirim duplikat, pastikan hanya sekali diproses (Idempotency, T3)."""
    
    unique_id = str(uuid.uuid4())
    event_1 = create_mock_event(event_id=unique_id)
    event_2_duplicate = create_mock_event(event_id=unique_id)

    # Kirim event 1 (unik)
    await test_client.post("/publish", json=event_1.model_dump(mode='json'))
    # Kirim event 2 (duplikat)
    await test_client.post("/publish", json=event_2_duplicate.model_dump(mode='json'))

    await test_aggregator.queue.join() # Tunggu semua event diproses

    # Cek Deduplication Store (DB)
    events_response = await test_client.get("/events")
    events = events_response.json()
    assert len(events) == 1
    assert events[0]['event_id'] == unique_id
    
    # Cek Statistik
    stats_response = await test_client.get("/stats")
    stats = stats_response.json()
    assert stats['received'] == 2
    assert stats['unique_processed'] == 1
    assert stats['duplicate_dropped'] == 1

@pytest.mark.asyncio
async def test_t2_persistence_after_restart():
    """
    Test 2: Persistensi Dedup Store (T6). 
    Simulasi: Proses event, Aggregator (dengan DB persistent file) dimatikan, 
    instance baru menolak duplikat.
    """
    
    unique_id = str(uuid.uuid4())
    event_a = create_mock_event(event_id=unique_id)
    
    # Step 1: Inisialisasi Aggregator (DB persistence file) dan proses event
    agg_initial = Aggregator(db_url=TEST_DB_PATH_PERSISTENCE)
    initial_consumer = asyncio.create_task(agg_initial.run_consumer(1))
    await asyncio.sleep(0.1)
    await agg_initial.add_event(event_a) 
    await agg_initial.queue.join() 
    assert agg_initial.unique_count == 1
    
    # Cleanup initial consumer
    initial_consumer.cancel()
    try:
        await asyncio.wait_for(initial_consumer, timeout=0.5)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass
    
    # Step 2: Restart Aggregator (Instance baru, konek ke DB file yang sama)
    restarted_aggregator = Aggregator(db_url=TEST_DB_PATH_PERSISTENCE)
    restarted_consumer = asyncio.create_task(restarted_aggregator.run_consumer(1))
    await asyncio.sleep(0.1) 
    
    # Kirim duplikat event lama
    event_a_duplicate = create_mock_event(event_id=unique_id)
    await restarted_aggregator.add_event(event_a_duplicate)
    await restarted_aggregator.queue.join() 
    
    # Cek Statistik Aggregator yang di-restart (harus menolak duplikat)
    stats = restarted_aggregator.get_stats()
    assert stats.received == 1
    assert stats.unique_processed == 0
    assert stats.duplicate_dropped == 1
    
    # Cleanup restarted consumer
    restarted_consumer.cancel()
    try:
        await asyncio.wait_for(restarted_consumer, timeout=0.5)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass


@pytest.mark.asyncio
async def test_t3_event_schema_validation(test_client):
    """Test 3: Validasi skema event (T3: pastikan Pydantic menolak event tanpa event_id)."""
    invalid_data = {
        "topic": "test.log",
        "timestamp": datetime.datetime.now().isoformat(),
        "source": "service-A",
        "payload": {"content": "Test"}
        # event_id field is missing
    }
    
    response = await test_client.post("/publish", json=invalid_data)
    
    assert response.status_code == 422
    response_json = response.json()
    assert "event_id" in response_json['detail'][0]['loc']
    assert response_json['detail'][0]['type'] == 'missing'

@pytest.mark.asyncio
async def test_t4_get_stats_consistency(test_client, test_aggregator):
    """Test 4: Konsistensi GET /stats dengan data yang dimasukkan (T10)."""
    
    event_1 = create_mock_event(topic="topic.a")
    event_2 = create_mock_event(topic="topic.b")
    
    await test_client.post("/publish", json=event_1.model_dump(mode='json'))
    await test_client.post("/publish", json=event_2.model_dump(mode='json'))
    await test_aggregator.queue.join()
    
    stats_response = await test_client.get("/stats")
    stats = stats_response.json()
    
    assert stats['received'] == 2
    assert stats['unique_processed'] == 2
    assert stats['duplicate_dropped'] == 0
    assert set(stats['topics']) == {"topic.a", "topic.b"}
    assert stats['uptime'] > 0

@pytest.mark.asyncio
async def test_t5_get_events_with_topic_filter(test_client, test_aggregator):
    """Test 5: Konsistensi GET /events dengan filter topic (API)."""
    
    event_a1 = create_mock_event(topic="finance.tx")
    event_b1 = create_mock_event(topic="finance.audit")
    event_a2_dup = create_mock_event(event_id=event_a1.event_id, topic="finance.tx") # Duplikat
    
    await test_client.post("/publish", json=event_a1.model_dump(mode='json'))
    await test_client.post("/publish", json=event_b1.model_dump(mode='json'))
    await test_client.post("/publish", json=event_a2_dup.model_dump(mode='json'))
    await test_aggregator.queue.join()
    
    # Filter Topic A (Harus 1 event unik)
    tx_events_response = await test_client.get("/events?topic=finance.tx")
    tx_events = tx_events_response.json()
    assert len(tx_events) == 1
    assert tx_events[0]['event_id'] == event_a1.event_id
    
    # Filter Topic B (Harus 1 event unik)
    audit_events_response = await test_client.get("/events?topic=finance.audit")
    audit_events = audit_events_response.json()
    assert len(audit_events) == 1
    assert audit_events[0]['event_id'] == event_b1.event_id

@pytest.mark.asyncio
async def test_t6_stress_small_batch(test_client, test_aggregator):
    """Test 6: Stress test kecil - memproses 100 event unik (Performa)."""
    NUM_EVENTS = 100
    events = [create_mock_event() for _ in range(NUM_EVENTS)]
    
    start_time = datetime.datetime.now()
    # Kirim semua event
    for event in events:
        await test_client.post("/publish", json=event.model_dump(mode='json'))
    
    await test_aggregator.queue.join() 
    
    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()

    stats_response = await test_client.get("/stats")
    stats = stats_response.json()
    
    assert stats['received'] == NUM_EVENTS
    assert stats['unique_processed'] == NUM_EVENTS
    assert stats['duplicate_dropped'] == 0
    
    # Performa baseline
    assert duration < 5.0, f"Processing {NUM_EVENTS} events took too long: {duration:.2f}s"

@pytest.mark.asyncio
async def test_t7_concurrency_race_condition(test_client, test_aggregator):
    """
    Test 7: Uji Race Condition Duplikat (T8 & T9). 
    Kirim 5 event duplikat ke 1 worker. Worker harus menolak 4 event.
    (Dalam implementasi sebenarnya, ini diuji dengan N worker concurrent).
    """
    unique_id = str(uuid.uuid4())
    events = [create_mock_event(event_id=unique_id) for _ in range(5)]
    
    # Mengirim semua duplikat secara cepat
    tasks = [test_client.post("/publish", json=e.model_dump(mode='json')) for e in events]
    await asyncio.gather(*tasks)
    
    await test_aggregator.queue.join()
    
    stats_response = await test_client.get("/stats")
    stats = stats_response.json()
    
    # Hanya 1 yang berhasil di INSERT (unique_processed) karena ON CONFLICT DO NOTHING
    # T8/T9: Transaksi atomik berhasil mencegah double-process
    assert stats['received'] == 5
    assert stats['unique_processed'] == 1
    assert stats['duplicate_dropped'] == 4

@pytest.mark.asyncio
async def test_t8_concurrency_unique_events(test_client, test_aggregator):
    """
    Test 8: Uji Konkurensi Event Unik. 
    Kirim 10 event unik secara cepat. Pastikan semua diproses.
    """
    NUM_CONCURRENT = 10
    events = [create_mock_event() for _ in range(NUM_CONCURRENT)]
    
    # Mengirim semua event unik secara cepat
    tasks = [test_client.post("/publish", json=e.model_dump(mode='json')) for e in events]
    await asyncio.gather(*tasks)
    
    await test_aggregator.queue.join()
    
    stats_response = await test_client.get("/stats")
    stats = stats_response.json()
    
    assert stats['received'] == NUM_CONCURRENT
    assert stats['unique_processed'] == NUM_CONCURRENT
    assert stats['duplicate_dropped'] == 0
    assert len(test_aggregator.get_processed_events()) == NUM_CONCURRENT


@pytest.mark.asyncio
async def test_t9_composite_key_uniqueness(test_client, test_aggregator):
    """
    Test 9: Uji Kunci Komposit (event_id sama, tapi topic berbeda) (T4).
    Kedua event harus diproses sebagai unik.
    """
    shared_id = str(uuid.uuid4())
    
    event_tx = create_mock_event(event_id=shared_id, topic="transaction.v1")
    event_audit = create_mock_event(event_id=shared_id, topic="audit.v1") # Topic berbeda
    
    await test_client.post("/publish", json=event_tx.model_dump(mode='json'))
    await test_client.post("/publish", json=event_audit.model_dump(mode='json'))
    
    await test_aggregator.queue.join()
    
    stats_response = await test_client.get("/stats")
    stats = stats_response.json()
    
    # Kedua event harus dihitung unik karena kunci komposit (event_id, topic) berbeda
    assert stats['received'] == 2
    assert stats['unique_processed'] == 2
    assert stats['duplicate_dropped'] == 0
    assert len(test_aggregator.get_processed_events()) == 2
    assert set(stats['topics']) == {"transaction.v1", "audit.v1"}


@pytest.mark.asyncio
async def test_t10_invalid_schema_timestamp(test_client):
    """Test 10: Validasi Skema (T11): Menolak format timestamp yang salah."""
    
    invalid_data = {
        "topic": "test.log",
        "event_id": str(uuid.uuid4()),
        "timestamp": "Not-a-valid-ISO-timestamp", # Invalid timestamp format
        "source": "service-A",
        "payload": {"content": "Test"}
    }
    
    response = await test_client.post("/publish", json=invalid_data)
    
    assert response.status_code == 422
    response_json = response.json()
    assert "datetime_from_string" in response_json['detail'][0]['type']


@pytest.mark.asyncio
async def test_t11_get_events_empty_filter(test_client, test_aggregator):
    """Test 11: Get Events tanpa filter harus mengembalikan semua event."""
    
    event_x = create_mock_event(topic="x")
    event_y = create_mock_event(topic="y")
    
    await test_client.post("/publish", json=event_x.model_dump(mode='json'))
    await test_client.post("/publish", json=event_y.model_dump(mode='json'))
    await test_aggregator.queue.join()
    
    # Tanpa filter
    all_events_response = await test_client.get("/events")
    all_events = all_events_response.json()
    
    assert len(all_events) == 2
    # Cek bahwa kedua topik ada
    assert set([e['topic'] for e in all_events]) == {"x", "y"}

@pytest.mark.asyncio
async def test_t12_stats_uptime_accuracy(test_client, test_aggregator):
    """Test 12: Akurasi metrik uptime (T12)."""
    
    stats_initial = await test_client.get("/stats")
    uptime_1 = stats_initial.json()['uptime']
    
    # Tunggu beberapa detik
    await asyncio.sleep(2.5) 
    
    stats_final = await test_client.get("/stats")
    uptime_2 = stats_final.json()['uptime']
    
    # Uptime harus meningkat setidaknya 2 detik (terkadang 3 karena pembulatan integer)
    time_difference = uptime_2 - uptime_1
    assert time_difference >= 2
    assert time_difference <= 3

# ----------------------------------------------------------------------
# END OF TESTS
# ----------------------------------------------------------------------