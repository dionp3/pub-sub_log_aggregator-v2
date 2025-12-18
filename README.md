# Pub-Sub Log Aggregator dengan Idempotent Consumer, Deduplikasi, dan Transaksi/Kontrol Konkurensi

Nama: Dion Prayoga\
NIM: 11221058

## Video Demo

[]

## Deskripsi Proyek

Layanan ini adalah sistem pengumpul log (Log Aggregator) berbasis arsitektur Publish-Subscribe. Sistem ini menjamin konsistensi data Exactly-Once Processing meskipun pengirim menggunakan mekanisme At-Least-Once Delivery.

Proyek ini menggunakan PostgreSQL sebagai Durable Deduplication Store untuk memastikan bahwa event dengan event_id dan topic yang sama tidak akan diproses dua kali, bahkan jika terjadi crash atau restart pada layanan.

Fitur Utama:
* Atomic Deduplication: Menggunakan kunci komposit (event_id, topic) dengan strategi ON CONFLICT DO NOTHING.

* Persistent Stats: Statistik (received, unique, duplicate) disimpan di database untuk mencegah lost updates pada skenario multi-worker.

* Concurrent Workers: Aggregator menggunakan beberapa consumer tasks asinkron untuk memproses antrean log secara paralel.

* Fault Tolerance: Menggunakan Docker Volumes untuk memastikan data log tetap aman meskipun container dihapus.

-----

## Arsitektur Sistem

Sistem berjalan di atas jaringan internal Docker Compose yang terisolasi:

1. Aggregator (FastAPI): Menerima event via HTTP POST, mengelola antrean internal (asyncio.Queue), dan menjalankan worker untuk menulis ke DB.

2. Storage (PostgreSQL 16): Menyimpan log unik dan metrik operasional.

3. Publisher (Simulator): Menghasilkan beban kerja (stress test) sebanyak 20.000+ event dengan rate duplikasi ~30%.

## Cara Menjalankan Layanan

## I. Eksekusi Layanan Docker

Pastikan Docker & Docker Compose sudah terinstal di sistem Anda.

1. **Clone dan Masuk ke Folder:**
```bash
cd pub-sub_log_aggregator-v2

```


2. **Jalankan Layanan:**
```bash
docker-compose up --build -d

```


* `aggregator` akan berjalan di port `8080`.
* `publisher` akan mulai mengirim 20.000 event setelah jeda startup 10 detik.


3. **Pantau Proses:**
```bash
docker logs -f log_aggregator

```

4. **Untuk Hapus Data:**
```bash
docker-compose down -v
```

-----

## II. Endpoint API Log Aggregator

Base URL: `http://localhost:8080`

| Metode | Path | Deskripsi | Contoh JSON Request/Response |
| :--- | :--- | :--- | :--- |
| `POST` | `/publish` | Menerima event dari publisher dan memasukkannya ke antrean pemrosesan. | **Request Body (JSON Event):**<br>`json{  "topic": "payment.notification",  "event_id": "3832d20e7-unique-payment-101",  "timestamp": "2025-10-21T10:00:00.000Z",  "source": "payment-gateway-A",  "payload": {    "content": "Transaksi sukses untuk user ID 456",    "metadata": {"amount": 500000}  }}`|
| `GET` | `/events` | Mengembalikan daftar event unik yang **telah berhasil diproses** (dari Deduplication Store). | Response: List JSON objek event. |
| `GET` | `/stats` | Mengembalikan metrik operasional sistem Aggregator. | Response: JSON objek statistik. |

-----

## III. Pengujian Unit & Integration Tests

Unit tests harus dijalankan setelah mengaktifkan *venv* dan sebelum *build* Docker.

  * **Set PYTHONPATH (Wajib di Windows):**
    ```powershell
    $env:PYTHONPATH="."
    ```
  * **Jalankan Pytest:**
    ```powershell
    pytest tests/test_aggregator.py
    ```

Pengujian dilakukan menggunakan **Pytest** dan membutuhkan database PostgreSQL yang sedang berjalan (test menggunakan database asli untuk akurasi driver `psycopg2`).

1. **Siapkan Environment:**
```bash
python -m venv venv
.\venv\Scripts\activate  # Windows
pip install -r requirements.txt

```


2. **Jalankan Test:**
```bash
$env:PYTHONPATH="."  # Windows PowerShell
pytest tests/test_aggregator.py -v

```

-----

### Unit Tests (Pytest)

Layanan Aggregator menjamin integritas data dan stabilitas sistem melalui 12 skenario pengujian komprehensif berbasis pytest dan pytest-asyncio. Rangkaian pengujian ini dirancang secara sistematis untuk memvalidasi mekanisme deduplikasi (idempotency), akurasi transaksi atomik pada multi-worker, serta toleransi kegagalan untuk memastikan persistensi data tetap terjaga meskipun terjadi gangguan pada layanan.

| Test ID | Fungsi yang Diuji | Deskripsi Singkat | Cakupan Rubrik |
| --- | --- | --- | --- |
| **Test 1** | `test_t1_deduplication_validity` | Memverifikasi **Idempotency** dengan mengirimkan dua event identik. Memastikan hanya 1 yang unik diproses dan 1 duplikat terdeteksi. | Dedup/Idempotency |
| **Test 2** | `test_t2_persistence_after_restart` | Menguji **Persistensi**. Memastikan data dalam *named volume* tetap ada sehingga instance baru tetap menolak duplikat dari masa lalu. | Toleransi Kegagalan |
| **Test 3** | `test_t3_event_schema_validation` | Memastikan **Pydantic** menolak request jika field wajib seperti `event_id` hilang (mengembalikan Status 422). | Validasi Skema |
| **Test 4** | `test_t4_get_stats_consistency` | Menguji akurasi `/stats`. Memastikan jumlah `received` dan daftar `topics` sesuai dengan jumlah data yang dikirimkan. | API, Observability |
| **Test 5** | `test_t5_get_events_with_topic_filter` | Memastikan endpoint `/events?topic=...` hanya mengembalikan event yang relevan dengan topik yang diminta. | API, Filtering |
| **Test 6** | `test_t6_stress_small_batch` | Menguji **Performa**. Memproses 100 event unik secara beruntun untuk memastikan waktu eksekusi berada di bawah 5 detik. | Performa |
| **Test 7** | `test_t7_concurrency_race_condition` | Menguji **Race Condition Duplikat**. Mengirim 5 duplikat secara simultan untuk memastikan database hanya mengizinkan 1 proses insert. | Transaksi/Konkurensi |
| **Test 8** | `test_t8_concurrency_unique_events` | Memastikan sistem mampu menangani 10 event unik yang masuk bersamaan (asinkron) tanpa ada data yang hilang. | Transaksi/Konkurensi |
| **Test 9** | `test_t9_composite_key_uniqueness` | Menguji **Composite Key**. Memastikan `event_id` yang sama bisa diterima jika dikirim ke `topic` yang berbeda. | Desain Database |
| **Test 10** | `test_t10_invalid_schema_timestamp` | Validasi tipe data. Memastikan sistem menolak request jika format `timestamp` tidak mengikuti standar ISO8601. | Validasi Skema |
| **Test 11** | `test_t11_get_events_empty_filter` | Memastikan endpoint `/events` tanpa parameter mengembalikan seluruh data unik dari semua topik yang tersedia. | API |
| **Test 12** | `test_t12_stats_uptime_accuracy` | Memastikan kalkulasi `uptime` pada statistik meningkat secara akurat (integer detik) seiring berjalannya waktu aplikasi. | Observability |

-----
