# Pub-Sub Log Aggregator dengan Idempotent Consumer, Deduplikasi, dan Transaksi/Kontrol Konkurensi

Nama: Dion Prayoga
NIM: 11221058

## Deskripsi Proyek

Layanan ini mengimplementasikan Log Aggregator berbasis pola **Publish-Subscribe**. Sistem dirancang untuk menangani *at-least-once delivery* dari *publisher* dengan menjamin **exactly-once processing** melalui **Idempotent Consumer** dan **Durable Deduplication Store (SQLite)** yang persisten di dalam *container* Docker.

-----

## Cara Menjalankan Layanan

### Persyaratan

  * **Python 3.11+** terinstal (untuk *unit testing* lokal dan *venv*).
  * Docker Desktop (Windows/Linux) terinstal dan berjalan.
  * Terminal Windows (PowerShell/CMD).

-----

## I. Persiapan Virtual Environment

Anda harus mengaktifkan *virtual environment* sebelum melakukan *build* atau *testing* di *host*.

1.  **Buat Virtual Environment:**

    ```powershell
    python -m venv venv
    ```

2.  **Aktifkan Environment:**

    ```powershell
    .\venv\Scripts\activate
    ```

3.  **Instal Dependensi (Termasuk Pytest):**

    ```powershell
    pip install -r requirements.txt
    ```

-----

## II. Eksekusi Layanan Docker

### A. Opsi Wajib: Single Container (Aggregator Saja)

Ini menjalankan Aggregator dan mengasumsikan *Publisher* adalah Postman/cURL dari *host*.

1.  **Build Image:** (Pastikan `venv` aktif)

    ```powershell
    docker build -t uts-aggregator .
    ```

2.  **Run Container:**

    ```powershell
    docker run -d --name aggregator-service -p 8080:8080 uts-aggregator
    ```

    (Layanan dapat diakses di `http://localhost:8080` dan gunakan Endpoint API Log Aggregator).

### B. Opsi Bonus: Docker Compose (Aggregator & Publisher)

Ini menjalankan **dua *service* terpisah** dan mensimulasikan lalu lintas log otomatis.

1.  **Run Services:**

    ```powershell
    docker-compose up --build -d
    ```

      * Container `log_aggregator` akan diekspos di `http://localhost:8080`.
      * Container `log_publisher` akan otomatis mengirim event ke jaringan internal dan keluar.

2.  **Monitor Log (Opsional):**

    ```powershell
    docker logs log_aggregator -f
    ```

-----

### Endpoint API Log Aggregator

Base URL: `http://localhost:8080`

| Metode | Path | Deskripsi | Contoh JSON Request/Response |
| :--- | :--- | :--- | :--- |
| `POST` | `/publish` | Menerima event dari publisher dan memasukkannya ke antrean pemrosesan. | **Request Body (JSON Event):**<br>`json{  "topic": "payment.notification",  "event_id": "3832d20e7-unique-payment-101",  "timestamp": "2025-10-21T10:00:00.000Z",  "source": "payment-gateway-A",  "payload": {    "content": "Transaksi sukses untuk user ID 456",    "metadata": {"amount": 500000}  }}`|
| `GET` | `/events` | Mengembalikan daftar event unik yang **telah berhasil diproses** (dari Deduplication Store). | Response: List JSON objek event. |
| `GET` | `/stats` | Mengembalikan metrik operasional sistem Aggregator. | Response: JSON objek statistik. |

-----

### Uji Performa dan Stress Test (Publisher Lokal)

Skrip `src/publisher_sender.py` akan mengirim **5.000 event** (dengan 20% duplikasi) ke Aggregator untuk mengukur *responsiveness*.

1.  **Eksekusi Skrip Publisher:**

    ```bash
    python src/publisher_sender.py
    ```

      * Publisher akan menunggu 10 detik agar Aggregator siap, kemudian mengirim semua event secara asinkron.
      * Publisher akan mencetak **Total Uptime/Processing Window** setelah selesai.

-----

## III. Menjalankan Unit Tests (Opsional di Host)

Unit tests harus dijalankan setelah mengaktifkan *venv* dan sebelum *build* Docker.

  * **Set PYTHONPATH (Wajib di Windows):**
    ```powershell
    $env:PYTHONPATH="."
    ```
  * **Jalankan Pytest:**
    ```powershell
    pytest tests/test_aggregator.py
    ```

-----

### Unit Tests (Pytest)

Layanan Aggregator dijamin berfungsi dengan benar dan stabil melalui 6 *unit tests* berbasis `pytest` dan `pytest-asyncio`. Pengujian berfokus pada validasi *state* sistem, *idempotency*, dan *toleransi kegagalan*.

| Test ID | Fungsi yang Diuji | Deskripsi Singkat | Cakupan Rubrik |
| :--- | :--- | :--- | :--- |
| **Test 1** | `test_t1_deduplication_validity` | Memverifikasi **Idempotency** dengan mengirimkan dua *event* yang identik (`event\_id` sama). Memastikan **`unique_processed`** adalah 1 dan **`duplicate_dropped`** adalah 1. | Dedup/Idempotency, API, Stats |
| **Test 2** | `test_t2_persistence_after_restart` | Menguji **Toleransi Kegagalan** dan **Persistensi Dedup Store**. Mensimulasikan *restart* (*instance* Aggregator baru) setelah event diproses, lalu mengirimkan duplikat event lama. Memastikan *instance* baru menolak event tersebut. | Idempotency, Toleransi Kegagalan |
| **Test 3** | `test_t3_event_schema_validation` | Memastikan validasi skema **Pydantic** pada *endpoint* `/publish` bekerja dengan benar, khususnya menolak *event* yang tidak memiliki `event_id` (kunci unik). | Validasi Skema |
| **Test 4** | `test_t4_get_stats_consistency` | Menguji **Konsistensi Statistik** dan *state cleanup* antar *test*. Memastikan `received`, `unique_processed`, `duplicate_dropped`, dan `topics` akurat setelah pengiriman dua *event* unik. | API, Stats |
| **Test 5** | `test_t5_get_events_with_topic_filter` | Menguji fungsi *filtering* pada *endpoint* `/events?topic=...`. Memastikan deduplikasi dan filter *topic* bekerja secara simultan, hanya mengembalikan event unik yang sesuai dengan filter. | API, Dedup |
| **Test 6** | `test_t6_stress_small_batch` | Menguji *baseline* **Performa** dan *state* sistem saat memproses $100$ *event* secara berturut-turut. Memastikan waktu eksekusi berada di bawah batas wajar ($< 5$ detik). | Performa |

-----

## Video Demo

[https://youtu.be/ya2SLrltG2I]