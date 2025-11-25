# src/publisher_sender.py
import httpx
import uuid
import datetime
import asyncio
import random
import time
import os

# --- Configuration from Environment ---
AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://aggregator:8080/publish")
NUM_EVENTS = int(os.getenv("NUM_EVENTS", "25000")) 
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.35"))

def create_event(event_id, is_duplicate=False):
    """
    Membuat struktur event JSON sesuai skema src/models.py.
    """
    # T5: Menggunakan datetime.timezone.utc untuk konsistensi waktu di sistem terdistribusi
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
    payload_content = "Initial log" if not is_duplicate else "Retry log - Duplicated"
    
    return {
        # Event Topics dibagi menjadi 3 level (T4)
        "topic": f"app.log.level-{random.randint(1, 3)}",
        "event_id": event_id,
        "timestamp": ts,
        "source": "publisher-service",
        "payload": {"content": payload_content, "seq": str(random.randint(100, 999))}
    }

async def send_events():
    """
    Mengirimkan sejumlah event secara asinkron ke Aggregator, 
    termasuk menyuntikkan duplikat sesuai DUPLICATE_RATE (T3).
    """
    unique_ids = {}
    
    # Meningkatkan timeout untuk mengakomodasi beban stress test yang lebih berat
    async with httpx.AsyncClient(timeout=30) as client: 
        print(f"--- Starting event submission to {AGGREGATOR_URL} ---")
        print(f"Configuration: {NUM_EVENTS} total events, {DUPLICATE_RATE*100:.0f}% duplicates.")
        
        start_time = time.monotonic() 
        
        for i in range(NUM_EVENTS):
            # Logika untuk memutuskan apakah event ini duplikat
            is_dup = random.random() < DUPLICATE_RATE and unique_ids
            
            if is_dup:
                # Pilih ID yang sudah pernah dikirim sebelumnya
                target_id = random.choice(list(unique_ids.keys()))
                event_data = create_event(target_id, is_duplicate=True)
            else:
                # Buat ID baru (T4)
                new_id = str(uuid.uuid4())
                unique_ids[new_id] = True
                event_data = create_event(new_id)

            try:
                # Pengiriman event (T3)
                response = await client.post(AGGREGATOR_URL, json=event_data)
                response.raise_for_status() # Raise exception for 4xx/5xx status codes
                
            except httpx.ConnectError:
                # T6: Failure Mode - Aggregator Down/Tidak Terjangkau
                print(f"\n!!! ERROR: Could not connect to aggregator at {AGGREGATOR_URL}. Aborting.")
                return
            except httpx.TimeoutException:
                # T6: Failure Mode - Timeout (mungkin harus retry/backoff)
                print(f"!!! WARNING: Request timed out for ID: {event_data['event_id']}")
            except httpx.HTTPStatusError as e:
                # T3: Publisher tidak peduli dengan 400/500, hanya mencatat pengiriman gagal
                print(f"!!! HTTP ERROR: Failed to publish ID {event_data['event_id']} (Status: {e.response.status_code})")
            
            # Mengurangi delay untuk mendorong throughput yang lebih tinggi
            await asyncio.sleep(0.0001) 

        end_time = time.monotonic() 
        
        total_send_time = end_time - start_time
        
        # --- Summary Section ---
        
        expected_unique_count = len(unique_ids)
        expected_duplicate_count = NUM_EVENTS - expected_unique_count
        
        print(f"\n--- Submission Completed ({NUM_EVENTS} events sent in {total_send_time:.2f}s) ---")
        
        wait_time = 20 # Waktu tunggu yang cukup lama untuk Aggregator memproses queue (T7)
        print(f"Waiting {wait_time} seconds for Aggregator to finalize queue processing...")
        await asyncio.sleep(wait_time) 
        
        
        total_time = time.monotonic() - start_time
        print("\n--- Performance Summary ---")
        print(f"Events Sent: {NUM_EVENTS}")
        print(f"Expected Unique IDs Generated: {expected_unique_count}")
        print(f"Expected Duplicates Sent: {expected_duplicate_count}")
        print(f"Total Test Window (Sending + Waiting): {total_time:.2f} seconds")


if __name__ == "__main__":
    print(f"--- Starting Performance Test for {NUM_EVENTS} Events ---")
    
    # Memberikan waktu tunggu awal yang cukup (10 detik) untuk Aggregator/Postgres siap
    print("Pre-wait for Aggregator/DB readiness (10s)...")
    asyncio.run(asyncio.sleep(10)) 
    
    asyncio.run(send_events())
    print("\n--- Test finished. Check /stats endpoint (http://localhost:8080/stats) for final accuracy. ---")