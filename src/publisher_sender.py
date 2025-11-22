# src/publisher_sender.py
import httpx
import uuid
import datetime
import asyncio
import random
import time

AGGREGATOR_URL = "http://aggregator:8080/publish"
NUM_EVENTS = 20000
DUPLICATE_RATE = 0.30 

def create_event(event_id, is_duplicate=False):
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
    payload = "Initial log" if not is_duplicate else "Retry log"
    
    return {
        "topic": f"app.log.level-{random.randint(1, 3)}",
        "event_id": event_id,
        "timestamp": ts,
        "source": "publisher-service",
        "payload": {"content": payload, "seq": str(random.randint(100, 999))}
    }

async def send_events():
    unique_ids = {}
    
    async with httpx.AsyncClient(timeout=10) as client:
        print(f"--- Starting event submission to {AGGREGATOR_URL} ---")
        
        start_time = time.monotonic() 
        
        for i in range(NUM_EVENTS):
            is_dup = random.random() < DUPLICATE_RATE and unique_ids
            
            if is_dup:
                target_id = random.choice(list(unique_ids.keys()))
                event_data = create_event(target_id, is_duplicate=True)
                # print(f"-> Sending DUPLICATE: {target_id}")
            else:
                new_id = str(uuid.uuid4())
                unique_ids[new_id] = True
                event_data = create_event(new_id)
                # print(f"-> Sending UNIQUE: {new_id}")

            try:
                response = await client.post(AGGREGATOR_URL, json=event_data)
                # print(f"   [Response] Status: {response.status_code}, ID: {event_data['event_id']}")
                
            except httpx.ConnectError:
                print(f"!!! ERROR: Could not connect to aggregator at {AGGREGATOR_URL}.")
                return
            except httpx.TimeoutException:
                print("!!! WARNING: Request timed out.")
            
            await asyncio.sleep(0.001) 

        end_time = time.monotonic() 
        
        total_send_time = end_time - start_time
        
        print(f"\n--- Submission Completed ({NUM_EVENTS} events sent in {total_send_time:.2f}s) ---")
        
        wait_time = 15 
        print(f"Waiting {wait_time} seconds for Aggregator to finalize queue processing...")
        await asyncio.sleep(wait_time)
        
        
        total_time = time.monotonic() - start_time
        print("\n--- Performance Summary ---")
        print(f"Status: Events Sent: {NUM_EVENTS}. Expected Duplicates: {int(NUM_EVENTS * DUPLICATE_RATE)}")
        print(f"Total Uptime/Processing Window: {total_time:.2f} seconds")


if __name__ == "__main__":
    print(f"--- Starting Performance Test for {NUM_EVENTS} Events ---")
    asyncio.run(asyncio.sleep(10)) 
    asyncio.run(send_events())
    print("--- Test finished. Check /stats endpoint for final accuracy. ---")