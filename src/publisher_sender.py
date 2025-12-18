import httpx
import uuid
import datetime
import asyncio
import random
import time
import os

AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://aggregator:8080/publish")
NUM_EVENTS = int(os.getenv("NUM_EVENTS", "20000")) 
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.30"))

def create_event(event_id, is_duplicate=False):

    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
    payload_content = "Initial log" if not is_duplicate else "Retry log - Duplicated"
    
    return {
        "topic": f"app.log.level-{random.randint(1, 3)}",
        "event_id": event_id,
        "timestamp": ts,
        "source": "publisher-service",
        "payload": {"content": payload_content, "seq": str(random.randint(100, 999))}
    }

async def send_events():

    unique_ids = set() 
    
    actual_unique_sent = 0 
    actual_duplicate_sent = 0
    
    async with httpx.AsyncClient(timeout=30) as client: 
        print(f"--- Starting event submission to {AGGREGATOR_URL} ---")
        print(f"Configuration: {NUM_EVENTS} total events, {DUPLICATE_RATE*100:.0f}% probability of duplication.")
        
        start_time = time.monotonic() 
        
        for i in range(NUM_EVENTS):
            
            # Logika duplikasi probabilistik
            is_dup = random.random() < DUPLICATE_RATE and unique_ids
            
            if is_dup:
                target_id = random.choice(list(unique_ids)) 
                event_data = create_event(target_id, is_duplicate=True)
                actual_duplicate_sent += 1 
            else:
                new_id = str(uuid.uuid4())
                unique_ids.add(new_id) 
                event_data = create_event(new_id)
                actual_unique_sent += 1 

            try:
                response = await client.post(AGGREGATOR_URL, json=event_data)
                response.raise_for_status() 
                
            except httpx.ConnectError:
                print(f"\n!!! ERROR: Could not connect to aggregator at {AGGREGATOR_URL}. Aborting.")
                return
            except httpx.TimeoutException:
                print(f"!!! WARNING: Request timed out for ID: {event_data['event_id']}")
            except httpx.HTTPStatusError as e:
                print(f"!!! HTTP ERROR: Failed to publish ID {event_data['event_id']} (Status: {e.response.status_code})")
            
            await asyncio.sleep(0.0001) 

        end_time = time.monotonic() 
        
        total_send_time = end_time - start_time
                
        print(f"\n--- Submission Completed ({NUM_EVENTS} events sent in {total_send_time:.2f}s) ---")
        
        wait_time = 20 
        print(f"Waiting {wait_time} seconds for Aggregator to finalize queue processing...")
        await asyncio.sleep(wait_time) 
        
        total_time = time.monotonic() - start_time
        print("\n--- Performance Summary ---")
        print(f"Events Sent: {NUM_EVENTS}")
        print(f"Total Send Time: {total_send_time:.2f} seconds")
        print(f"Approximate Send Rate: {NUM_EVENTS / total_send_time:.0f} events/sec")
        print(f"Total Test Window (Sending + Waiting): {total_time:.2f} seconds")


if __name__ == "__main__":
    NUM_EVENTS_DEFAULT = int(os.getenv("NUM_EVENTS", "20000")) 
    
    print(f"--- Starting Performance Test for {NUM_EVENTS_DEFAULT} Events ---")
    
    print("Pre-wait for Aggregator/DB readiness (10s)...")
    asyncio.run(asyncio.sleep(10)) 
    
    asyncio.run(send_events())
    print("\n--- Test finished. Check /stats endpoint (http://localhost:8080/stats) for final accuracy. ---")