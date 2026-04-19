import sys
import os
import json
import time
import argparse
import threading
import queue
from confluent_kafka import Producer

sys.path.insert(0, os.path.dirname(__file__))
from patient_generator import generate_patient_event
from database import get_connection

KAFKA_BOOTSTRAP = "localhost:9092"
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
db_queue = queue.Queue()

def delivery_report(err, msg):
    if err: print(f"[KAFKA ERROR] {err}")

def db_worker():
    """Background thread that uses ONE continuous connection to Databricks."""
    print("[DB WORKER] Connecting to Databricks...")
    con = get_connection()
    cursor = con.cursor()
    print("[DB WORKER] Connected. Listening for events in the background...")
    
    while True:
        event = db_queue.get()
        if event is None: # Stop signal
            break
            
        try:
            # Check columns dynamically
            try:
                cursor.execute("SHOW COLUMNS IN healthcare_db.ehr_stream")
                valid_cols = [row[0] for row in cursor.fetchall()]
            except:
                cursor.execute("PRAGMA table_info('healthcare_db.ehr_stream')")
                valid_cols = [row[1] for row in cursor.fetchall()]
                if not valid_cols:
                    cursor.execute("PRAGMA table_info('ehr_stream')")
                    valid_cols = [row[1] for row in cursor.fetchall()]

            if not valid_cols:
                valid_cols = list(event.keys())

            safe_event = {k: v for k, v in event.items() if k in valid_cols}
            if "spo2" in valid_cols and "spo2" not in safe_event:
                safe_event["spo2"] = None

            columns = ", ".join(safe_event.keys())
            placeholders = ", ".join(["?"] * len(safe_event))
            values = tuple(safe_event.values())
            
            try:
                cursor.execute(f"INSERT INTO healthcare_db.ehr_stream ({columns}) VALUES ({placeholders})", values)
            except:
                cursor.execute(f"INSERT INTO ehr_stream ({columns}) VALUES ({placeholders})", values)
            
            con.commit()
        except Exception as e:
            print(f"[DB ERROR] {e}")
        finally:
            db_queue.task_done()
            
    cursor.close()
    con.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()

    # Starting the background DB worker
    worker_thread = threading.Thread(target=db_worker, daemon=True)
    worker_thread.start()

    start_time = time.time()
    count = 0
    
    print(f"\nStarting LARF Dual-Producer (Micro-Batched)")
    while time.time() - start_time < args.duration:
        event = generate_patient_event(anomalous=False)
        
        # 1. Instant Kafka Write
        producer.produce("ehr-stream", key=event.get("patient_id", "unknown"), value=json.dumps(event), callback=delivery_report)
        producer.poll(0)
        
        # 2. Instant Queue push for DB
        db_queue.put(event)
        
        count += 1
        print(f"[{count}] Sent patient -> Kafka & DB Queue")
        time.sleep(args.interval)
        
    print("Timer finished! Waiting for the DB Worker to flush final rows to Databricks...")
    db_queue.join() 
    db_queue.put(None) 
    worker_thread.join()
    producer.flush()
    print(f"Stream Complete! Exactly {count} events generated and saved.")

if __name__ == "__main__":
    main()