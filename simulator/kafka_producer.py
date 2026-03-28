import json
import time
import argparse
import sys
import os
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from confluent_kafka import Producer
from simulator.patient_generator import generate_patient_event
from simulator.database import get_connection

KAFKA_BOOTSTRAP = "localhost:9092"

class BufferedProducer:
    def __init__(self, batch_size=50):
        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        self.batch_size = batch_size
        self.ehr_buffer = []
        self.iot_buffer = []
        self.lock = threading.Lock()

    def delivery_report(self, err, msg):
        if err:
            print(f"[ERROR] Kafka delivery failed: {err}")

    def _flush_to_databricks(self, ehr_batch, iot_batch):
        """Executes a single bulk insert for the entire buffer."""
        if not ehr_batch:
            return
            
        try:
            con = get_connection()
            cursor = con.cursor()
            
            # Bulk insert EHR events
            ehr_values = [
                (e["event_id"], e["patient_id"], e["ward"], e["heart_rate"], 
                 e["bp_systolic"], e["bp_diastolic"], e["spo2"], 
                 e["temperature_c"], e["timestamp"], time.strftime('%Y-%m-%d %H:%M:%S'))
                for e in ehr_batch
            ]
            cursor.executemany("""
                INSERT INTO healthcare_db.ehr_stream 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, ehr_values)

            # Bulk insert IoT events
            iot_values = [
                (i["patient_id"], i["heart_rate"], i["spo2"], 
                 i["timestamp"], time.strftime('%Y-%m-%d %H:%M:%S'))
                for i in iot_batch
            ]
            cursor.executemany("""
                INSERT INTO healthcare_db.iot_vitals 
                VALUES (?, ?, ?, ?, ?)
            """, iot_values)

            con.commit()
            cursor.close()
            con.close()
            print(f"[DB] Bulk Inserted {len(ehr_batch)} rows successfully.")
        except Exception as e:
            print(f"[DB-ERROR] Batch write failed: {e}")

    def stream_events(self, duration_seconds=60, interval=0.5):
        print(f"[INFO] Streaming started. Batch Size: {self.batch_size}")
        start = time.time()
        
        while time.time() - start < duration_seconds:
            event = generate_patient_event()
            
            # 1. Immediate Kafka Production (Still real-time)
            self.producer.produce(
                "ehr-stream",
                key=event["patient_id"],
                value=json.dumps(event),
                callback=self.delivery_report
            )
            
            iot_payload = {
                "patient_id": event["patient_id"], "heart_rate": event["heart_rate"],
                "spo2": event["spo2"], "timestamp": event["timestamp"],
            }
            self.producer.produce("iot-vitals", value=json.dumps(iot_payload))
            self.producer.poll(0)

            # 2. Add to Local Buffer
            with self.lock:
                self.ehr_buffer.append(event)
                self.iot_buffer.append(iot_payload)

                # 3. If buffer is full, fire background write
                if len(self.ehr_buffer) >= self.batch_size:
                    batch_to_send_ehr = list(self.ehr_buffer)
                    batch_to_send_iot = list(self.iot_buffer)
                    self.ehr_buffer.clear()
                    self.iot_buffer.clear()
                    
                    threading.Thread(
                        target=self._flush_to_databricks, 
                        args=(batch_to_send_ehr, batch_to_send_iot),
                        daemon=True
                    ).start()

            time.sleep(interval)

        self.producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=300)
    parser.add_argument("--interval", type=float, default=0.5)
    parser.add_argument("--batch", type=int, default=20)
    args = parser.parse_args()
    
    bp = BufferedProducer(batch_size=args.batch)
    bp.stream_events(duration_seconds=args.duration, interval=args.interval)
