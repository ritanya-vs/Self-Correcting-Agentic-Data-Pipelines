import random
import uuid
from datetime import datetime
from faker import Faker

fake = Faker()

# Physiological normal ranges
NORMAL_RANGES = {
    "heart_rate":    (60, 100),
    "bp_systolic":   (90, 140),
    "bp_diastolic":  (60, 90),
    "spo2":          (95, 100),
    "temperature_c": (36.1, 37.8),
    "respiratory_rate": (12, 20),
}

def generate_patient_event(anomalous=False):
    
    #Returns one patient vital signs event as a dict.
    #If anomalous=True, injects out-of-range values into ~30% of fields.

    def val(key):
        lo, hi = NORMAL_RANGES[key]
        if anomalous and random.random() < 0.3:
            return round(random.uniform(hi * 1.5, hi * 2.5), 1)
        return round(random.uniform(lo, hi), 1)

    return {
        "event_id":      str(uuid.uuid4()),
        "patient_id":    fake.bothify("PT-####"),
        "ward":          random.choice(["ICU", "GENERAL", "CARDIO", "NEURO"]),
        "heart_rate":    val("heart_rate"),
        "bp_systolic":   val("bp_systolic"),
        "bp_diastolic":  val("bp_diastolic"),
        "spo2":          val("spo2"),
        "temperature_c": val("temperature_c"),
        "respiratory_rate": val("respiratory_rate"),
        "timestamp":     datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    print("=== Normal events ===")
    for _ in range(3):
        print(generate_patient_event())

    print("\n=== Anomalous events ===")
    for _ in range(3):
        print(generate_patient_event(anomalous=True))