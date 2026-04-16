import json
from datetime import datetime, timezone

class CrisisPacketBuilder:
    def __init__(self):
        self.packet = {
            "crisis_id": f"CRISIS-{int(datetime.now(timezone.utc).timestamp())}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "fault_signals": [],
            "affected_components": set(),
            "severity": "UNKNOWN"
        }

    def add_alert(self, alert_dict):
        detector_name = alert_dict.get("detector", "unknown")

        # ── DATA QUALITY / DRIFT ─────────────────────────────
        if detector_name in ["isolation_forest", "zscore", "ks_test"]:
            self.packet["affected_components"].add("Kafka_Consumer")
            self.packet["affected_components"].add("Databricks_Warehouse")

        # ── SCHEMA FAULT ─────────────────────────────────────
        elif detector_name == "schema_entropy":
            self.packet["affected_components"].add("Data_Producer")
            self.packet["affected_components"].add("Kafka_Topic_ehr-stream")

        # ── SECURITY FAULT ───────────────────────────────────
        elif detector_name == "security":
            self.packet["affected_components"].add("Kafka_Topic_ehr-stream")
            self.packet["affected_components"].add("Databricks_Warehouse")

            # Ensure attacker info is explicitly present
            alert_dict["attacker_id"] = alert_dict.get("attacker_id", "PT-ATTACKER-0000")

        # ── PERFORMANCE / LATENCY ────────────────────────────
        elif detector_name in ["latency_monitor", "warehouse_monitor"]:
            self.packet["affected_components"].add("Databricks_Warehouse")

        # ── STALL FAULT ──────────────────────────────────────
        elif detector_name == "stall":
            self.packet["affected_components"].add("Kafka_Consumer")
            self.packet["affected_components"].add("Kafka_Connect")

        # ── DEFAULT FALLBACK ─────────────────────────────────
        else:
            self.packet["affected_components"].add("Unknown_Component")

        # Add alert
        self.packet["fault_signals"].append(alert_dict)

        # Severity logic
        if detector_name == "security":
            self.packet["severity"] = "CRITICAL"
        elif len(self.packet["fault_signals"]) > 1:
            self.packet["severity"] = "CRITICAL"
        else:
            self.packet["severity"] = "HIGH"

    def build(self):
        """Returns the finalized, structured Crisis Packet ready for the LLM Agent."""
        # Convert sets to lists for JSON serialization
        final_packet = dict(self.packet)
        final_packet["affected_components"] = list(final_packet["affected_components"])
        return final_packet

if __name__ == "__main__":
    # Quick Test
    builder = CrisisPacketBuilder()
    builder.add_alert({"detector": "schema_entropy", "missing_fields": ["ward"]})
    builder.add_alert({"detector": "isolation_forest", "consumer_lag": 129})
    print(json.dumps(builder.build(), indent=2))