import json
import os
import pandas as pd
from sklearn.ensemble import IsolationForest

class MultivariateDetector:
    def __init__(self, log_file="detectors/telemetry_log.jsonl", contamination=0.05):
        self.log_file = log_file
        self.contamination = contamination
        self.features = ["consumer_lag", "db_latency_sec", "cpu_percent", "memory_percent"]
        self.model = IsolationForest(contamination=self.contamination, random_state=42)

    def load_telemetry_data(self):
        """Loads the JSONL log file into a Pandas DataFrame."""
        if not os.path.exists(self.log_file):
            print(f"[WARNING] Telemetry log not found at {self.log_file}")
            return pd.DataFrame()

        data = []
        with open(self.log_file, "r") as f:
            for line in f:
                try:
                    data.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    continue
        
        return pd.DataFrame(data)

    def analyze_latest_state(self):
        """
        Trains the Isolation Forest on historical data and predicts if the 
        LATEST telemetry reading is a multivariate anomaly.
        """
        df = self.load_telemetry_data()
        
        # We need a minimum amount of data to establish a baseline
        if len(df) < 20:
            print("[IFOREST] Not enough data to train Isolation Forest (need 20+ samples).")
            return {"is_anomaly": False, "score": None, "details": "Insufficient data"}

        # Extract just the numerical features
        X = df[self.features].copy()

        # Fit the model and predict anomalies (-1 is anomaly, 1 is normal)
        self.model.fit(X)
        predictions = self.model.predict(X)
        
        # get the anomaly scores (lower/more negative means more anomalous)
        scores = self.model.decision_function(X)

        latest_prediction = predictions[-1]
        latest_score = scores[-1]
        latest_timestamp = df.iloc[-1]["timestamp"]

        is_anomaly = bool(latest_prediction == -1)

        result = {
            "timestamp": latest_timestamp,
            "is_anomaly": is_anomaly,
            "score": round(float(latest_score), 4),
            "metrics_analyzed": df.iloc[-1][self.features].to_dict()
        }

        if is_anomaly:
            print(f"[ALERT] ISOLATION FOREST triggered! Score: {result['score']}")
            print(f"        Anomalous Metrics: {result['metrics_analyzed']}")
        else:
            print(f"[IFOREST] System normal. Score: {result['score']}")

        return result

if __name__ == "__main__":
    detector = MultivariateDetector()
    print("=== Testing Isolation Forest Detector ===")
    detector.analyze_latest_state()