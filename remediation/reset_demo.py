import os
import sys
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

def reset_pipeline():
    print("🧹 RESETTING LARF DEMO ENVIRONMENT (CLEAN 10-COLUMN MODE)...")
    
    host = os.getenv("DATABRICKS_HOST")
    path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_TOKEN")
    
    if not all([host, path, token]):
        print("❌ ERROR: Missing Databricks environment variables in .env")
        return

    # EXACTLY 10 COLUMNS to match your patient_generator.py
    reset_queries = [
        "DROP TABLE IF EXISTS healthcare_db.ehr_stream",
        """
        CREATE TABLE healthcare_db.ehr_stream (
            event_id STRING,
            patient_id STRING,
            ward STRING,
            heart_rate DOUBLE,
            bp_systolic DOUBLE,
            bp_diastolic DOUBLE,
            spo2 DOUBLE,
            temperature_c DOUBLE,
            respiratory_rate DOUBLE,
            timestamp STRING
        ) USING DELTA
        """
    ]

    try:
        with sql.connect(server_hostname=host, http_path=path, access_token=token) as conn:
            with conn.cursor() as cursor:
                for query in reset_queries:
                    print(f"Executing: {query.strip().splitlines()[0][:60]}...")
                    cursor.execute(query)
        
        print("\n" + "="*60)
        print("✅ RESET SUCCESSFUL: 10/10 COLUMNS ALIGNED")
        print("="*60)
        print("Your Database now EXACTLY matches your Python Generator.")
        
    except Exception as e:
        print(f"\n❌ Error during reset: {e}")

if __name__ == "__main__":
    confirm = input("\nReset to ORIGINAL 10 columns? Type 'yes': ")
    if confirm.lower() == 'yes':
        reset_pipeline()