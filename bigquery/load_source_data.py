"""
Creates BigQuery source tables in the `reference` dataset and loads sample data.
Run this ONCE before triggering the Airflow DAGs.

Usage:
    python3 bigquery/load_source_data.py
"""

from google.cloud import bigquery

PROJECT_ID = "handson-claims-2026"
client = bigquery.Client(project=PROJECT_ID)

# ---------- Sample Data ----------

CLAIMS_DATA = [
    {"claim_id": "CLM001", "member_id": "MBR001", "service_date": "2026-01-15", "diagnosis_code": "J06.9",  "procedure_code": "99213", "amount": 150.00, "provider_id": "PRV001", "status": "approved"},
    {"claim_id": "CLM002", "member_id": "MBR002", "service_date": "2026-02-10", "diagnosis_code": "M54.5",  "procedure_code": "99214", "amount": 220.50, "provider_id": "PRV002", "status": "pending"},
    {"claim_id": "CLM003", "member_id": "MBR003", "service_date": "2026-03-05", "diagnosis_code": "E11.9",  "procedure_code": "99215", "amount": 340.75, "provider_id": "PRV001", "status": "approved"},
    {"claim_id": "CLM004", "member_id": "MBR001", "service_date": "2026-03-20", "diagnosis_code": "I10",    "procedure_code": "93000",  "amount": 95.00,  "provider_id": "PRV003", "status": "denied"},
    {"claim_id": "CLM005", "member_id": "MBR004", "service_date": "2026-04-01", "diagnosis_code": "Z00.00", "procedure_code": "99395",  "amount": 180.00, "provider_id": "PRV002", "status": "approved"},
]

MEMBERS_DATA = [
    {"member_id": "MBR001", "first_name": "John",    "last_name": "Smith",    "dob": "1985-03-15", "ssn": "123-45-6789", "address": "123 Main St, Austin TX 78701",      "plan_id": "PLAN_GOLD",   "enrollment_date": "2024-01-01"},
    {"member_id": "MBR002", "first_name": "Sarah",   "last_name": "Johnson",  "dob": "1990-07-22", "ssn": "234-56-7890", "address": "456 Oak Ave, Dallas TX 75201",      "plan_id": "PLAN_SILVER", "enrollment_date": "2024-02-01"},
    {"member_id": "MBR003", "first_name": "Michael", "last_name": "Williams", "dob": "1978-11-30", "ssn": "345-67-8901", "address": "789 Pine Rd, Houston TX 77001",     "plan_id": "PLAN_GOLD",   "enrollment_date": "2023-06-01"},
    {"member_id": "MBR004", "first_name": "Emily",   "last_name": "Brown",    "dob": "1995-05-10", "ssn": "456-78-9012", "address": "321 Elm St, San Antonio TX 78201",  "plan_id": "PLAN_BRONZE", "enrollment_date": "2025-01-01"},
    {"member_id": "MBR005", "first_name": "Robert",  "last_name": "Davis",    "dob": "1960-09-18", "ssn": "567-89-0123", "address": "654 Maple Dr, Fort Worth TX 76101", "plan_id": "PLAN_GOLD",   "enrollment_date": "2022-09-01"},
]

# ---------- Schema Definitions ----------

CLAIMS_SCHEMA = [
    bigquery.SchemaField("claim_id",       "STRING", mode="REQUIRED"),
    bigquery.SchemaField("member_id",      "STRING"),
    bigquery.SchemaField("service_date",   "DATE"),
    bigquery.SchemaField("diagnosis_code", "STRING"),
    bigquery.SchemaField("procedure_code", "STRING"),
    bigquery.SchemaField("amount",         "FLOAT"),
    bigquery.SchemaField("provider_id",    "STRING"),
    bigquery.SchemaField("status",         "STRING"),
]

MEMBERS_SCHEMA = [
    bigquery.SchemaField("member_id",       "STRING", mode="REQUIRED"),
    bigquery.SchemaField("first_name",      "STRING"),
    bigquery.SchemaField("last_name",       "STRING"),
    bigquery.SchemaField("dob",             "DATE"),
    bigquery.SchemaField("ssn",             "STRING"),
    bigquery.SchemaField("address",         "STRING"),
    bigquery.SchemaField("plan_id",         "STRING"),
    bigquery.SchemaField("enrollment_date", "DATE"),
]

# ---------- Helpers ----------

def create_and_load(table_id, schema, rows):
    table_ref = f"{PROJECT_ID}.reference.{table_id}"
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"Table ready: {table_ref}")

    errors = client.insert_rows_json(table, rows)
    if errors:
        print(f"Insert errors: {errors}")
    else:
        print(f"Inserted {len(rows)} rows into {table_id}")

# ---------- Main ----------

if __name__ == "__main__":
    print("Loading source data into BigQuery...\n")
    create_and_load("claims",  CLAIMS_SCHEMA,  CLAIMS_DATA)
    create_and_load("members", MEMBERS_SCHEMA, MEMBERS_DATA)
    print("\nDone! Source tables are ready.")
