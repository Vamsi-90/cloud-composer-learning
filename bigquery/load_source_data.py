"""
FILE: bigquery/load_source_data.py
PURPOSE: One-time setup script — creates two source tables in BigQuery and loads sample data.

RUN THIS ONCE in your terminal before triggering any Airflow DAGs:
    python3 bigquery/load_source_data.py

What this script does:
  1. Connects to your GCP project using your local gcloud credentials
  2. Creates table: handson-claims-2026.reference.claims
  3. Creates table: handson-claims-2026.reference.members
  4. Inserts 5 sample rows into each table

These tables act as the SOURCE data that the Airflow DAGs will later read from.
The DAGs will copy this data into a STAGING table, then run Dataflow on it.
"""

# google-cloud-bigquery is the official Python client for BigQuery
# Install it with: pip3 install google-cloud-bigquery
from google.cloud import bigquery

# The GCP project where your BigQuery datasets live
PROJECT_ID = "handson-claims-2026"

# Creates a BigQuery client using your local gcloud credentials (already set up)
# This is the object we use to talk to BigQuery
client = bigquery.Client(project=PROJECT_ID)


# ==============================================================================
# SAMPLE DATA
# This is the fake/demo data we are loading into BigQuery.
# In a real project, this data would come from an actual claims system.
# ==============================================================================

# Claims = medical insurance claim records
# Fields:
#   claim_id      - unique ID for this claim
#   member_id     - which member (patient) submitted the claim
#   service_date  - when the medical service happened
#   diagnosis_code- ICD-10 code (standard medical diagnosis code)
#   procedure_code- CPT code (what procedure was done)
#   amount        - dollar amount billed       <-- will be ENCRYPTED by Dataflow
#   provider_id   - which doctor/hospital
#   status        - approved / pending / denied
CLAIMS_DATA = [
    {"claim_id": "CLM001", "member_id": "MBR001", "service_date": "2026-01-15", "diagnosis_code": "J06.9",  "procedure_code": "99213", "amount": 150.00, "provider_id": "PRV001", "status": "approved"},
    {"claim_id": "CLM002", "member_id": "MBR002", "service_date": "2026-02-10", "diagnosis_code": "M54.5",  "procedure_code": "99214", "amount": 220.50, "provider_id": "PRV002", "status": "pending"},
    {"claim_id": "CLM003", "member_id": "MBR003", "service_date": "2026-03-05", "diagnosis_code": "E11.9",  "procedure_code": "99215", "amount": 340.75, "provider_id": "PRV001", "status": "approved"},
    {"claim_id": "CLM004", "member_id": "MBR001", "service_date": "2026-03-20", "diagnosis_code": "I10",    "procedure_code": "93000",  "amount": 95.00,  "provider_id": "PRV003", "status": "denied"},
    {"claim_id": "CLM005", "member_id": "MBR004", "service_date": "2026-04-01", "diagnosis_code": "Z00.00", "procedure_code": "99395",  "amount": 180.00, "provider_id": "PRV002", "status": "approved"},
]

# Members = health insurance member (patient) records
# Fields:
#   member_id       - unique ID for this member
#   first_name      - member's first name
#   last_name       - member's last name
#   dob             - date of birth                <-- will be ENCRYPTED by Dataflow
#   ssn             - social security number       <-- will be ENCRYPTED by Dataflow
#   address         - home address
#   plan_id         - which insurance plan they are on
#   enrollment_date - when they joined the plan
MEMBERS_DATA = [
    {"member_id": "MBR001", "first_name": "John",    "last_name": "Smith",    "dob": "1985-03-15", "ssn": "123-45-6789", "address": "123 Main St, Austin TX 78701",      "plan_id": "PLAN_GOLD",   "enrollment_date": "2024-01-01"},
    {"member_id": "MBR002", "first_name": "Sarah",   "last_name": "Johnson",  "dob": "1990-07-22", "ssn": "234-56-7890", "address": "456 Oak Ave, Dallas TX 75201",      "plan_id": "PLAN_SILVER", "enrollment_date": "2024-02-01"},
    {"member_id": "MBR003", "first_name": "Michael", "last_name": "Williams", "dob": "1978-11-30", "ssn": "345-67-8901", "address": "789 Pine Rd, Houston TX 77001",     "plan_id": "PLAN_GOLD",   "enrollment_date": "2023-06-01"},
    {"member_id": "MBR004", "first_name": "Emily",   "last_name": "Brown",    "dob": "1995-05-10", "ssn": "456-78-9012", "address": "321 Elm St, San Antonio TX 78201",  "plan_id": "PLAN_BRONZE", "enrollment_date": "2025-01-01"},
    {"member_id": "MBR005", "first_name": "Robert",  "last_name": "Davis",    "dob": "1960-09-18", "ssn": "567-89-0123", "address": "654 Maple Dr, Fort Worth TX 76101", "plan_id": "PLAN_GOLD",   "enrollment_date": "2022-09-01"},
]


# ==============================================================================
# BIGQUERY SCHEMA DEFINITIONS
# Schema = the structure/columns of the table, like defining columns in SQL
# Each SchemaField = one column: (column_name, data_type, required_or_not)
# ==============================================================================

CLAIMS_SCHEMA = [
    bigquery.SchemaField("claim_id",       "STRING", mode="REQUIRED"),  # Primary key - cannot be null
    bigquery.SchemaField("member_id",      "STRING"),                   # Links to members table
    bigquery.SchemaField("service_date",   "DATE"),                     # Format: YYYY-MM-DD
    bigquery.SchemaField("diagnosis_code", "STRING"),                   # ICD-10 medical code
    bigquery.SchemaField("procedure_code", "STRING"),                   # CPT procedure code
    bigquery.SchemaField("amount",         "FLOAT"),                    # Dollar amount (will be encrypted)
    bigquery.SchemaField("provider_id",    "STRING"),                   # Doctor/hospital ID
    bigquery.SchemaField("status",         "STRING"),                   # approved / pending / denied
]

MEMBERS_SCHEMA = [
    bigquery.SchemaField("member_id",       "STRING", mode="REQUIRED"), # Primary key - cannot be null
    bigquery.SchemaField("first_name",      "STRING"),
    bigquery.SchemaField("last_name",       "STRING"),
    bigquery.SchemaField("dob",             "DATE"),                    # Date of birth (will be encrypted)
    bigquery.SchemaField("ssn",             "STRING"),                  # Social security number (will be encrypted)
    bigquery.SchemaField("address",         "STRING"),
    bigquery.SchemaField("plan_id",         "STRING"),                  # e.g., PLAN_GOLD, PLAN_SILVER
    bigquery.SchemaField("enrollment_date", "DATE"),                    # When they joined the plan
]


# ==============================================================================
# HELPER FUNCTION
# ==============================================================================

def create_and_load(table_id, schema, rows):
    """
    Creates a BigQuery table (if it doesn't already exist) and inserts rows into it.

    Args:
        table_id (str): Table name, e.g. "claims" or "members"
        schema   (list): List of BigQuery SchemaField objects defining columns
        rows     (list): List of dicts, each dict is one row of data
    """

    # Full table path: project.dataset.table
    # "reference" is the dataset name (already exists in your project)
    table_ref = f"{PROJECT_ID}.reference.{table_id}"

    # Create a Table object with the schema
    table = bigquery.Table(table_ref, schema=schema)

    # exists_ok=True means: don't throw an error if the table already exists, just skip creation
    table = client.create_table(table, exists_ok=True)
    print(f"Table ready: {table_ref}")

    # Insert the rows into the table
    # insert_rows_json() takes a list of dicts and inserts them as rows
    errors = client.insert_rows_json(table, rows)

    if errors:
        # If there were any insert errors, print them so we can debug
        print(f"Insert errors: {errors}")
    else:
        print(f"Inserted {len(rows)} rows into {table_id}")


# ==============================================================================
# MAIN — runs when you execute: python3 bigquery/load_source_data.py
# ==============================================================================

if __name__ == "__main__":
    print("Loading source data into BigQuery...\n")

    # Create and load the claims table
    create_and_load("claims",  CLAIMS_SCHEMA,  CLAIMS_DATA)

    # Create and load the members table
    create_and_load("members", MEMBERS_SCHEMA, MEMBERS_DATA)

    print("\nDone! Source tables are ready in BigQuery -> reference dataset.")
