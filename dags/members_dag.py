"""
DAG: members_pipeline
----------------------
Orchestrates the members ETL pipeline:
  1. Create staging table (staging.stg_members) from reference.members
  2. Run Dataflow job: encrypt fields + load to MongoDB
  3. Delete staging table

Trigger: Manual (no schedule)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryDeleteTableOperator,
)
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType

# ---------- Config ----------

PROJECT_ID      = "handson-claims-2026"
REGION          = "us-central1"
DATASET_SOURCE  = "reference"
DATASET_STAGING = "staging"
SOURCE_TABLE    = "members"
STG_TABLE       = "stg_members"

# Set this in Airflow UI -> Admin -> Variables after Composer is ready
# Key: composer_bucket  Value: us-central1-claims-composer-XXXX-bucket
COMPOSER_BUCKET = Variable.get("composer_bucket")

MONGO_URI        = "mongodb+srv://Test_db_user:mSyFkyvh_WmMM2_@cluster0.75jzntx.mongodb.net/"
MONGO_DB         = "claims_pipeline_db"
MONGO_COLLECTION = "members"
ENCRYPTION_KEY   = "vEyMKrMRj861UOtlmKD6QH7zxjj8FfNT64tB0pkglfQ="

DATAFLOW_SCRIPT  = f"gs://{COMPOSER_BUCKET}/dataflow/pipeline.py"
TEMP_LOCATION    = f"gs://{COMPOSER_BUCKET}/temp"

# ---------- Default Args ----------

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 5, 19),
}

# ---------- DAG ----------

with DAG(
    dag_id="members_pipeline",
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["members", "dataflow", "mongodb"],
    doc_md="""
    ## Members Pipeline
    Reads members from BigQuery, encrypts sensitive fields, and loads into MongoDB Atlas.
    - **Encrypted fields**: `ssn`, `dob`
    - **Destination**: MongoDB `claims_pipeline_db.members`
    """,
) as dag:

    # Step 1: Create staging table as a copy of the source table
    create_stg_members = BigQueryInsertJobOperator(
        task_id="create_stg_members",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_STAGING}.{STG_TABLE}` AS
                    SELECT * FROM `{PROJECT_ID}.{DATASET_SOURCE}.{SOURCE_TABLE}`
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )

    # Step 2: Run Dataflow pipeline
    run_dataflow = BeamRunPythonPipelineOperator(
        task_id="run_dataflow_members",
        runner=BeamRunnerType.DataflowRunner,
        py_file=DATAFLOW_SCRIPT,
        pipeline_options={
            "project":          PROJECT_ID,
            "region":           REGION,
            "temp_location":    TEMP_LOCATION,
            "staging_location": f"gs://{COMPOSER_BUCKET}/staging",
            "source_table":     f"{PROJECT_ID}:{DATASET_STAGING}.{STG_TABLE}",
            "table_type":       "members",
            "mongo_uri":        MONGO_URI,
            "mongo_db":         MONGO_DB,
            "mongo_collection": MONGO_COLLECTION,
            "encryption_key":   ENCRYPTION_KEY,
        },
        py_interpreter="python3",
        py_requirements=["apache-beam[gcp]==2.55.0", "cryptography==42.0.5", "pymongo[srv]==4.6.1"],
        py_system_site_packages=False,
        dataflow_config={
            "job_name": "members-pipeline",
            "location":  REGION,
            "wait_until_finished": True,
        },
    )

    # Step 3: Delete staging table
    delete_stg_members = BigQueryDeleteTableOperator(
        task_id="delete_stg_members",
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_STAGING}.{STG_TABLE}",
    )

    # Pipeline order
    create_stg_members >> run_dataflow >> delete_stg_members
