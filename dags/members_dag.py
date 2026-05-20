"""
FILE: dags/members_dag.py
PURPOSE: Airflow DAG that orchestrates the full members data pipeline.

WHAT IS A DAG?
    DAG = Directed Acyclic Graph
    In Airflow, a DAG is a workflow — a sequence of tasks that run in a defined order.
    "Directed" = tasks run in a specific direction (Task A must finish before Task B starts)
    "Acyclic" = no loops (tasks don't repeat in circles)

WHAT THIS DAG DOES (in order):
    Task 1: create_stg_members
            -> Copies reference.members into a new staging table called staging.stg_members
            -> Why staging? So we have a clean working copy and don't touch the source data

    Task 2: run_dataflow_members
            -> Launches a Google Dataflow job using our pipeline.py script
            -> Dataflow reads stg_members, encrypts ssn + dob, writes to MongoDB

    Task 3: delete_stg_members
            -> Drops the staging table from BigQuery (cleanup after we're done)

VISUAL FLOW:
    [create_stg_members] --> [run_dataflow_members] --> [delete_stg_members]

HOW THIS DIFFERS FROM claims_dag.py:
    - Uses SOURCE_TABLE = "members" instead of "claims"
    - Uses STG_TABLE = "stg_members" instead of "stg_claims"
    - Passes table_type = "members" to Dataflow (encrypts ssn + dob instead of amount + member_id)
    - Writes to MongoDB collection "members" instead of "claims"
    Everything else is identical — both DAGs reuse the SAME Dataflow script (pipeline.py).

HOW TO TRIGGER:
    This DAG has no schedule (schedule_interval=None).
    You trigger it manually from the Airflow UI by clicking the "Play" button.

BEFORE RUNNING:
    Set the Airflow Variable "composer_bucket" in the Airflow UI:
    Admin -> Variables -> Add -> Key: composer_bucket, Value: us-central1-claims-composer-XXXX-bucket
    (Replace XXXX with your actual Composer bucket name from GCP Console)
"""

from datetime import datetime, timedelta

# DAG is the base class for all Airflow workflows
from airflow import DAG

# Variable lets us read values stored in Airflow's UI (Admin -> Variables)
# This is how we avoid hardcoding things like bucket names
from airflow.models import Variable

# BigQueryInsertJobOperator: runs a SQL query in BigQuery
# BigQueryDeleteTableOperator: deletes a BigQuery table
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryDeleteTableOperator,
)

# BeamRunPythonPipelineOperator: runs an Apache Beam Python script on Dataflow
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

# BeamRunnerType.DataflowRunner tells Beam to run on Google Dataflow (not locally)
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType


# ==============================================================================
# CONFIG — all settings in one place so they are easy to find and change
# ==============================================================================

PROJECT_ID      = "handson-claims-2026"   # Your GCP project
REGION          = "us-central1"            # GCP region for Dataflow jobs
DATASET_SOURCE  = "reference"              # BigQuery dataset where source tables live
DATASET_STAGING = "staging"                # BigQuery dataset for temporary staging tables
SOURCE_TABLE    = "members"                # Source table name: reference.members
STG_TABLE       = "stg_members"            # Staging table name: staging.stg_members

# COMPOSER_BUCKET: read from Airflow Variables (set this in Airflow UI after Composer is ready)
# Why a Variable? The bucket name has a random suffix we don't know until Composer is created.
# Example value: us-central1-claims-composer-a1b2c3d4-bucket
COMPOSER_BUCKET = Variable.get("composer_bucket", default_var="us-east1-claims-composer-a322975f-bucket")

# MongoDB connection settings
MONGO_URI        = "mongodb+srv://Test_db_user:mSyFkyvh_WmMM2_@cluster0.75jzntx.mongodb.net/"
MONGO_DB         = "claims_pipeline_db"    # Same DB as claims — will be auto-created if needed
MONGO_COLLECTION = "members"               # Different collection from claims — auto-created if needed

# Fernet encryption key — used to encrypt ssn and dob fields
# Must be the SAME key as in claims_dag.py so you can decrypt with one key
# IMPORTANT: In production, store this in GCP Secret Manager, not hardcoded here.
ENCRYPTION_KEY = "vEyMKrMRj861UOtlmKD6QH7zxjj8FfNT64tB0pkglfQ="

# GCS paths — all inside the Composer-managed bucket
DATAFLOW_SCRIPT  = f"gs://{COMPOSER_BUCKET}/dataflow/pipeline.py"      # Our Beam pipeline script
TEMP_LOCATION    = f"gs://{COMPOSER_BUCKET}/temp"                       # Dataflow temp files
STAGING_LOCATION = f"gs://{COMPOSER_BUCKET}/staging"                    # Dataflow staging files


# ==============================================================================
# DEFAULT ARGS
# These settings apply to every task in the DAG unless overridden per task.
# ==============================================================================
default_args = {
    "owner": "airflow",               # Who owns this DAG (shown in UI)
    "retries": 1,                     # If a task fails, retry it 1 time before marking as failed
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
    "start_date": datetime(2026, 5, 19),  # The DAG is valid from this date onward
}


# ==============================================================================
# DAG DEFINITION
# "with DAG(...) as dag:" is the standard way to define a DAG in Airflow.
# All tasks defined inside this block belong to this DAG.
# ==============================================================================
with DAG(
    dag_id="members_pipeline",        # Unique name shown in the Airflow UI
    default_args=default_args,
    schedule_interval=None,           # No automatic schedule — trigger manually from UI
    catchup=False,                    # Don't run missed past executions when DAG is first activated
    tags=["members", "dataflow", "mongodb"],  # Tags for filtering in the Airflow UI
    doc_md="""
    ## Members Pipeline
    Reads members from BigQuery, encrypts sensitive fields (`ssn`, `dob`), and loads into MongoDB Atlas.
    **Destination:** MongoDB `claims_pipeline_db.members`
    """,
) as dag:

    # ==========================================================================
    # TASK 1: Create staging table
    # Copies all rows from reference.members into a new staging.stg_members table.
    # "CREATE OR REPLACE" means: drop old stg_members if it exists and create a fresh one.
    # This ensures we always start with a clean copy of the source data.
    # ==========================================================================
    create_stg_members = BigQueryInsertJobOperator(
        task_id="create_stg_members",      # Unique name for this task inside the DAG

        # "configuration" is the BigQuery job config — here we're running a SQL query
        configuration={
            "query": {
                # This SQL creates a new table by selecting all rows from the source table
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_STAGING}.{STG_TABLE}` AS
                    SELECT * FROM `{PROJECT_ID}.{DATASET_SOURCE}.{SOURCE_TABLE}`
                """,
                "useLegacySql": False,  # Use Standard SQL (not BigQuery's old legacy dialect)
            }
        },
        location="US",  # BigQuery region — must match where your dataset is located
    )

    # ==========================================================================
    # TASK 2: Run Dataflow pipeline
    # Launches our pipeline.py script on Google Dataflow.
    # Dataflow will:
    #   1. Read rows from staging.stg_members
    #   2. Encrypt the "ssn" and "dob" fields (sensitive personal data!)
    #   3. Write each encrypted row into MongoDB Atlas -> claims_pipeline_db.members
    # This task WAITS until the Dataflow job is fully complete before continuing.
    # ==========================================================================
    run_dataflow = BeamRunPythonPipelineOperator(
        task_id="run_dataflow_members",

        # DataflowRunner = run on Google Cloud Dataflow (not on the Airflow worker machine)
        runner=BeamRunnerType.DataflowRunner,

        # The Beam pipeline script stored in GCS (we upload it there after Composer is ready)
        py_file=DATAFLOW_SCRIPT,

        # pipeline_options = all arguments passed to pipeline.py at runtime
        # These become the --argument_name values when running the script
        pipeline_options={
            "project":          PROJECT_ID,         # GCP project for billing and resources
            "region":           REGION,             # Where Dataflow workers will run
            "temp_location":    TEMP_LOCATION,      # GCS path for Dataflow temporary files
            "staging_location": STAGING_LOCATION,   # GCS path for Dataflow staging files

            # Custom args defined in pipeline.py
            "source_table":     f"{PROJECT_ID}:{DATASET_STAGING}.{STG_TABLE}",  # BQ table to read
            "table_type":       "members",          # Tells pipeline to encrypt ssn + dob
            "mongo_uri":        MONGO_URI,
            "mongo_db":         MONGO_DB,
            "mongo_collection": MONGO_COLLECTION,
            "encryption_key":   ENCRYPTION_KEY,
        },

        # Python runtime settings for the Dataflow workers
        py_interpreter="python3",
        py_requirements=[                            # Libraries installed on each Dataflow worker
            "apache-beam[gcp]==2.55.0",             # Apache Beam with GCP connectors
            "cryptography==42.0.5",                 # For Fernet encryption
            "pymongo[srv]==4.6.1",                  # MongoDB Python driver ([srv] = Atlas support)
        ],
        py_system_site_packages=False,              # Use only the packages we listed above

        # Dataflow-specific settings
        dataflow_config={
            "job_name": "members-pipeline",         # Name shown in GCP Dataflow UI
            "location":  REGION,
            "wait_until_finished": True,            # DAG task waits for Dataflow job to complete
        },
    )

    # ==========================================================================
    # TASK 3: Delete staging table
    # Once Dataflow has finished, the staging table is no longer needed.
    # We delete it to keep BigQuery clean and avoid storage costs.
    # ==========================================================================
    delete_stg_members = BigQueryDeleteTableOperator(
        task_id="delete_stg_members",

        # Full table path to delete: project.dataset.table
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_STAGING}.{STG_TABLE}",
    )

    # ==========================================================================
    # TASK ORDER (the >> operator means "then run")
    # create_stg_members must finish -> then run_dataflow -> then delete_stg_members
    # This is what makes it a DAG — a defined direction of execution
    # ==========================================================================
    create_stg_members >> run_dataflow >> delete_stg_members
