"""
FILE: dataflow/pipeline.py
PURPOSE: The actual data processing job — reads from BigQuery, encrypts fields, writes to MongoDB.

HOW IT FITS IN THE FLOW:
    Airflow DAG
        -> Step 1: Create staging table in BigQuery   (done by BigQueryInsertJobOperator)
        -> Step 2: Run THIS FILE on Google Dataflow   (done by BeamRunPythonPipelineOperator)
        -> Step 3: Delete staging table               (done by BigQueryDeleteTableOperator)

WHY DATAFLOW?
    Dataflow is Google's managed service for running Apache Beam pipelines.
    It automatically scales workers, manages resources, and handles failures.
    Apache Beam is the programming model — Dataflow is where it runs.

WHY ONE FILE FOR BOTH TABLES?
    The same logic applies to both claims and members:
      - Read rows from BigQuery
      - Encrypt some fields
      - Write to MongoDB
    The only difference is WHICH fields to encrypt, which is passed as a runtime argument.
    This avoids code duplication.

ENCRYPTION USED: Fernet (symmetric encryption)
    - Fernet is from the 'cryptography' Python library
    - Symmetric = same key is used to encrypt AND decrypt
    - The encrypted value looks like a random string of characters
    - Only someone with the same key can decrypt it

WHAT GETS ENCRYPTED:
    claims table  -> amount, member_id
    members table -> ssn, dob

LOCAL TEST (DirectRunner, no Dataflow needed):
    python3 dataflow/pipeline.py \
        --runner DirectRunner \
        --source_table handson-claims-2026:staging.stg_claims \
        --table_type claims \
        --mongo_uri "mongodb+srv://Test_db_user:..." \
        --mongo_db claims_pipeline_db \
        --mongo_collection claims \
        --encryption_key YOUR_KEY \
        --project handson-claims-2026 \
        --temp_location gs://YOUR_BUCKET/temp
"""

import argparse
import logging

# apache_beam is the Apache Beam SDK for Python
# It lets us define data pipelines as a series of transforms
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# ==============================================================================
# FIELD ENCRYPTION CONFIG
# This dictionary maps each table type to the list of fields that should
# be encrypted before being written to MongoDB.
# ==============================================================================
ENCRYPT_FIELDS = {
    "claims":  ["amount", "member_id"],  # Encrypt billing amount and patient ID
    "members": ["ssn", "dob"],           # Encrypt social security number and date of birth
}


# ==============================================================================
# STEP 1: ENCRYPT FIELDS
# This is a Beam DoFn (Do Function) — a class that processes one record at a time.
# Think of it like a function that runs on every single row coming from BigQuery.
# ==============================================================================
class EncryptFieldsDoFn(beam.DoFn):
    """
    For each row (element), encrypts the specified fields using Fernet encryption.

    Before encryption:  {"claim_id": "CLM001", "amount": 150.00, "member_id": "MBR001", ...}
    After encryption:   {"claim_id": "CLM001", "amount": "gAAAAA...xyz", "member_id": "gAAAAA...abc", ...}

    Fields that are NOT in the encrypt list are passed through unchanged.
    """

    def __init__(self, fields, encryption_key):
        """
        Args:
            fields         (list): Field names to encrypt, e.g. ["amount", "member_id"]
            encryption_key (str):  The Fernet key string used for encryption
        """
        self.fields = fields
        self.encryption_key = encryption_key

    def setup(self):
        """
        setup() is called ONCE when a Dataflow worker starts up.
        We create the Fernet cipher here so it is reused across many rows
        instead of being recreated for every single row (which would be slow).
        """
        from cryptography.fernet import Fernet
        # Fernet expects the key as bytes, so we encode the string
        self._fernet = Fernet(self.encryption_key.encode())

    def process(self, element):
        """
        process() is called ONCE PER ROW.

        Args:
            element (dict): One row from BigQuery, e.g. {"claim_id": "CLM001", "amount": 150.0, ...}

        Yields:
            dict: The same row but with sensitive fields replaced by encrypted values
        """
        # Make a copy so we don't mutate the original
        row = dict(element)

        for field in self.fields:
            # Only encrypt if the field exists in this row and is not null
            if field in row and row[field] is not None:
                # Convert the value to string, then to bytes (Fernet requires bytes)
                plain_bytes = str(row[field]).encode("utf-8")

                # Encrypt — produces a bytes object starting with "gAAAAA..."
                encrypted_bytes = self._fernet.encrypt(plain_bytes)

                # Store as a string in the row (MongoDB stores strings fine)
                row[field] = encrypted_bytes.decode("utf-8")

        # yield is used in Beam instead of return — it emits the record downstream
        yield row


# ==============================================================================
# STEP 2: WRITE TO MONGODB
# Another DoFn — this one takes the encrypted rows and inserts them into MongoDB.
# ==============================================================================
class WriteToMongoDoFn(beam.DoFn):
    """
    Inserts each row into a MongoDB collection.

    One MongoDB connection is opened per Dataflow worker (in setup),
    used for many rows, then closed when the worker finishes (in teardown).
    This is much more efficient than opening a new connection per row.
    """

    def __init__(self, mongo_uri, db_name, collection_name):
        """
        Args:
            mongo_uri       (str): MongoDB connection string (e.g. mongodb+srv://...)
            db_name         (str): Database name in MongoDB (e.g. claims_pipeline_db)
            collection_name (str): Collection name (equivalent to a table) in MongoDB
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name

    def setup(self):
        """
        Called once per worker. Creates the MongoDB connection and gets a reference
        to the target collection. This connection stays open for the lifetime of the worker.
        """
        from pymongo import MongoClient

        # MongoClient connects to MongoDB Atlas using the connection string
        self._client = MongoClient(self.mongo_uri)

        # Navigate to: client -> database -> collection
        # MongoDB auto-creates the database and collection if they don't exist
        self._collection = self._client[self.db_name][self.collection_name]

    def process(self, element):
        """
        Called once per row. Inserts the row into the MongoDB collection.

        Args:
            element (dict): One encrypted row from the previous step
        """
        # Remove "_id" if it somehow ended up in the row — MongoDB generates its own _id
        element.pop("_id", None)

        # insert_one() inserts a single document (row) into the MongoDB collection
        self._collection.insert_one(element)

        # We yield the element so it can continue down the pipeline if needed
        yield element

    def teardown(self):
        """
        Called once when the worker is done processing all records.
        We close the MongoDB connection here to free up resources cleanly.
        """
        if hasattr(self, "_client"):
            self._client.close()


# ==============================================================================
# PIPELINE DEFINITION
# This is where we wire everything together into a pipeline.
# ==============================================================================
def run():
    """
    Parses command-line arguments, builds the Beam pipeline, and runs it.

    The pipeline has 3 steps:
        1. ReadFromBigQuery  — reads all rows from the BQ staging table
        2. EncryptFields     — encrypts sensitive fields in each row
        3. WriteToMongoDB    — inserts each encrypted row into MongoDB
    """

    # argparse lets us pass configuration to this script at runtime
    # so the same script works for both "claims" and "members" pipelines
    parser = argparse.ArgumentParser(description="Claims/Members ETL pipeline")

    # --source_table: which BigQuery table to read from
    # Format: project:dataset.table  (note the colon, not dot — that's BigQuery's format for Beam)
    parser.add_argument("--source_table",     required=True,  help="BQ table in format project:dataset.table")

    # --table_type: tells us which fields to encrypt (see ENCRYPT_FIELDS dict above)
    parser.add_argument("--table_type",       required=True,  choices=["claims", "members"])

    # --mongo_uri: MongoDB Atlas connection string
    parser.add_argument("--mongo_uri",        required=True)

    # --mongo_db: which MongoDB database to write to
    parser.add_argument("--mongo_db",         required=True)

    # --mongo_collection: which collection (table) inside that database
    parser.add_argument("--mongo_collection", required=True)

    # --encryption_key: the Fernet key used to encrypt sensitive fields
    parser.add_argument("--encryption_key",   required=True)

    # parse_known_args() separates our custom args from Beam's built-in args
    # (e.g. --runner, --project, --temp_location are Beam/Dataflow args)
    known_args, pipeline_args = parser.parse_known_args()

    # Look up which fields to encrypt based on table type
    fields_to_encrypt = ENCRYPT_FIELDS[known_args.table_type]

    # PipelineOptions wraps all the Beam/Dataflow config (runner, project, region, etc.)
    # save_main_session=True ensures imports in this file are available on remote workers
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

    logging.info("Starting pipeline for table_type=%s", known_args.table_type)
    logging.info("Source: %s  ->  MongoDB: %s/%s", known_args.source_table, known_args.mongo_db, known_args.mongo_collection)
    logging.info("Encrypting fields: %s", fields_to_encrypt)

    # "with beam.Pipeline(...)" creates and runs the pipeline
    # When the "with" block ends, Beam submits the pipeline and waits for it to finish
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            # Step 1: Read all rows from the BigQuery staging table
            # Each row becomes a Python dict: {"claim_id": "CLM001", "amount": 150.0, ...}
            | "ReadFromBigQuery" >> beam.io.ReadFromBigQuery(table=known_args.source_table)

            # Step 2: Encrypt sensitive fields in each row using our DoFn
            # beam.ParDo() applies a DoFn to every element in the collection
            | "EncryptFields"    >> beam.ParDo(EncryptFieldsDoFn(fields_to_encrypt, known_args.encryption_key))

            # Step 3: Write each encrypted row to MongoDB Atlas
            | "WriteToMongoDB"   >> beam.ParDo(WriteToMongoDoFn(known_args.mongo_uri, known_args.mongo_db, known_args.mongo_collection))
        )

    logging.info("Pipeline finished successfully.")


# ==============================================================================
# ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    # Set logging level to INFO so we can see pipeline progress in the logs
    logging.getLogger().setLevel(logging.INFO)
    run()
