"""
Reusable Dataflow pipeline (Apache Beam) for both claims and members tables.

What it does:
  1. Reads from a BigQuery staging table
  2. Encrypts sensitive fields using Fernet symmetric encryption
  3. Writes the transformed records to MongoDB Atlas

This same script is used by both Airflow DAGs — the table type and field config
are passed as arguments at runtime.

Usage (local test):
    python3 dataflow/pipeline.py \
        --runner DirectRunner \
        --source_table handson-claims-2026:staging.stg_claims \
        --table_type claims \
        --mongo_uri "mongodb+srv://..." \
        --mongo_db claims_pipeline_db \
        --mongo_collection claims \
        --encryption_key YOUR_KEY \
        --project handson-claims-2026 \
        --temp_location gs://YOUR_BUCKET/temp
"""

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Fields to encrypt per table type
ENCRYPT_FIELDS = {
    "claims":  ["amount", "member_id"],
    "members": ["ssn", "dob"],
}


class EncryptFieldsDoFn(beam.DoFn):
    """Encrypts specified fields in a row using Fernet symmetric encryption."""

    def __init__(self, fields, encryption_key):
        self.fields = fields
        self.encryption_key = encryption_key

    def setup(self):
        from cryptography.fernet import Fernet
        self._fernet = Fernet(self.encryption_key.encode())

    def process(self, element):
        row = dict(element)
        for field in self.fields:
            if field in row and row[field] is not None:
                plain = str(row[field]).encode("utf-8")
                row[field] = self._fernet.encrypt(plain).decode("utf-8")
        yield row


class WriteToMongoDoFn(beam.DoFn):
    """Writes each row to a MongoDB collection. Opens one connection per worker."""

    def __init__(self, mongo_uri, db_name, collection_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name

    def setup(self):
        from pymongo import MongoClient
        self._client = MongoClient(self.mongo_uri)
        self._collection = self._client[self.db_name][self.collection_name]

    def process(self, element):
        # Remove _id if it came from BigQuery to avoid conflicts
        element.pop("_id", None)
        self._collection.insert_one(element)
        yield element

    def teardown(self):
        if hasattr(self, "_client"):
            self._client.close()


def run():
    parser = argparse.ArgumentParser(description="Claims/Members ETL pipeline")

    # Custom pipeline args
    parser.add_argument("--source_table",    required=True,  help="BQ table: project:dataset.table")
    parser.add_argument("--table_type",      required=True,  choices=["claims", "members"])
    parser.add_argument("--mongo_uri",       required=True)
    parser.add_argument("--mongo_db",        required=True)
    parser.add_argument("--mongo_collection",required=True)
    parser.add_argument("--encryption_key",  required=True)

    known_args, pipeline_args = parser.parse_known_args()

    fields_to_encrypt = ENCRYPT_FIELDS[known_args.table_type]

    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

    logging.info("Starting pipeline for table_type=%s", known_args.table_type)
    logging.info("Source: %s  ->  MongoDB: %s/%s", known_args.source_table, known_args.mongo_db, known_args.mongo_collection)
    logging.info("Encrypting fields: %s", fields_to_encrypt)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromBigQuery"  >> beam.io.ReadFromBigQuery(table=known_args.source_table)
            | "EncryptFields"     >> beam.ParDo(EncryptFieldsDoFn(fields_to_encrypt, known_args.encryption_key))
            | "WriteToMongoDB"    >> beam.ParDo(WriteToMongoDoFn(known_args.mongo_uri, known_args.mongo_db, known_args.mongo_collection))
        )

    logging.info("Pipeline finished.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
