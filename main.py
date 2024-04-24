import json
import os
import io
from flask import Flask, jsonify, request
import re

import xml.etree.ElementTree as ET
import pandas as pd

from google.cloud import storage, bigquery, secretmanager
from google.oauth2 import service_account
from cloudevents.http import from_http

import logging
import time
import base64

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)

app = Flask(__name__)
"""
json_file_path = os.path.join(os.path.dirname(__file__), 'fivetran-408613-a359e31bc8dd.json')

credentials = service_account.Credentials.from_service_account_file(json_file_path)
storage_client = storage.Client.from_service_account_json(json_file_path)
bigquery_client = bigquery.Client.from_service_account_json(json_file_path)

project_id = credentials.project_id
"""
# ________________

# Google Cloud Project ID
project_id = 'fivetran-408613'
secret_name = 'CRun_sec'
client_sm = secretmanager.SecretManagerServiceClient()

# Access the secret
name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
response = client_sm.access_secret_version(request={"name": name})

# Extract the secret value (JSON string)
secret_value_json = response.payload.data.decode("UTF-8")

# Load the JSON string into a dictionary
secret_value_dict = json.loads(secret_value_json)

# Log the retrieved credentials
# print("Retrieved credentials from Secret Manager: %s", secret_value_dict)

# Use the dictionary to create credentials
credentials = service_account.Credentials.from_service_account_info(
    secret_value_dict
)

# Use the content to initialize the clients
storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
project_id = credentials.project_id


# ________________


def load_csv_to_dataframe(file_content, separator):
    return pd.read_csv(io.StringIO(file_content), sep=separator)


def convert_df_to_str(df):
    df = df.astype(str)
    df.replace("<NA>", "None", inplace=True)
    return df


def schemas_are_compatible(schema1, schema2):
    """
    Checks if two BigQuery table schemas are compatible.
    Returns:
        bool: True if schemas are compatible, False otherwise.
    """
    # Check field count and order
    if len(schema1) != len(schema2):
        return False
    for i in range(len(schema1)):
        if schema1[i].name != schema2[i].name:
            return False
        type1 = schema1[i].field_type
        type2 = schema2[i].field_type
        if type1 != type2:
            if type1 in ("STRING", "INTEGER") and type2 in ("STRING", "INTEGER"):
                continue
            return False
    return True


def check_table_existence(dataset_id, table_name):
    try:
        tables = bigquery_client.list_tables(dataset_id)
        tables_names = [table.table_id for table in tables]

        if table_name in tables_names:
            logger.info(f' Table {table_name} exists in dataset {dataset_id}.')
            return True
        else:
            logger.info(f' Table {table_name} does not exist in dataset {dataset_id}.')
            return False
    except Exception as e:
        logger.error(f" Error checking table existence: {e}")
        return False


# Modified
def add_new_rows_streaming(table_ref, new_rows):
    logger.info("-------------- Adding rows in Table via Mule | Delta Mode -----------------")
    new_rows['file_source'] = "via Mulesoft"
    """
    # Convert timestamps to strings (using temporary Series)
    for col in ['insertion_timestamp', 'ingestion_timestamp']:
        if col in new_rows:  # Check if column exists before conversion
            temp_series = pd.Series([new_rows[col]])
            new_rows[col] = temp_series.dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    """
    # Create temporary Series for timestamps (single element)
    now_timestamp = pd.to_datetime('now')
    temp_series = pd.Series([now_timestamp])

    # Use dt.strftime on the Series to format timestamps
    new_rows['insertion_timestamp'] = temp_series.dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    new_rows['ingestion_timestamp'] = temp_series.dt.strftime('%Y-%m-%d %H:%M:%S.%f')

    new_rows = new_rows.astype(str)
    schema = [bigquery.SchemaField(col, "STRING") for col in new_rows.columns]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition='WRITE_APPEND'
    )
    job = bigquery_client.load_table_from_dataframe(new_rows, table_ref, job_config=job_config)

    logger.info(f"Added {len(new_rows)} new rows to table {table_ref}.")
    job.result()


@app.route("/", methods=["POST"])
def index():
    """Receive and parse Pub/Sub messages."""
    envelope = request.get_json()
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    pubsub_message = envelope["message"]

    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        message = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
        formatted_message = ""
        for line in message.splitlines():
            formatted_message += line.strip()

        data = json.loads(formatted_message)
        dataset_id = data.get("Dataset_ID")
        table_id = data.get("Table_ID")
        info = data.get("INFO", {})

        df = pd.DataFrame([info])  # Create DataFrame from the INFO dictionary
        logger.info(f"  - dataframe : {df}")

        logger.info(f"  - dataset ID : {dataset_id}")
        logger.info(f"  - table ID: {table_id}")
        logger.info(f"  - table INFO: {info}")

        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        logger.info(f"  - Dataset reference: {dataset_ref}")

        table_ref = dataset_ref.table(table_id)
        logger.info(f"  - Table reference: {table_ref}")

        table_exists = check_table_existence(dataset_ref, table_id)

        # Dynamically infer schema from DataFrame
        schema = [bigquery.SchemaField(col, "STRING") for col in df.columns]
        logger.info(f'Blob Schema : {schema} | Columns but String typed .')

        if not table_exists:
            logger.info("--------------Table not exist-----------------")
        else:
            logger.info("-------------- Table exist-----------------")
            existing_schema = bigquery_client.get_table(table_ref).schema
            existing_schema_without_timestamp = existing_schema[:-3]
            logger.info(f" Existing schema  : {existing_schema}")
            logger.info(f" Existing schema without timestamp  : {existing_schema_without_timestamp}")
            # Compare the existing schema with the inferred schema
            if not schemas_are_compatible(existing_schema_without_timestamp, schema):
                raise ValueError("DataFrame schema is incompatible with existing table schema")
            df = convert_df_to_str(df)
            add_new_rows_streaming(table_ref, df)

    return jsonify({'Message': 'inserted row to BigQuery tables successfully!'})


if __name__ == "__main__":
    app.run(debug=True, port=5000)