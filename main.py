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


def create_table(blob, table_ref, df, bucket_name):
    logger.info("-------------- Creating Table -----------------")
    full_path = f"gs://{bucket_name}/{blob.name}"
    df['file_source'] = full_path
    df['insertion_timestamp'] = pd.to_datetime('now')
    df['ingestion_timestamp'] = pd.to_datetime('now')
    schema = [bigquery.SchemaField(col, "STRING") for col in df.columns]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
    )
    job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    logger.warning(f" File {blob.name} successfully ingested into BigQuery!")
    job.result()


def update_table_full_mode(blob, table_ref, df, bucket_name):
    logger.info("-------------- Updating Table | Full Mode -----------------")
    full_path = f"gs://{bucket_name}/{blob.name}"
    df['file_source'] = full_path
    df['insertion_timestamp'] = pd.to_datetime('now')
    df['ingestion_timestamp'] = pd.to_datetime('now')
    schema = [bigquery.SchemaField(col, "STRING") for col in df.columns]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition='WRITE_TRUNCATE'
    )
    job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    logger.warning(f"File {blob.name} successfully ingested in full mode into BigQuery!")
    job.result()


# Modified
def update_table_rows(blob, table_ref, primary_keys, updatable_rows, bucket_name):
    logger.info("-------------- Updating Table | Delta Mode -----------------")
    full_path = f"gs://{bucket_name}/{blob.name}"
    updatable_rows['file_source'] = full_path
    updatable_rows['ingestion_timestamp'] = pd.to_datetime('now')
    non_primary_keys = [key for key in updatable_rows.columns if key not in primary_keys]

    update_values = ', '.join([f"{column} = @param{i + 1}" for i, column in enumerate(non_primary_keys)])
    where_clause = ' AND '.join(
        [f"{key} = @param{len(non_primary_keys) + i + 1}" for i, key in enumerate(primary_keys)])

    # Prepare SQL UPDATE statement
    update_query = f"""
        UPDATE `{table_ref}`
        SET {update_values}
        WHERE {where_clause}
    """
    logger.info(f" Update Query: {update_query}")

    for index, updatable_row_value in updatable_rows.iterrows():
        updatable_row_values = updatable_row_value.to_list()
        logger.info(f" Updatable row values : {updatable_row_values}")

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                                 bigquery.ScalarQueryParameter(f"param{i + 1}", "STRING",
                                                               str(updatable_row_value[column]))
                                 for i, column in enumerate(non_primary_keys)
                             ] + [
                                 bigquery.ScalarQueryParameter(f"param{len(non_primary_keys) + i + 1}", "STRING",
                                                               str(updatable_row_value[key]))
                                 for i, key in enumerate(primary_keys)
                             ]

        )
        query_job = bigquery_client.query(
            update_query,
            job_config=job_config
        )
        query_job.result()
    logger.info(f" Updated {len(updatable_rows)} new rows to table {table_ref}.")


# Modified
def add_new_rows(blob, table_ref, new_rows, bucket_name):
    logger.info("-------------- Adding rows in Table | Delta Mode -----------------")
    full_path = f"gs://{bucket_name}/{blob.name}"
    new_rows['file_source'] = full_path
    new_rows['insertion_timestamp'] = pd.to_datetime('now')
    new_rows['ingestion_timestamp'] = pd.to_datetime('now')
    """
    new_rows.insert(len(new_rows.columns), 'file_source', full_path)
    new_rows.insert(len(new_rows.columns), 'ingestion_timestamp', pd.to_datetime('now'))
    new_rows.insert(len(new_rows.columns), 'ingestion_timestamp', pd.to_datetime('now'))
    logger.error(f"new rows datadrame with timestamp : {new_rows}")
    """
    schema = [bigquery.SchemaField(col, "STRING") for col in new_rows.columns]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition='WRITE_APPEND'
    )
    job = bigquery_client.load_table_from_dataframe(new_rows, table_ref, job_config=job_config)

    logger.info(f"Added {len(new_rows)} new rows to table {table_ref}.")
    job.result()


# Modified
def add_new_rows_streaming(table_ref, new_rows):
    logger.info("-------------- Adding rows in Table via Mule | Delta Mode -----------------")
    new_rows['file_source'] = "via Mulesoft"
    new_rows['insertion_timestamp'] = pd.to_datetime('now')
    new_rows['ingestion_timestamp'] = pd.to_datetime('now')

    schema = [bigquery.SchemaField(col, "STRING") for col in new_rows.columns]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition='WRITE_APPEND'
    )
    job = bigquery_client.load_table_from_dataframe(new_rows, table_ref, job_config=job_config)

    logger.info(f"Added {len(new_rows)} new rows to table {table_ref}.")
    job.result()


def create_or_update_table(bucket, bucket_name, blob, table_ref, table_name, dataset_ref, df, keys, primary_keys, mode):
    try:
        # Dynamically infer schema from DataFrame
        schema = [bigquery.SchemaField(col, "STRING") for col in df.columns]
        logger.info(f'Blob Schema : {schema} | Columns but String typed .')

        # If the table does not exist, create it
        logger.info("-------------- Check Table Existence -----------------")
        table_exists = check_table_existence(dataset_ref, table_name)

        # Apply data type conversion to the entire DataFrame
        df = df.map(lambda x: str(x) if pd.notna(x) else None)
        # df = df.fillna('YOUR_NULL_VALUE_REPRESENTATION', dtype='str') #for large datasets
        df = convert_df_to_str(df)
        # df = convert_nan_to_int(df)

        if not table_exists:
            create_table(blob, table_ref, df, bucket_name)
        else:
            logger.info("-------------- Updating Table -----------------")
            # Retrieve the existing schema
            existing_schema = bigquery_client.get_table(table_ref).schema
            existing_schema_without_timestamp = existing_schema[:-3]
            logger.info(f" Existing schema  : {existing_schema}")
            logger.info(f" Existing schema without timestamp  : {existing_schema_without_timestamp}")
            # Compare the existing schema with the inferred schema
            if not schemas_are_compatible(existing_schema_without_timestamp, schema):
                move_blob_to_folder(bucket, blob, "Error")
                raise ValueError("DataFrame schema is incompatible with existing table schema")
            if mode == 'full':
                update_table_full_mode(blob, table_ref, df, bucket_name)
            elif mode == 'delta':
                logger.info("-------------- Updating Table | Delta Mode -----------------")
                existing_rows_query = f"""
                    SELECT {','.join(key for key in keys)}
                    FROM `{table_ref}`
                """
                existing_pk_rows_query = f"""
                     SELECT {','.join(primary_keys)}
                     FROM `{table_ref}`
                 """
                # Retrieve existing rows and their values
                existing_rows = bigquery_client.query(existing_rows_query).result()
                existing_values = set(tuple(row[key] for key in keys) for row in existing_rows)
                logger.info(f" Existing rows : {existing_values}")
                # Identify new rows using a set of primary key combinations
                if len(primary_keys) == 1:
                    # Retrieve primary keys rows and their values
                    existing_pk_rows = bigquery_client.query(existing_pk_rows_query).result()
                    existing_pk_values = [row[key] for key in primary_keys for row in existing_pk_rows]
                    logger.info(f" Existing primary keys values : {existing_pk_values}")
                    # Single primary key
                    primary_key = primary_keys[0]
                    logger.info(f"- Primary_key: {primary_key}")
                    new_rows = df[~df[primary_key].isin(existing_pk_values)]
                    logger.info(f"- New rows to add : {new_rows}")
                    updatable_rows = df[df[primary_key].isin(existing_pk_values)]
                    logger.info(f"- Rows to update : {updatable_rows}")
                else:
                    # Retrieve primary keys rows and their Values
                    existing_pk_rows = bigquery_client.query(existing_pk_rows_query).result()
                    existing_pk_values = list(tuple(row[key] for key in primary_keys) for row in existing_pk_rows)
                    logger.info(f" Existing primary keys values : {existing_pk_values}")
                    # Multiple primary keys
                    logger.info(f"- Primary_keys: {primary_keys}")
                    # Reset the index of the DataFrame before applying operations
                    df_reset = df.reset_index(drop=True)
                    # Identify new rows to add
                    new_rows_condition = ~df_reset[primary_keys].apply(tuple, axis=1).isin(existing_pk_values)
                    new_rows = df_reset[new_rows_condition]
                    logger.info(f"- New rows to add : {new_rows}")
                    # Identify rows to update based on multiple primary keys
                    updatable_rows_condition = df_reset[primary_keys].apply(tuple, axis=1).isin(existing_pk_values)
                    updatable_rows = df_reset[updatable_rows_condition]
                    logger.info(f"- Rows to update : {updatable_rows}")

                if not updatable_rows.empty:
                    updatable_rows = convert_df_to_str(updatable_rows)
                    update_table_rows(blob, table_ref, primary_keys, updatable_rows, bucket_name)
                else:
                    logger.info(f" No rows to update !!")
                if not new_rows.empty:
                    # Adjust data types if needed (replace with your specific conversions)
                    new_rows = convert_df_to_str(new_rows)
                    # new_rows = convert_nan_to_int(new_rows)
                    add_new_rows(blob, table_ref, new_rows, bucket_name)
                else:
                    logger.info(f" No new rows to insert !!")

                logger.warning(f" File {blob.name} successfully ingested in delta mode into BigQuery!")

    except Exception as e:
        move_blob_to_folder(bucket, blob, "Error")
        logger.error(f" Error creating or updating table: {e}")


def process_blob(bucket, bucket_name, blob, dataset_ref, table_name, separator, primary_keys, mode):
    file_content = blob.download_as_text()
    # logger.info(f" starting a new file having file content: {file_content}")
    # Check if the file content is empty
    if not file_content.strip():
        logger.warning(f"Skipping empty CSV file: {blob.name}")
        move_blob_to_folder(bucket, blob, "Error")
        return

    df = load_csv_to_dataframe(file_content, separator)
    logger.info(f" Blob Dataframe : {df}")
    target_keys = list(df.columns)
    logger.info(f" Blob Columns : {target_keys}")
    table_ref = dataset_ref.table(table_name)

    logger.info("-------------- Create Or Update Process  -----------------")
    create_or_update_table(bucket, bucket_name, blob, table_ref, table_name, dataset_ref, df, target_keys, primary_keys,
                           mode)


def create_folders(bucket):
    """
    Creates Archive/Error/Streaming folders if they do not exist in the bucket.
    """
    archive_folder = bucket.blob(f"Archive/")
    if not archive_folder.exists():
        archive_folder.upload_from_string('')
        logger.info(" Folder Archive created")

    error_folder = bucket.blob(f"Error/")
    if not error_folder.exists():
        error_folder.upload_from_string('')
        logger.info(" Folder Error created")

    streaming_folder = bucket.blob(f"Streaming/")
    if not streaming_folder.exists():
        streaming_folder.upload_from_string('')
        logger.info(" Folder Steaming created")


def move_blob_to_folder(bucket, blob, folder_name):
    """
    Moves a blob from the specified bucket to an archive / error folder within the same bucket.
    """
    try:
        source_blob = blob
        destination_blob = bucket.blob(f"{folder_name}/{blob.name}")
        destination_blob.upload_from_string(source_blob.download_as_string())
        source_blob.delete()

        logger.warning(f" Blob '{blob}' successfully moved to '{folder_name}' folder.")
    except Exception as e:
        logger.error(f" Error moving blob '{blob}': {e}")


def process_blob_with_error_handling(bucket, bucket_name, blob, dataset_ref, table_name, separator, primary_keys, mode):
    """
    Call the process blob function and then move the blob to archive folder if processed , Error else.
    """
    try:
        logger.info("-------------- Start Processing Blob -----------------")
        process_blob(bucket, bucket_name, blob, dataset_ref, table_name, separator, primary_keys, mode)
        if not blob.exists():
            logger.error(f"Blob | {blob} | not found or moved already to error folder")
        else:
            move_blob_to_folder(bucket, blob, "Archive")
            logger.info(f" Archiving after processing with error handling: {blob}")
    except Exception as e:
        move_blob_to_folder(bucket, blob, "Error")
        logger.info(f"Blob | {blob} | moved to ERROR after processing with error handling")
        raise  # Re-raise to allow for further handling


def get_configs(tree, blob_name):
    root = tree.getroot()

    parts_dataset = os.path.splitext(os.path.basename(blob_name))[0].rsplit('_')
    target_dataset = parts_dataset[0]
    parts_table = os.path.splitext(os.path.basename(blob_name))[0].rsplit('_')
    target_table_name = parts_table[-3]
    logger.info(f" Data extracted from the Request ; Dataset : {target_dataset}, Table : {target_table_name}")

    primary_keys_list = []
    separator = None
    mode = ""
    for dataset in root.find('bucket').findall('dataset'):
        dataset_name = dataset.find('name').text
        if dataset.find('name').text == target_dataset:
            for table in dataset.findall('table'):
                table_name = table.find('name').text
                if table_name == target_table_name:
                    logger.info(f" Table found in config.xml")
                    primary_keys_elements = table.findall('primary-keys/key')
                    if primary_keys_elements:
                        primary_keys_list = [key.text for key in primary_keys_elements]
                        mode = "delta"
                    else:
                        primary_keys_list = []
                        mode = "full"
                    separator = table.find('separator').text
                else:
                    logger.info(f"Table not found in config.xml")
                logger.info(
                    f"Bucket : esm el bucket, Dataset : {target_dataset}, Table : {target_table_name}, Mode : {mode}, Separator : {separator}, Primary keys : {primary_keys_list}")

    return target_dataset, target_table_name, mode, separator, primary_keys_list


@app.route('/primary_keys', methods=['GET', 'POST'])
def convert_csv_to_bigquery():
    print("start")
    start_time = time.time()

    try:
        data_config = request.json
        logger.info(f"Request received to /primary keys endpoint ")
        dataset_id = data_config.get('dataset')
        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        # dataset_ref = bigquery_client.dataset(dataset_id)
        bucket_name = data_config.get('bucket_name')
        bucket = storage_client.bucket(bucket_name)
        logger.info(f"Bucket: {bucket}")
        tables_info = data_config.get('tables')

        logger.info(
            f" Data extracted from the Request : bucket : {bucket}:{bucket_name}, Dataset reference: {dataset_ref},tables info : {tables_info}")
        logger.info("-------------- END of variables ----------------")

        create_folders(bucket)
        logger.info("-------------- Folders Created -----------------")

        filename_pattern = r'^\w+_\w+_\d{8}_\d{6}\.csv$'

        for blob in bucket.list_blobs():
            logger.info(f" Looping over Blobs | blob path: {blob.name}")

            # Exclude blobs in specific folders directly
            if blob.name.startswith("Error/") or blob.name.startswith("Archive/") or (blob.name == "Streaming/"):
                continue
            if not blob.name.endswith('.csv'):
                logger.warning(f"Skipping non CSV file: {blob.name}")
                move_blob_to_folder(bucket, blob, "Error")
                continue  # Skip non-CSV files
            if not re.match(filename_pattern, os.path.basename(blob.name)):
                logger.warning(f"Skipping non-respecting the naming format file: {blob.name}")
                move_blob_to_folder(bucket, blob, "Error")
                continue

            parts = os.path.splitext(os.path.basename(blob.name))[0].rsplit('_')
            table_name = parts[1]
            logger.info(f" Table name from Blob: {table_name}")
            if table_name in tables_info:  # Check if the table is present in the received data
                table_config = tables_info[table_name]  # Get the configuration for this table
                separator = table_config['separator']
                primary_keys = table_config['primary_keys'].split(',')  # Split string into a list
                mode = table_config['mode']
                try:
                    logger.info(f"Processing blob: {blob.name}")
                    process_blob_with_error_handling(bucket, bucket_name, blob, dataset_ref, table_name, separator,
                                                     primary_keys,
                                                     mode)
                except Exception as e:
                    logger.error(f"Error processing blob '{blob.name}': {e}")
            else:
                logger.info(f" Skipping Blob '{blob.name}' | Not a target table.")

        total_time = time.time() - start_time
        logger.error(f"Total execution time: {total_time:.2f} seconds")
        return jsonify({'message': 'CSV files converted to BigQuery tables successfully!'})

    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/streaming', methods=['GET', 'POST'])
def convert_csv_to_bigquery_streaming():
    print(f"start ingestion in streaming")

    start_time = time.time()
    try:
        # Create a CloudEvent object from the incoming request
        event = from_http(request.headers, request.data)
        logger.info(f"Event Request received to /Streaming endpoint ")

        object_name = event.get_data()['message']['attributes']['objectId']
        logger.info(f"  - Object ID: {object_name}")

        # Get the directory of the current script
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # Construct the path to the XML file relative to the current directory
        xml_file_path = os.path.join(current_dir, 'config.xml')

        # Parse the XML file
        tree = ET.parse(xml_file_path)

        target_dataset, target_table_name, mode, separator, primary_keys_list = get_configs(tree, object_name)

        bucket_name = event.get_data()['message']['attributes']['bucketId']
        bucket = storage_client.bucket(bucket_name)
        dataset_id = target_dataset
        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        # dataset_ref = bigquery_client.dataset(dataset_id)
        logger.info(f"  - Bucket: {bucket}")
        logger.info(f"  - Dataset reference: {dataset_ref}")
        logger.info(f"  - Mode : {mode}")
        logger.info(f"  - Separator : {separator}")
        logger.info(f"  - Primary keys list: {primary_keys_list}")

        logger.info("-------------- END of variables----------------")

        create_folders(bucket)
        logger.info("-------------- Folders Created -----------------")

        filename_pattern = r'^\w+_\w+_\d{8}_\d{6}\.csv$'
        for blob in bucket.list_blobs():
            logger.info(f" Looping over Blobs | blob path: {blob.name}")

            # Exclude blobs in specific folders directly
            if blob.name.startswith("Error/") or blob.name.startswith("Archive/") or (blob.name == "Streaming/"):
                continue
            if not blob.name.endswith('.csv'):
                logger.warning(f" Skipping non CSV file: {blob.name}")
                move_blob_to_folder(bucket, blob, "Error")
                continue  # Skip non-CSV files
            if not re.match(filename_pattern, os.path.basename(blob.name)):
                logger.warning(f" Skipping non-respecting the naming format file: {blob.name}")
                move_blob_to_folder(bucket, blob, "Error")
                continue

            parts = os.path.splitext(os.path.basename(blob.name))[0].rsplit('_')
            table_name = parts[1]
            logger.info(f" Table name from Blob: {table_name}")
            if table_name == target_table_name:  # Check if the table is present in the received data
                try:
                    logger.info(f"Processing blob: {blob.name}")
                    process_blob_with_error_handling(bucket, bucket_name, blob, dataset_ref, table_name, separator,
                                                     primary_keys_list,
                                                     mode)
                except Exception as e:
                    logger.error(f"Error processing blob '{blob.name}': {e}")
            else:
                logger.info(f" Skipping Blob '{blob.name}' | Not a target table.")

        total_time = time.time() - start_time
        logger.error(f" Total execution time: {total_time:.2f} seconds")
        return jsonify({'Message': 'CSV files converted to BigQuery tables successfully!'})

    except Exception as e:
        return jsonify({'error': str(e)})


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
