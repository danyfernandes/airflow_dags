"""
This module defines an Airflow DAG for retrieving data from APIs, 
processing it, and uploading the resulting
files to a MinIO bucket. The pipeline supports different 
authentication methods and can handle dynamic 
parameters and file formats.

Features:
- Fetches data from APIs using GET requests with support for parameters and authentication.
- Supports multiple authentication types: None, Basic, OAuth1, OAuth2, and API Token.
- Saves API responses in CSV, JSON, or Parquet formats.
- Uploads processed files to MinIO storage for further usage.

Components:
1. `handle_authentication`: Handles various authentication types 
    and generates appropriate headers or tokens.
2. `fetch_api_and_save`: Fetches data from APIs, processes it, and saves it to local files.
3. `extract_data`: Extracts relevant data from JSON responses using JSONPath expressions.
4. `upload_to_minio`: Uploads the saved files to MinIO storage.
5. `create_api_to_minio_dag`: Dynamically creates DAGs based 
    on configuration for API-to-MinIO workflows.
"""

import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from boto3 import client
from jsonpath_ng.ext import parse
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth1


def handle_authentication(auth_type: str, auth_config: dict):
    """
    Handles authentication for API requests.
    """
    if auth_type.lower() == "none":
        return None

    if auth_type.lower() == "basic":
        return HTTPBasicAuth(auth_config["username"], auth_config["password"])

    if auth_type.lower() == "oauth1":
        return OAuth1(
            client_key=auth_config["consumer_key"],
            client_secret=auth_config["consumer_secret"],
            resource_owner_key=auth_config["access_token"],
            resource_owner_secret=auth_config["token_secret"],
            signature_method=auth_config.get("signature_method", "HMAC-SHA256"),
        )

    if auth_type == "oauth2":
        token_url = auth_config["token_url"]
        client_id = auth_config["client_id"]
        client_secret = auth_config["client_secret"]
        scope = auth_config.get("scope")

        try:
            data = {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            }
            if scope:
                data["scope"] = scope

            response = requests.post(
                token_url,
                data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30,
            )
            response.raise_for_status()
            access_token = response.json().get("access_token")
            if not access_token:
                raise ValueError("Failed to retrieve access token.")
            return {"Authorization": f"Bearer {access_token}"}
        except requests.exceptions.RequestException as e:
            logging.error("Failed to fetch OAuth2 token: %s", e)
            raise AirflowFailException(f"Failed to fetch OAuth2 token: {e}") from e

    if auth_type == "api_token":
        key_location = auth_config.get("key_location", "header")
        api_key = auth_config["api_key"]

        if key_location == "header":
            return {auth_config["key_name"]: api_key}

        if key_location == "query":
            return {"params": {auth_config["key_name"]: api_key}}

        if key_location == "url":
            return {
                "api_url": f"{auth_config['api_url']}?{auth_config['key_name']}={api_key}"
            }

        raise ValueError(f"Unsupported API token location: {key_location}")

    raise ValueError(f"Unsupported authentication type: {auth_type}")


def fetch_api_and_save(
    api_url: str,
    headers: dict,
    params: dict,
    auth_type: str,
    auth_config: dict,
    output_files: dict,
    tmp_dir: str = "/tmp/",
    **kwargs,
):
    """
    Fetches data from an API, processes it, and saves the output to local files.
    """
    task_instance = kwargs["task_instance"]
    dag_id = kwargs["dag"].dag_id
    local_files = []

    auth = handle_authentication(auth_type, auth_config)

    if isinstance(auth, dict):
        if "params" in auth:
            params.update(auth["params"])
        if "api_url" in auth:
            api_url = auth["api_url"]
        else:
            headers.update(auth)

    last_run_timestamp = Variable.get(f"{dag_id}_last_run", default_var=None)
    if not last_run_timestamp:
        last_run_timestamp = "1970-01-01 00:00:00"
    else:
        last_run_timestamp = datetime.fromisoformat(last_run_timestamp).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    current_run_timestamp = datetime.now().isoformat()
    formatted_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    params = {
        key: (
            value.format(last_run_timestamp=last_run_timestamp)
            if isinstance(value, str) and "{last_run_timestamp}" in value
            else value
        )
        for key, value in params.items()
    }

    try:
        logging.info("Making API request to %s", api_url)
        response = requests.get(
            api_url,
            headers=headers,
            params=params,
            auth=auth if not isinstance(auth, dict) else None,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        logging.error("API request failed: %s", e)
        raise AirflowFailException(f"API request failed: {e}") from e

    for filename, details in output_files.items():
        file_format = details["format"]
        data_path = details.get("data_path", "$[*]")
        dynamic_file_name = details.get("dynamic_file_name", False)
        extracted_data = extract_data(data, data_path)
        df = pd.json_normalize(extracted_data)
        filename_constructed = f"{filename}.{file_format}"

        if dynamic_file_name:
            filename_constructed = f"{filename}_{formatted_timestamp}.{file_format}"

        local_filepath = os.path.join(tmp_dir, filename_constructed)
        if file_format == "csv":
            df.to_csv(local_filepath, index=False)
        elif file_format == "parquet":
            df.to_parquet(local_filepath, index=False)
        elif file_format == "json":
            with open(local_filepath, "w", encoding="utf-8") as json_file:
                json.dump(extracted_data, json_file)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        local_files.append({"original_name": filename, "filepath": local_filepath})
        logging.info("Saved file: %s", local_filepath)

    Variable.set(f"{dag_id}_last_run", current_run_timestamp)
    task_instance.xcom_push(key="files_metadata", value=local_files)


def extract_data(data, json_path):
    """
    Extracts data from a JSON response using a JSONPath expression.
    """
    jsonpath_expr = parse(json_path)
    extracted = [match.value for match in jsonpath_expr.find(data)]

    if all(isinstance(item, list) for item in extracted):
        return [element for sublist in extracted for element in sublist]

    return extracted


def upload_to_minio(bucket_name: str, object_path: str, **kwargs):
    """
    Uploads local files to a MinIO bucket.
    """
    conn = BaseHook.get_connection("minio_conn")
    s3_client = boto3.client(
        "s3",
        endpoint_url=Variable.get("MINIO_ENDPOINT"),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )

    task_instance = kwargs["task_instance"]
    files_metadata = task_instance.xcom_pull(
        key="files_metadata", task_ids="fetch_api_and_save"
    )

    if not files_metadata:
        logging.warning("No file metadata found in XCom. Skipping!")
        return

    for file_info in files_metadata:
        local_filepath = file_info["filepath"]
        file_name_in_minio = os.path.basename(local_filepath)

        object_name = os.path.join(object_path, file_name_in_minio)
        try:
            s3_client.upload_file(local_filepath, bucket_name, object_name)
            logging.info(
                "Uploaded %s to bucket %s at %s",
                local_filepath,
                bucket_name,
                object_name,
            )
        except Exception as e:
            logging.error("Failed to upload %s to MinIO: %s", local_filepath, e)
            raise AirflowFailException(
                f"Failed to upload {local_filepath} to MinIO: {e}"
            ) from e


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 10, 10),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def create_api_to_minio_dag(dag_config: dict) -> DAG:
    """
    Dynamically creates an Airflow DAG for fetching data from APIs and uploading to MinIO.

    Args:
        dag_config (dict): Configuration dictionary for the DAG, including:
            - dag_id (str): Unique ID for the DAG.
            - api_url (str): API endpoint URL.
            - headers (dict): Additional headers for the API request.
            - params (dict): Query parameters for the API request.
            - auth_type (str): Type of authentication used for the API.
            - auth_config (dict): Configuration for authentication.
            - output_files (dict): File output settings (e.g., format, dynamic naming).
            - bucket_name (str): MinIO bucket name.
            - object_path (str): Path in MinIO to upload files.
            - interval (str): Cron expression for scheduling the DAG.

    Returns:
        DAG: An Airflow DAG instance configured for the specified API workflow.
    """

    dag_id = dag_config["dag_id"]
    api_url = dag_config["api_url"]
    headers = dag_config.get("headers", {})
    params = dag_config.get("params", {})
    auth_type = dag_config.get("auth_type", "none")
    auth_config = dag_config.get("auth_config", {})
    output_files = dag_config["output_files"]
    bucket_name = dag_config["bucket_name"]
    object_path = dag_config["object_path"]

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description="API-to-MinIO pipeline",
        schedule_interval=dag_config.get("interval", None),
        max_active_runs=1,
        catchup=False,
        tags=["api", "minio"],
    ) as dag:
        fetch_api_task = PythonOperator(
            task_id="fetch_api_and_save",
            python_callable=fetch_api_and_save,
            provide_context=True,
            op_kwargs={
                "api_url": api_url,
                "headers": headers,
                "params": params,
                "auth_type": auth_type,
                "auth_config": auth_config,
                "output_files": output_files,
            },
        )

        upload_to_minio_task = PythonOperator(
            task_id="upload_to_minio",
            python_callable=upload_to_minio,
            provide_context=True,
            op_kwargs={
                "bucket_name": bucket_name,
                "object_path": object_path,
            },
        )

        fetch_api_task.set_downstream(upload_to_minio_task)

    return dag


configs_json = Variable.get("api_configs")
api_configs = json.loads(configs_json)

for config in api_configs:
    d_id = config.get("dag_id", "Unknown DAG ID")
    globals()[d_id] = create_api_to_minio_dag(config)
