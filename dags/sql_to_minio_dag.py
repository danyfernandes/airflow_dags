"""
This module defines an Airflow DAG for extracting data from SQL databases, 
saving the results locally and uploading the processed files to a MinIO bucket. 
The DAG operates as a pipeline, executing SQL queries,
saving results in chunks, and transferring the data to MinIO for storage or further processing.

The key components of this pipeline include:
1. SQL query execution with chunked processing for large datasets.
2. File storage in formats such as CSV, Parquet and JSON.
3. Integration with MinIO for file upload and management.

Environment variables and Airflow Variables are used to dynamically configure the DAG, 
enabling flexible deployment across different environments and datasets.
"""

import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from jinja2 import Template

DEFAULT_DATE = pendulum.datetime(1970, 1, 1)


def execute_sql_and_save(
    sql_conn_id: str,
    queries: dict,
    tmp_dir: str = "/tmp/",
    chunksize: int = 50000,
    **kwargs,
):
    """
    Executes SQL queries defined in the provided configuration, saves results to local files,
    and handles large datasets using chunking.
    """
    conn = BaseHook.get_connection(sql_conn_id)
    conn_type = conn.conn_type

    if conn_type.lower() == "mysql":
        hook = MySqlHook(mysql_conn_id=sql_conn_id)
        logging.info("MySQL hook initialized!")
    elif conn_type.lower() == "postgres":
        hook = PostgresHook(postgres_conn_id=sql_conn_id)
        logging.info("PostgreSQL hook initialized!")
    else:
        raise AirflowFailException(f"Unsupported SQL connection ID: {sql_conn_id}")

    task_instance = kwargs["task_instance"]
    dag_id = kwargs["dag"].dag_id
    local_files = []

    dag_run_date = kwargs["dag_run"].start_date.strftime("%Y-%m-%d %H:%M:%S")

    last_run_timestamp = Variable.get(f"{dag_id}_last_run", default_var=None)
    if not last_run_timestamp:
        last_run_timestamp = "1970-01-01 00:00:00"

    current_run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    logging.info("Last run time: %s", last_run_timestamp)

    rendered_context = {
        "now": current_run_timestamp,
        "last_run_timestamp": last_run_timestamp,
    }

    for filename, details in queries.items():
        raw_sql = details["sql"]

        # If `raw_sql` is a file path, load the SQL content
        if os.path.exists(raw_sql):
            with open(raw_sql, "r", encoding="utf-8") as sql_file:
                raw_sql = sql_file.read()
            logging.info("SQL query loaded from file: %s", sql_file.name)

        # Render SQL with Jinja templates
        try:
            template = Template(raw_sql)
            sql = template.render(rendered_context)
            logging.debug("Rendered SQL query: %s", sql)
        except Exception as e:
            raise AirflowFailException(f"Failed to render SQL template: {e}") from e

        file_format = details["format"]
        dynamic_file_name = details.get("dynamic_file_name", False)
        constructed_filename = f"{filename}.{file_format}"

        if dynamic_file_name:
            constructed_filename = f"{filename}_{formatted_timestamp}.{file_format}"

        local_filepath = os.path.join(tmp_dir, constructed_filename)

        try:
            with hook.get_sqlalchemy_engine().connect().execution_options(stream_results=True) as conn:
                temp_files = []
                data_returned = False
                chunk_count = 0
                logging.info("Executing query: %s", sql)
                for chunk_df in pd.read_sql(sql, conn, chunksize=chunksize):
                    chunk_count += 1
                    data_returned = True

                    logging.info("Processing chunk %d with %d rows", chunk_count, len(chunk_df))

                    chunk_df["source_date"] = dag_run_date

                    chunk_df.replace(to_replace=r"\x00", value="", regex=True, inplace=True)

                    chunk_df = chunk_df.apply(
                        lambda col: col.map(
                            lambda x: str(x).replace("\n", " ").replace("\r", " ") if isinstance(x, str) else x
                        )
                    )

                    if file_format.lower() == "csv":
                        chunk_df.to_csv(
                            local_filepath,
                            mode="a",
                            header=not os.path.exists(local_filepath),
                            index=False,
                            encoding="utf-8"
                        )
                    elif file_format.lower() == "parquet":
                        temp_file = os.path.join(
                            tmp_dir, f"temp_{filename}_{len(temp_files)}.parquet"
                        )
                        logging.info("Creating temporary Parquet file in %s...", temp_file)
                        chunk_df.to_parquet(
                            temp_file, engine="pyarrow", compression="snappy"
                        )
                        temp_files.append(temp_file)
                    elif file_format.lower() == "json":
                        temp_file = os.path.join(
                            tmp_dir, f"temp_{filename}_{len(temp_files)}.json"
                        )
                        logging.info("Creating temporary JSON file in %s...", temp_file)
                        chunk_df.to_json(temp_file, orient="records", index=False, date_format="iso")
                        temp_files.append(temp_file)
                    else:
                        raise ValueError(f"Unsupported file format: {file_format}")

                if data_returned:
                    if file_format.lower() == "parquet":
                        logging.info("Merging temporary Parquet files into final file...")
                        combined_df = pd.concat(
                            [pd.read_parquet(temp_file) for temp_file in temp_files],
                            ignore_index=True,
                        )
                        combined_df.to_parquet(
                            local_filepath, engine="pyarrow", compression="snappy"
                        )

                        for temp_file in temp_files:
                            os.remove(temp_file)
                        logging.info(
                            "Parquet file successfully written to %s", local_filepath
                        )
                    elif file_format.lower() == "json":
                        logging.info("Merging temporary JSON files into final file...")
                        dataframes = [pd.read_json(temp_file) for temp_file in temp_files]
                        combined_df = pd.concat(dataframes, ignore_index=True)

                        combined_df.to_json(local_filepath, orient="records", index=False, date_format="iso")

                        for temp_file in temp_files:
                            os.remove(temp_file)

                        logging.info("JSON file successfully written to %s", local_filepath)
                    local_files.append(
                        {"original_name": filename, "filepath": local_filepath}
                    )
                    logging.info("File %s successfully processed!", constructed_filename)
                else:
                    logging.warning("No data returned for query %s. No file created.", sql)
        except Exception as e:
            logging.error("Failed to execute query for %s: %s", filename, e)
            raise AirflowFailException(
                f"Failed to execute query for {filename}: {e}"
            ) from e

    Variable.set(f"{dag_id}_last_run", current_run_timestamp)
    task_instance.xcom_push(key="files_metadata", value=local_files)

    return local_files


def upload_to_minio(bucket_name: str, object_path: str, **kwargs):
    """
    Uploads the local files generated by the SQL task to a specified MinIO bucket.
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
        key="files_metadata", task_ids="execute_sql_and_save"
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


try:
    configs_json = Variable.get("sql_configs")
    logging.info("Configs JSON: %s", configs_json)
    sql_configs = json.loads(configs_json)
except Exception as e:
    raise AirflowFailException(f"Failed to load SQL configurations: {e}") from e


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 10, 10),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


def create_sql_to_minio_dag(dag_config: dict) -> DAG:
    """
    Creates an Airflow DAG that extracts data from a SQL database,
    processes it, and uploads it to MinIO.

    Args:
        dag_config (dict): Configuration dictionary for the DAG, including:
                           - dag_id: Unique ID for the DAG.
                           - sql_conn_id: Connection ID for the SQL database.
                           - queries: Dictionary of queries to execute.
                           - bucket_name: MinIO bucket for uploads.
                           - object_path: MinIO object path prefix.

    Returns:
        DAG: An Airflow DAG instance configured with the specified settings.
    """
    dag_id = dag_config["dag_id"]
    sql_conn_id = dag_config["sql_conn_id"]
    queries = dag_config["queries"]
    bucket_name = dag_config["bucket_name"]
    object_path = dag_config["object_path"]

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description="SQL-to-MinIO pipeline",
        schedule_interval=dag_config.get("interval", None),
        max_active_runs=1,
        catchup=False,
        tags=["sql", "minio"],
    ) as dag:

        execute_sql_task = PythonOperator(
            task_id="execute_sql_and_save",
            python_callable=execute_sql_and_save,
            provide_context=True,
            op_kwargs={
                "sql_conn_id": sql_conn_id,
                "queries": queries,
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

        execute_sql_task.set_downstream(upload_to_minio_task)

    return dag


for config in sql_configs:
    d_id = config.get("dag_id", "Unknown DAG ID")
    try:
        globals()[d_id] = create_sql_to_minio_dag(config)
    except KeyError as e:
        logging.error("Missing key in configuration for DAG %s: %s", d_id, e)
    except AirflowException as e:
        logging.error("Airflow error creating DAG %s: %s", d_id, e)
    except TypeError as e:
        logging.error("Type error creating DAG %s: %s", d_id, e)
