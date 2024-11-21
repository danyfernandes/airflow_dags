"""
This module defines functions and tasks to create an Airflow DAG that monitors
an SFTP server for new files, downloads them, and uploads them to a MinIO bucket.
The module includes functions for checking file stability, polling for new files, 
and performing file transfer operations, as well as handling configurations from Airflow Variables.

Key functionalities:
- `get_last_poll_time` and `update_last_poll_time` manage the last polling time.
- `check_file_size_stable` verifies the stability of a file's size on the SFTP server.
- `poll_sftp_for_files` periodically checks the SFTP folder for new files.
- `download_and_upload_to_minio` handles downloading files from SFTP and uploading to MinIO.
- `create_sftp_to_minio_dag` constructs the DAG with tasks for polling and transferring files.

The module uses `PythonOperator` and `TriggerDagRunOperator` to create a self-triggering
DAG that continuously monitors the SFTP server and uploads detected files to MinIO.
"""

import json
import logging
import os
import re
import socket
import time
from datetime import datetime, timedelta
from typing import Any, Optional

import boto3
import botocore.exceptions
import pytz
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from dotenv import load_dotenv
from paramiko import SSHException

load_dotenv()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 10, 10),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 100,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
}


def get_last_poll_time(dag_id: str) -> Optional[datetime]:
    """
    Retrieves the last poll time for the specified DAG from Airflow variables.
    """
    last_poll_time = Variable.get(f"{dag_id}_last_poll_time", default_var=None)
    return datetime.fromisoformat(last_poll_time) if last_poll_time else None


def update_last_poll_time(dag_id: str, poll_time: datetime) -> None:
    """
    Updates the last poll time for the specified DAG in Airflow variables.
    """
    Variable.set(f"{dag_id}_last_poll_time", poll_time.isoformat())


def check_file_size_stable(
    ssh_conn_id: str,
    remote_file_path: str,
    poke_interval: int = 5,
    max_retries: int = 3,
    retry_delay: int = 10,
) -> bool:
    """
    Continuously checks if the file size on the SFTP server has stabilized.
    Retries if the file size cannot be obtained due to connection issues.
    """
    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)

    attempt = 0
    while attempt < max_retries:
        try:
            ssh_client = ssh_hook.get_conn()
            sftp_client = ssh_client.open_sftp()
            last_size = sftp_client.stat(remote_file_path).st_size
            sftp_client.close()
            ssh_client.close()
            break
        except (SSHException, IOError) as e:
            attempt += 1
            logging.error(
                "Error getting file size for %s: %s. Retrying in %d seconds...",
                remote_file_path,
                e,
                retry_delay,
            )
            time.sleep(retry_delay)
    else:
        logging.error(
            "Failed to get file size for %s after %d attempts.",
            remote_file_path,
            max_retries,
        )
        return False

    logging.info("Checking file size stability for %s", remote_file_path)

    attempt = 0
    while True:
        time.sleep(poke_interval)
        try:
            ssh_client = ssh_hook.get_conn()
            sftp_client = ssh_client.open_sftp()
            new_size = sftp_client.stat(remote_file_path).st_size
            sftp_client.close()
            ssh_client.close()
        except (SSHException, IOError) as e:
            attempt += 1
            logging.error(
                "Error getting file size for %s: %s. Retrying...", remote_file_path, e
            )
            if attempt == max_retries:
                logging.error(
                    "Failed to get file size for %s after %d attempts.",
                    remote_file_path,
                    max_retries,
                )
                return False
            continue

        if new_size == last_size:
            logging.info("File size is stable for file: %s", remote_file_path)
            return True

        logging.info(
            "File size not stable yet for %s. Last size: %d, New size: %d. Retrying...",
            remote_file_path,
            last_size,
            new_size,
        )
        last_size = new_size


def poll_sftp_for_files(
    dag_id: str,
    sftp_conn_id: str,
    sftp_folder_path: str,
    poke_interval: int = 30,
    buffer_seconds: int = 300,
    **context: Any,
) -> None:
    """
    Continuously polls the SFTP directory until new files are detected,
    with indefinite retries on connection issues.
    """
    last_poll_time = get_last_poll_time(dag_id)
    if not last_poll_time:
        last_poll_time = datetime.now()
        update_last_poll_time(dag_id, last_poll_time)
        logging.info(
            "Initialized last poll time: %s. Skipping pre-existing files.",
            last_poll_time,
        )

    new_files = []

    while True:
        try:
            if last_poll_time:
                logging.info(
                    "Starting new poll. Last poll time was: %s", last_poll_time
                )
            else:
                logging.info("Starting initial poll. No previous poll time recorded.")

            ssh_hook = SSHHook(ssh_conn_id=sftp_conn_id)
            ssh_client = ssh_hook.get_conn()
            sftp_client = ssh_client.open_sftp()

            # List and sort files in the SFTP directory by modification time
            all_files = sftp_client.listdir_attr(sftp_folder_path)
            all_files.sort(key=lambda x: x.st_mtime if x.st_mtime is not None else 0)

            # Filter files based on last poll time with buffer (buffer_seconds)
            adjusted_last_poll_time = last_poll_time - timedelta(seconds=buffer_seconds)
            for file_attr in all_files:
                if (
                    file_attr.st_mtime is not None
                    and datetime.fromtimestamp(file_attr.st_mtime)
                    > adjusted_last_poll_time
                ):
                    new_files.append(f"{sftp_folder_path}/{file_attr.filename}")

            sftp_client.close()
            ssh_client.close()

            if new_files:
                context["task_instance"].xcom_push(key="new_files", value=new_files)
                logging.info("Files found: %s", new_files)
                update_last_poll_time(dag_id, datetime.now())
                return

            logging.info("No new files found. Sleeping for %d seconds.", poke_interval)
            time.sleep(poke_interval)

        except (SSHException, socket.error, EOFError) as e:
            logging.error(
                "Connection error: %s. Retrying after %d seconds...", e, poke_interval
            )
            time.sleep(poke_interval)

        except Exception as e:
            logging.error("Unexpected error: %s", e)
            raise


def download_and_upload_to_minio(
    sftp_conn_id: str,
    sftp_folder_path: str,
    sftp_server_name: str,
    max_retries: int = 3,
    **context: Any,
) -> None:
    """
    Downloads files from an SFTP server and uploads them to a MinIO bucket.
    Files are processed based on configurations stored in a PostgreSQL database,
    with options for retrying on failure and deleting the original file after upload.
    """
    files_found = context["task_instance"].xcom_pull(
        task_ids="poll_sftp_for_files", key="new_files"
    )
    if not files_found:
        logging.info("No new files found to process.")
        return

    pg_hook = PostgresHook(postgres_conn_id="postgres_tool")
    conn = BaseHook.get_connection("minio_conn")
    s3_client = boto3.client(
        "s3",
        endpoint_url=Variable.get("MINIO_ENDPOINT"),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )

    query = f"""
        SELECT file_pattern, bucket_name, object_path, suppression 
        FROM tec.config_source WHERE sftp_name = '{sftp_server_name}'
    """
    config_records = pg_hook.get_records(query)

    ssh_hook = SSHHook(ssh_conn_id=sftp_conn_id)
    ssh_client = ssh_hook.get_conn()
    sftp_client = ssh_client.open_sftp()

    for file in files_found:
        if not check_file_size_stable(sftp_conn_id, file):
            logging.info("Skipping unstable file: %s", file)
            continue

        file_name = os.path.basename(file)
        local_filepath = f"/tmp/{file_name}"
        file_matched = False

        for file_pattern, bucket_name, object_path, suppression in config_records:
            file_pattern = file_pattern.encode("utf-8").decode("unicode_escape")
            if re.match(file_pattern, file_name):
                file_matched = True

                if suppression == 0:
                    logging.info("Suppression set to 0. Skipping file: %s", file)
                    break

                # Retrieve the modification time from the SFTP server
                try:
                    file_stat = sftp_client.stat(file)
                    paris_tz = pytz.timezone("Europe/Paris")
                    file_creation_time = None
                    if file_stat.st_mtime is not None:
                        file_creation_time = datetime.fromtimestamp(
                            file_stat.st_mtime, paris_tz
                        ).strftime("%Y-%m-%d %H:%M:%S")
                except FileNotFoundError:
                    logging.warning(
                        "File %s not found on SFTP server. Setting creation time to None.",
                        file,
                    )
                    file_creation_time = None

                insert_query = """
                    INSERT INTO tec.sensor_detection (sftp_server, folder, filename, date_creation)
                    VALUES (%s, %s, %s, %s)
                """
                pg_hook.run(
                    insert_query,
                    parameters=(
                        sftp_server_name,
                        sftp_folder_path,
                        file_name,
                        file_creation_time,
                    ),
                )

                success = False
                for attempt in range(max_retries):
                    try:
                        sftp_client.get(file, local_filepath)
                        logging.info("Downloaded file %s to %s", file, local_filepath)

                        s3_client.upload_file(
                            local_filepath,
                            bucket_name,
                            os.path.join(object_path, file_name),
                        )
                        logging.info(
                            "File %s uploaded to MinIO bucket %s", file, bucket_name
                        )
                        success = True
                        break
                    except (
                        SSHException,
                        socket.error,
                        EOFError,
                        IOError,
                        botocore.exceptions.BotoCoreError,
                    ) as e:
                        delay = 10 * (2**attempt)
                        logging.error(
                            "Error during file download/upload for %s: %s. "
                            "Retrying in %d seconds...",
                            file,
                            e,
                            delay,
                        )
                        time.sleep(delay)

                if not success:
                    logging.error(
                        "Failed to download/upload file %s after %d attempts.",
                        file,
                        max_retries,
                    )
                    continue

                os.remove(local_filepath)
                if suppression == 2:
                    sftp_client.remove(file)
                    logging.info("File %s deleted from SFTP server.", file)

        if not file_matched:
            logging.warning("No configuration matched for file: %s", file)

    sftp_client.close()
    ssh_client.close()


try:
    configs_json = Variable.get("sftp_poller_configs")
    logging.info("Configs JSON: %s", configs_json)
    sftp_poller_configs = json.loads(configs_json)
except Exception as e:
    raise AirflowFailException(f"Failed to load SFTP poller configurations: {e}") from e


def create_sftp_to_minio_dag(dag_config: dict) -> DAG:
    """
    Creates an Airflow DAG that monitors an SFTP server for new files,
    downloads them, and uploads them to MinIO. The DAG uses a polling
    task to detect new files and triggers itself to continuously monitor
    the SFTP server.
    """
    dag_id = dag_config["dag_id"]
    sftp_conn_id = dag_config["sftp_conn_id"]
    sftp_server_name = dag_config["sftp_server_name"]
    sftp_folder_path = dag_config["sftp_folder_path"]
    poke_interval = dag_config.get("poke_interval", 30)

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description="Robust DAG to monitor SFTP, download files, and upload to MinIO",
        schedule_interval=None,
        max_active_runs=1,
        catchup=False,
    ) as dag:

        poll_sftp_for_files_task = PythonOperator(
            task_id="poll_sftp_for_files",
            python_callable=poll_sftp_for_files,
            provide_context=True,
            op_kwargs={
                "dag_id": dag_id,
                "sftp_conn_id": sftp_conn_id,
                "sftp_folder_path": sftp_folder_path,
                "poke_interval": poke_interval,
            },
        )

        download_and_upload_to_minio_task = PythonOperator(
            task_id="download_and_upload_to_minio",
            python_callable=download_and_upload_to_minio,
            provide_context=True,
            op_kwargs={
                "sftp_conn_id": sftp_conn_id,
                "sftp_folder_path": sftp_folder_path,
                "sftp_server_name": sftp_server_name,
            },
        )

        trigger_self = TriggerDagRunOperator(
            task_id="trigger_self",
            trigger_dag_id=dag_id,
            wait_for_completion=False,
            reset_dag_run=True,
            doc="Trigger the same DAG again to continue polling.",
        )

        poll_sftp_for_files_task.set_downstream(download_and_upload_to_minio_task)
        download_and_upload_to_minio_task.set_downstream(trigger_self)

    return dag


for config in sftp_poller_configs:
    d_id = config.get("dag_id", "Unknown DAG ID")
    try:
        globals()[d_id] = create_sftp_to_minio_dag(config)
    except KeyError as e:
        logging.error("Missing key in configuration for DAG %s: %s", d_id, e)
    except AirflowException as e:
        logging.error("Airflow error creating DAG %s: %s", d_id, e)
    except TypeError as e:
        logging.error("Type error creating DAG %s: %s", d_id, e)
