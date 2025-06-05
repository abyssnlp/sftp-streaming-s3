"""
Example script demonstrating SFTP to S3 streaming with monitoring
"""

import os
import boto3
import paramiko
from sftp_streaming.main import SFTPToS3Streamer


def main():
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh_client.connect(
        hostname=os.getenv("SFTP_HOST", "localhost"),
        username=os.getenv("SFTP_USER", "airflow"),
        password=os.getenv("SFTP_PASSWORD", "airflow123"),
        port=int(os.getenv("SFTP_PORT", "30022")),
    )

    sftp_client = ssh_client.open_sftp()

    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    streamer = SFTPToS3Streamer(
        sftp_client=sftp_client,
        session=session,
        bucket_name=os.getenv("S3_BUCKET", "data"),
        push_gateway_url=os.getenv("PUSH_GATEWAY", "http://localhost:9091"),
        job_name="large-file-transfer",
    )

    file_path = "/home/airflow/upload/customers_10gb.csv"

    print(f"Starting monitored transfer of {file_path}")
    print("Metrics will be pushed to Prometheus Push Gateway every 2 seconds")
    print("Check Grafana dashboard for real-time monitoring")

    streamer.upload_to_s3(file_path)

    print("Transfer completed! Check final metrics in Prometheus/Grafana")

    sftp_client.close()
    ssh_client.close()


if __name__ == "__main__":
    main()
