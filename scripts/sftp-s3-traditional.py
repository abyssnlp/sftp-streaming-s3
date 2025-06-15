import os
import time
import logging
from typing import Optional
import boto3
import paramiko
from paramiko import SFTPClient, SSHClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SFTPToS3:

    def __init__(
        self,
        ssh_host: str,
        ssh_port: int,
        ssh_username: str,
        ssh_password: str,
        s3_bucket: str,
        s3_endpoint_url: str = "http://localhost:9000",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        temp_dir: str = "./temp",
    ):
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.s3_bucket = s3_bucket
        self.s3_endpoint_url = s3_endpoint_url
        self.temp_dir = temp_dir

        os.makedirs(self.temp_dir, exist_ok=True)

        self.s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        logger.info(f"Initialized TraditionalSFTPToS3")
        logger.info(f"SFTP: {ssh_host}:{ssh_port}")
        logger.info(f"S3: {s3_endpoint_url}/{s3_bucket}")
        logger.info(f"Temp dir: {temp_dir}")

    def _create_sftp_connection(self) -> tuple[SFTPClient, SSHClient]:
        try:
            ssh_client = SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            logger.info(f"Connecting to SFTP server {self.ssh_host}:{self.ssh_port}")
            ssh_client.connect(
                hostname=self.ssh_host,
                port=self.ssh_port,
                username=self.ssh_username,
                password=self.ssh_password,
            )

            sftp_client = ssh_client.open_sftp()
            logger.info("SFTP connection established")
            return sftp_client, ssh_client

        except Exception as e:
            logger.error(f"Failed to create SFTP connection: {e}")
            raise

    def _download_from_sftp(self, remote_path: str, local_path: str) -> int:
        sftp_client, ssh_client = None, None

        try:
            sftp_client, ssh_client = self._create_sftp_connection()

            file_stat = sftp_client.stat(remote_path)
            file_size = file_stat.st_size or 0
            logger.info(
                f"Remote file size: {file_size} bytes ({file_size / (1024*1024):.1f} MB)"
            )

            start_time = time.time()
            logger.info(f"Starting download: {remote_path} -> {local_path}")

            def progress_callback(transferred, total):
                if total > 0:
                    percent = (transferred / total) * 100
                    elapsed = time.time() - start_time
                    if elapsed > 0:
                        speed_mbps = (transferred / (1024**2)) / elapsed
                        if (
                            transferred % (50 * 1024 * 1024) == 0
                            or transferred == total
                        ):  # Log every 50MB
                            logger.info(
                                f"Download progress: {percent:.1f}% | Speed: {speed_mbps:.1f} MB/s"
                            )

            sftp_client.get(remote_path, local_path, callback=progress_callback)

            download_time = time.time() - start_time
            download_speed = (
                (file_size / (1024**2)) / download_time if download_time > 0 else 0
            )

            logger.info(f"Download completed in {download_time:.2f}s")
            logger.info(f"Download speed: {download_speed:.2f} MB/s")

            return file_size

        except Exception as e:
            logger.error(f"Error downloading from SFTP: {e}")
            raise
        finally:
            if sftp_client:
                sftp_client.close()
            if ssh_client:
                ssh_client.close()

    def _upload_to_s3(self, local_path: str, s3_key: str, file_size: int) -> float:
        try:
            logger.info(
                f"Starting S3 upload: {local_path} -> s3://{self.s3_bucket}/{s3_key}"
            )

            start_time = time.time()

            self.s3_client.upload_file(local_path, self.s3_bucket, s3_key)

            upload_time = time.time() - start_time
            upload_speed = (
                (file_size / (1024**2)) / upload_time if upload_time > 0 else 0
            )

            logger.info(f"S3 upload completed in {upload_time:.2f}s")
            logger.info(f"S3 upload speed: {upload_speed:.2f} MB/s")

            s3_object = self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
            s3_size = s3_object["ContentLength"]

            if s3_size == file_size:
                logger.info(f"✓ Upload verified: {s3_size} bytes")
            else:
                logger.error(
                    f"✗ Upload verification failed: expected {file_size}, got {s3_size}"
                )

            return upload_time

        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            raise

    def transfer_file(self, remote_path: str, s3_key: Optional[str] = None) -> dict:
        if s3_key is None:
            s3_key = os.path.basename(remote_path)

        # Create temporary file path
        local_filename = (
            f"sftp_download_{int(time.time())}_{os.path.basename(remote_path)}"
        )
        local_path = os.path.join(self.temp_dir, local_filename)

        total_start_time = time.time()

        try:
            logger.info("=" * 60)
            logger.info("STARTING TRADITIONAL SFTP TO S3 TRANSFER")
            logger.info(f"Remote file: {remote_path}")
            logger.info(f"S3 destination: s3://{self.s3_bucket}/{s3_key}")
            logger.info(f"Temp file: {local_path}")
            logger.info("=" * 60)

            # Step 1: Download from SFTP to temp directory
            logger.info("STEP 1: Downloading from SFTP...")
            file_size = self._download_from_sftp(remote_path, local_path)

            # Step 2: Upload from temp directory to S3
            logger.info("STEP 2: Uploading to S3...")
            upload_time = self._upload_to_s3(local_path, s3_key, file_size)

            total_time = time.time() - total_start_time
            overall_throughput = (
                (file_size / (1024**2)) / total_time if total_time > 0 else 0
            )

            logger.info("=" * 60)
            logger.info("TRADITIONAL TRANSFER COMPLETED!")
            logger.info(f"File: {remote_path} -> s3://{self.s3_bucket}/{s3_key}")
            logger.info(
                f"File size: {file_size} bytes ({file_size / (1024**2):.1f} MB)"
            )
            logger.info(f"Total time: {total_time:.2f}s")
            logger.info(f"S3 upload time: {upload_time:.2f}s")
            logger.info(f"Overall throughput: {overall_throughput:.2f} MB/s")
            logger.info(f"Temp file used: {local_path}")
            logger.info("=" * 60)

            return {
                "file_size": file_size,
                "total_time": total_time,
                "upload_time": upload_time,
                "overall_throughput": overall_throughput,
                "temp_file": local_path,
                "success": True,
            }

        except Exception as e:
            logger.error(f"Transfer failed: {e}")
            return {"error": str(e), "temp_file": local_path, "success": False}
        finally:
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                    logger.info(f"Cleaned up temp file: {local_path}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup temp file {local_path}: {e}")


def main():
    from dotenv import load_dotenv

    load_dotenv()

    config = {
        "ssh_host": os.getenv("SFTP_HOST", "localhost"),
        "ssh_port": int(os.getenv("SFTP_PORT", "30022")),
        "ssh_username": os.getenv("SFTP_USER", "airflow"),
        "ssh_password": os.getenv("SFTP_PASSWORD", "airflow123"),
        "s3_bucket": os.getenv("S3_BUCKET", "data"),
        "s3_endpoint_url": os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }

    traditional_transfer = SFTPToS3(**config)
    remote_file = "/upload/customers_10gb.csv"
    result = traditional_transfer.transfer_file(
        remote_file, s3_key="traditional_customers_10gb.csv"
    )

    if result["success"]:
        print(f"\n🎉 Transfer completed successfully!")
        print(f"⏱️  Upload time to S3: {result['upload_time']:.2f} seconds")
        print(f"📊 Overall throughput: {result['overall_throughput']:.2f} MB/s")
    else:
        print(f"\n❌ Transfer failed: {result['error']}")


if __name__ == "__main__":
    main()
