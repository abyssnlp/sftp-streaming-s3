"""
Stream SFTP from server to S3 bucket with Prometheus monitoring
"""

from typing import Final
import boto3
import hashlib
import logging
import psutil
import time
import threading
from prometheus_client import (
    CollectorRegistry,
    Gauge,
    Counter,
    Histogram,
    push_to_gateway,
)
from dotenv import load_dotenv
from paramiko import SFTPClient

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
registry = CollectorRegistry()


s3_client = boto3.client("s3", endpoint_url="http://localhost:9000")


"""
    Read from SFTP server in chunks
    Write to S3 bucket using multipart upload
    Compute checksum for each chunk and store it in S3 metadata
    Handle errors and retries
"""


class SFTPToS3Streamer:
    CHUNK_SIZE: Final[int] = 100 * 1024 * 1024

    def __init__(
        self,
        sftp_client: SFTPClient,
        session: boto3.Session,
        bucket_name: str,
        push_gateway_url: str,
        job_name: str = "sftp-s3-streaming",
    ):
        self.sftp_client = sftp_client
        self.s3_client = session.client("s3", endpoint_url="http://localhost:9000")
        self.bucket_name = bucket_name
        self.push_gateway_url = push_gateway_url
        self.job_name = job_name

        self.registry = CollectorRegistry()
        self.setup_metrics()

        self.process = psutil.Process()
        self.monitoring_active = False
        self.monitoring_thread = None

    def setup_metrics(self):
        """Initialize Prometheus metrics"""
        self.memory_usage_bytes = Gauge(
            "sftp_s3_memory_usage_bytes",
            "Memory usage in bytes during streaming",
            registry=self.registry,
        )
        self.memory_percent = Gauge(
            "sftp_s3_memory_percent",
            "Memory usage percentage during streaming",
            registry=self.registry,
        )

        self.disk_usage_bytes = Gauge(
            "sftp_s3_disk_usage_bytes",
            "Disk usage in bytes during streaming",
            registry=self.registry,
        )
        self.disk_io_read_bytes = Counter(
            "sftp_s3_disk_io_read_bytes_total",
            "Total disk read bytes",
            registry=self.registry,
        )
        self.disk_io_write_bytes = Counter(
            "sftp_s3_disk_io_write_bytes_total",
            "Total disk write bytes",
            registry=self.registry,
        )

        self.bytes_transferred = Counter(
            "sftp_s3_bytes_transferred_total",
            "Total bytes transferred from SFTP to S3",
            registry=self.registry,
        )
        self.chunks_processed = Counter(
            "sftp_s3_chunks_processed_total",
            "Total chunks processed",
            registry=self.registry,
        )
        self.transfer_duration = Histogram(
            "sftp_s3_transfer_duration_seconds",
            "Time taken for file transfer",
            registry=self.registry,
        )

        self.throughput_mbps = Gauge(
            "sftp_s3_throughput_mbps",
            "Current throughput in MB/s",
            registry=self.registry,
        )

    def start_monitoring(self):
        """Start background monitoring of system resources"""
        self.monitoring_active = True
        self.monitoring_thread = threading.Thread(
            target=self._monitor_resources, daemon=True
        )
        self.monitoring_thread.start()
        logger.info("Started system resource monitoring")

    def stop_monitoring(self):
        """Stop background monitoring"""
        self.monitoring_active = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=1)
        logger.info("Stopped system resource monitoring")

    def _monitor_resources(self):
        """Background thread to monitor system resources"""
        initial_disk_io = psutil.disk_io_counters()
        last_read_bytes = initial_disk_io.read_bytes if initial_disk_io else 0
        last_write_bytes = initial_disk_io.write_bytes if initial_disk_io else 0

        while self.monitoring_active:
            try:
                memory_info = self.process.memory_info()
                memory_percent = self.process.memory_percent()

                self.memory_usage_bytes.set(memory_info.rss)
                self.memory_percent.set(memory_percent)

                try:
                    disk_usage = psutil.disk_usage(".")
                    self.disk_usage_bytes.set(disk_usage.used)

                    current_disk_io = psutil.disk_io_counters()
                    if current_disk_io:
                        read_diff = current_disk_io.read_bytes - last_read_bytes
                        write_diff = current_disk_io.write_bytes - last_write_bytes

                        if read_diff > 0:
                            self.disk_io_read_bytes.inc(read_diff)
                        if write_diff > 0:
                            self.disk_io_write_bytes.inc(write_diff)

                        last_read_bytes = current_disk_io.read_bytes
                        last_write_bytes = current_disk_io.write_bytes

                except Exception as e:
                    logger.warning(f"Error monitoring disk usage: {e}")

                self.push_metrics()

                time.sleep(2)

            except Exception as e:
                logger.error(f"Error in resource monitoring: {e}")
                time.sleep(2)

    def push_metrics(self):
        """Push metrics to Prometheus Push Gateway"""
        try:
            push_to_gateway(
                self.push_gateway_url, job=self.job_name, registry=self.registry
            )
        except Exception as e:
            logger.warning(f"Failed to push metrics: {e}")

    def _get_chunk_from_sftp(self, remote_path: str, offset: int) -> bytes:
        with self.sftp_client.open(remote_path, "rb") as remote_file:
            remote_file.seek(offset)
            return remote_file.read(self.CHUNK_SIZE)

    def upload_to_s3(self, path: str) -> None:
        try:
            logger.info(f"Attempting to access file: {path}")

            import os

            parent_dir = os.path.dirname(path)
            filename = os.path.basename(path)

            logger.info(f"Checking parent directory: {parent_dir}")
            try:
                files_in_dir = self.sftp_client.listdir(parent_dir)
                logger.info(f"Files in {parent_dir}: {files_in_dir}")

                if filename not in files_in_dir:
                    logger.error(
                        f"File '{filename}' not found in directory '{parent_dir}'"
                    )
                    logger.info(f"Available files: {', '.join(files_in_dir)}")
                    raise FileNotFoundError(
                        f"File '{filename}' not found in '{parent_dir}'. Available files: {', '.join(files_in_dir)}"
                    )
            except Exception as dir_error:
                logger.warning(f"Could not list directory {parent_dir}: {dir_error}")

            file_stat = self.sftp_client.stat(path)
            remote_file_size = file_stat.st_size or 0
            logger.info(f"File found! Size: {remote_file_size} bytes")

        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
            raise
        except Exception as e:
            logger.error(f"Error accessing file {path}: {e}")
            raise

        offset = 0
        part_number = 1
        total_bytes_processed = 0

        self.start_monitoring()
        start_time = time.time()

        overall_hash = hashlib.sha256()

        upload_id = self.s3_client.create_multipart_upload(
            Bucket=self.bucket_name, Key=path
        )["UploadId"]
        parts = []

        try:
            while offset < remote_file_size:
                chunk_start_time = time.time()
                chunk = self._get_chunk_from_sftp(path, offset)

                if not chunk:
                    break

                overall_hash.update(chunk)
                total_bytes_processed += len(chunk)

                part = self.s3_client.upload_part(
                    Bucket=self.bucket_name,
                    Key=path,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk,
                )

                parts.append({"ETag": part["ETag"], "PartNumber": part_number})

                self.bytes_transferred.inc(len(chunk))
                self.chunks_processed.inc()

                chunk_duration = time.time() - chunk_start_time
                throughput_mbps = 0.0
                if chunk_duration > 0:
                    throughput_mbps = (len(chunk) / (1024 * 1024)) / chunk_duration
                    self.throughput_mbps.set(throughput_mbps)

                offset += len(chunk)
                part_number += 1

                logger.info(
                    f"Uploaded part {part_number-1}/{(remote_file_size + self.CHUNK_SIZE - 1) // self.CHUNK_SIZE} "
                    f"({len(chunk)} bytes, {throughput_mbps:.2f} MB/s)"
                )

            total_duration = time.time() - start_time
            self.transfer_duration.observe(total_duration)

            final_hash = overall_hash.hexdigest()

            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=path,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            self.s3_client.copy_object(
                Bucket=self.bucket_name,
                CopySource={"Bucket": self.bucket_name, "Key": path},
                Key=path,
                Metadata={
                    "original-size": str(remote_file_size),
                    "sha256-checksum": final_hash,
                },
                MetadataDirective="REPLACE",
            )

            s3_object = self.s3_client.head_object(Bucket=self.bucket_name, Key=path)
            s3_size = s3_object["ContentLength"]

            if s3_size != remote_file_size:
                raise ValueError(
                    f"File size mismatch: SFTP={remote_file_size}, S3={s3_size}"
                )

            if total_bytes_processed != remote_file_size:
                raise ValueError(
                    f"Bytes processed mismatch: Expected={remote_file_size}, Processed={total_bytes_processed}"
                )

            final_throughput = (remote_file_size / (1024 * 1024)) / total_duration
            logger.info(f"Upload completed for {path} to bucket {self.bucket_name}")
            logger.info(
                f"Transfer stats: {remote_file_size} bytes in {total_duration:.2f}s ({final_throughput:.2f} MB/s)"
            )
            logger.info(f"File integrity: SHA256={final_hash}")
            logger.info(
                "Basic integrity verification passed (size and byte count match)"
            )

        except Exception as e:
            logger.error(f"Upload failed: {e}")
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name, Key=path, UploadId=upload_id
                )
            except Exception:
                pass
            raise
        finally:
            self.stop_monitoring()
            self.push_metrics()

    def _compute_sftp_file_checksum(self, path: str) -> str:
        hasher = hashlib.sha256()
        with self.sftp_client.open(path, "rb") as remote_file:
            while True:
                data = remote_file.read(self.CHUNK_SIZE)
                if not data:
                    break
                hasher.update(data)
        return hasher.hexdigest()
