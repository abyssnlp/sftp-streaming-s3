from typing import Final, Dict, Any, Optional
import aioboto3
import logging
import psutil
import time
import asyncio
from dataclasses import dataclass
from prometheus_client import (
    CollectorRegistry,
    Gauge,
    Counter,
    push_to_gateway,
)
from dotenv import load_dotenv
import asyncssh

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ChunkData:
    """Container for chunk data and metadata"""

    part_number: int
    data: bytes
    offset: int
    size: int


class AsyncSFTPToS3Streamer:
    CHUNK_SIZE: Final[int] = 64 * 1024 * 1024
    MAX_CONCURRENT_UPLOADS: Final[int] = 10
    MAX_CONCURRENT_READS: Final[int] = 6

    def __init__(
        self,
        bucket_name: str,
        push_gateway_url: str,
        job_name: str = "sftp-s3-streaming",
        ssh_connection_params: Optional[Dict[str, Any]] = None,
        s3_endpoint_url: str = "http://localhost:9000",
    ):
        self.bucket_name = bucket_name
        self.push_gateway_url = push_gateway_url
        self.job_name = job_name
        self.s3_endpoint_url = s3_endpoint_url
        self.ssh_connection_params = ssh_connection_params or {}

        self.registry = CollectorRegistry()
        self.setup_metrics()
        self.process = psutil.Process()
        self.initial_disk_usage = psutil.disk_usage(".").used
        self.s3_session = aioboto3.Session()
        self.read_semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_READS)
        self.upload_semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_UPLOADS)

    def setup_metrics(self):
        """Initialize simplified Prometheus metrics"""
        self.memory_usage_mb = Gauge(
            "sftp_s3_memory_usage_mb",
            "Memory usage in MB during streaming",
            registry=self.registry,
        )
        self.disk_usage_increase_mb = Gauge(
            "sftp_s3_disk_usage_increase_mb",
            "Disk usage increase in MB (indicates writing to disk)",
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
        self.throughput_mbps = Gauge(
            "sftp_s3_throughput_mbps",
            "Current throughput in MB/s",
            registry=self.registry,
        )

    async def _create_sftp_connection(self):
        """Create a new SFTP connection"""
        try:
            conn_params = self.ssh_connection_params.copy()
            conn_params["known_hosts"] = None
            conn = await asyncssh.connect(**conn_params)
            sftp = await conn.start_sftp_client()
            return sftp, conn
        except Exception as e:
            logger.error(f"Failed to create SFTP connection: {e}")
            raise

    async def _read_chunk(
        self, remote_path: str, part_number: int, offset: int, chunk_size: int
    ) -> Optional[ChunkData]:
        """Read a single chunk from SFTP"""
        async with self.read_semaphore:
            sftp, conn = None, None
            try:
                sftp, conn = await self._create_sftp_connection()

                async with sftp.open(remote_path, "rb") as remote_file:
                    await remote_file.seek(offset)
                    data = await remote_file.read(chunk_size)

                    if not data:
                        return None

                    return ChunkData(
                        part_number=part_number,
                        data=data,
                        offset=offset,
                        size=len(data),
                    )

            except Exception as e:
                logger.error(f"Error reading chunk {part_number}: {e}")
                return None
            finally:
                if conn:
                    conn.close()

    async def _upload_chunk(
        self, upload_id: str, remote_path: str, chunk_data: ChunkData
    ) -> Optional[Dict[str, Any]]:
        """Upload a single chunk to S3"""
        async with self.upload_semaphore:
            try:
                async with self.s3_session.client(
                    "s3", endpoint_url=self.s3_endpoint_url
                ) as s3_client:
                    part = await s3_client.upload_part(
                        Bucket=self.bucket_name,
                        Key=remote_path,
                        PartNumber=chunk_data.part_number,
                        UploadId=upload_id,
                        Body=chunk_data.data,
                    )

                self.bytes_transferred.inc(chunk_data.size)
                self.chunks_processed.inc()

                logger.debug(
                    f"Uploaded part {chunk_data.part_number} ({chunk_data.size} bytes)"
                )

                return {"ETag": part["ETag"], "PartNumber": chunk_data.part_number}

            except Exception as e:
                logger.error(f"Error uploading chunk {chunk_data.part_number}: {e}")
                return None

    async def _process_chunk(
        self,
        upload_id: str,
        remote_path: str,
        part_number: int,
        offset: int,
        chunk_size: int,
    ) -> Optional[Dict[str, Any]]:
        """Read and upload a single chunk"""
        chunk_data = await self._read_chunk(
            remote_path, part_number, offset, chunk_size
        )
        if not chunk_data:
            return None

        return await self._upload_chunk(upload_id, remote_path, chunk_data)

    def _update_metrics(self):
        """Update system metrics"""
        memory_mb = self.process.memory_info().rss / (1024 * 1024)
        self.memory_usage_mb.set(memory_mb)

        current_disk_usage = psutil.disk_usage(".").used
        disk_increase_mb = (current_disk_usage - self.initial_disk_usage) / (
            1024 * 1024
        )
        self.disk_usage_increase_mb.set(disk_increase_mb)

    async def _push_metrics(self):
        """Push metrics to Prometheus Push Gateway"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: push_to_gateway(
                    self.push_gateway_url, job=self.job_name, registry=self.registry
                ),
            )
        except Exception as e:
            logger.warning(f"Failed to push metrics: {e}")

    async def _verify_integrity(self, remote_path: str, s3_size: int) -> bool:
        """Simple integrity verification by comparing file sizes"""
        try:
            sftp, conn = await self._create_sftp_connection()
            try:
                file_stat = await sftp.stat(remote_path)
                sftp_size = file_stat.size

                if sftp_size == s3_size:
                    logger.info(
                        f"✓ Integrity verified: SFTP size ({sftp_size}) == S3 size ({s3_size})"
                    )
                    return True
                else:
                    logger.error(
                        f"✗ Integrity check failed: SFTP size ({sftp_size}) != S3 size ({s3_size})"
                    )
                    return False

            finally:
                conn.close()

        except Exception as e:
            logger.error(f"Error during integrity verification: {e}")
            return False

    async def upload_to_s3(self, path: str) -> None:
        """Main method to upload file from SFTP to S3"""
        start_time = time.time()

        sftp, conn = await self._create_sftp_connection()
        try:
            file_stat = await sftp.stat(path)
            file_size = file_stat.size or 0
            logger.info(
                f"File size: {file_size} bytes ({file_size / (1024*1024):.1f} MB)"
            )
        finally:
            conn.close()

        total_chunks = (file_size + self.CHUNK_SIZE - 1) // self.CHUNK_SIZE
        logger.info(
            f"Starting transfer: {total_chunks} chunks of {self.CHUNK_SIZE // (1024*1024)}MB each"
        )

        async with self.s3_session.client(
            "s3", endpoint_url=self.s3_endpoint_url
        ) as s3_client:
            upload_response = await s3_client.create_multipart_upload(
                Bucket=self.bucket_name, Key=path
            )
            upload_id = upload_response["UploadId"]

        try:
            tasks = []
            for i in range(total_chunks):
                part_number = i + 1
                offset = i * self.CHUNK_SIZE
                chunk_size = min(self.CHUNK_SIZE, file_size - offset)

                task = asyncio.create_task(
                    self._process_chunk(
                        upload_id, path, part_number, offset, chunk_size
                    )
                )
                tasks.append(task)

            parts = []
            completed = 0

            for task in asyncio.as_completed(tasks):
                result = await task
                if result:
                    parts.append(result)
                    completed += 1

                    self._update_metrics()

                    progress = (completed / total_chunks) * 100
                    elapsed = time.time() - start_time
                    throughput = (
                        (completed * self.CHUNK_SIZE / (1024 * 1024)) / elapsed
                        if elapsed > 0
                        else 0
                    )
                    self.throughput_mbps.set(throughput)

                    if completed % 5 == 0 or completed == total_chunks:
                        logger.info(
                            f"Progress: {progress:.1f}% ({completed}/{total_chunks}) - {throughput:.1f} MB/s"
                        )
                        await self._push_metrics()

            parts.sort(key=lambda x: x["PartNumber"])

            async with self.s3_session.client(
                "s3", endpoint_url=self.s3_endpoint_url
            ) as s3_client:
                await s3_client.complete_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=path,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )

                s3_object = await s3_client.head_object(
                    Bucket=self.bucket_name, Key=path
                )
                s3_size = s3_object["ContentLength"]

            integrity_ok = await self._verify_integrity(path, s3_size)

            total_duration = time.time() - start_time
            final_throughput = (file_size / (1024 * 1024)) / total_duration

            self._update_metrics()
            await self._push_metrics()

            logger.info(f"🚀 UPLOAD COMPLETED! 🚀")
            logger.info(f"File: {path} -> bucket {self.bucket_name}")
            logger.info(f"Size: {file_size} bytes in {total_duration:.2f}s")
            logger.info(f"Throughput: {final_throughput:.2f} MB/s")
            logger.info(f"Memory usage: {self.memory_usage_mb._value._value:.1f} MB")
            logger.info(
                f"Disk usage increase: {self.disk_usage_increase_mb._value._value:.1f} MB"
            )
            logger.info(f"Integrity: {'✓ VERIFIED' if integrity_ok else '✗ FAILED'}")

        except Exception as e:
            logger.error(f"Upload failed: {e}")
            try:
                async with self.s3_session.client(
                    "s3", endpoint_url=self.s3_endpoint_url
                ) as s3_client:
                    await s3_client.abort_multipart_upload(
                        Bucket=self.bucket_name, Key=path, UploadId=upload_id
                    )
            except Exception:
                pass
            raise
