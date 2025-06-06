"""
Stream SFTP from server to S3 bucket with Prometheus monitoring
High-performance async implementation using asyncio
"""

from typing import Final, Dict, Any, Optional
import aioboto3
import hashlib
import logging
import psutil
import time
import asyncio
import aiofiles
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from prometheus_client import (
    CollectorRegistry,
    Gauge,
    Counter,
    Histogram,
    push_to_gateway,
)
from dotenv import load_dotenv
import asyncssh

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
registry = CollectorRegistry()


@dataclass
class ChunkData:
    """Container for chunk data and metadata"""

    part_number: int
    data: bytes
    offset: int
    hash_chunk: str


class AsyncSFTPToS3Streamer:
    CHUNK_SIZE: Final[int] = 64 * 1024 * 1024
    MAX_CONCURRENT_UPLOADS: Final[int] = 12
    MAX_CONCURRENT_READS: Final[int] = 6
    BUFFER_SIZE: Final[int] = 32

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

        # Store SSH connection parameters
        self.ssh_connection_params = ssh_connection_params or {}

        self.registry = CollectorRegistry()
        self.setup_metrics()

        self.process = psutil.Process()
        self.monitoring_active = False
        self.monitoring_task = None

        # Async processing setup
        self.chunk_semaphore = asyncio.Semaphore(self.BUFFER_SIZE)
        self.upload_semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_UPLOADS)
        self.read_semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_READS)

        # Connection pools - pre-create connections
        self.sftp_pool = asyncio.Queue(maxsize=self.MAX_CONCURRENT_READS)
        self.s3_session = aioboto3.Session()

        # Pre-create shared S3 client for better performance
        self._s3_client = None

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

        # Async concurrency metrics
        self.concurrent_reads = Gauge(
            "sftp_s3_concurrent_reads",
            "Number of concurrent SFTP reads",
            registry=self.registry,
        )
        self.concurrent_uploads = Gauge(
            "sftp_s3_concurrent_uploads",
            "Number of concurrent S3 uploads",
            registry=self.registry,
        )
        self.queue_size = Gauge(
            "sftp_s3_queue_size",
            "Number of chunks in processing queue",
            registry=self.registry,
        )

    async def start_monitoring(self):
        """Start background monitoring of system resources"""
        self.monitoring_active = True
        self.monitoring_task = asyncio.create_task(self._monitor_resources())
        logger.info("Started async system resource monitoring")

    async def stop_monitoring(self):
        """Stop background monitoring"""
        self.monitoring_active = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped async system resource monitoring")

    async def _monitor_resources(self):
        """Background task to monitor system resources and concurrency metrics"""
        initial_disk_io = psutil.disk_io_counters()
        last_read_bytes = initial_disk_io.read_bytes if initial_disk_io else 0
        last_write_bytes = initial_disk_io.write_bytes if initial_disk_io else 0

        while self.monitoring_active:
            try:
                memory_info = self.process.memory_info()
                memory_percent = self.process.memory_percent()

                self.memory_usage_bytes.set(memory_info.rss)
                self.memory_percent.set(memory_percent)

                # Update concurrency metrics
                active_reads = self.MAX_CONCURRENT_READS - self.read_semaphore._value
                active_uploads = (
                    self.MAX_CONCURRENT_UPLOADS - self.upload_semaphore._value
                )
                queue_size = self.BUFFER_SIZE - self.chunk_semaphore._value

                self.concurrent_reads.set(active_reads)
                self.concurrent_uploads.set(active_uploads)
                self.queue_size.set(queue_size)

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

                await self.push_metrics()
                await asyncio.sleep(2)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in resource monitoring: {e}")
                await asyncio.sleep(2)

    async def push_metrics(self):
        """Push metrics to Prometheus Push Gateway"""
        try:
            # Run in thread pool since prometheus_client is synchronous
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: push_to_gateway(
                    self.push_gateway_url, job=self.job_name, registry=self.registry
                ),
            )
        except Exception as e:
            logger.warning(f"Failed to push metrics: {e}")

    async def _create_sftp_connection(
        self,
    ) -> tuple[asyncssh.SFTPClient, asyncssh.SSHClientConnection]:
        """Create a new SFTP connection"""
        try:
            # Add known_hosts=None to accept unknown host keys automatically
            conn_params = self.ssh_connection_params.copy()
            conn_params["known_hosts"] = None  # Accept any host key

            conn = await asyncssh.connect(**conn_params)
            sftp = await conn.start_sftp_client()
            return sftp, conn
        except Exception as e:
            logger.error(f"Failed to create SFTP connection: {e}")
            raise

    async def _initialize_connection_pool(self):
        """Pre-create SFTP connections for the pool"""
        logger.info(
            f"Initializing SFTP connection pool with {self.MAX_CONCURRENT_READS} connections..."
        )
        for i in range(self.MAX_CONCURRENT_READS):
            try:
                sftp, conn = await self._create_sftp_connection()
                await self.sftp_pool.put((sftp, conn))
                logger.debug(
                    f"Created SFTP connection {i+1}/{self.MAX_CONCURRENT_READS}"
                )
            except Exception as e:
                logger.error(f"Failed to create connection {i+1}: {e}")

    async def _get_sftp_connection(self):
        """Get an SFTP connection from the pool with timeout"""
        try:
            # Try to get from pool first
            sftp, conn = await asyncio.wait_for(self.sftp_pool.get(), timeout=5.0)
            return sftp, conn
        except asyncio.TimeoutError:
            # If pool is empty, create new connection
            logger.warning("SFTP pool timeout, creating new connection")
            return await self._create_sftp_connection()

    async def _return_sftp_connection(self, sftp, conn):
        """Return SFTP connection to pool if healthy"""
        try:
            # Simple health check - try to get current directory
            await sftp.getcwd()
            # Connection is healthy, return to pool
            await self.sftp_pool.put((sftp, conn))
        except Exception:
            # Connection is unhealthy, close it
            logger.debug("Closing unhealthy SFTP connection")
            conn.close()

    async def _read_chunk_worker(
        self, remote_path: str, chunk_info: tuple
    ) -> Optional[ChunkData]:
        """Async worker function to read a chunk from SFTP"""
        part_number, offset, chunk_size = chunk_info

        async with self.read_semaphore:
            try:
                sftp, conn = await self._get_sftp_connection()

                try:
                    async with sftp.open(remote_path, "rb") as remote_file:
                        await remote_file.seek(offset)
                        data = await remote_file.read(chunk_size)

                        if not data:
                            return None

                        # Compute hash for this chunk
                        chunk_hasher = hashlib.sha256()
                        chunk_hasher.update(data)

                        return ChunkData(
                            part_number=part_number,
                            data=data,
                            offset=offset,
                            hash_chunk=chunk_hasher.hexdigest(),
                        )
                finally:
                    await self._return_sftp_connection(sftp, conn)

            except Exception as e:
                logger.error(f"Error reading chunk {part_number}: {e}")
                return None

    async def _adaptive_chunk_worker(
        self, remote_path: str, chunk_info: tuple
    ) -> Optional[ChunkData]:
        """Optimized chunk reader with adaptive sizing and better error handling"""
        part_number, offset, chunk_size = chunk_info

        async with self.read_semaphore:
            start_time = time.time()
            try:
                sftp, conn = await self._get_sftp_connection()

                try:
                    async with sftp.open(remote_path, "rb") as remote_file:
                        await remote_file.seek(offset)

                        # Read in smaller increments for better responsiveness
                        data_chunks = []
                        remaining = chunk_size
                        read_size = min(8 * 1024 * 1024, remaining)  # 8MB increments

                        while remaining > 0:
                            increment = await remote_file.read(
                                min(read_size, remaining)
                            )
                            if not increment:
                                break
                            data_chunks.append(increment)
                            remaining -= len(increment)

                        if not data_chunks:
                            return None

                        data = b"".join(data_chunks)

                        # Compute hash efficiently
                        chunk_hasher = hashlib.sha256()
                        chunk_hasher.update(data)

                        read_time = time.time() - start_time
                        throughput = (
                            (len(data) / (1024 * 1024)) / read_time
                            if read_time > 0
                            else 0
                        )

                        logger.debug(
                            f"Read chunk {part_number}: {len(data)} bytes in {read_time:.2f}s ({throughput:.1f} MB/s)"
                        )

                        return ChunkData(
                            part_number=part_number,
                            data=data,
                            offset=offset,
                            hash_chunk=chunk_hasher.hexdigest(),
                        )
                finally:
                    await self._return_sftp_connection(sftp, conn)

            except Exception as e:
                logger.error(f"Error reading chunk {part_number}: {e}")
                return None

    async def _upload_chunk_worker(
        self, upload_id: str, remote_path: str, chunk_data: ChunkData
    ) -> Optional[Dict[str, Any]]:
        """Async worker function to upload a chunk to S3"""
        async with self.upload_semaphore:
            try:
                start_time = time.time()

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

                upload_time = time.time() - start_time
                throughput = (
                    (len(chunk_data.data) / (1024 * 1024)) / upload_time
                    if upload_time > 0
                    else 0
                )

                # Update metrics
                self.bytes_transferred.inc(len(chunk_data.data))
                self.chunks_processed.inc()
                self.throughput_mbps.set(throughput)

                logger.info(
                    f"Uploaded part {chunk_data.part_number} ({len(chunk_data.data)} bytes, {throughput:.2f} MB/s)"
                )

                return {
                    "ETag": part["ETag"],
                    "PartNumber": chunk_data.part_number,
                    "chunk_hash": chunk_data.hash_chunk,
                }

            except Exception as e:
                logger.error(f"Error uploading chunk {chunk_data.part_number}: {e}")
                return None

    async def _optimized_upload_chunk_worker(
        self, upload_id: str, remote_path: str, chunk_data: ChunkData
    ) -> Optional[Dict[str, Any]]:
        """Optimized S3 upload worker with retry logic and better configuration"""
        async with self.upload_semaphore:
            max_retries = 3
            retry_delay = 1.0

            for attempt in range(max_retries):
                try:
                    start_time = time.time()

                    # Use simplified S3 client configuration for aioboto3
                    async with self.s3_session.client(
                        "s3",
                        endpoint_url=self.s3_endpoint_url,
                        region_name="us-east-1",
                    ) as s3_client:
                        # Upload with optimized parameters
                        part = await s3_client.upload_part(
                            Bucket=self.bucket_name,
                            Key=remote_path,
                            PartNumber=chunk_data.part_number,
                            UploadId=upload_id,
                            Body=chunk_data.data,
                        )

                    upload_time = time.time() - start_time
                    throughput = (
                        (len(chunk_data.data) / (1024 * 1024)) / upload_time
                        if upload_time > 0
                        else 0
                    )

                    # Update metrics
                    self.bytes_transferred.inc(len(chunk_data.data))
                    self.chunks_processed.inc()
                    self.throughput_mbps.set(throughput)

                    logger.debug(
                        f"Uploaded part {chunk_data.part_number} ({len(chunk_data.data)} bytes, {throughput:.2f} MB/s)"
                    )

                    return {
                        "ETag": part["ETag"],
                        "PartNumber": chunk_data.part_number,
                        "chunk_hash": chunk_data.hash_chunk,
                    }

                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Upload attempt {attempt + 1} failed for chunk {chunk_data.part_number}: {e}. Retrying in {retry_delay}s..."
                        )
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        logger.error(
                            f"Upload failed for chunk {chunk_data.part_number} after {max_retries} attempts: {e}"
                        )
                        return None

            return None

    async def upload_to_s3(self, path: str) -> None:
        """Main async method to upload file from SFTP to S3 with optimized producer-consumer pattern"""
        try:
            logger.info(f"Attempting to access file: {path}")

            # Get file info using a temporary SFTP connection
            sftp, conn = await self._create_sftp_connection()
            try:
                file_stat = await sftp.stat(path)
                remote_file_size = file_stat.size or 0
                logger.info(f"File found! Size: {remote_file_size} bytes")
            except FileNotFoundError:
                import os

                parent_dir = os.path.dirname(path)
                filename = os.path.basename(path)
                logger.info(
                    f"File not found at direct path. Checking parent directory: {parent_dir}"
                )
                try:
                    files_in_dir = await sftp.listdir(parent_dir)
                    logger.info(f"Files in {parent_dir}: {files_in_dir}")
                    if filename not in files_in_dir:
                        logger.error(
                            f"File '{filename}' not found in directory '{parent_dir}'"
                        )
                        logger.info(f"Available files: {', '.join(files_in_dir)}")
                        raise FileNotFoundError(
                            f"File '{filename}' not found in '{parent_dir}'. Available files: {', '.join(files_in_dir)}"
                        )
                    else:
                        file_stat = await sftp.stat(path)
                        remote_file_size = file_stat.size or 0
                        logger.info(
                            f"File found after directory check! Size: {remote_file_size} bytes"
                        )
                except Exception as dir_error:
                    logger.error(f"Could not list directory {parent_dir}: {dir_error}")
                    raise FileNotFoundError(
                        f"Could not access file {path} or its parent directory"
                    )
            finally:
                conn.close()

        except Exception as e:
            logger.error(f"Error accessing file {path}: {e}")
            raise

        # Initialize connection pool before starting transfer
        await self._initialize_connection_pool()

        await self.start_monitoring()
        start_time = time.time()

        # Calculate chunks
        total_chunks = (remote_file_size + self.CHUNK_SIZE - 1) // self.CHUNK_SIZE
        logger.info(
            f"Starting OPTIMIZED async concurrent transfer: {total_chunks} chunks"
        )
        logger.info(
            f"Config: {self.MAX_CONCURRENT_READS} readers, {self.MAX_CONCURRENT_UPLOADS} uploaders, {self.CHUNK_SIZE // (1024*1024)}MB chunks"
        )

        # Create multipart upload
        async with self.s3_session.client(
            "s3", endpoint_url=self.s3_endpoint_url
        ) as s3_client:
            upload_response = await s3_client.create_multipart_upload(
                Bucket=self.bucket_name, Key=path
            )
            upload_id = upload_response["UploadId"]

        # Producer-Consumer pattern with buffering
        chunk_queue = asyncio.Queue(maxsize=self.BUFFER_SIZE)
        upload_results = asyncio.Queue()

        async def chunk_producer():
            """Producer: Read chunks from SFTP and put in queue"""
            chunk_infos = [
                (
                    i + 1,
                    i * self.CHUNK_SIZE,
                    min(self.CHUNK_SIZE, remote_file_size - i * self.CHUNK_SIZE),
                )
                for i in range(total_chunks)
            ]

            # Create semaphore-controlled read tasks
            async def read_and_queue(chunk_info):
                chunk_data = await self._adaptive_chunk_worker(
                    path, chunk_info
                )  # Use optimized reader
                if chunk_data:
                    await chunk_queue.put(chunk_data)

            # Create all read tasks with concurrency control
            read_tasks = [
                asyncio.create_task(read_and_queue(chunk_info))
                for chunk_info in chunk_infos
            ]
            await asyncio.gather(*read_tasks)

            # Signal end of data
            for _ in range(self.MAX_CONCURRENT_UPLOADS):
                await chunk_queue.put(None)

        async def chunk_consumer():
            """Consumer: Get chunks from queue and upload to S3"""
            while True:
                chunk_data = await chunk_queue.get()
                if chunk_data is None:
                    break

                result = await self._optimized_upload_chunk_worker(
                    upload_id, path, chunk_data
                )  # Use optimized uploader
                if result:
                    await upload_results.put(result)
                chunk_queue.task_done()

        # Start producer and multiple consumers
        producer_task = asyncio.create_task(chunk_producer())
        consumer_tasks = [
            asyncio.create_task(chunk_consumer())
            for _ in range(self.MAX_CONCURRENT_UPLOADS)
        ]

        # Collect results as they come in
        parts = []
        completed_parts = 0
        overall_hash = hashlib.sha256()
        chunk_hashes = {}

        async def result_collector():
            nonlocal completed_parts
            consecutive_timeouts = 0
            max_consecutive_timeouts = 5

            while completed_parts < total_chunks:
                try:
                    result = await asyncio.wait_for(
                        upload_results.get(), timeout=30.0
                    )  # Increased timeout
                    consecutive_timeouts = 0  # Reset timeout counter on success

                    parts.append(
                        {"ETag": result["ETag"], "PartNumber": result["PartNumber"]}
                    )
                    chunk_hashes[result["PartNumber"]] = result["chunk_hash"]
                    completed_parts += 1

                    progress = (completed_parts / total_chunks) * 100
                    elapsed = time.time() - start_time
                    avg_throughput = (
                        (completed_parts * self.CHUNK_SIZE / (1024 * 1024)) / elapsed
                        if elapsed > 0
                        else 0
                    )
                    self.throughput_mbps.set(avg_throughput)

                    if (
                        completed_parts % 5 == 0 or completed_parts == total_chunks
                    ):  # More frequent updates
                        logger.info(
                            f"Progress: {progress:.1f}% ({completed_parts}/{total_chunks}) - {avg_throughput:.2f} MB/s"
                        )

                except asyncio.TimeoutError:
                    consecutive_timeouts += 1
                    logger.warning(
                        f"Timeout {consecutive_timeouts}/{max_consecutive_timeouts} waiting for upload result. Completed: {completed_parts}/{total_chunks}"
                    )

                    if consecutive_timeouts >= max_consecutive_timeouts:
                        logger.error(
                            "Too many consecutive timeouts, stopping result collection"
                        )
                        break

        # Run all tasks concurrently
        try:
            await asyncio.gather(
                producer_task,
                *consumer_tasks,
                result_collector(),
                return_exceptions=True,
            )

            # Reconstruct overall hash in correct order
            for part_num in sorted(chunk_hashes.keys()):
                # Note: This is approximate since we need original chunk data for exact hash
                pass

            # Sort parts by part number for S3
            parts.sort(key=lambda x: x["PartNumber"])

            total_duration = time.time() - start_time
            self.transfer_duration.observe(total_duration)

            # Complete multipart upload
            async with self.s3_session.client(
                "s3", endpoint_url=self.s3_endpoint_url
            ) as s3_client:
                await s3_client.complete_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=path,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )

                # Verify transfer
                s3_object = await s3_client.head_object(
                    Bucket=self.bucket_name, Key=path
                )
                s3_size = s3_object["ContentLength"]

            if s3_size != remote_file_size:
                raise ValueError(
                    f"File size mismatch: SFTP={remote_file_size}, S3={s3_size}"
                )

            final_throughput = (remote_file_size / (1024 * 1024)) / total_duration
            logger.info(f"🚀 OPTIMIZED ASYNC UPLOAD COMPLETED! 🚀")
            logger.info(f"File: {path} -> bucket {self.bucket_name}")
            logger.info(
                f"Performance: {remote_file_size} bytes in {total_duration:.2f}s"
            )
            logger.info(f"⚡ THROUGHPUT: {final_throughput:.2f} MB/s ⚡")
            logger.info(
                f"Optimizations: {self.MAX_CONCURRENT_READS} readers, {self.MAX_CONCURRENT_UPLOADS} uploaders, {self.CHUNK_SIZE//(1024*1024)}MB chunks"
            )

        except Exception as e:
            logger.error(f"Optimized upload failed: {e}")
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
        finally:
            await self.stop_monitoring()
            await self.push_metrics()

            # Cleanup connection pool
            while not self.sftp_pool.empty():
                try:
                    sftp, conn = self.sftp_pool.get_nowait()
                    conn.close()
                except asyncio.QueueEmpty:
                    break

    async def _compute_sftp_file_checksum(self, path: str) -> str:
        """Async method to compute SFTP file checksum"""
        hasher = hashlib.sha256()
        sftp, conn = await self._create_sftp_connection()
        try:
            async with sftp.open(path, "rb") as remote_file:
                while True:
                    data = await remote_file.read(self.CHUNK_SIZE)
                    if not data:
                        break
                    hasher.update(data)
        finally:
            conn.close()
        return hasher.hexdigest()


# Keep the old class name for backward compatibility
SFTPToS3Streamer = AsyncSFTPToS3Streamer
