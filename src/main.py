"""
Stream SFTP from server to S3 bucket
"""

import os
from typing import Final
import boto3
import hashlib
import logging
from dotenv import load_dotenv
from paramiko import SFTPClient
from prometheus_client import CollectorRegistry, push_to_gateway

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
        self, sftp_client: SFTPClient, session: boto3.Session, bucket_name: str
    ):
        self.sftp_client = sftp_client
        self.s3_client = session.client("s3", endpoint_url="http://localhost:9000")
        self.bucket_name = bucket_name

    def _get_chunk_from_sftp(self, remote_path: str, offset: int) -> bytes:
        with self.sftp_client.open(remote_path, "rb") as remote_file:
            remote_file.seek(offset)
            return remote_file.read(self.CHUNK_SIZE)

    def upload_to_s3(self, path: str) -> None:
        remote_file_size = self.sftp_client.stat(path).st_size or 0
        offset = 0
        part_number = 1
        total_bytes_processed = 0

        overall_hash = hashlib.sha256()

        upload_id = self.s3_client.create_multipart_upload(
            Bucket=self.bucket_name, Key=path
        )["UploadId"]
        parts = []

        try:
            while offset < remote_file_size:
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

                offset += len(chunk)
                part_number += 1
                print(
                    f"Uploaded part {part_number-1} of {(remote_file_size + self.CHUNK_SIZE - 1) // self.CHUNK_SIZE}"
                )

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

            print(f"Upload completed for {path} to bucket {self.bucket_name}")
            print(f"File integrity: SHA256={final_hash}, Size={remote_file_size}")
            print("Basic integrity verification passed (size and byte count match)")

        except Exception as e:
            print(f"Upload failed: {e}")
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name, Key=path, UploadId=upload_id
                )
            except Exception:
                pass
            raise

    def _compute_sftp_file_checksum(self, path: str) -> str:
        hasher = hashlib.sha256()
        with self.sftp_client.open(path, "rb") as remote_file:
            while True:
                data = remote_file.read(self.CHUNK_SIZE)
                if not data:
                    break
                hasher.update(data)
        return hasher.hexdigest()
