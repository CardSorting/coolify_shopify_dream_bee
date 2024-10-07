# src/handlers/backblaze_handler.py

import aioboto3
from botocore.config import Config
import os  # Ensure this is imported
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

from utils.logger import Logger

# Load environment variables
load_dotenv()

class BackblazeHandler:
    """
    Handles operations related to Backblaze B2 storage, including uploading, retrieving,
    deleting, and listing files within a specified bucket using the S3-compatible API.
    """

    def __init__(self, bucket_name: str, max_workers: int = 10):
        self.key_id = os.getenv("BACKBLAZE_KEY_ID")
        self.application_key = os.getenv("BACKBLAZE_APPLICATION_KEY")
        self.region_name = os.getenv("BACKBLAZE_REGION_NAME", "us-east-005")

        if not all([self.key_id, self.application_key, bucket_name]):
            missing = [k for k, v in zip(
                ['key_id', 'application_key', 'bucket_name'],
                [self.key_id, self.application_key, bucket_name]
            ) if not v]
            raise ValueError(f"Missing required parameters: {', '.join(missing)}")

        self.bucket_name = bucket_name
        self.endpoint_url = f"https://s3.{self.region_name}.backblazeb2.com"

        self.logger = Logger.get_instance("BackblazeHandler")
        self.logger.debug("BackblazeHandler initialized with provided API keys and bucket information.")

        self._session = aioboto3.Session()
        self._client_config = Config(
            signature_version='s3v4',
            s3={'addressing_style': 'virtual'}
        )

        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    async def initialize(self) -> None:
        """
        Initializes the connection by checking the existence of the bucket.
        """
        try:
            async with self._session.client(
                's3',
                aws_access_key_id=self.key_id,
                aws_secret_access_key=self.application_key,
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
                config=self._client_config
            ) as s3_client:
                await s3_client.head_bucket(Bucket=self.bucket_name)
                self.logger.info(f"Connected to Backblaze B2 bucket '{self.bucket_name}' successfully.")

        except Exception as e:
            self.logger.error(f"Initialization failed for BackblazeHandler: {e}", exc_info=True)
            raise

    async def upload_image(self, file_name: str, image_content: bytes, acl: str = 'public-read') -> Optional[str]:
        """Uploads an image to the bucket."""
        if not self._validate_upload_parameters(file_name, image_content):
            return None

        try:
            async with self._session.client(
                's3',
                aws_access_key_id=self.key_id,
                aws_secret_access_key=self.application_key,
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
                config=self._client_config
            ) as s3_client:
                content_type = self._determine_mime_type(file_name)
                self.logger.debug(f"Uploading '{file_name}' with MIME type '{content_type}'.")

                await s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=file_name,
                    Body=image_content,
                    ACL=acl,
                    ContentType=content_type
                )

                file_url = self._generate_public_url(file_name)
                self.logger.info(f"Image '{file_name}' uploaded successfully. URL: {file_url}")
                return file_url

        except Exception as e:
            self.logger.error(f"Failed to upload image '{file_name}': {e}", exc_info=True)
            return None

    async def get_file_url(self, file_key: str) -> Optional[str]:
        """Retrieves the URL of a file stored in the bucket."""
        if self._is_valid_url(file_key):
            self.logger.debug(f"Provided file key '{file_key}' is already a valid URL.")
            return file_key

        try:
            async with self._session.client(
                's3',
                aws_access_key_id=self.key_id,
                aws_secret_access_key=self.application_key,
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
                config=self._client_config
            ) as s3_client:
                await s3_client.head_object(Bucket=self.bucket_name, Key=file_key)
                file_url = self._generate_public_url(file_key)
                self.logger.info(f"Retrieved file URL for '{file_key}': {file_url}")
                return file_url

        except s3_client.exceptions.NoSuchKey:
            self.logger.warning(f"File '{file_key}' does not exist in bucket '{self.bucket_name}'.")
            return None

        except Exception as e:
            self.logger.error(f"Error retrieving file URL for '{file_key}': {e}", exc_info=True)
            return None

    async def delete_file(self, file_key: str) -> bool:
        """Deletes a file from the bucket."""
        try:
            async with self._session.client(
                's3',
                aws_access_key_id=self.key_id,
                aws_secret_access_key=self.application_key,
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
                config=self._client_config
            ) as s3_client:
                await s3_client.delete_object(Bucket=self.bucket_name, Key=file_key)
                self.logger.info(f"File '{file_key}' deleted successfully from bucket '{self.bucket_name}'.")
                return True

        except s3_client.exceptions.NoSuchKey:
            self.logger.warning(f"Attempted to delete non-existent file '{file_key}'.")
            return False

        except Exception as e:
            self.logger.error(f"Failed to delete file '{file_key}': {e}", exc_info=True)
            return False

    async def list_files(self, prefix: str = "") -> Optional[List[Dict[str, Any]]]:
        """Lists all files in the bucket with an optional prefix."""
        try:
            async with self._session.client(
                's3',
                aws_access_key_id=self.key_id,
                aws_secret_access_key=self.application_key,
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
                config=self._client_config
            ) as s3_client:
                paginator = s3_client.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

                files = []
                async for page in page_iterator:
                    contents = page.get('Contents', [])
                    files.extend(contents)

                self.logger.info(f"Listed {len(files)} files in bucket '{self.bucket_name}' with prefix '{prefix}'.")
                return files

        except Exception as e:
            self.logger.error(f"Failed to list files with prefix '{prefix}': {e}", exc_info=True)
            return None

    def _generate_public_url(self, file_key: str) -> str:
        """Generates a public URL for a specified file key."""
        return f"https://{self.bucket_name}.s3.{self.region_name}.backblazeb2.com/{file_key}"

    def _determine_mime_type(self, file_name: str) -> str:
        """Determines the MIME type based on the file extension."""
        if file_name.lower().endswith(('.jpg', '.jpeg')):
            return "image/jpeg"
        elif file_name.lower().endswith('.png'):
            return "image/png"
        else:
            self.logger.warning(f"Unknown file extension for '{file_name}'. Defaulting to 'application/octet-stream'.")
            return "application/octet-stream"

    def _validate_upload_parameters(self, file_name: str, image_content: bytes) -> bool:
        """Validates parameters for uploading a file."""
        if not file_name:
            self.logger.error("Upload failed: 'file_name' is empty.")
            return False
        if not image_content:
            self.logger.error("Upload failed: 'image_content' is empty.")
            return False
        return True

    def _is_valid_url(self, url: str) -> bool:
        """Checks if a given string is a valid URL."""
        parsed = urlparse(url)
        is_valid = all([parsed.scheme, parsed.netloc])
        if not is_valid:
            self.logger.debug(f"Invalid URL detected: '{url}'.")
        return is_valid