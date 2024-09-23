# handlers/backblaze_handler.py

import aioboto3
from botocore.config import Config
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
import asyncio

from aiobotocore.client import AioBaseClient  # For type hinting
from utils.logger import Logger


class BackblazeHandler:
    """
    Handles operations related to Backblaze B2 storage, including uploading, retrieving,
    deleting, and listing files within a specified bucket using the S3-compatible API.
    """

    def __init__(
        self,
        key_id: str,
        application_key: str,
        bucket_name: str,
        region_name: str = "us-east-005",
        max_workers: int = 10
    ):
        """
        Initializes the BackblazeHandler with necessary credentials and configurations.

        Args:
            key_id (str): Backblaze B2 Account ID.
            application_key (str): Backblaze B2 Application Key.
            bucket_name (str): Name of the Backblaze B2 bucket.
            region_name (str, optional): Backblaze B2 region name. Defaults to "us-east-005".
            max_workers (int, optional): Maximum number of threads for the executor. Defaults to 10.

        Raises:
            ValueError: If any of the required parameters are missing.
        """
        if not all([key_id, application_key, bucket_name]):
            missing = [k for k, v in zip(['key_id', 'application_key', 'bucket_name'],
                                         [key_id, application_key, bucket_name]) if not v]
            raise ValueError(f"Missing required parameters: {', '.join(missing)}")

        self.key_id: str = key_id
        self.application_key: str = application_key
        self.bucket_name: str = bucket_name
        self.region_name: str = region_name
        self.endpoint_url: str = f"https://s3.{self.region_name}.backblazeb2.com"

        self.logger: Logger = Logger.get_instance("BackblazeHandler")

        self._session: Optional[aioboto3.Session] = None
        self._s3_client: Optional[AioBaseClient] = None

        self._client_config: Config = Config(
            signature_version='s3v4',
            s3={'addressing_style': 'virtual'}
        )

        # Thread pool executor for any potential blocking operations
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=max_workers)

    async def initialize(self) -> None:
        """
        Asynchronously initializes the aioboto3 session and S3 client.

        Raises:
            Exception: If initialization fails.
        """
        try:
            self._session = aioboto3.Session()
            self._s3_client = await self._session.client(
                's3',
                aws_access_key_id=self.key_id,
                aws_secret_access_key=self.application_key,
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
                config=self._client_config
            ).__aenter__()
            # Verify connection by checking bucket existence
            await self._s3_client.head_bucket(Bucket=self.bucket_name)
            self.logger.info(f"Connected to Backblaze B2 bucket '{self.bucket_name}' successfully.")
        except Exception as e:
            self.logger.error(f"Initialization failed for BackblazeHandler: {e}", exc_info=True)
            raise e

    async def close(self) -> None:
        """
        Asynchronously closes the S3 client and aioboto3 session, and shuts down the executor.
        """
        try:
            if self._s3_client:
                await self._s3_client.__aexit__(None, None, None)
                self.logger.info("Closed aioboto3 S3 client successfully.")
            if self._session:
                await self._session.close()
                self.logger.info("Closed aioboto3 Session successfully.")
            self._executor.shutdown(wait=True)
            self.logger.info("ThreadPoolExecutor shut down successfully.")
        except Exception as e:
            self.logger.error(f"Error during shutdown of BackblazeHandler: {e}", exc_info=True)

    async def upload_image(
        self,
        file_name: str,
        image_content: bytes,
        acl: str = 'public-read'
    ) -> Optional[str]:
        """
        Uploads an image to the Backblaze B2 bucket and returns its public URL.

        Args:
            file_name (str): Desired name of the file in the bucket.
            image_content (bytes): Binary content of the image.
            acl (str, optional): Access control list for the file. Defaults to 'public-read'.

        Returns:
            Optional[str]: Public URL of the uploaded image if successful, else None.
        """
        if not self._validate_upload_parameters(file_name, image_content):
            return None

        try:
            if not self._s3_client:
                self.logger.error("S3 client is not initialized.")
                return None

            content_type: str = self._determine_mime_type(file_name)
            self.logger.debug(f"Uploading '{file_name}' with MIME type '{content_type}'.")

            await self._s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=image_content,
                ACL=acl,
                ContentType=content_type
            )

            file_url: str = self._generate_public_url(file_name)
            self.logger.info(f"Image '{file_name}' uploaded successfully. URL: {file_url}")
            return file_url
        except Exception as e:
            self.logger.error(f"Failed to upload image '{file_name}': {e}", exc_info=True)
            return None

    async def get_file_url(self, file_key: str) -> Optional[str]:
        """
        Retrieves the public URL of a file stored in the Backblaze B2 bucket.

        Args:
            file_key (str): Key (name) of the file in the bucket.

        Returns:
            Optional[str]: Public URL of the file if it exists, else None.
        """
        if self._is_valid_url(file_key):
            self.logger.debug(f"Provided file key '{file_key}' is already a valid URL.")
            return file_key

        try:
            if not self._s3_client:
                self.logger.error("S3 client is not initialized.")
                return None

            await self._s3_client.head_object(Bucket=self.bucket_name, Key=file_key)
            file_url: str = self._generate_public_url(file_key)
            self.logger.info(f"Retrieved file URL for '{file_key}': {file_url}")
            return file_url
        except self._s3_client.exceptions.NoSuchKey:
            self.logger.warning(f"File '{file_key}' does not exist in bucket '{self.bucket_name}'.")
            return None
        except Exception as e:
            self.logger.error(f"Error retrieving file URL for '{file_key}': {e}", exc_info=True)
            return None

    async def delete_file(self, file_key: str) -> bool:
        """
        Deletes a file from the Backblaze B2 bucket.

        Args:
            file_key (str): Key (name) of the file to delete.

        Returns:
            bool: True if deletion was successful, else False.
        """
        try:
            if not self._s3_client:
                self.logger.error("S3 client is not initialized.")
                return False

            await self._s3_client.delete_object(Bucket=self.bucket_name, Key=file_key)
            self.logger.info(f"File '{file_key}' deleted successfully from bucket '{self.bucket_name}'.")
            return True
        except self._s3_client.exceptions.NoSuchKey:
            self.logger.warning(f"Attempted to delete non-existent file '{file_key}'.")
            return False
        except Exception as e:
            self.logger.error(f"Failed to delete file '{file_key}': {e}", exc_info=True)
            return False

    async def list_files(self, prefix: str = "") -> Optional[List[Dict[str, Any]]]:
        """
        Lists all files in the Backblaze B2 bucket, optionally filtered by a prefix.

        Args:
            prefix (str, optional): Prefix to filter files by. Defaults to "".

        Returns:
            Optional[List[Dict[str, Any]]]: List of file metadata dictionaries if successful, else None.
        """
        try:
            if not self._s3_client:
                self.logger.error("S3 client is not initialized.")
                return None

            paginator = self._s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

            files: List[Dict[str, Any]] = []
            async for page in page_iterator:
                contents = page.get('Contents', [])
                files.extend(contents)

            self.logger.info(f"Listed {len(files)} files in bucket '{self.bucket_name}' with prefix '{prefix}'.")
            return files
        except Exception as e:
            self.logger.error(f"Failed to list files with prefix '{prefix}': {e}", exc_info=True)
            return None

    def _generate_public_url(self, file_key: str) -> str:
        """
        Generates a public URL for a file stored in the Backblaze B2 bucket.

        Args:
            file_key (str): Key (name) of the file.

        Returns:
            str: Public URL of the file.
        """
        return f"https://{self.bucket_name}.s3.{self.region_name}.backblazeb2.com/{file_key}"

    def _determine_mime_type(self, file_name: str) -> str:
        """
        Determines the MIME type based on the file extension.

        Args:
            file_name (str): Name of the file.

        Returns:
            str: MIME type string.
        """
        if file_name.lower().endswith(('.jpg', '.jpeg')):
            return "image/jpeg"
        elif file_name.lower().endswith('.png'):
            return "image/png"
        else:
            self.logger.warning(f"Unknown file extension for '{file_name}'. Defaulting to 'application/octet-stream'.")
            return "application/octet-stream"

    def _validate_upload_parameters(self, file_name: str, image_content: bytes) -> bool:
        """
        Validates parameters required for uploading an image.

        Args:
            file_name (str): Name of the file.
            image_content (bytes): Binary content of the image.

        Returns:
            bool: True if parameters are valid, else False.
        """
        if not file_name:
            self.logger.error("Upload failed: 'file_name' is empty.")
            return False
        if not image_content:
            self.logger.error("Upload failed: 'image_content' is empty.")
            return False
        return True

    def _is_valid_url(self, url: str) -> bool:
        """
        Validates whether a string is a well-formed URL.

        Args:
            url (str): The URL string to validate.

        Returns:
            bool: True if valid URL, else False.
        """
        parsed = urlparse(url)
        is_valid = all([parsed.scheme, parsed.netloc])
        if not is_valid:
            self.logger.debug(f"Invalid URL detected: '{url}'.")
        return is_valid