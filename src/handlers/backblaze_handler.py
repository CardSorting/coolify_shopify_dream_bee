# handlers/backblaze_handler.py

import aioboto3
from botocore.config import Config
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse

from aiobotocore.client import AioBaseClient  # Importing AioBaseClient for type hinting
from utils.logger import Logger


class BackblazeHandler:
    """
    Handles operations related to Backblaze B2 storage, such as uploading, retrieving,
    and managing files in a specified bucket.
    """

    def __init__(self, key_id: str, application_key: str, bucket_name: str):
        """
        Initialize the BackblazeHandler with necessary credentials.

        Args:
            key_id (str): Backblaze B2 account ID.
            application_key (str): Backblaze B2 application key.
            bucket_name (str): Name of the Backblaze B2 bucket.
        """
        self.key_id = key_id
        self.application_key = application_key
        self.bucket_name = bucket_name
        self.region_name = "us-east-005"  # Update as per your region
        self.endpoint_url = f"https://s3.{self.region_name}.backblazeb2.com"
        self.logger = Logger.get_instance("BackblazeHandler")
        self._session: Optional[aioboto3.Session] = None
        self._client_config = Config(
            signature_version='s3v4',
            s3={'addressing_style': 'virtual'}
        )
        self._s3_client: Optional[AioBaseClient] = None  # Using AioBaseClient for type hinting

    async def initialize(self) -> None:
        """
        Asynchronously initialize the BackblazeHandler by creating an aioboto3 session and S3 client.

        Raises:
            Exception: If the S3 client cannot be created.
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
            # Optionally, verify the connection by performing a simple operation
            await self._s3_client.head_bucket(Bucket=self.bucket_name)
            self.logger.info("Initialized aioboto3 S3 client for BackblazeHandler.")
        except Exception as e:
            self.logger.error(f"Failed to initialize BackblazeHandler: {e}", exc_info=True)
            raise e

    async def close(self) -> None:
        """
        Asynchronously close the S3 client and aioboto3 session.
        """
        try:
            if self._s3_client:
                await self._s3_client.__aexit__(None, None, None)
                self.logger.info("Closed aioboto3 S3 client for BackblazeHandler.")
            if self._session:
                await self._session.close()
                self.logger.info("Closed aioboto3 Session for BackblazeHandler.")
        except Exception as e:
            self.logger.error(f"Error closing BackblazeHandler resources: {e}", exc_info=True)

    async def upload_image(self, file_name: str, image_content: bytes) -> Optional[str]:
        """
        Uploads an image to the specified Backblaze B2 bucket and returns the public URL of the image.

        Args:
            file_name (str): The name of the file to upload.
            image_content (bytes): The binary content of the image.

        Returns:
            Optional[str]: The public URL of the uploaded image if successful, otherwise None.
        """
        if not self._validate_upload_parameters(file_name, image_content):
            return None

        try:
            if not self._s3_client:
                self.logger.error("S3 client is not initialized.")
                return None

            content_type = self._determine_mime_type(file_name)
            self.logger.debug(f"Uploading image '{file_name}' with MIME type: {content_type}")

            await self._s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=image_content,
                ACL='public-read',
                ContentType=content_type
            )

            file_url = self._generate_public_url(file_name)
            self.logger.info(f"Image '{file_name}' uploaded successfully to Backblaze. URL: {file_url}")
            return file_url
        except Exception as e:
            self.logger.error(f"Failed to upload image '{file_name}' to Backblaze: {e}", exc_info=True)
            return None

    async def get_file_url(self, file_key: str) -> Optional[str]:
        """
        Retrieves the public URL for a file stored in Backblaze.

        Args:
            file_key (str): The key (file name) of the file.

        Returns:
            Optional[str]: The public URL of the file if it exists, otherwise None.
        """
        if self._is_valid_url(file_key):
            self.logger.debug(f"File key '{file_key}' is already a valid URL.")
            return file_key

        try:
            if not self._s3_client:
                self.logger.error("S3 client is not initialized.")
                return None

            await self._s3_client.head_object(Bucket=self.bucket_name, Key=file_key)
            file_url = self._generate_public_url(file_key)
            self.logger.info(f"Retrieved file URL from Backblaze: {file_url}")
            return file_url
        except Exception as e:
            self.logger.error(f"Error retrieving file '{file_key}': {e}", exc_info=True)
            return None

    async def delete_file(self, file_key: str) -> bool:
        """
        Deletes a file from the Backblaze bucket.

        Args:
            file_key (str): The key (file name) of the file to delete.

        Returns:
            bool: True if the file was deleted successfully, otherwise False.
        """
        try:
            if not self._s3_client:
                self.logger.error("S3 client is not initialized.")
                return False

            await self._s3_client.delete_object(Bucket=self.bucket_name, Key=file_key)
            self.logger.info(f"File '{file_key}' deleted successfully from Backblaze.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete file '{file_key}': {e}", exc_info=True)
            return False

    async def list_files(self, prefix: str = "") -> List[Dict[str, Any]]:
        """
        Lists all files in the Backblaze bucket with an optional prefix filter.

        Args:
            prefix (str, optional): The prefix to filter the files by. Defaults to "".

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing file metadata.
        """
        try:
            if not self._s3_client:
                self.logger.error("S3 client is not initialized.")
                return []

            paginator = self._s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            files = []
            async for page in page_iterator:
                files.extend(page.get('Contents', []))
            self.logger.info(f"Retrieved {len(files)} files from Backblaze with prefix '{prefix}'.")
            return files
        except Exception as e:
            self.logger.error(f"Failed to list files in Backblaze: {e}", exc_info=True)
            return []

    def _generate_public_url(self, file_key: str) -> str:
        """
        Generates a public URL for a file stored in the Backblaze bucket.

        Args:
            file_key (str): The key (file name) of the file.

        Returns:
            str: The public URL of the file.
        """
        return f"https://{self.bucket_name}.s3.{self.region_name}.backblazeb2.com/{file_key}"

    def _determine_mime_type(self, file_name: str) -> str:
        """
        Determines the MIME type based on the file extension.

        Args:
            file_name (str): The name of the file.

        Returns:
            str: The MIME type as a string.
        """
        if file_name.lower().endswith('.jpg') or file_name.lower().endswith('.jpeg'):
            return "image/jpeg"
        elif file_name.lower().endswith('.png'):
            return "image/png"
        else:
            self.logger.warning(f"Unknown file extension for file '{file_name}'. Defaulting to 'application/octet-stream'.")
            return "application/octet-stream"

    def _validate_upload_parameters(self, file_name: str, image_content: bytes) -> bool:
        """
        Validates the parameters for uploading an image.

        Args:
            file_name (str): The name of the file.
            image_content (bytes): The binary content of the image.

        Returns:
            bool: True if parameters are valid, otherwise False.
        """
        if not file_name or not image_content:
            self.logger.error("File name and image content must be provided.")
            return False
        return True

    def _is_valid_url(self, url: str) -> bool:
        """
        Checks if the provided string is a valid URL.

        Args:
            url (str): The string to check.

        Returns:
            bool: True if valid, otherwise False.
        """
        parsed_url = urlparse(url)
        is_valid = all([parsed_url.scheme, parsed_url.netloc])
        if not is_valid:
            self.logger.debug(f"Invalid URL detected: {url}")
        return is_valid