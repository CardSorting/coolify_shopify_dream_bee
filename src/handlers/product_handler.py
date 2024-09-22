# handlers/product_handler.py

import asyncio
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse

from utils.logger import Logger
from handlers.backblaze_handler import BackblazeHandler
from services.shopify_service import ShopifyService


class ProductHandler:
    """
    Handles product creation and management within Shopify, including collection association.
    """

    def __init__(
        self,
        shopify_service: ShopifyService,
        backblaze_handler: BackblazeHandler,
    ):
        """
        Initialize the ProductHandler with necessary services.

        Args:
            shopify_service (ShopifyService): Service for interacting with Shopify API.
            backblaze_handler (BackblazeHandler): Handler for Backblaze interactions.
        """
        self.shopify_service = shopify_service
        self.backblaze_handler = backblaze_handler
        self.logger = Logger.get_instance("ProductHandler")

    async def add_product_to_shopify(
        self, product_data: Dict[str, Any], username: str
    ) -> Optional[Dict[str, Any]]:
        """
        Create a product in Shopify and upload its image, then associate it with a collection.

        Args:
            product_data (Dict[str, Any]): Data required to create the product.
            username (str): Username of the creator, used for collection naming.

        Returns:
            Optional[Dict[str, Any]]: Contains product details and product URL if successful.
        """
        self.logger.debug(
            f"Received product data for Shopify creation: {product_data} by user '{username}'"
        )

        # Step 1: Create the product in Shopify
        product = await self._create_shopify_product(product_data)
        if not product:
            self.logger.error("Product creation failed. Aborting process.")
            return None

        # Step 2: Handle image upload
        image_url = await self._get_image_url(product_data.get("image_url"))
        if not image_url:
            self.logger.error("Image URL retrieval/upload failed. Aborting process.")
            return None

        image_upload_response = await self._upload_image_to_shopify(product["id"], image_url)
        if not image_upload_response:
            self.logger.error("Image upload to Shopify failed. Aborting process.")
            return None

        # Step 3: Associate product with a collection
        collection_title = f"Artist-{username}"
        collection = await self._get_or_create_collection(collection_title)
        if not collection:
            self.logger.error("Collection handling failed. Aborting process.")
            return None

        association_success = await self._add_product_to_collection(product["id"], collection["id"])
        if not association_success:
            self.logger.error("Failed to associate product with collection.")
            return None

        # Step 4: Construct the product URL
        product_url = self._construct_product_url(product["handle"])
        self.logger.info(
            f"Product '{product['title']}' added successfully with image and collection association. URL: {product_url}"
        )

        return {"product": product, "product_url": product_url}

    async def _create_shopify_product(self, product_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a product in Shopify.

        Args:
            product_data (Dict[str, Any]): Data required to create the product.

        Returns:
            Optional[Dict[str, Any]]: Shopify product data if successful.
        """
        try:
            product = await self.shopify_service.create_product(
                title=product_data["title"],
                description=product_data["body_html"],
                images=[],  # Images will be uploaded separately
                vendor=product_data["vendor"],
                price=float(product_data["variants"][0]["price"]),
                product_type=product_data.get("product_type", "Artist Trading Card"),
                inventory_quantity=int(product_data.get("inventory_quantity", 100)),
                tags=product_data.get("tags", [])  # 'tags' is now supported
            )
            self.logger.debug(f"Shopify response for product creation: {product}")
            return product
        except Exception as e:
            self.logger.error(f"Failed to create product '{product_data['title']}' in Shopify: {e}")
            return None

    async def _get_image_url(self, image_key_or_url: str) -> Optional[str]:
        """
        Retrieve the image URL, either directly or from Backblaze.

        Args:
            image_key_or_url (str): Direct URL or Backblaze image key.

        Returns:
            Optional[str]: The final image URL to be used in Shopify.
        """
        if not image_key_or_url:
            self.logger.warning("No image URL or key provided.")
            return None

        if self._is_valid_url(image_key_or_url):
            self.logger.info(f"Using provided image URL directly: {image_key_or_url}")
            return image_key_or_url
        try:
            self.logger.debug(f"Retrieving image URL from Backblaze for key: {image_key_or_url}")
            image_url = await self.backblaze_handler.get_file_url(image_key_or_url)
            if image_url:
                self.logger.info(f"Retrieved image URL from Backblaze: {image_url}")
                return image_url
            else:
                raise ValueError(f"Failed to retrieve image URL for key: {image_key_or_url}")
        except Exception as e:
            self.logger.error(f"Error retrieving image URL: {e}")
            return None

    async def _upload_image_to_shopify(
        self, product_id: int, image_url: str
    ) -> Optional[Dict[str, Any]]:
        """
        Upload an image to a Shopify product.

        Args:
            product_id (int): The Shopify product ID.
            image_url (str): The URL of the image to upload.

        Returns:
            Optional[Dict[str, Any]]: Response from Shopify if successful.
        """
        try:
            self.logger.debug(
                f"Uploading image to product ID {product_id} with URL: {image_url}"
            )
            image_response = await self.shopify_service.upload_product_image(
                product_id, image_url
            )
            if image_response:
                self.logger.info(f"Image uploaded successfully for product ID {product_id}.")
                return image_response
            else:
                raise ValueError(f"Failed to upload image for product ID {product_id}.")
        except Exception as e:
            self.logger.error(f"Error uploading image to Shopify: {e}")
            return None

    async def _get_or_create_collection(self, collection_title: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve an existing collection by title or create a new one if it doesn't exist.

        Args:
            collection_title (str): The title of the collection.

        Returns:
            Optional[Dict[str, Any]]: The collection data if successful, else None.
        """
        try:
            collection = await self.shopify_service.get_custom_collection_by_title(collection_title)
            if collection:
                self.logger.info(
                    f"Found existing collection '{collection_title}' with ID: {collection['id']}."
                )
                return collection
            else:
                self.logger.info(
                    f"No collection found with title '{collection_title}'. Attempting to create one."
                )
                collection = await self.shopify_service.create_custom_collection(title=collection_title)
                if collection:
                    self.logger.info(
                        f"Created new collection '{collection_title}' with ID: {collection['id']}."
                    )
                    return collection
                else:
                    self.logger.error(f"Failed to create collection '{collection_title}'.")
                    return None
        except Exception as e:
            self.logger.error(f"Error handling collection '{collection_title}': {e}")
            return None

    async def _add_product_to_collection(self, product_id: int, collection_id: int) -> bool:
        """
        Associate a Shopify product with a collection.

        Args:
            product_id (int): The Shopify product ID.
            collection_id (int): The Shopify collection ID.

        Returns:
            bool: True if association was successful, False otherwise.
        """
        self.logger.debug(
            f"Associating product ID {product_id} with collection ID {collection_id}."
        )
        try:
            collect = await self.shopify_service.create_collect(
                product_id=product_id, collection_id=collection_id
            )
            if collect:
                self.logger.info(
                    f"Successfully associated product ID {product_id} with collection ID {collection_id}."
                )
                return True
            else:
                self.logger.error(
                    f"Failed to associate product ID {product_id} with collection ID {collection_id}."
                )
                return False
        except Exception as e:
            self.logger.error(
                f"Error associating product ID {product_id} with collection ID {collection_id}: {e}"
            )
            return False

    def _construct_product_url(self, handle: str) -> str:
        """
        Construct the Shopify product URL based on the product handle.

        Args:
            handle (str): The Shopify product handle.

        Returns:
            str: The full URL to the Shopify product.
        """
        product_url = f"https://{self.shopify_service.base_url.split('//')[1].split('/')[0]}/products/{handle}"
        self.logger.debug(f"Constructed product URL: {product_url}")
        return product_url

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        """
        Validate if a string is a well-formed URL.

        Args:
            url (str): The URL string to validate.

        Returns:
            bool: True if valid, False otherwise.
        """
        parsed_url = urlparse(url)
        is_valid = all([parsed_url.scheme, parsed_url.netloc])
        if not is_valid:
            Logger.get_instance("ProductHandler").debug(f"Invalid URL detected: {url}")
        return is_valid

    async def close(self):
        """
        Close resources associated with ProductHandler.
        """
        try:
            await self.shopify_service.close()
            self.logger.info("ShopifyService resources closed.")
        except Exception as e:
            self.logger.error(f"Error closing ShopifyService: {e}")