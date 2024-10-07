# src/services/shopify_service.py

import aiohttp
import asyncio
from typing import Dict, Any, Optional, List
from urllib.parse import urlencode
import os
from dotenv import load_dotenv
from utils.logger import Logger
from aiohttp import ClientSession, ClientResponseError
from asyncio import Semaphore
import backoff  # Ensure this package is installed via `pip install backoff`

# Load environment variables
load_dotenv()

class ShopifyService:
    """
    Service class for interacting with the Shopify API, including product and image management.
    """

    def __init__(
        self,
        shop_name: str,
        admin_api_token: str,
        max_retries: int = 3,
        retry_backoff_factor: float = 0.5,
        max_concurrent_requests: int = 10
    ):
        self.base_url = f"https://{shop_name}.myshopify.com/admin/api/2024-07"
        self.headers = {
            "X-Shopify-Access-Token": admin_api_token,
            "Content-Type": "application/json"
        }
        self.logger = Logger.get_instance("ShopifyService")
        self.session: Optional[ClientSession] = None
        self.max_retries = max_retries
        self.retry_backoff_factor = retry_backoff_factor
        self.semaphore = Semaphore(max_concurrent_requests)  # Control concurrent requests

    async def initialize(self):
        """
        Initialize the aiohttp ClientSession.
        """
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)
            self.logger.info("Initialized aiohttp ClientSession for ShopifyService.")

    async def close(self):
        """
        Close the aiohttp ClientSession.
        """
        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.info("Closed aiohttp ClientSession for ShopifyService.")

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=4,
        factor=0.5,
        jitter=backoff.full_jitter
    )
    async def _request(self, method: str, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Makes an HTTP request to the Shopify API with retry logic.
        """
        await self.initialize()
        url = f"{self.base_url}/{endpoint}"
        self.logger.debug(f"Making {method} request to {url} with data: {data}")

        async with self.semaphore:
            try:
                async with self.session.request(method, url, json=data) as response:
                    response_text = await response.text()
                    if response.status in [200, 201]:
                        self.logger.info(f"API {method} request to {url} succeeded with status {response.status}.")
                        return await response.json()
                    else:
                        self.logger.error(f"Shopify API error ({response.status}): {response_text}")
                        if 500 <= response.status < 600:
                            raise ClientResponseError(
                                status=response.status,
                                request_info=response.request_info,
                                history=response.history,
                                message=f"Server error: {response.status}"
                            )
                        return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                self.logger.error(f"Network error during API {method} request to {url}: {str(e)}")
                raise

    async def create_product(
        self,
        title: str,
        description: str,
        images: Optional[List[Dict[str, str]]],
        vendor: str,
        price: float,
        product_type: str = "Artist Trading Card",
        inventory_quantity: int = 100,
        tags: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Creates a new product on Shopify.
        """
        product_data = {
            "product": {
                "title": title,
                "body_html": description,
                "vendor": vendor,
                "product_type": product_type,
                "images": images if images else [],
                "variants": [{
                    "price": f"{price:.2f}",
                    "inventory_management": "shopify",
                    "inventory_quantity": inventory_quantity
                }]
            }
        }

        if tags:
            product_data["product"]["tags"] = ",".join(tags)

        self.logger.debug(f"Creating product with data: {product_data}")

        response = await self._request("POST", "products.json", product_data)
        if response:
            product = response.get("product")
            if product:
                self.logger.info(f"Product '{title}' created successfully with ID: {product['id']}")
                return product
            else:
                self.logger.error(f"Unexpected response format while creating product '{title}'.")
        else:
            self.logger.error(f"Failed to create product '{title}'.")
        return None

    async def upload_product_image(self, product_id: int, image_url: str) -> Optional[Dict[str, Any]]:
        """
        Uploads an image to an existing Shopify product.
        """
        if not self._is_valid_url(image_url):
            self.logger.error(f"Invalid image URL: {image_url}")
            return None

        image_data = {
            "image": {
                "src": image_url
            }
        }
        endpoint = f"products/{product_id}/images.json"
        self.logger.debug(f"Uploading image to product {product_id} with data: {image_data}")

        response = await self._request("POST", endpoint, image_data)
        if response:
            image = response.get("image")
            if image:
                self.logger.info(f"Image uploaded successfully to product ID {product_id} with image ID: {image['id']}")
                return image
            else:
                self.logger.error(f"Unexpected response format while uploading image to product ID {product_id}.")
        else:
            self.logger.error(f"Failed to upload image to product ID {product_id}.")
        return None

    async def update_product(self, product_id: int, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Updates an existing product on Shopify.
        """
        product_data = {"product": updates}
        self.logger.debug(f"Updating product {product_id} with data: {product_data}")

        response = await self._request("PUT", f"products/{product_id}.json", product_data)
        if response:
            product = response.get("product")
            if product:
                self.logger.info(f"Product ID {product_id} updated successfully.")
                return product
            else:
                self.logger.error(f"Unexpected response format while updating product ID {product_id}.")
        else:
            self.logger.error(f"Failed to update product ID {product_id}.")
        return None

    async def delete_product(self, product_id: int) -> bool:
        """
        Deletes a product from Shopify.
        """
        self.logger.debug(f"Deleting product ID {product_id}.")
        response = await self._request("DELETE", f"products/{product_id}.json")
        if response is not None:
            self.logger.info(f"Product ID {product_id} deleted successfully.")
            return True
        else:
            self.logger.error(f"Failed to delete product ID {product_id}.")
            return False

    async def get_product(self, product_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieves a product from Shopify.
        """
        self.logger.debug(f"Retrieving product ID {product_id}.")
        response = await self._request("GET", f"products/{product_id}.json")
        if response:
            product = response.get("product")
            if product:
                self.logger.info(f"Product ID {product_id} retrieved successfully.")
                return product
            else:
                self.logger.error(f"Unexpected response format while retrieving product ID {product_id}.")
        else:
            self.logger.error(f"Failed to retrieve product ID {product_id}.")
        return None

    async def list_products(self, limit: int = 50, page_info: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lists products from Shopify.
        """
        params = {"limit": limit}
        if page_info:
            params["page_info"] = page_info
        query_string = urlencode(params)
        endpoint = f"products.json?{query_string}"
        self.logger.debug(f"Listing products with endpoint: {endpoint}")

        response = await self._request("GET", endpoint)
        if response:
            products = response.get("products", [])
            self.logger.info(f"Retrieved {len(products)} products.")
            return products
        else:
            self.logger.error("Failed to list products.")
            return []

    async def update_inventory(
        self,
        inventory_item_id: int,
        location_id: int,
        available: int
    ) -> bool:
        """
        Updates inventory levels for a product.
        """
        inventory_data = {
            "location_id": location_id,
            "inventory_item_id": inventory_item_id,
            "available": available
        }
        self.logger.debug(f"Updating inventory with data: {inventory_data}")

        response = await self._request("POST", "inventory_levels/set.json", inventory_data)
        if response:
            self.logger.info(f"Inventory for item ID {inventory_item_id} updated successfully.")
            return True
        else:
            self.logger.error(f"Failed to update inventory for item ID {inventory_item_id}.")
            return False

    async def get_inventory_levels(self, inventory_item_ids: List[int]) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieves inventory levels for a list of items.
        """
        params = {"inventory_item_ids": ",".join(map(str, inventory_item_ids))}
        query_string = urlencode(params)
        endpoint = f"inventory_levels.json?{query_string}"
        self.logger.debug(f"Retrieving inventory levels with endpoint: {endpoint}")

        response = await self._request("GET", endpoint)
        if response:
            inventory_levels = response.get("inventory_levels", [])
            self.logger.info(f"Retrieved inventory levels for item IDs {inventory_item_ids}.")
            return inventory_levels
        else:
            self.logger.error(f"Failed to retrieve inventory levels for item IDs {inventory_item_ids}.")
            return None

    async def get_custom_collection_by_title(self, collection_title: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a custom collection by its title.
        """
        params = {"title": collection_title}
        query_string = urlencode(params)
        endpoint = f"custom_collections.json?{query_string}"
        self.logger.debug(f"Retrieving custom collection with endpoint: {endpoint}")

        response = await self._request("GET", endpoint)
        if response:
            collections = response.get("custom_collections", [])
            if collections:
                self.logger.info(f"Found collection '{collection_title}' with ID: {collections[0]['id']}.")
                return collections[0]
            else:
                self.logger.info(f"No collection found with title '{collection_title}'.")
                return None
        else:
            self.logger.error(f"Failed to retrieve collection '{collection_title}'.")
            return None

    async def create_custom_collection(self, title: str) -> Optional[Dict[str, Any]]:
        """
        Creates a new custom collection on Shopify.
        """
        collection_data = {
            "custom_collection": {
                "title": title
            }
        }
        self.logger.debug(f"Creating custom collection with data: {collection_data}")

        response = await self._request("POST", "custom_collections.json", collection_data)
        if response:
            collection = response.get("custom_collection")
            if collection:
                self.logger.info(f"Created custom collection '{title}' with ID: {collection['id']}.")
                return collection
            else:
                self.logger.error(f"Unexpected response format while creating collection '{title}'.")
        else:
            self.logger.error(f"Failed to create collection '{title}'.")
        return None

    async def create_collect(self, product_id: int, collection_id: int) -> Optional[Dict[str, Any]]:
        """
        Associates a product with a custom collection.
        """
        collect_data = {
            "collect": {
                "product_id": product_id,
                "collection_id": collection_id
            }
        }
        self.logger.debug(f"Creating collect with data: {collect_data}")

        response = await self._request("POST", "collects.json", collect_data)
        if response:
            collect = response.get("collect")
            if collect:
                self.logger.info(f"Product ID {product_id} associated with collection ID {collection_id}.")
                return collect
            else:
                self.logger.error(f"Unexpected response format while creating collect for product ID {product_id} and collection ID {collection_id}.")
        else:
            self.logger.error(f"Failed to create collect for product ID {product_id} and collection ID {collection_id}.")
        return None

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        """
        Validates if a string is a well-formed URL.
        """
        from urllib.parse import urlparse

        parsed_url = urlparse(url)
        return all([parsed_url.scheme, parsed_url.netloc]).env