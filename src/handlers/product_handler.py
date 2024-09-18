from typing import Dict, Any, Optional
from urllib.parse import urlparse
from utils.logger import Logger
from handlers.backblaze_handler import BackblazeHandler
from services.shopify_service import ShopifyService

class ProductHandler:
    def __init__(self, admin_api_token: str, shop_name: str, backblaze_handler: BackblazeHandler):
        self.shopify_service = ShopifyService(shop_name, admin_api_token)
        self.backblaze_handler = backblaze_handler
        self.shop_name = shop_name
        self.logger = Logger.get_instance("ProductHandler")

    async def add_product_to_shopify(self, product_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.logger.debug(f"Received product data for Shopify creation: {product_data}")
        product = await self._create_shopify_product(product_data)
        if not product:
            return None

        image_url = await self._get_image_url(product_data['image_url'])
        if not image_url:
            return None

        image_upload_response = await self._upload_image_to_shopify(product['id'], image_url)
        if not image_upload_response:
            return None

        product_url = self._construct_product_url(product['handle'])
        self.logger.info(f"Product '{product['title']}' added successfully with image. URL: {product_url}")
        return {'product': product, 'product_url': product_url}

    async def _create_shopify_product(self, product_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            product = await self.shopify_service.create_product(
                title=product_data['title'],
                description=product_data['description'],
                images=[],
                vendor=product_data['vendor'],
                price=product_data['price'],
                product_type=product_data.get('product_type', 'Artist Trading Card'),
                inventory_quantity=product_data.get('inventory_quantity', 100)
            )
            self.logger.debug(f"Shopify response for product creation: {product}")
            return product
        except Exception as e:
            self.logger.error(f"Failed to create product '{product_data['title']}' in Shopify: {e}")
            return None

    async def _get_image_url(self, image_key_or_url: str) -> Optional[str]:
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

    async def _upload_image_to_shopify(self, product_id: int, image_url: str) -> Optional[Dict[str, Any]]:
        try:
            self.logger.debug(f"Uploading image to product ID {product_id} with URL: {image_url}")
            image_response = await self.shopify_service.upload_product_image(product_id, image_url)
            if image_response:
                self.logger.info(f"Image uploaded successfully for product ID {product_id}.")
                return image_response
            else:
                raise ValueError(f"Failed to upload image for product ID {product_id}.")
        except Exception as e:
            self.logger.error(f"Error uploading image to Shopify: {e}")
            return None

    def _construct_product_url(self, handle: str) -> str:
        product_url = f"https://{self.shop_name}.myshopify.com/products/{handle}"
        self.logger.debug(f"Constructed product URL: {product_url}")
        return product_url

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        parsed_url = urlparse(url)
        is_valid = all([parsed_url.scheme, parsed_url.netloc])
        if not is_valid:
            Logger.get_instance("ProductHandler").debug(f"Invalid URL detected: {url}")
        return is_valid