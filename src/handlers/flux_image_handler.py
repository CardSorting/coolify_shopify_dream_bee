import os
from typing import Optional, Dict, Any
import fal_client
import aiohttp
from utils.logger import Logger

class FluxImageHandler:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("FAL_KEY")
        if not self.api_key:
            raise ValueError("FAL_KEY not found in environment variables and not provided to the constructor.")
        self.logger = Logger.get_instance("FluxImageHandler")
        # Initialize the fal_client here if needed

    async def generate_image(self, prompt: str, image_size: str = "portrait_16_9", num_inference_steps: int = 28, 
                             guidance_scale: float = 3.5, num_images: int = 1, enable_safety_checker: bool = True) -> Optional[str]:
        try:
            # Call fal_client.submit without await if it returns a SyncRequestHandle
            handler = fal_client.submit(
                "fal-ai/flux-pro",
                arguments={
                    "prompt": prompt,
                    "image_size": image_size,
                    "num_inference_steps": num_inference_steps,
                    "guidance_scale": guidance_scale,
                    "num_images": num_images,
                    "enable_safety_checker": enable_safety_checker
                }
            )
            # Since handler.get() is a synchronous call, do not use await
            result = handler.get()
            if result and 'images' in result and result['images']:
                return result['images'][0]['url']
            self.logger.warning("No images found in the result.")
            return None
        except Exception as e:
            self.logger.error(f"FLUX.1 API Error: {str(e)}")
            return None

    async def download_image(self, image_url: str) -> Optional[bytes]:
                try:
                    async with aiohttp.ClientSession() as session, session.get(image_url) as response:
                        if response.status == 200:
                            return await response.read()
                        self.logger.error(f"Failed to download image. Status: {response.status}")
                        return None
                except Exception as e:
                    self.logger.error(f"Error downloading image: {str(e)}")
                    return None

    async def get_request_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        try:
            # Await if fal_client.get_status is asynchronous
            status = await fal_client.get_status(request_id)
            return status
        except Exception as e:
            self.logger.error(f"Error getting request status: {str(e)}")
            return None

    async def cancel_request(self, request_id: str) -> bool:
        # Implement proper request cancellation if supported by fal_client
        self.logger.warning(f"Cancel request not implemented for request_id: {request_id}")
        return False