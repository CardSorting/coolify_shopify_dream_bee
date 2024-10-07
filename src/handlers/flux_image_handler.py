# src/handlers/flux_image_handler.py

import os
import re
import asyncio
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor
import fal_client
import aiohttp
import difflib  # For fuzzy matching
from dotenv import load_dotenv

from utils.logger import Logger

# Load environment variables
load_dotenv()

# Configuration Constants
MAX_RETRIES = 3
BACKOFF_FACTOR = 2
THREAD_POOL_WORKERS = 5
IMAGE_SIZE_DEFAULT = "landscape_16_9"
SAFETY_TOLERANCE_DEFAULT = "5"
VALID_IMAGE_SIZES = {
    "square_hd",
    "square",
    "portrait_4_3",
    "portrait_16_9",
    "landscape_4_3",
    "landscape_16_9"
}
VALID_SAFETY_TOLERANCES = {"1", "2", "3", "4", "5", "6"}
VALID_OUTPUT_FORMATS = {"jpeg", "png"}

class FluxImageHandler:
    """
    A handler class for generating images using the FLUX.1 [pro] API,
    downloading them, and managing request statuses.
    """

    def __init__(self):
        self.api_key = os.getenv("FAL_KEY")
        if not self.api_key:
            raise ValueError("FAL_KEY environment variable is not set.")

        self.logger = Logger.get_instance("FluxImageHandler")
        self.logger.debug("FluxImageHandler initialized with provided API key.")

        # Initialize a thread pool executor for blocking operations
        self.executor = ThreadPoolExecutor(max_workers=THREAD_POOL_WORKERS)

        # Set the API key for fal_client
        fal_client.api_key = self.api_key

    def determine_image_size(self, prompt: str) -> str:
        """
        Determine the image size based on keywords present in the prompt.
        Uses fuzzy matching to account for typos.
        """
        portrait_keywords = [
            "portrait", "person", "people", "face", "vertical", "standing", "tall",
            "headshot", "close-up", "full-body", "individual", "solo", "figure",
            "upright", "head and shoulders", "profile", "selfie", "emotional",
            "expressive", "detailed", "subject-focused", "upper body", "face close-up",
            "bust", "half-length", "waist up", "knee up", "medium shot", "mid shot"
        ]

        landscape_keywords = [
            "landscape", "scenery", "horizon", "wide", "horizontal", "panorama",
            "nature", "vista", "view", "outdoor", "expansive", "broad", "spacious",
            "background", "skyline", "mountains", "forest", "seascape", "sunset",
            "sunrise", "cityscape", "architecture", "wide-angle", "aerial", "vast",
            "countryside", "river", "beach", "desert", "meadow", "garden", "valley",
            "waterfall", "hill", "lake", "field", "sea", "ocean", "wilderness",
            "plain", "plateau", "canyon", "cliff"
        ]

        prompt_words = re.findall(r'\b\w+\b', prompt.lower())

        def count_matches_fuzzy(words: List[str], keywords: List[str]) -> int:
            count = 0
            for word in words:
                matches = difflib.get_close_matches(word, keywords, n=1, cutoff=0.8)
                if matches:
                    self.logger.debug(f"Fuzzy match found: '{word}' matched with '{matches[0]}'")
                    count += 1
            return count

        portrait_score = count_matches_fuzzy(prompt_words, portrait_keywords)
        landscape_score = count_matches_fuzzy(prompt_words, landscape_keywords)

        self.logger.debug(f"Portrait score: {portrait_score}, Landscape score: {landscape_score}")

        if portrait_score > landscape_score:
            self.logger.debug("Portrait orientation selected based on prompt keywords.")
            return "portrait_16_9"
        elif landscape_score > portrait_score:
            self.logger.debug("Landscape orientation selected based on prompt keywords.")
            return "landscape_16_9"
        else:
            self.logger.debug("Defaulting to landscape orientation.")
            return IMAGE_SIZE_DEFAULT

    async def generate_image(
        self,
        prompt: str,
        image_size: Optional[str] = None,
        num_inference_steps: int = 28,
        seed: Optional[int] = None,
        guidance_scale: float = 3.5,
        num_images: int = 1,
        safety_tolerance: str = SAFETY_TOLERANCE_DEFAULT,
        output_format: str = "jpeg"
    ) -> Optional[Dict[str, Any]]:
        """
        Generate an image based on the provided prompt.
        """
        sanitized_prompt = self._sanitize_prompt(prompt)
        if not sanitized_prompt:
            self.logger.warning("Invalid prompt provided for image generation.")
            return None

        if not image_size:
            image_size = self.determine_image_size(sanitized_prompt)

        if image_size not in VALID_IMAGE_SIZES:
            self.logger.warning(f"Invalid image_size '{image_size}' provided. Defaulting to '{IMAGE_SIZE_DEFAULT}'.")
            image_size = IMAGE_SIZE_DEFAULT

        arguments = {
            "prompt": sanitized_prompt,
            "image_size": image_size,
            "num_inference_steps": num_inference_steps,
            "guidance_scale": guidance_scale,
            "num_images": num_images,
            "safety_tolerance": safety_tolerance,
            "output_format": output_format,
            "enable_safety_checker": True
        }

        if seed is not None:
            arguments["seed"] = seed

        self.logger.debug(f"Arguments for image generation: {arguments}")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                loop = asyncio.get_running_loop()
                handler = await loop.run_in_executor(
                    self.executor,
                    lambda: fal_client.submit("fal-ai/flux-pro/v1.1", arguments)
                )
                self.logger.debug(
                    f"Submitted image generation request with prompt: '{sanitized_prompt}' and size: '{image_size}'. "
                    f"Attempt {attempt}."
                )

                result = await loop.run_in_executor(
                    self.executor,
                    handler.get
                )
                if result and 'images' in result and result['images']:
                    self.logger.info(
                        f"Image generated successfully. URL: {result['images'][0]['url']}"
                    )
                    return result
                else:
                    self.logger.warning("No images found in the result.")
                    return None

            except Exception as e:
                self.logger.error(
                    f"FLUX API Error during image generation attempt {attempt}: {str(e)}",
                    exc_info=True
                )
                if attempt < MAX_RETRIES:
                    backoff_time = BACKOFF_FACTOR ** (attempt - 1)
                    self.logger.info(f"Retrying image generation in {backoff_time} seconds...")
                    await asyncio.sleep(backoff_time)
                else:
                    self.logger.error("Max retries reached. Image generation failed.")
                    return None

    def _sanitize_prompt(self, prompt: str) -> Optional[str]:
        """
        Sanitize the user prompt to prevent injection attacks and ensure validity.
        """
        sanitized = re.sub(r'[<>]', '', prompt)
        sanitized = sanitized.strip()
        if 3 <= len(sanitized) <= 200:
            return sanitized
        else:
            self.logger.warning("Prompt length is out of acceptable range (3-200 characters).")
            return None

    async def download_image(self, image_url: str) -> Optional[bytes]:
        """
        Download the image from the provided URL.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(image_url) as response:
                    if response.status == 200:
                        image_data = await response.read()
                        self.logger.info(
                            f"Image downloaded successfully from URL: {image_url}"
                        )
                        return image_data
                    else:
                        self.logger.error(
                            f"Failed to download image. Status: {response.status} "
                            f"for URL: {image_url}"
                        )
                        return None
        except Exception as e:
            self.logger.error(
                f"Error downloading image from {image_url}: {str(e)}",
                exc_info=True
            )
            return None

    async def shutdown(self):
        """
        Shutdown the thread pool executor gracefully.
        """
        self.executor.shutdown(wait=True)
        self.logger.debug("FluxImageHandler executor has been shut down.")