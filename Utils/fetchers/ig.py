import logging

import requests

from Utils import config
from Utils import persistence

import random
import time
from typing import Dict, Optional, Any, Tuple, Callable, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Rotating headers pool
USER_AGENTS = [
    "Instagram 219.0.0.12.119 Android (30/11; 480dpi; 1080x1920; samsung; SM-G998B; beyond2; exynos990; en_US)",
    "Instagram 219.0.0.12.119 Android (29/10; 440dpi; 1080x2220; google; Pixel 5; redfin; qcom; en_GB)",
    "Instagram 219.0.0.12.119 iOS (13.7; iPhone12,1; iPhone; en_US)",
    "Instagram 219.0.0.12.119 iOS (14.0; iPad13,4; iPad; en_GB)"
]

IG_APP_IDS = [
    "936619743392459",   # main Android ID
    "124024574287414",   # alternative (sometimes used by iOS)
    "567067343352427"    # another common one
]

def fetch_ig_urls(
    account: str,
    max_retries: int = 3,
    base_delay: float = 2.0,
    post_limit: int = 12,
    sessionid: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch recent Instagram post URLs and metadata for a given account.

    Args:
        account: Instagram username (with or without '@')
        max_retries: Number of retries on 429 or other temporary failures
        base_delay: Initial delay in seconds before retry (doubles each time)
        post_limit: Maximum number of posts to return
        sessionid: Optional Instagram sessionid cookie for higher rate limits

    Returns:
        List of dicts with keys: url, caption, media_url, is_video
    """
    account = account.lstrip('@')
    posts = []

    # Prepare headers with random choices from pools
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "*/*",
        "Accept-Language": "en-US",
        "Accept-Encoding": "gzip, deflate",
        "X-IG-App-ID": random.choice(IG_APP_IDS),
        "X-IG-WWW-Claim": "0",
        "Connection": "keep-alive"
    }

    # Add authentication cookie if provided
    if sessionid:
        headers["Cookie"] = f"sessionid={sessionid}"

    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={account}"

    # Retry loop with exponential backoff
    for attempt in range(max_retries):
        try:
            # Add a random delay before each request (except first attempt)
            if attempt > 0:
                sleep_time = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                logger.debug(f"Retry {attempt} for @{account}, sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)

            resp = requests.get(url, headers=headers, timeout=20)

            if resp.status_code == 429:
                # Rate limited – check for Retry-After header
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait = int(retry_after)
                else:
                    wait = base_delay * (2 ** attempt) + random.uniform(1, 3)
                logger.warning(f"429 for @{account}, waiting {wait:.2f}s before retry")
                time.sleep(wait)
                continue  # go to next retry

            if resp.status_code != 200:
                logger.warning(f"IG fetch failed for @{account}: status {resp.status_code}")
                return []   # non‑retryable error

            data = resp.json()
            user_data = data.get("data", {}).get("user", {})
            if not user_data:
                logger.warning(f"No user data for @{account}")
                return []

            edges = user_data.get("edge_owner_to_timeline_media", {}).get("edges", [])
            for edge in edges[:post_limit]:
                node = edge.get("node", {})
                shortcode = node.get("shortcode")
                if not shortcode:
                    continue

                post_url = f"https://www.instagram.com/p/{shortcode}/"
                is_video = node.get("is_video", False)
                caption = ""
                if node.get("edge_media_to_caption", {}).get("edges"):
                    caption_edges = node["edge_media_to_caption"]["edges"]
                    if caption_edges:
                        caption = caption_edges[0]["node"].get("text", "")

                if is_video:
                    media_url = node.get("video_url", "")
                else:
                    resources = node.get("display_resources", [])
                    media_url = resources[-1]["src"] if resources else node.get("display_url", "")

                if media_url:
                    posts.append({
                        "url": post_url,
                        "caption": caption,
                        "media_url": media_url,
                        "is_video": is_video
                    })

            logger.info(f"Successfully fetched {len(posts)} IG posts for @{account}")
            return posts

        except requests.exceptions.RequestException as e:
            logger.warning(f"Request exception for @{account} (attempt {attempt+1}): {e}")
            if attempt == max_retries - 1:
                return []
            time.sleep(base_delay * (2 ** attempt) + random.uniform(0, 1))

        except Exception as e:
            logger.warning(f"fetch_ig_urls exception for @{account}: {e}")
            return []

    return posts