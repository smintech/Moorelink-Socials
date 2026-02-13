import logging
from typing import List, Dict, Any

import requests

from Utils import config
from Utils import persistence

def fetch_ig_urls(account: str) -> List[Dict[str, Any]]:
    account = account.lstrip('@')
    posts = []

    headers = {
        "User-Agent": "Instagram 219.0.0.12.119 Android (30/11; 480dpi; 1080x1920; samsung; SM-G998B; beyond2; exynos990; en_US)",
        "Accept": "*/*",
        "Accept-Language": "en-US",
        "Accept-Encoding": "gzip, deflate",
        "X-IG-App-ID": "936619743392459",
        "X-IG-WWW-Claim": "0",
        "Connection": "keep-alive"
    }

    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={account}"

    try:
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200:
            logging.warning(f"IG fetch failed for @{account}: status {resp.status_code}")
            return []

        data = resp.json()
        user_data = data.get("data", {}).get("user", {})
        if not user_data:
            logging.warning(f"No user data for @{account}")
            return []

        edges = user_data.get("edge_owner_to_timeline_media", {}).get("edges", [])
        for edge in edges[:config.POST_LIMIT]:
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

        logging.info(f"Successfully fetched {len(posts)} IG posts for @{account}")

    except Exception as e:
        logging.warning(f"fetch_ig_urls exception for @{account}: {e}")

    return posts