import logging
import time
import html
from typing import List, Optional, Dict, Any

import requests

from Utils.config import *
from Utils.persistence import *

def rapidapi_get(path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 20, retries: int = 2) -> Dict[str, Any]:
    if not config.RAPIDAPI_KEY:
        raise RuntimeError("RAPIDAPI_KEY not set in environment")

    url = f"{config.RAPIDAPI_BASE.rstrip('/')}/{path.lstrip('/')}"
    headers = {
        "x-rapidapi-key": config.RAPIDAPI_KEY,
        "x-rapidapi-host": config.RAPIDAPI_HOST,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; Bot/1.0; +https://example.com/bot)"
    }

    last_exc = None
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params or {}, timeout=timeout)
            if resp.status_code != 200:
                logging.warning(
                    "rapidapi_get non-200 status %s for %s (attempt %d). Body: %.500s",
                    resp.status_code, url, attempt + 1, resp.text[:500]
                )
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            last_exc = e
            status = getattr(e.response, "status_code", None)
            if status in (429, 502, 503, 504):
                backoff = 1.5 * (attempt + 1)
                time.sleep(backoff)
                continue
            raise
        except Exception as e:
            last_exc = e
            time.sleep(1.0 * (attempt + 1))
            continue

    raise RuntimeError(f"RapidAPI call failed after {retries + 1} attempts")

def fetch_fb_urls(account_or_url: str, limit: int = config.POST_LIMIT) -> List[Dict[str, Any]]:
    input_str = account_or_url.strip()

    if any(x in input_str for x in ["share/", "mibextid=", "/posts/", "/photo.php", "/reel/"]):
        clean_url = input_str.split("?")[0].rstrip("/")
        return [{
            "post_id": "",
            "post_url": clean_url,
            "caption": "Single shared Facebook post",
            "media_url": "",
            "is_video": False,
            "likes": 0,
            "comments": 0,
            "shares": 0,
        }]

    clean_account = input_str.lstrip("@").split("/")[-1].split("?")[0]
    if not clean_account:
        return []

    profile_url = f"https://www.facebook.com/{clean_account}"
    posts: List[Dict[str, Any]] = []
    seen_ids = set()
    end_cursor: Optional[str] = None

    try:
        while len(posts) < limit:
            params = {"link": profile_url, "timezone": "UTC"}
            if end_cursor:
                params["end_cursor"] = end_cursor

            data = rapidapi_get("get_facebook_posts_details", params=params)

            inner_data = data.get("data") if isinstance(data, dict) else data
            if not isinstance(inner_data, dict):
                logging.warning("Unexpected response format (not dict): %s", type(inner_data))
                break

            raw_posts = inner_data.get("posts", [])
            page_info = inner_data.get("page_info", {})

            if not raw_posts:
                break

            for item in raw_posts:
                if len(posts) >= limit:
                    break

                post_id = (
                    item.get("details", {}).get("post_id") or
                    item.get("values", {}).get("post_id") or
                    ""
                )
                if not post_id or post_id in seen_ids:
                    continue
                seen_ids.add(post_id)

                caption = (
                    item.get("values", {}).get("text") or
                    item.get("details", {}).get("text") or
                    ""
                )
                caption = html.unescape(caption)

                post_url = (
                    item.get("details", {}).get("post_link") or
                    item.get("values", {}).get("post_link") or
                    f"https://www.facebook.com/{clean_account}/posts/{post_id}"
                )

                is_video = (
                    "reel" in post_url.lower() or
                    item.get("values", {}).get("is_media") == "Video" or
                    bool(item.get("attachments"))
                )

                media_url = ""
                attachments = item.get("attachments", [])
                if attachments and isinstance(attachments, list):
                    first_att = attachments[0]
                    if first_att.get("__typename") == "Video":
                        media_url = first_att.get("thumbnail_url", "")
                    elif first_att.get("__typename") == "Photo":
                        photo = first_att.get("photo_image", {})
                        media_url = photo.get("uri", "") if isinstance(photo, dict) else ""

                reactions = item.get("reactions", {})
                likes = reactions.get("Like") or reactions.get("total_reaction_count", 0)

                comments = item.get("details", {}).get("comments_count", "0")
                comments = int(''.join(filter(str.isdigit, str(comments)))) if comments else 0

                shares = item.get("details", {}).get("share_count", "0")
                shares = int(''.join(filter(str.isdigit, str(shares)))) if shares else 0

                posts.append({
                    "post_id": post_id,
                    "post_url": post_url,
                    "caption": caption,
                    "media_url": media_url,
                    "is_video": is_video,
                    "likes": likes,
                    "comments": comments,
                    "shares": shares,
                })

            end_cursor = page_info.get("end_cursor")
            if not end_cursor or not page_info.get("has_next", False):
                break

        logging.info("Facebook success: fetched %d posts for %s", len(posts), profile_url)

    except Exception as e:
        logging.error("Failed to fetch Facebook posts for %s: %s", profile_url, e)

    return posts[:limit]