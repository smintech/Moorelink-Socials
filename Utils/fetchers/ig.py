import logging

import requests

from Utils import config
from Utils import persistence

import random
import time
from typing import Dict, Optional, Any, Tuple, Callable, List

from instagrapi import Client
from instagrapi.exceptions import ClientError, LoginRequired
from pathlib import Path
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_ig_urls(account: str, cl: Client = None) -> List[Dict[str, Any]]:
    """
    Fetch Instagram posts using instagrapi.
    
    Args:
        account: Instagram username (with or without @)
        cl: Optional pre-authenticated instagrapi Client instance
        
    Returns:
        List of dictionaries containing post data
    """
    account = account.lstrip('@')
    posts = []
    
    # Create client if not provided
    if cl is None:
        cl = Client()
        
        # # OPTION 1: Login with credentials
        # username = "your_ig_username"
        # password = "your_ig_password"
        # try:
        #     cl.login(username, password)
        # except Exception as e:
        #     logging.error(f"Instagram login failed: {e}")
        #     return []
        
        # # OPTION 2: Load session from file (recommended for production)
        # session_file = Path("ig_session.json")
        # if session_file.exists():
        #     try:
        #         cl.load_settings(session_file)
        #         cl.login(username, password)  # Relogin with saved session
        #     except Exception as e:
        #         logging.warning(f"Session load failed, logging in fresh: {e}")
        #         cl.login(username, password)
        # else:
        #     cl.login(username, password)
        #     cl.dump_settings(session_file)  # Save session for next time
        
        # # OPTION 3: Proxy support (if needed)
        # cl.set_proxy("http://proxy:port")
        # # or with auth:
        # # cl.set_proxy("http://user:pass@proxy:port")
    
    try:
        # Get user ID from username
        user_id = cl.user_id_from_username(account)
        
        # Fetch user's media posts
        medias = cl.user_medias(user_id, amount=config.POST_LIMIT)
        
        for media in medias:
            post_url = f"https://www.instagram.com/p/{media.code}/"
            caption = media.caption_text or ""
            is_video = media.media_type == 2  # 1=photo, 2=video, 8=album
            
            # Get media URL
            if is_video:
                media_url = str(media.video_url) if media.video_url else ""
            else:
                # Use thumbnail_url for better quality, or display_url for highest
                media_url = str(media.thumbnail_url) if media.thumbnail_url else ""
                # # For highest quality image:
                # media_url = str(media.display_url) if media.display_url else ""
            
            if media_url:
                posts.append({
                    "url": post_url,
                    "caption": caption,
                    "media_url": media_url,
                    "is_video": is_video
                })
        
        logging.info(f"Successfully fetched {len(posts)} IG posts for @{account}")
        
    except LoginRequired:
        logging.error(f"Login required to fetch @{account}. Enable authentication above.")
    except ClientError as e:
        logging.warning(f"Instagrapi client error for @{account}: {e}")
    except Exception as e:
        logging.warning(f"fetch_ig_urls exception for @{account}: {e}")
    
    return posts


# # HELPER FUNCTION: For production use with session management
# def get_instagram_client(
#     username: str = "your_username",
#     password: str = "your_password",
#     session_file: str = "ig_session.json"
# ) -> Client:
#     """
#     Get an authenticated Instagram client with session persistence.
#     """
#     cl = Client()
#     session_path = Path(session_file)
#     
#     if session_path.exists():
#         try:
#             cl.load_settings(session_path)
#             cl.login(username, password)
#             logging.info("Logged in with existing session")
#         except Exception as e:
#             logging.warning(f"Session invalid, logging in fresh: {e}")
#             cl.login(username, password)
#             cl.dump_settings(session_path)
#     else:
#         cl.login(username, password)
#         cl.dump_settings(session_path)
#         logging.info("Fresh login, session saved")
#     
#     return cl
#
#
# # USAGE EXAMPLE:
# # Initialize client once (at app startup)
# # ig_client = get_instagram_client()
# #
# # Then reuse it:
# # posts = fetch_ig_urls("username", cl=ig_client)
