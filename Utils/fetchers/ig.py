import logging

import requests

from Utils import config
from Utils import persistence

import random
import time
from typing import Dict, Optional, Any, Tuple, Callable, List

import instaloader
import logging

from itertools import islice

def fetch_ig_urls(account: str) -> List[Dict[str, Any]]:
    """
    Fetch Instagram post URLs, captions, and media URLs using Instaloader.
    
    Args:
        account: Instagram username (with or without @)
        
    Returns:
        List of dicts with url, caption, media_url, is_video
    """
    # Clean username
    account = account.lstrip('@')
    posts = []
    
    # Initialize Instaloader (anonymous context, no login required for public profiles)
    L = instaloader.Instaloader(
        download_pictures=False,  # We only want metadata, not downloads
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False
    )
    
    try:
        # Get profile
        profile = instaloader.Profile.from_username(L.context, account)
        
        # Get posts iterator (lazy loading, memory efficient)
        posts_iterator = profile.get_posts()
        
        # Limit posts using islice (efficient, doesn't load all posts into memory)
        for post in islice(posts_iterator, config.POST_LIMIT):
            try:
                # Build post URL from shortcode
                post_url = f"https://www.instagram.com/p/{post.shortcode}/"
                
                # Get caption (None if no caption)
                caption = post.caption or ""
                
                # Determine media URL and type
                is_video = post.is_video
                
                if is_video:
                    # For videos, get the video URL
                    media_url = post.video_url or ""
                else:
                    # For images, get the highest resolution URL
                    # post.url gives the display URL for single images
                    # For sidecars (carousel), we get the first image or you can iterate
                    if post.typename == 'GraphSidecar':
                        # Sidecar has multiple media - get first image as representative
                        # Or iterate through: list(post.get_sidecar_nodes())
                        sidecar_nodes = list(post.get_sidecar_nodes())
                        if sidecar_nodes:
                            media_url = sidecar_nodes[0].display_url
                        else:
                            media_url = post.url
                    else:
                        media_url = post.url
                
                if media_url:
                    posts.append({
                        "url": post_url,
                        "caption": caption,
                        "media_url": media_url,
                        "is_video": is_video
                    })
                    
            except Exception as post_error:
                logging.warning(f"Error processing post for @{account}: {post_error}")
                continue
        
        logging.info(f"Successfully fetched {len(posts)} IG posts for @{account}")
        
    except instaloader.exceptions.ProfileNotExistsException:
        logging.warning(f"Instagram profile @{account} does not exist")
    except instaloader.exceptions.ConnectionException as e:
        logging.warning(f"Connection error fetching @{account}: {e}")
    except instaloader.exceptions.TooManyRequestsException:
        logging.warning(f"Rate limited while fetching @{account}")
    except Exception as e:
        logging.warning(f"fetch_ig_urls exception for @{account}: {e}")
    
    return posts
