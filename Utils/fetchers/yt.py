import logging
from typing import List, Optional, Dict, Any

from googleapiclient.discovery import build

from Utils.config import *

def fetch_yt_videos(channel_handle: str, max_results: Optional[int] = None) -> List[Dict[str, Any]]:
    if max_results is None:
        max_results = config.POST_LIMIT

    if not config.YOUTUBE_API_KEY:
        logging.warning("YOUTUBE_API_KEY not set")
        return []

    youtube = build('youtube', 'v3', developerKey=config.YOUTUBE_API_KEY)
    videos: List[Dict[str, Any]] = []

    try:
        handle = channel_handle.lstrip('@')
        channel_response = youtube.channels().list(
            part='contentDetails',
            forHandle=handle
        ).execute()

        items = channel_response.get('items', [])
        if not items:
            logging.info(f"No channel found for @{handle}")
            return []

        uploads_playlist_id = items[0]['contentDetails']['relatedPlaylists']['uploads']

        next_page_token = None
        fetched = 0

        while fetched < max_results:
            request = youtube.playlistItems().list(
                part='snippet',
                playlistId=uploads_playlist_id,
                maxResults=min(50, max_results - fetched),
                pageToken=next_page_token
            )
            playlist_response = request.execute()

            for item in playlist_response.get('items', []):
                snippet = item.get('snippet', {})
                title = snippet.get('title', '')
                if title in ('Private video', 'Deleted video'):
                    continue

                resource = snippet.get('resourceId', {})
                video_id = resource.get('videoId')
                if not video_id:
                    continue

                thumbs = snippet.get('thumbnails', {})
                thumb_url = (
                    thumbs.get('maxres', {}).get('url') or
                    thumbs.get('standard', {}).get('url') or
                    thumbs.get('high', {}).get('url') or
                    thumbs.get('default', {}).get('url') or
                    None
                )

                videos.append({
                    "post_id": video_id,
                    "post_url": f"https://www.youtube.com/watch?v={video_id}",
                    "caption": f"<b>{title}</b>\n\n{snippet.get('description', '')[:800]}",
                    "media_url": thumb_url,
                    "is_video": True,
                    "title": title,
                    "published_at": snippet.get('publishedAt'),
                    "channel_title": snippet.get('channelTitle')
                })
                fetched += 1
                if fetched >= max_results:
                    break

            next_page_token = playlist_response.get('nextPageToken')
            if not next_page_token:
                break

        videos.sort(key=lambda x: x.get('published_at') or "", reverse=True)
        logging.info(f"Fetched {len(videos)} latest YouTube videos from @{handle}")

    except Exception as e:
        logging.exception(f"YouTube fetch error for @{channel_handle}: {e}")

    return videos