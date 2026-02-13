import logging
import time
import re
from typing import List, Optional, Dict, Any

import requests

from Utils.config import *
from Utils.persistence import *

def _normalize_account_input(account: str) -> str:
    if not account:
        return ""
    a = account.strip()
    if a.startswith("@"):
        a = a[1:]
    if a.startswith("http://") or a.startswith("https://"):
        parts = [p for p in a.split("/") if p]
        if len(parts) >= 3:
            candidate = parts[2]
            if candidate:
                a = candidate
    return a

def _safe_get_tweet_id(tweet: dict) -> Optional[str]:
    tid = tweet.get("id_str") or tweet.get("id")
    if tid is None:
        return None
    return str(tid)

def _extract_tweets_from_response(data: dict) -> List[dict]:
    if isinstance(data, list):
        return data
    for key in ("data", "statuses", "results"):
        val = data.get(key)
        if isinstance(val, list):
            return val
    return []

def fetch_x_urls(account: str, limit: int = 10, max_retries: int = 3) -> List[str]:
    account_raw = account
    account_clean = _normalize_account_input(account)

    if not account_clean:
        logging.warning("fetch_x_urls called with empty account argument")
        return []

    if account_clean.isdigit():
        user_id = account_clean
        logging.debug("Using numeric user_id passed directly: %s", user_id)
    else:
        match = re.search(r'\d{10,}', account_clean)
        if match:
            user_id = match.group(0)
            logging.debug("Extracted numeric user_id from input: %s", user_id)
        else:
            logging.warning(
                "Invalid input: no numeric user_id found in '%s' (normalized '%s'). "
                "You must now pass the numeric user_id directly.",
                account_raw, account_clean
            )
            return []

    if not config.RAPIDAPI_KEY:
        logging.warning("RAPIDAPI_KEY not set – skipping X fetch for %s", account_raw)
        return []

    headers = {
        "x-rapidapi-key": config.RAPIDAPI_KEY,
        "x-rapidapi-host": config.RAPIDAPIHOST or "",
        "Accept": "application/json",
    }

    params = {"user_id": user_id, "count": max(limit, 1) + 2}
    urls: List[str] = []
    attempt = 0

    while attempt <= max_retries:
        try:
            attempt += 1
            resp = requests.get(config.TWEETS_URL, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            tweets = data.get("data", []) or data.get("statuses", []) or data.get("results", []) or []
            if not tweets:
                logging.info("No recent tweets found for %s (user_id=%s).", account_raw, user_id)
                return []
            for tweet in tweets:
                tid = _safe_get_tweet_id(tweet)
                if not tid:
                    continue
                display_account = user_id
                TWITTER_FIXER_DOMAIN = "fixupx.com"
                urls.append(f"https://{TWITTER_FIXER_DOMAIN}/{display_account}/status/{tid}")
                try:
                    persistence.save_url("x", display_account, urls[-1])
                except Exception:
                    logging.debug("save_url failed for %s", urls[-1], exc_info=True)
                if len(urls) >= limit:
                    break
            logging.info("Fetched %d posts for %s (user_id=%s, attempt=%d).", len(urls), account_raw, user_id, attempt)
            return urls[:limit]
        except requests.exceptions.HTTPError as http_err:
            status = http_err.response.status_code if http_err.response else "unknown"
            body = http_err.response.text[:500] if http_err.response else str(http_err)
            logging.warning("RapidAPI HTTP error %s for %s (user_id %s): %s", status, account_raw, user_id, body)
            if isinstance(status, int) and 400 <= status < 500 and status != 429:
                break
        except requests.exceptions.RequestException as e:
            logging.warning("RapidAPI request failed for %s (attempt %d): %s", account_raw, attempt, e)
        except ValueError as e:
            logging.warning("Invalid JSON response for %s: %s", account_raw, e)
            break
        except Exception as e:
            logging.warning("Unexpected error for %s: %s", account_raw, e, exc_info=True)
            break

        if attempt <= max_retries:
            time.sleep(2 ** attempt)

    logging.info("Giving up fetch for %s after %d attempts.", account_raw, attempt)
    return urls[:limit]