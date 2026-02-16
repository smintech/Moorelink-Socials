import asyncio
import json
import random
import time
import re
from typing import Dict, Optional, Any, Tuple, Callable, List
from playwright.async_api import async_playwright, Page
import logging
from urllib.parse import quote
import os
from Utils import config
from Utils import persistence  # Keep if needed, otherwise can remove if not used

class DetailedLogger:
    def __init__(self, name: str = "Instagram Scraper"):
        self.name = name
        self.step_count = 0
        logging.basicConfig(
            level=logging.DEBUG,
            format='[%(asctime)s.%(msecs)03d] %(levelname)-8s | %(funcName)-20s | %(message)s',
            datefmt='%H:%M:%S'
        )
        self.logger = logging.getLogger(name)
    
    def step(self, title: str, details: str = ""):
        self.step_count += 1
        self.logger.info("=" * 70)
        self.logger.info(f"📍 STEP {self.step_count}: {title}")
        if details:
            self.logger.info(f"   {details}")
        self.logger.info("=" * 70)
    
    def info(self, message: str, indent: int = 1):
        prefix = "   " * indent
        self.logger.info(f"{prefix}ℹ️ {message}")
    
    def success(self, message: str, indent: int = 1):
        prefix = "   " * indent
        self.logger.info(f"{prefix}✅ {message}")
    
    def warning(self, message: str, indent: int = 1):
        prefix = "   " * indent
        self.logger.warning(f"{prefix}⚠️ {message}")
    
    def error(self, message: str, indent: int = 1):
        prefix = "   " * indent
        self.logger.error(f"{prefix}❌ {message}")
    
    def debug(self, message: str, indent: int = 1):
        prefix = "   " * indent
        self.logger.debug(f"{prefix}🐛 {message}")

logger = DetailedLogger("Instagram Scraper")

async def _log_current_url(page: Page, context_msg: str = ""):
    """Log current URL with context"""
    current_url = page.url
    logger.info(f"📍 URL {context_msg}: {current_url}", indent=1)
    return current_url

class InstagramScraper:
    def __init__(self, cookies: List[Dict], logger: DetailedLogger):
        self.cookies = cookies
        self.logger = logger
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        self.intercepted_videos = {}
        self.csrf_token = None
        self.ig_app_id = "936619743392459"  # Instagram Web App ID
        self.post_doc_id = "8845758582119845"  # GraphQL doc_id for posts (updated regularly)
        
        # Extract csrf from cookies
        for cookie in cookies:
            if cookie['name'] == 'csrftoken':
                self.csrf_token = cookie['value']
                break
    
    def get_random_delay(self, min_sec=1.0, max_sec=3.0) -> float:
        delay = random.uniform(min_sec, max_sec)
        self.logger.debug(f"Random delay: {delay:.2f}s")
        return delay
    
    async def dismiss_popups(self, page: Page):
        """Click 'Not now' and 'Save info' buttons"""
        self.logger.info("🛑 Handling popups...")
        try:
            selectors = [
                'button:has-text("Not now")',
                'button:has-text("Save info")',
                'button:has-text("Allow all cookies")',
                'button:has-text("Decline optional cookies")',
            ]
            popup_handled = False
            for selector in selectors:
                try:
                    elem = await page.locator(selector).first
                    if elem and await elem.is_visible(timeout=2000):
                        await elem.click()
                        self.logger.success(f"Dismissed popup: {selector}", indent=2)
                        popup_handled = True
                        await asyncio.sleep(0.5)
                except Exception as e:
                    self.logger.debug(f"Popup selector {selector} failed: {str(e)[:50]}", indent=2)
            if not popup_handled:
                self.logger.info("No popups found", indent=2)
        except Exception as e:
            self.logger.debug(f"Popup handling error: {str(e)[:50]}")
    
    async def intercept_video_urls(self, page: Page):
        """Intercept actual video URLs from network requests"""
        async def handle_route(route):
            url = route.request.url
            if any(x in url for x in ['.mp4', '.m3u8', 'video', 'media']):
                if not url.startswith('blob'):
                    self.intercepted_videos[len(self.intercepted_videos)] = url
                    self.logger.debug(f"Intercepted media URL: {url[:60]}...", indent=2)
            try:
                await route.continue_()
            except:
                pass
        
        await page.route("**/*", handle_route)
        self.logger.success("Video interception enabled")
    
    async def get_headers(self) -> Dict[str, str]:
        """Get headers for GraphQL requests"""
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://www.instagram.com',
            'referer': 'https://www.instagram.com/',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': self.user_agents[0],
            'x-csrftoken': self.csrf_token or '',
            'x-ig-app-id': self.ig_app_id,
            'x-ig-www-claim': '0',
            'x-requested-with': 'XMLHttpRequest',
        }
        self.logger.debug("Prepared GraphQL headers")
        return headers
    
    async def extract_caption_graphql(self, page: Page, shortcode: str) -> Optional[str]:
        """
        STRATEGY 1: Use Instagram's GraphQL API (Most Reliable)
        """
        try:
            self.logger.info("Trying GraphQL API strategy...", indent=2)
            
            headers = await self.get_headers()
            
            variables = quote(json.dumps({
                'shortcode': shortcode,
                'fetch_tagged_user_count': None,
                'hoisted_comment_id': None,
                'hoisted_reply_id': None
            }, separators=(',', ':')))
            
            body = f"variables={variables}&doc_id={self.post_doc_id}"
            self.logger.debug(f"GraphQL body: {body[:100]}...", indent=3)
            
            # Make the request using page.evaluate to bypass CORS
            script = f"""
                async () => {{
                    try {{
                        const response = await fetch('https://www.instagram.com/graphql/query', {{
                            method: 'POST',
                            headers: {json.dumps(headers)},
                            body: '{body}',
                            credentials: 'include'
                        }});
                        const data = await response.json();
                        return data;
                    }} catch (e) {{
                        return {{error: e.message}};
                    }}
                }}
            """
            
            result = await page.evaluate(script)
            self.logger.debug(f"GraphQL response received: {len(str(result))} chars", indent=3)
            
            if result and 'data' in result and 'xdt_shortcode_media' in result['data']:
                media = result['data']['xdt_shortcode_media']
                
                # Try edge_media_to_caption first
                caption_edges = media.get('edge_media_to_caption', {}).get('edges', [])
                if caption_edges and len(caption_edges) > 0:
                    caption_text = caption_edges[0]['node'].get('text', '')
                    if caption_text:
                        self.logger.success(f"GraphQL caption found: {len(caption_text)} chars", indent=3)
                        return caption_text
                
                # Fallback to accessibility_caption
                alt_text = media.get('accessibility_caption', '')
                if alt_text:
                    self.logger.success(f"GraphQL alt text found: {len(alt_text)} chars", indent=3)
                    return alt_text
            
            self.logger.warning("GraphQL returned no caption", indent=3)
            return None
            
        except Exception as e:
            self.logger.debug(f"GraphQL error: {str(e)[:60]}", indent=3)
            return None
    
    async def extract_caption_from_dom(self, page: Page) -> Optional[str]:
        """
        STRATEGY 2: Multiple DOM-based extraction methods
        """
        self.logger.info("Trying DOM extraction strategies...", indent=2)
        
        # Comprehensive DOM extraction script with multiple strategies
        script = r"""
            () => {
                const strategies = [];
                
                // Strategy A: Look for _aacl _a9zr _a9zo _a9z9 (Instagram's caption classes 2024-2025)
                let captionDiv = document.querySelector('div._aacl._a9zr._a9zo._a9z9, div._aacl._a9zr');
                if (captionDiv) {
                    const text = captionDiv.innerText?.trim();
                    if (text && text.length > 5) {
                        strategies.push({text: text, source: '_aacl caption div'});
                    }
                }
                
                // Strategy B: Look for h1 with specific classes or any h1 with substantial text
                const h1Elements = document.querySelectorAll('h1');
                for (let h1 of h1Elements) {
                    const text = h1.innerText?.trim();
                    // Filter out short navigation text, keep substantial captions
                    if (text && text.length > 20 && text.length < 5000) {
                        // Check if it's not just a username or nav element
                        const parent = h1.closest('article, div[role="dialog"], main');
                        if (parent) {
                            strategies.push({text: text, source: 'h1 in content area'});
                            break;
                        }
                    }
                }
                
                // Strategy C: Look for spans with class containing text or caption indicators
                const spans = document.querySelectorAll('span[class*="caption"], span._aacl, div[role="dialog"] span');
                for (let span of spans) {
                    const text = span.innerText?.trim();
                    if (text && text.length > 30 && text.length < 5000) {
                        strategies.push({text: text, source: 'span caption class'});
                        break;
                    }
                }
                
                // Strategy D: Look for the first substantial text block in article or dialog
                const containers = [
                    document.querySelector('article'),
                    document.querySelector('div[role="dialog"]'),
                    document.querySelector('main section')
                ];
                
                for (let container of containers) {
                    if (container) {
                        // Get all text nodes, filter by length
                        const walker = document.createTreeWalker(
                            container,
                            NodeFilter.SHOW_TEXT,
                            null,
                            false
                        );
                        
                        let node;
                        const texts = [];
                        while (node = walker.nextNode()) {
                            const text = node.textContent?.trim();
                            if (text && text.length > 40 && text.length < 2000 && !text.includes('http')) {
                                texts.push(text);
                            }
                        }
                        
                        if (texts.length > 0) {
                            // Return the longest one that's likely a caption
                            const longest = texts.reduce((a, b) => a.length > b.length ? a : b);
                            strategies.push({text: longest, source: 'treeWalker text nodes'});
                            break;
                        }
                    }
                }
                
                // Strategy E: Meta description fallback
                const metaDesc = document.querySelector('meta[name="description"], meta[property="og:description"]');
                if (metaDesc) {
                    const content = metaDesc.getAttribute('content');
                    if (content && content.length > 20) {
                        // Clean up meta description format
                        const cleaned = content.replace(/^[^,]+,\s*/, '').trim();
                        if (cleaned.length > 20) {
                            strategies.push({text: cleaned, source: 'meta description'});
                        }
                    }
                }
                
                // Strategy F: JSON-LD structured data
                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                for (let script of scripts) {
                    try {
                        const data = JSON.parse(script.innerText);
                        if (data.caption || data.description) {
                            const text = data.caption || data.description;
                            if (text.length > 10) {
                                strategies.push({text: text, source: 'JSON-LD'});
                            }
                        }
                    } catch (e) {}
                }
                
                // Return the best candidate (prefer longer, more detailed captions)
                if (strategies.length > 0) {
                    // Sort by length descending, prefer sources that aren't meta/JSON first
                    const priorityOrder = ['_aacl', 'h1', 'span', 'treeWalker', 'JSON-LD', 'meta'];
                    strategies.sort((a, b) => {
                        const aPriority = priorityOrder.findIndex(p => a.source.includes(p));
                        const bPriority = priorityOrder.findIndex(p => b.source.includes(p));
                        if (aPriority !== bPriority) return aPriority - bPriority;
                        return b.text.length - a.text.length;
                    });
                    
                    return strategies[0];
                }
                
                return null;
            }
        """
        
        try:
            self.logger.debug("Evaluating DOM extraction script", indent=3)
            result = await page.evaluate(script)
            if result and result['text']:
                # Clean up the caption
                caption = result['text']
                # Remove "more" text if present
                caption = re.sub(r'\s*more\s*$', '', caption, flags=re.IGNORECASE).strip()
                self.logger.success(f"DOM caption found via {result['source']}: {len(caption)} chars", indent=3)
                return caption
        except Exception as e:
            self.logger.debug(f"DOM extraction error: {str(e)[:60]}", indent=3)
        
        return None
    
    async def extract_caption_from_shared_data(self, page: Page) -> Optional[str]:
        """
        STRATEGY 3: Extract from window._sharedData or window.require
        """
        try:
            self.logger.info("Trying sharedData extraction...", indent=2)
            
            script = r"""
                () => {
                    // Try _sharedData
                    if (window._sharedData && window._sharedData.entry_data) {
                        const postPage = window._sharedData.entry_data.PostPage;
                        if (postPage && postPage[0] && postPage[0].graphql) {
                            const media = postPage[0].graphql.shortcode_media;
                            if (media && media.edge_media_to_caption && media.edge_media_to_caption.edges.length > 0) {
                                return {
                                    text: media.edge_media_to_caption.edges[0].node.text,
                                    source: '_sharedData'
                                };
                            }
                        }
                    }
                    
                    // Try __additionalDataLoaded
                    if (window.__additionalDataLoaded) {
                        for (let key in window.__additionalDataLoaded) {
                            if (key.includes('/p/') || key.includes('/reel/')) {
                                const data = window.__additionalDataLoaded[key];
                                if (data && data.graphql && data.graphql.shortcode_media) {
                                    const media = data.graphql.shortcode_media;
                                    if (media.edge_media_to_caption && media.edge_media_to_caption.edges.length > 0) {
                                        return {
                                            text: media.edge_media_to_caption.edges[0].node.text,
                                            source: '__additionalDataLoaded'
                                        };
                                    }
                                }
                            }
                        }
                    }
                    
                    return null;
                }
            """
            
            self.logger.debug("Evaluating sharedData script", indent=3)
            result = await page.evaluate(script)
            if result and result['text']:
                self.logger.success(f"SharedData caption found: {len(result['text'])} chars", indent=3)
                return result['text']
                
        except Exception as e:
            self.logger.debug(f"SharedData error: {str(e)[:60]}", indent=3)
        
        return None
    
    async def extract_caption_from_post(self, page: Page, shortcode: str = "") -> str:
        """
        MASTER CAPTION EXTRACTION: Tries all strategies in order of reliability
        """
        caption = ""
        
        # Wait for content to settle
        await asyncio.sleep(2)
        
        # Strategy 1: GraphQL API (Most reliable, avoids DOM changes)
        if shortcode:
            caption = await self.extract_caption_graphql(page, shortcode) or ""
        
        # Strategy 2: DOM extraction
        if not caption:
            caption = await self.extract_caption_from_dom(page) or ""
        
        # Strategy 3: SharedData
        if not caption:
            caption = await self.extract_caption_from_shared_data(page) or ""
        
        # Strategy 4: Final fallback - raw text extraction
        if not caption:
            self.logger.info("Trying raw text fallback...", indent=2)
            try:
                raw_text = await page.evaluate('() => document.body.innerText')
                self.logger.debug(f"Raw text length: {len(raw_text)}", indent=3)
                lines = [l.strip() for l in raw_text.split('\n') if 30 < len(l.strip()) < 1000]
                if lines:
                    # Filter out common non-caption lines
                    filtered = [l for l in lines if not any(x in l.lower() for x in ['followers', 'following', 'posts', 'meta', 'privacy', 'terms'])]
                    if filtered:
                        caption = filtered[0]
                        self.logger.success(f"Raw text fallback: {len(caption)} chars", indent=3)
            except Exception as e:
                self.logger.debug(f"Raw text error: {str(e)[:60]}", indent=3)
        
        if not caption:
            self.logger.warning("All caption extraction strategies failed", indent=2)
        
        return caption
    
    async def extract_media_from_post(self, page: Page, post_url: str) -> Tuple[str, bool]:
        """Extract media URL (image or video)"""
        is_video = '/reel/' in post_url or '/tv/' in post_url
        await asyncio.sleep(1)
        
        if is_video:
            self.logger.debug("Extracting video...", indent=2)
            
            # Check intercepted videos first
            if self.intercepted_videos:
                for video_url in self.intercepted_videos.values():
                    if video_url and not video_url.startswith('blob') and '.mp4' in video_url:
                        self.logger.success(f"Intercepted video URL: {video_url[:60]}...", indent=3)
                        self.intercepted_videos.clear()
                        return video_url, True
            
            # Try video element
            script = r"""
                () => {
                    // Look for video with src
                    let video = document.querySelector('video[src]');
                    if (video?.src && !video.src.startsWith('blob')) return video.src;
                    
                    // Look for source inside video
                    let source = document.querySelector('video source[src]');
                    if (source?.src && !source.src.startsWith('blob')) return source.src;
                    
                    // Look for data URLs
                    video = document.querySelector('video[data-video-url]');
                    if (video) return video.getAttribute('data-video-url');
                    
                    return '';
                }
            """
            
            try:
                self.logger.debug("Evaluating video extraction script", indent=3)
                media_url = await page.evaluate(script)
                if media_url and not media_url.startswith('blob'):
                    self.logger.success(f"Video extracted from element: {media_url[:60]}...", indent=3)
                    return media_url, True
            except Exception as e:
                self.logger.debug(f"Video extraction error: {str(e)[:40]}", indent=3)
        
        else:
            self.logger.debug("Extracting image...", indent=2)
            
            script = r"""
                () => {
                    // Priority: meta tag image
                    const metaImg = document.querySelector('meta[property="og:image"]');
                    if (metaImg) {
                        const content = metaImg.getAttribute('content');
                        if (content && content.includes('cdninstagram')) return content;
                    }
                    
                    // Look for high-res images
                    const imgs = document.querySelectorAll('img[src]');
                    const candidates = [];
                    
                    for (const img of imgs) {
                        const src = img.src;
                        if ((src.includes('cdninstagram') || src.includes('fbcdn')) && 
                            img.naturalWidth > 400) {
                            candidates.push({
                                src: src,
                                width: img.naturalWidth
                            });
                        }
                    }
                    
                    // Sort by size, return largest
                    if (candidates.length > 0) {
                        candidates.sort((a, b) => b.width - a.width);
                        return candidates[0].src;
                    }
                    
                    return '';
                }
            """
            
            try:
                self.logger.debug("Evaluating image extraction script", indent=3)
                media_url = await page.evaluate(script)
                if media_url and not media_url.startswith('blob'):
                    self.logger.success(f"Image extracted: {media_url[:60]}...", indent=3)
                    return media_url, False
            except Exception as e:
                self.logger.debug(f"Image extraction error: {str(e)[:40]}", indent=3)
        
        self.logger.warning("No media URL found", indent=2)
        return "", is_video
    
    async def scrape_profile(self, username: str, post_limit: int = 10) -> List[Dict]:
        self.logger.step("Start Scraping", f"Target: @{username} | Limit: {post_limit}")
        
        async with async_playwright() as p:
            self.logger.info("Launching browser...", indent=1)
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--no-sandbox',
                    '--window-size=1920,1080'
                ]
            )
            self.logger.success("Browser launched", indent=1)
            
            self.logger.info("Creating context...", indent=1)
            context = await browser.new_context(
                user_agent=random.choice(self.user_agents),
                viewport={'width': 1920, 'height': 1080}
            )
            self.logger.success(f"Context created", indent=1)
            
            await context.add_cookies(self.cookies)
            self.logger.success(f"Added {len(self.cookies)} cookies", indent=1)
            page = await context.new_page()
            page.set_default_navigation_timeout(60000)
            page.set_default_timeout(30000)
            
            await self.intercept_video_urls(page)
            
            try:
                profile_url = f"https://www.instagram.com/{username}/"
                self.logger.step("Load Profile", profile_url)
                
                self.logger.info(f"Navigating to {profile_url}", indent=1)
                await page.goto(profile_url, wait_until='domcontentloaded', timeout=60000)
                await _log_current_url(page, "[PROFILE_LOADED]")
                await asyncio.sleep(3)
                
                if 'accounts/login' in page.url:
                    self.logger.error("Redirected to login - cookies expired")
                    return []
                
                await self.dismiss_popups(page)
                
                try:
                    await page.wait_for_load_state('networkidle', timeout=20000)
                    self.logger.success("Page loaded to networkidle", indent=1)
                except:
                    self.logger.warning("Network timeout, continuing", indent=1)
                
                # Scroll to load posts
                self.logger.info("Scrolling to load posts...", indent=1)
                last_height = 0
                no_change_count = 0
                
                for i in range(30):  # Increased for 10+ posts optimization
                    links = await page.locator('a[href*="/p/"], a[href*="/reel/"]').all()
                    self.logger.debug(f"Found {len(links)} post links after scroll {i+1}", indent=2)
                    if len(links) >= post_limit:
                        self.logger.success(f"Found sufficient posts: {len(links)}", indent=2)
                        break
                    
                    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    await asyncio.sleep(self.get_random_delay(2, 3))
                    
                    new_height = await page.evaluate('document.body.scrollHeight')
                    self.logger.debug(f"New scroll height: {new_height}", indent=2)
                    if new_height == last_height:
                        no_change_count += 1
                        if no_change_count >= 3:
                            self.logger.warning("No more posts loading, breaking scroll loop", indent=2)
                            break
                    else:
                        no_change_count = 0
                        last_height = new_height
                
                # Extract posts
                self.logger.step("Extracting Posts", f"Processing up to {post_limit} posts")
                
                posts = []
                processed_urls = set()
                
                script = r"""
                    () => {
                        const links = Array.from(document.querySelectorAll('a[href]'));
                        return links
                            .filter(a => a.href.includes('/p/') || a.href.includes('/reel/') || a.href.includes('/tv/'))
                            .map(a => a.href.split('?')[0])
                            .filter((url, idx, arr) => arr.indexOf(url) === idx);
                    }
                """
                
                self.logger.debug("Evaluating post URL extraction script", indent=1)
                post_urls = await page.evaluate(script)
                self.logger.info(f"Found {len(post_urls)} unique post URLs", indent=1)
                
                for idx, post_url in enumerate(post_urls[:post_limit]):
                    if idx >= post_limit:
                        break
                    
                    if post_url in processed_urls:
                        self.logger.debug(f"Skipping duplicate URL: {post_url}", indent=2)
                        continue
                    
                    processed_urls.add(post_url)
                    self.logger.info(f"\n[{idx+1}/{post_limit}] Processing: {post_url.split('/')[-2]}", indent=1)
                    
                    # Extract shortcode for GraphQL
                    shortcode = ""
                    try:
                        shortcode = post_url.split('/p/')[-1].split('/')[0]
                        if not shortcode:
                            shortcode = post_url.split('/reel/')[-1].split('/')[0]
                        self.logger.debug(f"Extracted shortcode: {shortcode}", indent=2)
                    except:
                        self.logger.warning("Failed to extract shortcode", indent=2)
                        pass
                    
                    try:
                        self.intercepted_videos.clear()
                        self.logger.debug("Cleared intercepted videos", indent=2)
                        
                        self.logger.info(f"Navigating to post: {post_url}", indent=2)
                        await page.goto(post_url, wait_until='domcontentloaded', timeout=30000)
                        await _log_current_url(page, "[POST_LOADED]")
                        await asyncio.sleep(2)
                        
                        try:
                            await page.wait_for_load_state('networkidle', timeout=20000)
                            self.logger.success("Post loaded to networkidle", indent=2)
                        except:
                            self.logger.warning("Post network timeout, continuing", indent=2)
                        
                        # Extract caption using all strategies
                        self.logger.debug("Starting caption extraction", indent=2)
                        caption = await self.extract_caption_from_post(page, shortcode)
                        media_url, is_video = await self.extract_media_from_post(page, post_url)
                        
                        if media_url and not media_url.startswith('blob'):
                            posts.append({
                                "url": post_url,
                                "caption": caption,
                                "media_url": media_url,
                                "is_video": is_video,
                            })
                            self.logger.success(f"Saved ({'VIDEO 🎥' if is_video else 'IMAGE 📸'}, caption: {len(caption)} chars)", indent=2)
                        else:
                            self.logger.warning(f"Invalid media URL, skipping", indent=2)
                        
                        self.logger.info("Going back to profile", indent=2)
                        await page.go_back(wait_until='domcontentloaded', timeout=20000)
                        await _log_current_url(page, "[BACK_TO_PROFILE]")
                        await asyncio.sleep(self.get_random_delay(1, 2))
                        
                    except Exception as e:
                        self.logger.error(f"Post error: {str(e)[:60]}", indent=2)
                        try:
                            self.logger.info("Recovering by navigating back to profile", indent=2)
                            await page.goto(profile_url, wait_until='domcontentloaded', timeout=30000)
                            await _log_current_url(page, "[RECOVERY]")
                        except:
                            self.logger.error("Recovery failed", indent=2)
                            pass
                
                self.logger.success(f"Scraping complete: {len(posts)} posts", indent=1)
                return posts
                
            except Exception as e:
                self.logger.error(f"Fatal error: {e}", indent=1)
                import traceback
                self.logger.debug(traceback.format_exc(), indent=2)
                return []
            
            finally:
                self.logger.info("Closing browser", indent=1)
                await browser.close()
                self.logger.success("Browser closed", indent=1)

async def fetch_ig_urls(account: str, cookies: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Fetch Instagram post URLs, captions, and media URLs using Playwright.
    Args:
        account: Instagram username (with or without @)
        cookies: Optional list of cookies for logged-in session

    Returns:
        List of dicts with url, caption, media_url, is_video
    """
    account = account.lstrip('@')

    logger.step("Configuration", "Setting up scraper")
    
    if cookies is None:
        cookies_str = os.getenv('IG_COOKIES')
        if not cookies_str:
            logger.error("No IG_COOKIES environment variable set - stopping")
            return []
        try:
            cookies = json.loads(cookies_str)
            logger.success(f"Loaded {len(cookies)} cookies from environment")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid IG_COOKIES JSON: {str(e)}")
            return []

    scraper = InstagramScraper(cookies=cookies, logger=logger)
    posts = await scraper.scrape_profile(username=account, post_limit=config.POST_LIMIT)
    
    return posts