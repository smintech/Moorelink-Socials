import logging
import requests
from Utils import config
from Utils import persistence
import random
import time
from typing import Dict, Optional, Any, Tuple, Callable, List
from itertools import islice
import asyncio
import json
import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ═══════════════════════════════════════════════════════════════════════════
# ENHANCED LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════

def _setup_logging():
    """Setup detailed logging with timestamps and context"""
    logger = logging.getLogger(__name__)
    
    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    
    return logger

logger = _setup_logging()

# ═══════════════════════════════════════════════════════════════════════════
# INSTAGRAM SELECTORS (Updated for 2024)
# ═══════════════════════════════════════════════════════════════════════════

INSTAGRAM_SELECTORS = {
    # Posts
    "post_links": [
        'a[href^="/p/"]',  # Primary selector
        'article a[href^="/p/"]',  # Within article
        'div[role="feed"] a[href^="/p/"]',  # Within feed
    ],
    
    # Captions/Text
    "caption": [
        'h2 + div span',  # Caption near username
        'div[data-testid="post-comment-text"]',  # Comment area caption
        'span:has-text("liked by")',  # Alternative
        'article div[role="button"] span',  # Carousel
    ],
    
    # Media
    "image": [
        'article img[alt]:not([alt=""])',  # Main image
        'div[role="img"] img',  # Div-based image
        'img[alt*="post"]',  # Alt contains post
        'img[loading="lazy"]',  # Lazy loaded
    ],
    
    "video": [
        'article video',  # Video element
        'video[src]',  # With src
    ],
    
    # User Info
    "username": [
        'a[title]:not([href*="/"])',  # Username link
        'span[data-testid="profile-username"]',  # Profile username
        'h1',  # Heading
    ],
    
    "follower_count": [
        'button span span',  # Follower info
        'span:has-text("followers")',  # Follower text
    ],
    
    # Popups/Modals
    "cookie_accept": [
        'button:has-text("Allow all")',
        'button:has-text("Accept")',
        'button[aria-label*="Allow"]',
    ],
    
    "login_popup": [
        'div[role="dialog"] button:has-text("Not Now")',
        'div[role="dialog"]:has-text("Log in")',
    ],
    
    "notification_popup": [
        'button:has-text("Not Now")',
        'button:has-text("Turn on Notifications")',
    ],
    
    # Private account
    "private_notice": [
        'h2:has-text("This account is private")',
        'span:has-text("This account is private")',
    ],
    
    # Challenge
    "challenge_prompt": [
        'text="It looks like you were misusing"',
        'text="Confirm it\'s you"',
        'button:has-text("This was me")',
    ],
}

# ═══════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS WITH ENHANCED LOGGING
# ═══════════════════════════════════════════════════════════════════════════

def _random_delay(min_sec: float = 0.5, max_sec: float = 2.0) -> float:
    """Generate random delay with jitter"""
    delay = random.uniform(min_sec, max_sec)
    logger.debug(f"Random delay: {delay:.2f}s")
    return delay

def _get_enhanced_stealth_script() -> str:
    """Generate comprehensive anti-detection JavaScript"""
    logger.info("Loading enhanced stealth script")
    
    return """
    // ============================================
    // COMPREHENSIVE STEALTH SCRIPT v2024
    // ============================================
    
    // 1. WebDriver Detection
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true
    });
    
    // 2. Chrome Detection
    window.chrome = {
        runtime: {},
        loadTimes: function() {},
        csi: function() {},
        app: {}
    };
    
    // 3. Permissions API
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
    );
    
    // 4. Plugins Array (fake plugins)
    Object.defineProperty(navigator, 'plugins', {
        get: () => [
            {
                0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
                description: "Portable Document Format",
                filename: "internal-pdf-viewer",
                length: 1,
                name: "Chrome PDF Plugin"
            },
            {
                0: {type: "application/pdf", suffixes: "pdf", description: "Portable Document Format"},
                description: "Portable Document Format", 
                filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
                length: 1,
                name: "Chrome PDF Viewer"
            },
            {
                0: {type: "application/x-nacl", suffixes: "", description: "Native Client Executable"},
                1: {type: "application/x-pnacl", suffixes: "", description: "Portable Native Client Executable"},
                description: "Native Client",
                filename: "internal-nacl-plugin",
                length: 2,
                name: "Native Client"
            }
        ]
    });
    
    // 5. Languages
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
        configurable: true
    });
    
    // 6. Platform
    Object.defineProperty(navigator, 'platform', {
        get: () => 'Win32',
        configurable: true
    });
    
    // 7. Hardware Concurrency (CPU cores)
    Object.defineProperty(navigator, 'hardwareConcurrency', {
        get: () => 8,
        configurable: true
    });
    
    // 8. Device Memory
    Object.defineProperty(navigator, 'deviceMemory', {
        get: () => 8,
        configurable: true
    });
    
    // 9. User Agent
    Object.defineProperty(navigator, 'userAgent', {
        get: () => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        configurable: true
    });
    
    // 10. Vendor
    Object.defineProperty(navigator, 'vendor', {
        get: () => 'Google Inc.',
        configurable: true
    });
    
    // 11. WebGL Vendor/Renderer
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
        if (parameter === 37445) {
            return 'Intel Inc.';
        }
        if (parameter === 37446) {
            return 'Intel Iris OpenGL Engine';
        }
        return getParameter.apply(this, arguments);
    };
    
    // 12. Battery API
    if (navigator.getBattery) {
        const originalGetBattery = navigator.getBattery;
        navigator.getBattery = function() {
            return originalGetBattery.apply(this, arguments).then(battery => {
                Object.defineProperty(battery, 'charging', { value: true });
                Object.defineProperty(battery, 'chargingTime', { value: 0 });
                Object.defineProperty(battery, 'dischargingTime', { value: Infinity });
                Object.defineProperty(battery, 'level', { value: 1.0 });
                return battery;
            });
        };
    }
    
    // 13. Connection API
    if (navigator.connection) {
        Object.defineProperty(navigator.connection, 'rtt', { value: 100 });
        Object.defineProperty(navigator.connection, 'downlink', { value: 10 });
        Object.defineProperty(navigator.connection, 'effectiveType', { value: '4g' });
    }
    
    // 14. Screen properties
    Object.defineProperty(screen, 'availWidth', { value: 1920 });
    Object.defineProperty(screen, 'availHeight', { value: 1040 });
    Object.defineProperty(screen, 'width', { value: 1920 });
    Object.defineProperty(screen, 'height', { value: 1080 });
    Object.defineProperty(screen, 'colorDepth', { value: 24 });
    Object.defineProperty(screen, 'pixelDepth', { value: 24 });
    
    // 15. Date object (timezone consistency)
    Date.prototype.getTimezoneOffset = function() {
        return 300; // EST timezone
    };
    
    // 16. Canvas Fingerprinting Protection
    const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function(type) {
        if (type === 'image/png' && this.width === 16 && this.height === 16) {
            // Likely a fingerprinting attempt
            return originalToDataURL.apply(this, arguments);
        }
        return originalToDataURL.apply(this, arguments);
    };
    
    // 17. Media Devices
    if (navigator.mediaDevices && navigator.mediaDevices.enumerateDevices) {
        const originalEnumerateDevices = navigator.mediaDevices.enumerateDevices;
        navigator.mediaDevices.enumerateDevices = function() {
            return originalEnumerateDevices.apply(this, arguments).then(devices => {
                return devices.map(device => ({
                    deviceId: device.deviceId,
                    kind: device.kind,
                    label: device.label,
                    groupId: device.groupId,
                    toJSON: function() { return this; }
                }));
            });
        };
    }
    
    // 18. Remove automation indicators
    delete navigator.__proto__.webdriver;
    
    // 19. Iframe detection evasion
    Object.defineProperty(window, 'outerWidth', { value: window.innerWidth });
    Object.defineProperty(window, 'outerHeight', { value: window.innerHeight + 85 });
    
    console.log('🛡️ Stealth mode activated');
    """

def _get_random_viewport() -> Dict[str, int]:
    """Get randomized viewport size"""
    viewports = [
        {'width': 1920, 'height': 1080},
        {'width': 1366, 'height': 768},
        {'width': 1536, 'height': 864},
        {'width': 1440, 'height': 900},
        {'width': 1600, 'height': 900},
    ]
    selected = random.choice(viewports)
    logger.info(f"Selected viewport: {selected['width']}x{selected['height']}")
    return selected

def _get_realistic_user_agent() -> str:
    """Get a realistic, recent user agent"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
    ]
    selected = random.choice(user_agents)
    logger.info(f"Selected user agent: {selected[:60]}...")
    return selected

# ═══════════════════════════════════════════════════════════════════════════
# SELECTOR MATCHING WITH FALLBACK
# ═══════════════════════════════════════════════════════════════════════════

async def _find_element(page, selector_list: List[str], timeout: int = 3000, description: str = "element"):
    """Try multiple selectors with fallback"""
    logger.debug(f"Looking for {description} with {len(selector_list)} selector(s)")
    
    for idx, selector in enumerate(selector_list, 1):
        try:
            logger.debug(f"  [{idx}/{len(selector_list)}] Trying: {selector[:60]}")
            element = page.locator(selector)
            
            # Check visibility with short timeout
            if await element.is_visible(timeout=timeout):
                logger.debug(f"  ✓ Found {description} with selector #{idx}")
                return element
        except Exception as e:
            logger.debug(f"  ✗ Selector #{idx} failed: {str(e)[:50]}")
            continue
    
    logger.warning(f"⚠️  Could not find {description} with any selector")
    return None

async def _random_scroll(page):
    """Perform random scrolling to mimic human behavior"""
    try:
        scroll_amount = random.randint(100, 500)
        logger.debug(f"Scrolling by {scroll_amount}px")
        await page.evaluate(f'window.scrollBy(0, {scroll_amount})')
        await asyncio.sleep(_random_delay(0.3, 0.8))
        await page.evaluate(f'window.scrollBy(0, -{scroll_amount // 2})')
    except Exception as e:
        logger.debug(f"Scroll failed: {e}")

async def _human_like_type(page, selector: str, text: str):
    """Type text with human-like random delays"""
    logger.info(f"Human-like typing into: {selector}")
    await page.click(selector)
    await asyncio.sleep(_random_delay(0.1, 0.3))
    
    for char in text:
        await page.keyboard.type(char)
        await asyncio.sleep(_random_delay(0.05, 0.15))
    
    await asyncio.sleep(_random_delay(0.3, 0.6))
    logger.debug("Typing complete")

# ═══════════════════════════════════════════════════════════════════════════
# POPUP HANDLING WITH ENHANCED LOGGING
# ═══════════════════════════════════════════════════════════════════════════

async def _handle_popups(page) -> bool:
    """Handle various popups with improved logging"""
    logger.info("🔍 Checking for popups...")
    popup_handled = False
    
    popup_types = {
        "cookie_consent": INSTAGRAM_SELECTORS["cookie_accept"],
        "login_popup": INSTAGRAM_SELECTORS["login_popup"],
        "notification": INSTAGRAM_SELECTORS["notification_popup"],
    }
    
    for popup_name, selectors in popup_types.items():
        try:
            element = await _find_element(page, selectors, timeout=1500, description=f"{popup_name} popup")
            
            if element:
                logger.info(f"  ✓ Found {popup_name} popup - dismissing...")
                await asyncio.sleep(_random_delay(0.5, 1.0))
                await element.click()
                logger.info(f"  ✓ Dismissed {popup_name}")
                await asyncio.sleep(_random_delay(2.0, 3.0))
                popup_handled = True
                break
        except Exception as e:
            logger.debug(f"  ✗ {popup_name} check failed: {str(e)[:50]}")
            continue
    
    if not popup_handled:
        logger.info("  ℹ️ No popups found")
    
    return popup_handled

async def _handle_challenge(page) -> bool:
    """Handle Instagram challenge with logging"""
    logger.warning("🚨 Challenge detected - attempting recovery...")
    
    await asyncio.sleep(_random_delay(1.0, 2.0))
    
    challenge_selectors = [
        'button:has-text("This was me")',
        'button[type="button"]:has-text("This was me")',
    ]
    
    try:
        element = await _find_element(page, challenge_selectors, timeout=3000, description="'This was me' button")
        if element:
            logger.info("  ✓ Found 'This was me' button - clicking...")
            await element.click()
            logger.info("  ✓ Challenge button clicked")
            await asyncio.sleep(_random_delay(3.0, 5.0))
            return True
    except Exception as e:
        logger.error(f"  ✗ Failed to handle challenge: {e}")
    
    logger.error("  ❌ Could not auto-handle challenge - manual intervention needed")
    raise Exception("Challenge required - manual intervention needed")

async def _handle_2fa(page) -> bool:
    """Handle 2FA with logging"""
    logger.error("🔐 2FA code required - cannot proceed in headless mode")
    raise Exception("2FA code required - manual intervention needed")

# ═══════════════════════════════════════════════════════════════════════════
# MAIN SCRAPING FUNCTION
# ═══════════════════════════════════════════════════════════════════════════

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
    posts = []
    
    logger.info("=" * 70)
    logger.info(f"🚀 Starting Instagram scrape for @{account}")
    logger.info("=" * 70)
    
    viewport = _get_random_viewport()
    user_agent = _get_realistic_user_agent()
    
    async with async_playwright() as p:
        # Launch browser with stealth args
        logger.info("📱 Launching browser...")
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu',
                f'--window-size={viewport["width"]},{viewport["height"]}'
            ]
        )
        logger.info("  ✓ Browser launched")
        
        # Create context with realistic settings
        logger.info("🔧 Creating browser context...")
        context = await browser.new_context(
            viewport=viewport,
            locale='en-US',
            timezone_id='America/New_York',
            user_agent=user_agent,
            device_scale_factor=1,
            has_touch=False,
            java_script_enabled=True,
            permissions=['geolocation'],
            color_scheme='light',
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        
        # Apply stealth script
        await context.add_init_script(_get_enhanced_stealth_script())
        logger.info("  ✓ Stealth script injected")
        
        # Add cookies if provided
        if cookies:
            await context.add_cookies(cookies)
            logger.info(f"  ✓ Added {len(cookies)} cookies (logged-in session)")
        
        page = await context.new_page()
        
        try:
            # Load profile page
            logger.info(f"🌐 Loading profile: https://www.instagram.com/{account}/")
            await page.goto(
                f'https://www.instagram.com/{account}/',
                wait_until='networkidle',
                timeout=60000
            )
            await asyncio.sleep(_random_delay(2.0, 4.0))
            logger.info("  ✓ Profile page loaded")
            
            # Check for challenge
            current_url = page.url
            if '/challenge/' in current_url or 'suspicious' in current_url.lower():
                logger.warning("⚠️  Challenge detected on profile load")
                await _handle_challenge(page)
                await page.goto(f'https://www.instagram.com/{account}/', timeout=60000)
            
            # Handle popups
            logger.info("🛑 Handling initial popups...")
            popup_attempts = 0
            max_popup_attempts = 3
            
            while popup_attempts < max_popup_attempts:
                popup_found = await _handle_popups(page)
                if popup_found:
                    popup_attempts += 1
                    await asyncio.sleep(_random_delay(0.5, 1.5))
                else:
                    break
            
            logger.info(f"  ✓ Popup handling complete ({popup_attempts} handled)")
            
            # Simulate human behavior
            await _random_scroll(page)
            await asyncio.sleep(_random_delay(1.0, 2.0))
            
            # Check if private
            logger.info("🔒 Checking if profile is private...")
            private_element = await _find_element(
                page,
                INSTAGRAM_SELECTORS["private_notice"],
                timeout=5000,
                description="private notice"
            )
            
            if private_element:
                if not cookies:
                    logger.warning("  ⚠️ Private profile - need logged-in session (cookies)")
                    return []
                else:
                    logger.warning("  ⚠️ Private profile detected even with cookies")
                    return []
            
            logger.info("  ✓ Profile is public")
            
            # Scroll and load posts
            logger.info(f"📸 Loading posts (limit: {config.POST_LIMIT})...")
            loaded_posts = 0
            max_scroll_attempts = 20
            scroll_attempts = 0
            
            while loaded_posts < config.POST_LIMIT and scroll_attempts < max_scroll_attempts:
                await _handle_popups(page)
                
                # Find post links
                post_links = await page.locator(INSTAGRAM_SELECTORS["post_links"][0]).all()
                logger.debug(f"Found {len(post_links)} post links on page")
                
                # Process new links
                for link in post_links[loaded_posts:]:
                    if loaded_posts >= config.POST_LIMIT:
                        break
                    
                    try:
                        post_url = await link.get_attribute('href')
                        post_url = f"https://www.instagram.com{post_url}"
                        
                        logger.info(f"  [{loaded_posts + 1}/{config.POST_LIMIT}] Processing: {post_url}")
                        
                        # Visit post
                        await page.goto(post_url, wait_until='networkidle', timeout=30000)
                        await asyncio.sleep(_random_delay(1.0, 2.0))
                        
                        # Check for challenge
                        if '/challenge/' in page.url:
                            logger.warning("    ⚠️  Challenge on post - skipping")
                            await page.goto(f'https://www.instagram.com/{account}/', timeout=30000)
                            continue
                        
                        # Handle popups
                        await _handle_popups(page)
                        
                        # Extract caption
                        logger.debug("    Extracting caption...")
                        caption = ""
                        try:
                            caption_element = await _find_element(
                                page,
                                INSTAGRAM_SELECTORS["caption"],
                                timeout=2000,
                                description="caption"
                            )
                            
                            if caption_element:
                                caption = await caption_element.inner_text()
                                logger.debug(f"    ✓ Caption found: {caption[:50]}...")
                        except Exception as e:
                            logger.debug(f"    ✗ Caption extraction failed: {e}")
                        
                        # Extract media
                        logger.debug("    Extracting media...")
                        is_video = False
                        media_url = ""
                        
                        try:
                            video_element = await _find_element(
                                page,
                                INSTAGRAM_SELECTORS["video"],
                                timeout=2000,
                                description="video"
                            )
                            
                            if video_element:
                                is_video = True
                                media_url = await video_element.get_attribute('src')
                                logger.debug(f"    ✓ Video found: {media_url[:60]}...")
                            else:
                                image_element = await _find_element(
                                    page,
                                    INSTAGRAM_SELECTORS["image"],
                                    timeout=2000,
                                    description="image"
                                )
                                
                                if image_element:
                                    media_url = await image_element.get_attribute('src')
                                    logger.debug(f"    ✓ Image found: {media_url[:60]}...")
                        except Exception as e:
                            logger.debug(f"    ✗ Media extraction failed: {e}")
                        
                        # Store post
                        if media_url:
                            posts.append({
                                "url": post_url,
                                "caption": caption.strip() if caption else "",
                                "media_url": media_url,
                                "is_video": is_video
                            })
                            loaded_posts += 1
                            logger.info(f"    ✓ Post #{loaded_posts} saved")
                        else:
                            logger.warning(f"    ⚠️ No media found - skipping")
                        
                        # Go back with human delay
                        await page.go_back(timeout=30000)
                        await asyncio.sleep(_random_delay(2.0, 3.0))
                        
                    except Exception as e:
                        logger.error(f"    ✗ Error processing post: {e}")
                        await page.goto(f'https://www.instagram.com/{account}/', timeout=30000)
                        await asyncio.sleep(_random_delay(1.0, 2.0))
                
                if loaded_posts >= config.POST_LIMIT:
                    break
                
                # Scroll down
                logger.debug(f"Scrolling to load more posts... (attempt {scroll_attempts + 1}/{max_scroll_attempts})")
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await asyncio.sleep(_random_delay(2.0, 4.0))
                scroll_attempts += 1
                
                if random.random() > 0.7:
                    await _random_scroll(page)
            
            logger.info("=" * 70)
            logger.info(f"✅ Scrape complete: {len(posts)} posts fetched")
            logger.info("=" * 70)
        
        except Exception as e:
            logger.error("=" * 70)
            logger.error(f"❌ Fatal error during scrape: {e}")
            logger.exception("Full traceback:")
            logger.error("=" * 70)
        
        finally:
            await browser.close()
            logger.info("🔌 Browser closed")
    
    return posts