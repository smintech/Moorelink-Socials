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

# INSTAGRAM SELECTORS (Updated for 2024 - Based on Real HTML Analysis)

# ═══════════════════════════════════════════════════════════════════════════

INSTAGRAM_SELECTORS = {
    # Posts
    "post_links": [
        'a[href^="/p/"]',  # Primary selector
        'article a[href^="/p/"]',  # Within article
        'div[role="feed"] a[href^="/p/"]',  # Within feed
    ],

    # Captions/Text (UPDATED - Based on actual post page HTML)
    "caption": [
        'h1._ab1a',  # Main caption heading (single post view)
        'h1._ap3a._aaco._aacu._aacx._aad7._aade',  # Full caption selector
        'article h1',  # Generic article heading
        'div[data-testid="post-caption"]',  # Fallback caption area
        'h2 + div span',  # Caption near username
    ],

    # Media
    "image": [
        'article img[alt]:not([alt=""])',  # Main image
        'div[role="img"] img',  # Div-based image
        'img[alt*="photo"]',  # Alt contains photo
        'img[loading="lazy"]',  # Lazy loaded
    ],

    "video": [
        'article video',  # Video element
        'video[src]',  # With src attribute
        'video[poster]',  # Video with poster
    ],

    # User Info
    "username": [
        'a[title]:not([href*="/"])',  # Username link
        'span[data-testid="profile-username"]',  # Profile username
        'header a[title]',  # Header username
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

    # LOGIN BANNER (UPDATED - For anonymous public page viewing)
    "login_banner": [
        'span:text("Not now")',  # "Not now" button on login banner
        'span:text-matches("Not now", "i")',  # Case insensitive
        'button:has-text("Not now")',  # Button version
        'div[role="none"] span:has-text("Not now")',  # Specific structure
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
    logger.info(f"Selected user agent: {selected[:60]}…")
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

async def _log_current_url(page, context_msg: str = ""):
    """Log current URL with context"""
    current_url = page.url
    logger.info(f"📍 URL {context_msg}: {current_url}")
    return current_url

async def _handle_popups(page) -> bool:
    """Handle various popups with improved logging"""
    logger.info("🔍 Checking for popups…")
    popup_handled = False

    popup_types = {
        "cookie_consent": INSTAGRAM_SELECTORS["cookie_accept"],
        "login_banner": INSTAGRAM_SELECTORS["login_banner"],  # NEW: Login banner for anonymous viewing
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
    logger.warning("🚨 Challenge detected - attempting recovery…")
    await _log_current_url(page, "[CHALLENGE]")

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
        else:
            logger.info("  ℹ️ No cookies provided (anonymous mode)")
        
        page = await context.new_page()
        
        try:
            # Load profile page
            profile_url = f'https://www.instagram.com/{account}/'
            logger.info(f"🌐 Loading profile: {profile_url}")
            await page.goto(
                profile_url,
                wait_until='networkidle',
                timeout=60000
            )
            await _log_current_url(page, "[PROFILE_LOADED]")
            await asyncio.sleep(_random_delay(2.0, 4.0))
            logger.info("  ✓ Profile page loaded")
            
            # Check for challenge
            current_url = page.url
            if '/challenge/' in current_url or 'suspicious' in current_url.lower():
                logger.warning("⚠️  Challenge detected on profile load")
                await _handle_challenge(page)
                await page.goto(profile_url, timeout=60000)
                await _log_current_url(page, "[AFTER_CHALLENGE]")
            
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
            await _log_current_url(page, "[AFTER_POPUPS]")
            
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
                    await _log_current_url(page, "[PRIVATE_NO_COOKIES]")
                    return []
                else:
                    logger.warning("  ⚠️ Private profile detected even with cookies")
                    await _log_current_url(page, "[PRIVATE_WITH_COOKIES]")
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
                        await _log_current_url(page, "[POST_LOADED]")
                        await asyncio.sleep(_random_delay(1.0, 2.0))
                        
                        # Check for challenge
                        if '/challenge/' in page.url:
                            logger.warning("    ⚠️  Challenge on post - skipping")
                            await _log_current_url(page, "[CHALLENGE_ON_POST]")
                            await page.goto(profile_url, timeout=30000)
                            await _log_current_url(page, "[BACK_TO_PROFILE]")
                            continue
                        
                        # Handle popups
                        await _handle_popups(page)
                        
                        # Extract caption (UPDATED)
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
                            else:
                                logger.debug("    ℹ️ No caption found (may be image-only post)")
                        except Exception as e:
                            logger.debug(f"    ✗ Caption extraction failed: {e}")
                        
                        # Extract media (UPDATED - Detect by URL pattern + wait for lazy loading)
                        logger.debug("    Extracting media...")
                        is_video = False
                        media_url = ""
                        
                        # Determine type by URL pattern
                        if '/reel/' in post_url:
                            is_video = True
                            logger.debug("    ℹ️ Detected REEL post (video) by URL pattern")
                        elif '/p/' in post_url:
                            is_video = False
                            logger.debug("    ℹ️ Detected PHOTO post by URL pattern")
                        
                        try:
                            # Wait a bit for media to lazy-load
                            await asyncio.sleep(_random_delay(1.5, 2.5))
                            
                            # Method 1: Try to find img/video elements in DOM
                            logger.debug("    Attempting to extract from DOM elements...")
                            
                            if is_video:
                                # Try video element
                                video_element = await _find_element(
                                    page,
                                    INSTAGRAM_SELECTORS["video"],
                                    timeout=1500,
                                    description="video element"
                                )
                                
                                if video_element:
                                    media_url = await video_element.get_attribute('src')
                                    if media_url:
                                        logger.debug(f"    ✓ Video element found: {media_url[:60]}...")
                            else:
                                # Try image element
                                image_element = await _find_element(
                                    page,
                                    INSTAGRAM_SELECTORS["image"],
                                    timeout=1500,
                                    description="image element"
                                )
                                
                                if image_element:
                                    media_url = await image_element.get_attribute('src')
                                    if media_url:
                                        logger.debug(f"    ✓ Image element found: {media_url[:60]}...")
                            
                            # Method 2: Extract from page metadata/data attributes
                            if not media_url:
                                logger.debug("    Attempting to extract from page data attributes...")
                                
                                try:
                                    # Instagram stores media info in page script data
                                    page_data = await page.evaluate("""
                                    () => {
                                        // Try to find media in window data
                                        if (window._sharedData) return window._sharedData;
                                        if (window.__additionalDataIsLoaded) return window.__additionalDataIsLoaded;
                                        
                                        // Try to extract from img/video src attributes
                                        const imgs = Array.from(document.querySelectorAll('img')).map(img => img.src);
                                        const videos = Array.from(document.querySelectorAll('video')).map(vid => vid.src);
                                        
                                        return {
                                            images: imgs.filter(src => src && src.length > 0),
                                            videos: videos.filter(src => src && src.length > 0)
                                        };
                                    }
                                    """)
                                    
                                    if page_data:
                                        if is_video and page_data.get('videos'):
                                            media_url = page_data['videos'][0]
                                            logger.debug(f"    ✓ Video from page data: {media_url[:60]}...")
                                        elif not is_video and page_data.get('images'):
                                            # Filter out small thumbnails/profile images
                                            large_images = [img for img in page_data['images'] if 'jpg' in img or 'png' in img]
                                            if large_images:
                                                media_url = large_images[0]
                                                logger.debug(f"    ✓ Image from page data: {media_url[:60]}...")
                                except Exception as e:
                                    logger.debug(f"    ℹ️ Page data extraction skipped: {str(e)[:40]}")
                            
                            # Method 3: Fallback - get ALL img/video sources
                            if not media_url:
                                logger.debug("    Attempting fallback: scanning all media elements...")
                                
                                all_imgs = await page.query_selector_all('img')
                                all_videos = await page.query_selector_all('video')
                                
                                logger.debug(f"    Found {len(all_imgs)} img elements, {len(all_videos)} video elements")
                                
                                if is_video and all_videos:
                                    for idx, vid in enumerate(all_videos):
                                        src = await vid.get_attribute('src')
                                        if src:
                                            media_url = src
                                            logger.debug(f"    ✓ Video from fallback scan: {media_url[:60]}...")
                                            break
                                elif not is_video and all_imgs:
                                    # Get largest image (likely not icon/thumbnail)
                                    for idx, img in enumerate(all_imgs):
                                        src = await img.get_attribute('src')
                                        alt = await img.get_attribute('alt')
                                        
                                        if src and alt and ('photo' in alt.lower() or 'image' in alt.lower() or alt == ""):
                                            media_url = src
                                            logger.debug(f"    ✓ Image from fallback scan: {media_url[:60]}...")
                                            break
                                
                                if not media_url and all_imgs:
                                    # Last resort: take first high-quality image
                                    src = await all_imgs[0].get_attribute('src')
                                    if src and ('jpg' in src or 'png' in src):
                                        media_url = src
                                        logger.debug(f"    ✓ Image from last resort: {media_url[:60]}...")
                        
                        except Exception as e:
                            logger.warning(f"    ✗ Media extraction error: {str(e)[:60]}")
                        
                        # Store post
                        if media_url:
                            posts.append({
                                "url": post_url,
                                "caption": caption.strip() if caption else "",
                                "media_url": media_url,
                                "is_video": is_video
                            })
                            loaded_posts += 1
                            logger.info(f"    ✓ Post #{loaded_posts} saved ({'VIDEO' if is_video else 'IMAGE'})")
                        else:
                            logger.warning(f"    ⚠️ No media URL found - skipping")
                        
                        # Go back with human delay
                        await page.go_back(timeout=30000)
                        await _log_current_url(page, "[BACK_TO_FEED]")
                        await asyncio.sleep(_random_delay(2.0, 3.0))
                        
                    except Exception as e:
                        logger.error(f"    ✗ Error processing post: {e}")
                        await page.goto(profile_url, timeout=30000)
                        await _log_current_url(page, "[ERROR_RECOVERY]")
                        await asyncio.sleep(_random_delay(1.0, 2.0))
                
                if loaded_posts >= config.POST_LIMIT:
                    break
                
                # Scroll down
                logger.debug(f"Scrolling to load more posts... (attempt {scroll_attempts + 1}/{max_scroll_attempts})")
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await _log_current_url(page, "[SCROLLED]")
                await asyncio.sleep(_random_delay(2.0, 4.0))
                scroll_attempts += 1
                
                if random.random() > 0.7:
                    await _random_scroll(page)
            
            logger.info("=" * 70)
            logger.info(f"✅ Scrape complete: {len(posts)} posts fetched")
            logger.info(f"📊 Final URL: {page.url}")
            logger.info("=" * 70)
        
        except Exception as e:
            logger.error("=" * 70)
            logger.error(f"❌ Fatal error during scrape: {e}")
            logger.exception("Full traceback:")
            await _log_current_url(page, "[FATAL_ERROR]")
            logger.error("=" * 70)
        
        finally:
            await browser.close()
            logger.info("🔌 Browser closed")

    return posts
