<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="A Telegram bot that fetches public data from X, Instagram, and Facebook profiles on demand and returns structured JSON. Built for reliability in an arms race.">
</head>
<body>

<h1>Moorelink Socials Scraper</h1>

<p>A Telegram bot that fetches public data from X, Instagram, and Facebook profiles on demand and returns it as structured JSON.</p>

<p>I built this because off-the-shelf scrapers kept breaking every few weeks. Platforms change endpoints, fingerprints, and rate limits constantly. This version prioritizes reliability over feature bloat.</p>

<hr>

<h2>What it actually does</h2>

<ul>
    <li>User picks a platform and sends a @username</li>
    <li>Bot checks its local database for cached/recent data</li>
    <li>If stale or missing, it runs a live fetch using current bypass techniques</li>
    <li>Parses the raw response into clean JSON (posts, bio, followers, media URLs where available)</li>
    <li>Sends the JSON back in Telegram</li>
</ul>

<p>No scheduled crawling. No bulk exports. On-demand only — because anything automated at scale gets blocked fast.</p>

<hr>

<h2>Trade-offs I accepted</h2>

<ul>
    <li>Relies on a third-party scrapers API instead of maintaining my own headless browsers.<br>
        Cheaper and less brittle than rolling Selenium/Playwright clusters, but adds one dependency.</li>
    <li>Caches results in a simple database to avoid hammering the API on repeats.<br>
        Means data can be minutes old, not seconds — I chose consistency over perfect freshness.</li>
    <li>No support for private accounts or logged-in features.<br>
        Public data only. Trying to do more invites instant bans.</li>
    <li>Focused on three platforms. Adding more dilutes the bypass logic.</li>
</ul>

<p>These weren't accidents. They were deliberate restraints to keep the bot running longer than a week.</p>

<hr>

<h2>Setup</h2>

<pre><code>git clone https://github.com/smintech/Moorelink-Socials.git
cd Moorelink-Socials</code></pre>

<p>Create <code>.env</code>:</p>

<pre><code>TELEGRAM_BOT_TOKEN=your_bot_token
DATABASE_URL=your_postgres_or_sqlite_url
SCRAPERS_API_KEY=your_third_party_api_key</code></pre>

<p>Install:</p>

<pre><code>pip install -r requirements.txt</code></pre>

<p>Run locally or deploy (Heroku, Render, Fly.io, etc.).</p>

<p>Bot starts in "awaiting username" mode.</p>

<hr>

<h2>Current reality (2026)</h2>

<p>Scraping social platforms is an arms race. What works today can break tomorrow.</p>

<p>This repo stays useful as long as:</p>
<ul>
    <li>The third-party API keeps updating their bypasses (most paid ones do)</li>
    <li>You don't abuse rate limits</li>
</ul>

<p>If it stops working, the fix is usually updating the API integration or switching providers — not rewriting the whole scraper.</p>

<hr>

<h2>Contributing</h2>

<p>Welcome pulls that:</p>
<ul>
    <li>Improve parsing robustness</li>
    <li>Add better error handling for API changes</li>
    <li>Optimize caching logic</li>
</ul>

<p>Open an issue first. No new platforms unless you maintain the bypass code.</p>

<hr>

<h2>License</h2>

<p>Non-commercial use only. No selling, no SaaS wrappers, no paid services built on this.</p>

<p>See LICENSE file.</p>

<hr>

<h2>Author</h2>

<p><strong>smintech</strong><br>
<a href="https://github.com/smintech">GitHub</a> ·
<a href="https://www.linkedin.com/in/israel-timi-99b339360">LinkedIn: Israel Timi</a></p>

</body>
</html>