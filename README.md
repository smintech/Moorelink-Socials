# 🛰️ Moorelink Socials Scraper

> **The Problem:** Social media platforms are engineered to maximize "Time on Site" through endless scrolling and algorithmic noise.  
> **The Solution:** Moorelink is a high-precision extraction engine that bypasses the feed entirely, delivering structured, actionable data directly to Telegram. 

**Get the signal. Ignore the noise.**

---

## 🧠 Engineering Philosophy

Most scrapers are built for bulk data hoarding. Moorelink is built for **intentional consumption**.

- **State-Aware Logic:** Instead of blind scraping, the engine queries a database to verify the "last-seen" state. If the data hasn't changed, we don't fetch. This preserves IP health and reduces compute overhead.
- **Structured-First Delivery:** We don't just dump text. Raw HTML/JS is parsed into strict JSON schemas, ensuring that the Telegram delivery is clean, readable, and free of platform-specific bloat.
- **Asynchronous Flow:** Built with Python’s `asyncio` to handle multiple platform requests without blocking the user-state verification.

---

## 📝 User Workflow

1. **Platform Selection** — User selects a platform (X, Instagram, Facebook)
2. **Target Identification** — User sends the target `@username`
3. **State Verification** — Database is queried to verify the target and last-seen state
4. **Live Fetch** — Engine performs a real-time scrape of the latest data
5. **Data Structuring** — Raw content is parsed into clean, structured JSON
6. **Delivery** — Final output is pushed to the user via Telegram

---

## 🎥 Demo

▶ [Watch the video demo on LinkedIn](https://www.linkedin.com/posts/israel-timi-99b339360_built-this-python-automation-bot-to-reduce-activity-7411577723371896832-IVzF?utm_medium=ios_app&rcm=ACoAAFnAGUcBUj5MzZln7aHj0BKPcS2K4I5sLwo&utm_source=social_share_video_v2&utm_campaign=copy_link)

---

## 🛠️ Technical Architecture

### 1. State Verification
Before a fetch is triggered, the engine checks the `DATABASE_URL` to compare the target's current metadata against our records. This prevents redundant API calls and identifies "stale" targets.

### 2. The Extraction Pipeline
1. **Selection:** User-defined target (@username).
2. **Bypass:** Implementation of rotation and header-spoofing to mimic organic traffic.
3. **Parse:** Extraction of key entities (Post text, Timestamps, Media links).
4. **Push:** Asynchronous delivery via Telegram Bot API.

---

## 🚀 Deployment & Setup

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/smintech/Moorelink-Socials.git
cd Moorelink-Socials
```

### 2️⃣ Environment Configuration

Create a `.env` file in the root directory with the following content:

```env
TELEGRAM_BOT_TOKEN=your_token_here
DATABASE_URL=your_database_connection_string
SCRAPER_API_KEY=your_api_here
```

> **Note:** The `SCRAPER_API_KEY` is required for reliable access to third-party scraping services (e.g., Bright Data, ScraperAPI, or Oxylabs) to handle anti-bot measures on platforms like Instagram and Facebook. Sign up for a free trial or paid plan from a reputable provider and insert your actual API key here for seamless operation.

### 3️⃣ Install Dependencies

```bash
pip install -r requirements.txt
# If any frontend/Node components are included:
npm install
```

### 4️⃣ Launch

Deploy the bot (e.g., on Heroku, VPS, or Render) or run locally. The bot will initialize in the **awaiting username** state.

---

## 🤝 Contributing & License

**Non-Commercial Use Only**

Contributions are welcome in areas such as:
- Optimizing fetch logic
- Improving bypass techniques
- Refining JSON parsing

**Process:** Open an issue before submitting a Pull Request.  
**Restrictions:** Selling, renting, or monetizing this software or services built on it is strictly prohibited under the Non-Commercial [📄 License](LICENSE).

---

## 👤 Author

**smintech**  
[GitHub](https://github.com/smintech) · [LinkedIn](https://www.linkedin.com/in/israel-timi-99b339360?utm_source=share&utm_campaign=share_via&utm_content=profile&utm_medium=ios_app)