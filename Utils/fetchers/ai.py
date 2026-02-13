import logging
from typing import Dict, Optional, Any, Tuple, Callable, List

from openai import AsyncOpenAI

from Utils import config

async def call_social_ai(platform: str, account: str, posts: List[Dict]) -> str:
    if not posts:
        return "No new posts to analyze."

    captions_text = "\n---\n".join([p.get("caption", "No caption") for p in posts if p.get("caption")])

    prompt = f"""
You are a sharp, street-smart Nigerian social media analyst who knows X, IG, FB, and YT inside out.

Analyze these recent {platform.upper()} posts from @{account}.

Post captions/text:
{captions_text}

Focus on:
1. The core content in the images/videos – describe wetin dey happen for the media proper (main subject, action, text overlays, vibe, or hidden details).
2. Overall message/purpose – na promotion, education, drama, awareness, campaign, meme, political, or wetin?
3. Tone & intent – serious, funny, motivational, controversial, emotional?
4. Trend/vibe check – e dey blow (viral potential), people dey talk am, or just normal post?

Answer in short, sweet Pidgin-mixed English with Naija slang. Keep am punchy – 4-6 sentences max. No long story!
"""

    api_key = config.GROQ_API_KEY
    if not api_key:
        logging.warning("GROQ API key missing")
        return "🤖 AI analysis unavailable (missing API key)."

    MODEL_CANDIDATES = [
        "llama-3.3-70b-versatile",
        "llama-3.1-70b-versatile",
        "llama-3.1-8b-instant",
        "gemma2-9b-it",
    ]

    try:
        client = AsyncOpenAI(
            api_key=api_key,
            base_url="https://api.groq.com/openai/v1"
        )

        for model in MODEL_CANDIDATES:
            try:
                logging.info(f"Trying Groq model: {model}")
                response = await client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.8,
                    max_tokens=400
                )
                result = response.choices[0].message.content.strip()
                logging.info(f"AI analysis succeeded with model: {model}")
                return result
            except Exception as e:
                err = str(e).lower()
                if "not found" in err or "decommissioned" in err:
                    logging.info(f"Model {model} unavailable – skipping to next")
                    continue
                else:
                    logging.warning(f"Model {model} failed: {e}")
                    continue

        return "🤖 AI analysis unavailable – all models failed or unavailable right now."

    except Exception as e:
        logging.exception(f"Groq API unexpected error: {e}")
        return "🤖 AI analysis unavailable right now. Try again later!"