import os
import asyncio
import logging
from openai import AsyncOpenAI
from telegram.ext import ContextTypes

# Global dictionary to track running AI tasks
ai_tasks: Dict[int, asyncio.Task] = {}

logger = logging.getLogger(__name__)

async def run_ai_task(user_id: int, text: str, chat_id: int, context: ContextTypes.DEFAULT_TYPE, source: str = "manual"):
    logging.info("run_ai_task started for user %s (source=%s)", user_id, source)

    system_msg = (
        "You are a sharp, insightful Nigerian social media analyst with deep knowledge of trends on X, Instagram, and Facebook. "
        "Your responses must be concise (max 6-8 sentences), direct, and engaging. "
        "Mix standard English with natural Nigerian Pidgin where it adds flavor and relatability—never forced. "
        "Focus on key insights: sentiment, intent, potential impact, and hidden nuances. "
        "Be honest, witty when fitting, and always provide value that makes the reader think deeper about the post."
    )
    user_msg = text

    MODEL_CANDIDATES = [
        "llama-3.3-70b-versatile",
        "llama-3.1-8b-instant",
        "openai/gpt-oss-120b",
    ]

    try:
        api_key = os.getenv("GROQ_KEY")
        if not api_key:
            logging.error("GROQ_API_KEY not set")
            await context.bot.send_message(chat_id=chat_id, text="❌ Server misconfigured: missing GROQ_API_KEY.")
            return

        client = AsyncOpenAI(api_key=api_key, base_url="https://api.groq.com/openai/v1")

        working_msg = None
        try:
            working_msg = await context.bot.send_message(chat_id=chat_id, text="⏳ AI is thinking... (this may take a few seconds)")
        except Exception:
            working_msg = None

        last_exc = None
        success = False

        for model_id in MODEL_CANDIDATES:
            try:
                logging.info("Attempting model '%s' for user %s", model_id, user_id)
                response = await client.chat.completions.create(
                    model=model_id,
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_msg}
                    ],
                    temperature=0.7,
                    max_tokens=700
                )

                content = None
                try:
                    content = response.choices[0].message.content.strip()
                except Exception:
                    logging.exception("Malformed response structure from model %s", model_id)
                    content = None

                if content:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🤖 AI Result (model: {model_id}, source: {source}):\n\n{content}"
                    )
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🤖 AI returned no content (model: {model_id})."
                    )

                logging.info("Model %s succeeded for user %s", model_id, user_id)
                success = True
                last_exc = None
                break

            except asyncio.CancelledError:
                logging.info("AI task cancelled (model loop) for user %s", user_id)
                try:
                    await context.bot.send_message(chat_id=chat_id, text="🛑 AI analysis cancelled.")
                except Exception:
                    pass
                raise

            except Exception as e:
                last_exc = e
                emsg = str(e).lower()
                logging.warning("Model %s failed for user %s: %s", model_id, user_id, emsg)

                if any(tok in emsg for tok in ("decommissioned", "model_decommissioned", "model not found", "not found")):
                    logging.info("Model %s appears decommissioned or missing; trying next candidate.", model_id)
                    continue
                continue

        try:
            if working_msg:
                await working_msg.delete()
        except Exception:
            pass

        if not success:
            logging.exception("All model candidates failed for user %s", user_id)
            try:
                await context.bot.send_message(chat_id=chat_id, text=f"⚠️ AI error (all models failed): {last_exc}")
            except Exception:
                pass

    except asyncio.CancelledError:
        logging.info("run_ai_task cancelled for user %s", user_id)
        try:
            await context.bot.send_message(chat_id=chat_id, text="🛑 AI analysis cancelled.")
        except Exception:
            pass
        raise

    except Exception as e:
        logging.exception("run_ai_task unexpected failure for user %s: %s", user_id, e)
        try:
            await context.bot.send_message(chat_id=chat_id, text=f"⚠️ AI unexpected error: {e}")
        except Exception:
            pass

    finally:
        try:
            ai_tasks.pop(user_id, None)
        except Exception:
            pass
        try:
            if isinstance(context.user_data, dict):
                context.user_data.pop("ai_task", None)
        except Exception:
            pass

    logging.info("run_ai_task finished for user %s", user_id)