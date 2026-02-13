import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegram.ext import ContextTypes
from telegram.constants import ChatAction
from settings import TEST_MODE, is_admin, POST_LIMIT
from Bot.helpers import safe_send_media_or_link, schedule_delete, normalize_account
from Utils.utils import (
    fetch_latest_urls,
    fetch_ig_urls,
    fetch_fb_urls,
    fetch_yt_videos,
    extract_post_id,
    is_post_new,
    mark_posts_seen,
    get_user_badge,
    check_and_increment_cooldown,
)

logger = logging.getLogger(__name__)

async def send_ai_button(message, count, platform, account, badge, context=None, auto_delete_after: int | None = None):
    """Send the AI analyze button."""
    button_text = f"Analyze {count} new post(s) with AI 🤖"
    if badge['name'] in ('Diamond', 'Admin'):
        button_text += " (Unlimited)"

    analyze_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(button_text, callback_data=f"ai_analyze_{platform}_{account}")
    ]])

    final_msg = await message.reply_text(
        f"✨ {count} new post(s) processed!\nTap below for sharp AI breakdown:",
        reply_markup=analyze_kb
    )

    if auto_delete_after and context and getattr(context, "job_queue", None):
        try:
            context.job_queue.run_once(
                lambda c: c.bot.delete_message(chat_id=final_msg.chat.id, message_id=final_msg.message_id),
                when=auto_delete_after,
                data=None
            )
        except Exception as e:
            logging.debug("Failed to schedule AI button deletion: %s", e)

    return final_msg

async def send_next_post_with_confirmation(update_or_query, context: ContextTypes.DEFAULT_TYPE, platform: str, account: str):
    """Robust preview sender: edits previous message if possible, otherwise sends new one."""
    user_data_key = f"pending_posts_{platform}_{account}"
    pending = context.user_data.get(user_data_key)

    cq = getattr(update_or_query, "callback_query", None)
    message = None
    uid = None

    if cq:
        try:
            await cq.answer()
        except Exception:
            pass
        message = cq.message
        uid = cq.from_user.id
    elif hasattr(update_or_query, "effective_message") and update_or_query.effective_message:
        message = update_or_query.effective_message
        uid = getattr(update_or_query.effective_user, "id", None)
    elif hasattr(update_or_query, "message") and update_or_query.message:
        message = update_or_query.message
        uid = getattr(update_or_query.from_user, "id", None)

    if not pending or pending.get("index", 0) >= pending.get("total", 0):
        if not uid and message and getattr(message, "from_user", None):
            uid = message.from_user.id
        if not uid:
            logger.warning("Cannot determine user id; clearing pending.")
            context.user_data.pop(user_data_key, None)
            return

        badge = get_user_badge(uid)
        posts_to_store = pending.get("posts", [])[:] if pending else context.user_data.get(f"last_ai_context_{platform}_{account}", [])
        context.user_data[f"last_ai_context_{platform}_{account}"] = posts_to_store

        processed = pending.get("index", 0) if pending else 0
        total = pending.get("total", processed) if pending else len(posts_to_store)

        target = message or update_or_query
        await send_ai_button(target, max(processed, total), platform, account, badge, context=context, auto_delete_after=None)
        context.user_data.pop(user_data_key, None)
        return

    chat_for_send = message or getattr(update_or_query, "effective_chat", None)
    if not chat_for_send:
        logger.warning("No chat available to send preview.")
        return

    if not uid and getattr(message, "from_user", None):
        uid = message.from_user.id

    current_idx = pending.get("index", 0)
    posts = pending.get("posts", [])
    if current_idx >= len(posts):
        logger.warning("Index out of range; clearing pending.")
        context.user_data.pop(user_data_key, None)
        return

    post = posts[current_idx]

    view_text = {"x": "View on 𝕏", "fb": "View on Facebook ⓕ", "ig": "View on Instagram 🅮", "yt": "View on YouTube 📺"}.get(platform, "View Post 🔗")
    link_html = f"<a href='{post.get('post_url','')}'>{view_text}</a>" if post.get('post_url') else ""
    caption = (post.get("caption") or "")[:1024]
    full_caption = f"{link_html}\n\n{caption}" if link_html else caption
    preview_text = (full_caption + "\n\nMove to next post⏭️?") if full_caption else "Move to next post⏭️?"

    total_posts = pending.get("total", len(posts))
    keyboard_rows = [[
        InlineKeyboardButton(f"✅ Send this post ({current_idx + 1}/{total_posts})",
                             callback_data=f"confirm_post_{platform}_{account}_{current_idx}"),
        InlineKeyboardButton("⏭️ Skip this post", callback_data=f"skip_post_{platform}_{account}_{current_idx}")
    ]]
    if not pending.get("has_sent_single", False):
        remaining = max(0, total_posts - current_idx)
        if remaining > 1:
            keyboard_rows.append([InlineKeyboardButton(f"✅ Send all remaining ({remaining})",
                                                      callback_data=f"send_all_{platform}_{account}")])
    keyboard_rows.append([InlineKeyboardButton("❌ Cancel remaining", callback_data=f"cancel_posts_{platform}_{account}")])
    keyboard = InlineKeyboardMarkup(keyboard_rows)

    sent_preview = False
    preview_msg = None
    edited = False

    if message and getattr(message, "text", None) is not None:
        if getattr(message, "from_user", None) and message.from_user.is_bot:
            try:
                await asyncio.wait_for(
                    context.bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=message.message_id,
                        text=preview_text,
                        parse_mode="HTML",
                        reply_markup=keyboard
                    ),
                    timeout=8.0
                )
                sent_preview = True
                preview_msg = message
                edited = True
                logger.info("Successfully edited preview message (idx %s)", current_idx)
            except Exception as e:
                logger.debug("Edit failed (will send new): %s", e)

    if not sent_preview:
        preview_msg = await safe_send_media_or_link(
            chat=chat_for_send,
            context=context,
            media_url=post.get("media_url"),
            is_video=post.get("is_video", False),
            caption=preview_text,
            parse_mode="HTML",
            reply_markup=keyboard
        )
        if preview_msg:
            sent_preview = True
            logger.info("Sent new preview message (idx %s, msg_id %s)", current_idx, preview_msg.message_id)
        else:
            logger.error("Failed to send any preview for idx %s", current_idx)

    if not sent_preview:
        failed_list = pending.setdefault("failed", [])
        failed_list.append({"index": current_idx, "post_id": post.get("post_id"), "reason": "preview_send_failed"})
        logger.info("Preview completely failed for idx %s — marking failed and advancing", current_idx)
    else:
        logger.info("Preview shown successfully for idx %s", current_idx)

    pending["index"] = current_idx + 1
    context.user_data[user_data_key] = pending

    try:
        if preview_msg:
            await schedule_delete(context, preview_msg.chat.id, preview_msg.message_id)
        if edited and message:
            await schedule_delete(context, message.chat.id, message.message_id)
    except Exception as e:
        logger.debug("Failed to schedule delete: %s", e)

async def handle_fetch_and_ai(update, context, platform, account, query=None, force: bool = False):
    uid = update.effective_user.id
    message = query.message if query else update.effective_message

    if TEST_MODE.get("enabled"):
        force = True

    force_send = TEST_MODE.get("enabled", False)

    if is_admin(uid):
        pass  # no cooldown
    else:
        cooldown_msg = check_and_increment_cooldown(uid)
        if cooldown_msg:
            await message.reply_text(cooldown_msg)
            return

    await message.chat.send_action(ChatAction.TYPING)

    # Fetch raw posts
    if platform == "x":
        raw_posts = fetch_latest_urls("x", account)
        post_list = [{"post_id": extract_post_id("x", url), "post_url": url, "caption": ""} for url in raw_posts]
    elif platform == "ig":
        raw_ig = fetch_ig_urls(account)
        post_list = []
        for p in raw_ig:
            pid = extract_post_id("ig", p['url'])
            post_list.append({
                "post_id": pid,
                "post_url": p['url'],
                "caption": p.get("caption", ""),
                "media_url": p.get("media_url"),
                "is_video": p.get("is_video", False)
            })
    elif platform == "fb":
        raw_fb = fetch_fb_urls(account)
        post_list = []
        for p in raw_fb:
            pid = p.get("post_id") or p.get("post_url", "")
            post_list.append({
                "post_id": pid,
                "post_url": p['post_url'],
                "caption": p.get("caption", ""),
                "media_url": p.get("media_url"),
                "is_video": p.get("is_video", False)
            })
    elif platform == "yt":
        raw_yt = fetch_yt_videos(channel_handle=account)
        post_list = []
        for v in raw_yt:
            post_list.append({
                "post_id": v["post_id"],
                "post_url": v["post_url"],
                "caption": v["caption"],
                "media_url": v["media_url"],
                "is_video": True
            })
    else:
        await message.reply_text("Unsupported platform.")
        return

    clean_account = normalize_account(account, platform)

    new_posts = [p for p in post_list if is_post_new(uid, platform, clean_account, p['post_id'])]

    if force_send:
        logging.info("🧪 Force mode ACTIVE for user %s — sending latest posts (ignoring seen status)", uid)
        new_posts = post_list[:POST_LIMIT]
        mark_posts_seen(uid, platform, clean_account, [{"post_id": p['post_id'], "post_url": p['post_url']} for p in new_posts])
    elif not new_posts:
        await message.reply_text(f"No new posts from @{clean_account} since your last check.")
        return
    else:
        mark_posts_seen(uid, platform, clean_account, [{"post_id": p['post_id'], "post_url": p['post_url']} for p in new_posts])

    context.user_data[f"pending_posts_{platform}_{clean_account}"] = {
        "posts": new_posts,
        "index": 0,
        "total": len(new_posts),
        "has_sent_single": False
    }
    context.user_data[f"last_ai_context_{platform}_{clean_account}"] = new_posts

    if not new_posts:
        return

    await send_next_post_with_confirmation(update, context, platform, clean_account)