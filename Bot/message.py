import asyncio
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ChatAction
from openai import AsyncOpenAI
import os

from Bot.settings import is_admin, TEST_MODE
from Bot.helpers import record_user_and_check_ban, schedule_delete
from Bot.ui import build_saved_menu, build_back_markup, build_main_menu
from Bot.fetch import handle_fetch_and_ai
from Bot.ai import ai_tasks, run_ai_task
from utils import (
    get_user_badge,
    update_saved_account_label,
    save_user_account,
    count_saved_accounts,
    list_saved_accounts,
    get_saved_account,
    remove_saved_account,
    list_active_tg_users,
)

logger = logging.getLogger(__name__)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    uid = update.effective_user.id
    badge = get_user_badge(uid)

    # AI Follow-up Chat (only Diamond & Admin)
    if context.user_data.get("ai_chat_active") and badge['name'] in ('Diamond', 'Admin'):
        chat_context = context.user_data["ai_chat_active"]
        posts = chat_context["posts"]
        question = update.message.text.strip()

        captions_text = "\n---\n".join([
            p.get("caption", "No caption") or ""
            for p in posts
        ])

        prompt = f"""
    "You are a sharp Nigerian social media analyst. "
    "Use natural Nigerian Pidgin mixed with clear English — keep it authentic and relatable, never forced.\n\n"
    "Context: The user has just seen the latest post(s) from @{account} on {platform_upper}.\n"
    "Post captions/transcripts:\n"
    "{captions_text}\n\n"
    "User follow-up question: {question}\n\n"
    "Guidelines:\n"
    "- Respond in max 6 sentences\n"
    "- Be direct, insightful, and add value — cover intent, sentiment, impact, or hidden nuances\n"
    "- Add light wit only if it fits naturally\n"
    "- End with a short, thought-provoking takeaway or question\n"
    "- Stay engaging and make the user think deeper about the content"
    """

        await update.message.chat.send_action(ChatAction.TYPING)

        try:
            client = AsyncOpenAI(
                api_key=os.getenv("GROQ_KEY"),
                base_url="https://api.groq.com/openai/v1"
            )
            response = await client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.8,
                max_tokens=500
            )
            answer = response.choices[0].message.content.strip()
            await update.message.reply_text(
                f"🤖 <b>AI Follow-up</b>:\n\n{answer}\n\n<i>Reply again for more questions!</i>",
                parse_mode="HTML"
            )
        except Exception as e:
            logging.error(f"AI follow-up failed: {e}")
            await update.message.reply_text("🤖 AI temporary unavailable. Try again later.")

        return

    # Manual AI Analysis (Admin only, multi-turn)
    elif context.user_data.get("awaiting_manual_ai"):
        if not is_admin(uid):
            context.user_data.pop("awaiting_manual_ai", None)
            await update.effective_message.reply_text("❌ Only admins can use Manual AI.")
            return

        user_text = update.message.text.strip()

        if not user_text:
            await update.effective_message.reply_text(
                "📝 Send the text, post, link, or caption you want analyzed.\n"
                "I go analyze each one sharp-sharp.\n"
                "/cancel to stop Manual AI mode."
            )
            return

        task = asyncio.create_task(
            run_ai_task(
                user_id=uid,
                text=user_text,
                chat_id=update.effective_chat.id,
                context=context,
                source="manual_admin"
            )
        )
        ai_tasks[uid] = task
        context.user_data["ai_task"] = task

        await update.effective_message.reply_text(
            "🚀 AI dey think on top your text...\n"
            "Hold...."
        )
        return

    text = update.message.text.strip().lower()

    # Detect Facebook single post share links
    if ("facebook.com/share/" in text or "mibextid=" in text or 
        text.startswith("https://www.facebook.com/") or text.startswith("https://fb.watch/")):
        clean_link = update.message.text.split("?")[0].rstrip("/")
        await update.message.reply_text(
            f"🌐 Single Facebook post:\n{clean_link}",
            disable_web_page_preview=False
        )
        return

    # Admin broadcast
    if context.user_data.get("admin_broadcast"):
        if not is_admin(uid):
            context.user_data.pop("admin_broadcast", None)
            await update.effective_message.reply_text("❌ Only admins can broadcast.")
            return
        text_to_send = update.message.text
        await update.effective_message.reply_text("Broadcast starting... (send /cancel to abort while it runs)")
        users = list_active_tg_users(limit=10000)
        sent = 0
        failed = 0
        cancelled = False
        for u in users:
            if not context.user_data.get("admin_broadcast"):
                cancelled = True
                break
            try:
                await context.bot.send_message(chat_id=u.get("telegram_id"), text=text_to_send)
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                failed += 1
        context.user_data.pop("admin_broadcast", None)
        if cancelled:
            await update.effective_message.reply_text(f"Broadcast cancelled. Sent so far: {sent}, failed: {failed}")
        else:
            await update.effective_message.reply_text(f"Broadcast done. Sent: {sent}, failed: {failed}")
        return

    # Rename flow
    if context.user_data.get("awaiting_rename_id"):
        sid = context.user_data.pop("awaiting_rename_id")
        new_label = update.message.text.strip()
        ok = update_saved_account_label(uid, sid, new_label)
        if ok:
            await update.effective_message.reply_text(f"Saved account {sid} renamed to: {new_label}", reply_markup=build_saved_menu())
        else:
            await update.effective_message.reply_text("Could not rename saved account.", reply_markup=build_saved_menu())
        return

    # Add saved flow
    if context.user_data.get("awaiting_save"):
        text = update.message.text.strip()
        parts = text.split(maxsplit=2)
        if len(parts) < 2:
            await update.effective_message.reply_text("Send: <platform> <username_or_url> [label]")
            return

        platform_input = parts[0].lower()
        raw_input = parts[1].strip()
        label = parts[2] if len(parts) == 3 else None

        if platform_input in ("twitter", "x"):
            platform = "x"
        elif platform_input in ("instagram", "ig"):
            platform = "ig"
        elif platform_input in ("facebook", "fb"):
            platform = "fb"
        elif platform_input in ("youtube", "yt"):
            platform = "yt"
        else:
            await update.effective_message.reply_text("Platform must be: x, ig, fb, or yt (YouTube)")
            return

        account = raw_input

        if raw_input.startswith("http"):
            if platform == "fb":
                if "facebook.com" in raw_input or "fb.com" in raw_input:
                    account = raw_input.split('?')[0].rstrip('/')
                else:
                    await update.effective_message.reply_text("Invalid Facebook URL.")
                    context.user_data.pop("awaiting_save", None)
                    return
            elif platform == "yt":
                if "youtube.com" in raw_input or "youtu.be" in raw_input:
                    account = raw_input
                else:
                    await update.effective_message.reply_text("Invalid YouTube link.")
                    context.user_data.pop("awaiting_save", None)
                    return
            else:
                await update.effective_message.reply_text("Full URLs only supported for fb and yt.")
                context.user_data.pop("awaiting_save", None)
                return
        else:
            cleaned = raw_input.lstrip("@").strip()
            if cleaned.isdigit() and len(cleaned) >= 10:
                account = cleaned
            else:
                account = cleaned

        current_count = count_saved_accounts(uid)
        save_slots = badge.get('save_slots')
        if isinstance(save_slots, (int, float)) and current_count >= save_slots:
            await update.effective_message.reply_text(f"You've reached your save limit ({int(save_slots)}). Invite friends to upgrade!")
            context.user_data.pop("awaiting_save", None)
            return

        try:
            saved = save_user_account(uid, platform, account, label)
            if account.startswith("http"):
                if platform == "fb":
                    display_name = account.split('/')[-1] or "Facebook Page"
                elif platform == "yt":
                    if '@' in account:
                        display_name = account.split('@')[-1]
                    else:
                        display_name = account.split('/')[-1] or "YouTube Channel"
                else:
                    display_name = account
            else:
                if platform == "x" and account.isdigit() and len(account) >= 10:
                    display_name = f"User ID: {account}"
                else:
                    display_name = f"@{account}"

            await update.effective_message.reply_text(
                f"✅ Saved {platform.upper()} account:\n"
                f"{display_name}\n"
                f"Label: {label or 'None'}\n"
                f"ID: {saved.get('id')}",
                reply_markup=build_saved_menu()
            )
        except Exception as e:
            logging.error(f"Save error for user {uid}: {e}")
            await update.effective_message.reply_text(f"❌ Error saving: {str(e)}", reply_markup=build_saved_menu())

        context.user_data.pop("awaiting_save", None)
        return

    # Prompted username flow (from menu)
    if context.user_data.get("awaiting_username"):
        raw_input = update.message.text.strip()
        platform = context.user_data.get("platform", "x")
        context.user_data["awaiting_username"] = False

        account = raw_input
        if raw_input.startswith("http") and platform in ("fb", "yt"):
            if platform == "fb" and ("facebook.com" in raw_input or "fb.com" in raw_input):
                account = raw_input.split('?')[0].rstrip('/')
            elif platform == "yt" and ("youtube.com" in raw_input or "youtu.be" in raw_input):
                account = raw_input
        else:
            account = raw_input.lstrip("@")

        await handle_fetch_and_ai(update, context, platform, account)
        return

    # /saved_send command
    if text.startswith("/saved_send"):
        parts = text.split()
        if len(parts) < 2:
            await update.effective_message.reply_text("Usage: /saved_send <id>")
            return
        try:
            sid = int(parts[1])
        except:
            await update.effective_message.reply_text("Invalid id.")
            return
        saved = get_saved_account(uid, sid)
        if not saved:
            await update.effective_message.reply_text("Saved account not found.")
            return
        await handle_fetch_and_ai(update, context, saved["platform"], saved["account_name"])
        return

    # /saved_remove
    if text.startswith("/saved_remove"):
        parts = text.split()
        if len(parts) < 2:
            await update.effective_message.reply_text("Usage: /saved_remove <id>")
            return
        try:
            sid = int(parts[1])
        except:
            await update.effective_message.reply_text("Invalid id.")
            return
        ok = remove_saved_account(uid, sid)
        await update.effective_message.reply_text(
            f"Removed saved account {sid}." if ok else "Could not remove account."
        )
        return

    # /saved_rename
    if text.startswith("/saved_rename"):
        parts = text.split(maxsplit=2)
        if len(parts) < 3:
            await update.effective_message.reply_text("Usage: /saved_rename <id> <new label>")
            return
        try:
            sid = int(parts[1])
        except:
            await update.effective_message.reply_text("Invalid id.")
            return
        new_label = parts[2].strip()
        ok = update_saved_account_label(uid, sid, new_label)
        await update.effective_message.reply_text(
            f"Renamed account {sid} → {new_label}" if ok else "Could not rename account."
        )
        return

    # /save command (direct command version)
    if text.startswith("/save"):
        parts = text.split(maxsplit=3)
        if len(parts) < 3:
            await update.effective_message.reply_text(
                "Usage: /save <platform> <username_or_url> [label]\n\n"
                "Examples:\n"
                "/save x elonmusk\n"
                "/save ig davido\n"
                "/save fb https://www.facebook.com/BBCNews BBC News\n"
                "/save yt @MrBeast"
            )
            return

        platform_input = parts[1].lower()
        raw_input = parts[2].strip()
        label = parts[3] if len(parts) == 4 else None

        if platform_input in ("twitter", "x"):
            platform = "x"
        elif platform_input in ("instagram", "ig"):
            platform = "ig"
        elif platform_input in ("facebook", "fb"):
            platform = "fb"
        elif platform_input in ("youtube", "yt"):
            platform = "yt"
        else:
            await update.effective_message.reply_text("Platform must be x, ig, fb, or yt")
            return

        account = raw_input
        if raw_input.startswith("http"):
            if platform not in ("fb", "yt"):
                await update.effective_message.reply_text("Full URLs only for fb and yt")
                return
            if platform == "fb" and not ("facebook.com" in raw_input or "fb.com" in raw_input):
                await update.effective_message.reply_text("Invalid Facebook URL")
                return
            if platform == "yt" and not ("youtube.com" in raw_input or "youtu.be" in raw_input):
                await update.effective_message.reply_text("Invalid YouTube link")
                return
            account = raw_input.split('?')[0].rstrip('/') if platform == "fb" else raw_input
        else:
            account = raw_input.lstrip('@')

        current_count = count_saved_accounts(uid)
        save_slots = badge.get('save_slots')
        if isinstance(save_slots, (int, float)) and current_count >= save_slots:
            await update.effective_message.reply_text(f"Save limit reached ({int(save_slots)})")
            return

        try:
            saved = save_user_account(uid, platform, account, label)
            if account.startswith("http"):
                display = account.split('/')[-1] or account
            else:
                display = "@" + account

            await update.effective_message.reply_text(
                f"✅ Saved {platform.upper()}:\n{display}\nLabel: {label or 'None'}\nID: {saved.get('id')}"
            )
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Save failed: {e}")
        return

    # /saved_list
    if text.startswith("/saved_list"):
        items = list_saved_accounts(uid)
        if not items:
            await update.effective_message.reply_text("No saved accounts yet. Use /save to add one.")
            return

        text_out = "Your saved accounts:\n\n"
        rows = []
        for it in items:
            sid = it["id"]
            plat = it["platform"].upper()
            acc = it["account_name"]
            lbl = it.get("label") or ""

            if acc.startswith("http"):
                if plat == "FB":
                    name = acc.split('/')[-1] or "Page"
                elif plat == "YT":
                    name = acc.split('@')[-1] if '@' in acc else acc.split('/')[-1]
                else:
                    name = acc
                display = f"{sid}. [{plat}] {name}"
            else:
                display = f"{sid}. [{plat}] @{acc}"

            if lbl:
                display += f" — {lbl}"

            text_out += display + "\n"

            rows.append([
                InlineKeyboardButton(f"Send", callback_data=f"saved_sendcb_{sid}"),
                InlineKeyboardButton("Rename", callback_data=f"saved_rename_start_{sid}"),
                InlineKeyboardButton("Remove", callback_data=f"saved_removecb_{sid}")
            ])

        rows.append([InlineKeyboardButton("↩️ Back to Menu", callback_data="saved_menu")])
        await update.effective_message.reply_text(text_out, reply_markup=InlineKeyboardMarkup(rows))
        return

    # Default fallback
    await record_user_and_check_ban(update, context)
    await update.effective_message.reply_text("Use the menu or /help for commands.")