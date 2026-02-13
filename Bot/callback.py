import io
import logging
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import ContextTypes
from .settings import *
from .helpers import *
from .ui import *
from .fetch import *
from Utils.utils import *

logger = logging.getLogger(__name__)

# Global AI tasks dict (needed for cancellation)
from .ai import ai_tasks

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    uid = user.id if user else None

    # record_user_and_check_ban is already called in message handler, but we call it here too for safety
    from helpers import record_user_and_check_ban
    await record_user_and_check_ban(update, context)
    data = query.data or ""

    # AI Analysis Button
    if data.startswith("ai_analyze_"):
        _, _, plat_acc = data.partition("ai_analyze_")
        platform, _, account = plat_acc.partition("_")

        badge = get_user_badge(uid)

        if badge['name'] not in ('Diamond', 'Admin'):
            cooldown_msg = check_and_increment_cooldown(uid)
            if cooldown_msg:
                await query.answer("AI limit reached! Invite friends to upgrade.", show_alert=True)
                return

        await safe_edit(query, text="🤖 Analyzing with Nigerian fire...")

        posts = context.user_data.get(f"last_ai_context_{platform}_{account}", [])
        analysis = await call_social_ai(platform, account, posts)

        final_text = f"🤖 <b>AI Insight</b>:\n\n{analysis}"

        if badge['name'] in ('Diamond', 'Admin'):
            final_text += "\n\n💎 <b>You can ask me follow-up questions about these posts!</b>\nJust reply to this message."
            context.user_data["ai_chat_active"] = {
                "platform": platform,
                "account": account,
                "posts": posts
            }

        await safe_edit(query, text=final_text, parse_mode="HTML")
        return

    # Saved quick send
    if data.startswith("saved_sendcb_"):
        _, _, sid_s = data.partition("saved_sendcb_")
        try:
            sid = int(sid_s)
        except:
            await context.bot.edit_message_text(
                chat_id=query.message.chat.id,
                message_id=query.message.message_id,
                text="Invalid saved id."
            )
            return

        saved = get_saved_account(uid, sid)
        if not saved:
            await context.bot.edit_message_text(
                chat_id=query.message.chat.id,
                message_id=query.message.message_id,
                text="Saved account not found."
            )
            return

        await handle_fetch_and_ai(update, context, saved["platform"], saved["account_name"], query)
        return

    # Confirm (send) single post
    if data.startswith("confirm_post_"):
        await query.answer()

        parts = data.split("_")
        if len(parts) < 5:
            await query.answer("Invalid callback.", show_alert=True)
            return

        platform = parts[2]
        account = "_".join(parts[3:-1])
        idx = int(parts[-1])

        user_data_key = f"pending_posts_{platform}_{account}"
        pending = context.user_data.get(user_data_key)
        if not pending or pending.get("index", 0) != idx:
            logger.info(f"Stale click detected: expected index {pending.get('index', 'None')}, got {idx} for @{account}")
            await query.answer("Post expired or out of order.", show_alert=True)
            await send_next_post_with_confirmation(update, context, platform, account)
            return

        pending["has_sent_single"] = True
        context.user_data[user_data_key] = pending
        logger.info("Flag set: has_sent_single=True for %s/%s", platform, account)

        post = pending["posts"][idx]
        media_bytes = await download_media(post.get("media_url"))
        if not media_bytes:
            try:
                await context.bot.edit_message_caption(
                    chat_id=query.message.chat.id,
                    message_id=query.message.message_id,
                    caption=(query.message.caption or "") + "\n\n❌ Media failed to load",
                    reply_markup=None
                )
            except Exception:
                try:
                    await context.bot.edit_message_text(
                        chat_id=query.message.chat.id,
                        message_id=query.message.message_id,
                        text=(query.message.text or "") + "\n\n❌ Media failed to load",
                        reply_markup=None
                    )
                except Exception as e:
                    logging.warning("Failed to mark preview as failed: %s", e)

            pending["index"] += 1
            context.user_data[user_data_key] = pending
            await send_next_post_with_confirmation(query, context, platform, account)
            return

        view_text = {"x": "View on X🐦", "fb": "View on Facebook 🌐", "ig": "View on Instagram 📸"}.get(platform, "View Post 🔗")
        link_html = f"<a href='{post.get('post_url','')}'>{view_text}</a>" if post.get('post_url') else ""
        caption = (post.get("caption", "") or "")[:1024]
        full_caption = f"{link_html}\n\n{caption}" if link_html else caption

        bio = io.BytesIO(media_bytes)
        if post.get("is_video"):
            bio.name = "video.mp4"
            sent = await query.message.reply_video(video=bio, caption=full_caption, parse_mode="HTML")
        else:
            bio.name = "photo.jpg"
            sent = await query.message.reply_photo(photo=bio, caption=full_caption, parse_mode="HTML")

        await schedule_delete(context, sent.chat.id, sent.message_id)

        await safe_edit(query, text=full_caption + "\n\n✅ <b>Sent!</b>", parse_mode="HTML", reply_markup=None)

        pending["index"] += 1
        context.user_data[user_data_key] = pending

        await send_next_post_with_confirmation(query, context, platform, account)
        return

    # Send all remaining
    if data.startswith("send_all_"):
        query = update.callback_query
        await query.answer()

        parts = data.split("_")
        if len(parts) < 4:
            await query.answer("Invalid callback.", show_alert=True)
            return
        platform = parts[2]
        account = "_".join(parts[3:])

        user_data_key = f"pending_posts_{platform}_{account}"
        pending = context.user_data.get(user_data_key)
        if not pending:
            await query.answer("No pending posts.", show_alert=True)
            return

        if pending.get("has_sent_single"):
            await query.answer("Bulk send disabled after single send.", show_alert=True)
            await send_next_post_with_confirmation(update, context, platform, account)
            return

        pending["has_sent_single"] = True
        context.user_data[user_data_key] = pending
        logging.info("send_all: has_sent_single set for %s/%s", platform, account)

        posts = pending.get("posts", []) or []
        current_idx = int(pending.get("index", 0))
        total_posts = int(pending.get("total", len(posts)))

        try:
            preview_text = (query.message.caption or query.message.text or "") + "\n\n🚀 Sending all remaining..."
            try:
                await asyncio.wait_for(
                    context.bot.edit_message_caption(
                        chat_id=query.message.chat.id,
                        message_id=query.message.message_id,
                        caption=preview_text,
                        reply_markup=None
                    ),
                    timeout=6.0
                )
            except (asyncio.TimeoutError, Exception):
                await asyncio.wait_for(
                    context.bot.edit_message_text(
                        chat_id=query.message.chat.id,
                        message_id=query.message.message_id,
                        text=preview_text,
                        reply_markup=None
                    ),
                    timeout=6.0
                )
        except Exception as e:
            logging.warning("send_all: could not mark preview as sending: %s", e)

        total_sent = 0

        for idx in range(current_idx, min(total_posts, len(posts))):
            post = posts[idx]
            try:
                media_bytes = await download_media(post.get("media_url"))
            except Exception as e:
                logging.warning("send_all: download_media exception for idx %s: %s", idx, e)
                media_bytes = None

            if not media_bytes:
                logging.info("send_all: skipping idx %s (media failed)", idx)
                pending["index"] = idx + 1
                context.user_data[user_data_key] = pending
                continue

            view_text = {
                "x": "View on X🐦",
                "fb": "View on Facebook 🌐",
                "ig": "View on Instagram 📸"
            }.get(platform, "View Post 🔗")
            link_html = f"<a href='{post.get('post_url','')}'>{view_text}</a>" if post.get('post_url') else ""
            caption = (post.get("caption") or "")[:1024]
            full_caption = f"{link_html}\n\n{caption}" if link_html else caption

            bio = io.BytesIO(media_bytes)
            try:
                if post.get("is_video"):
                    bio.name = "video.mp4"
                    sent = await context.bot.send_video(
                        chat_id=query.message.chat.id,
                        video=bio,
                        caption=full_caption,
                        parse_mode="HTML"
                    )
                else:
                    bio.name = "photo.jpg"
                    sent = await context.bot.send_photo(
                        chat_id=query.message.chat.id,
                        photo=bio,
                        caption=full_caption,
                        parse_mode="HTML"
                    )

                try:
                    await schedule_delete(context, sent.chat.id, sent.message_id)
                except TypeError:
                    try:
                        schedule_delete(context, sent.chat.id, sent.message_id)
                    except Exception:
                        logging.debug("schedule_delete failed for sent message.")
                except Exception:
                    logging.debug("schedule_delete failed for sent message.")

                total_sent += 1
                logging.info("send_all: sent idx %s for %s/%s", idx, platform, account)

            except Exception as e:
                logging.exception("send_all: failed to send idx %s: %s", idx, e)

            pending["index"] = idx + 1
            context.user_data[user_data_key] = pending

            await asyncio.sleep(5)

        pending["index"] = pending.get("total", pending.get("index", 0))
        context.user_data[user_data_key] = pending

        try:
            done_text = (query.message.caption or query.message.text or "") + f"\n\n✅ Sent all remaining ({total_sent} posts)!"
            try:
                await asyncio.wait_for(
                    context.bot.edit_message_caption(
                        chat_id=query.message.chat.id,
                        message_id=query.message.message_id,
                        caption=done_text,
                        reply_markup=None
                    ),
                    timeout=6.0
                )
            except (asyncio.TimeoutError, Exception):
                await asyncio.wait_for(
                    context.bot.edit_message_text(
                        chat_id=query.message.chat.id,
                        message_id=query.message.message_id,
                        text=done_text,
                        reply_markup=None
                    ),
                    timeout=6.0
                )
        except Exception as e:
            logging.warning("send_all: could not mark preview done: %s", e)

        if total_sent > 0:
            badge = get_user_badge(uid)
            await send_ai_button(query.message, total_sent, platform, account, badge)

        context.user_data.pop(user_data_key, None)
        return

    if data.startswith("skip_post_"):
        _, _, plat_acc_idx = data.partition("skip_post_")
        platform, _, acc_idx = plat_acc_idx.partition("_")
        account, _, idx_s = acc_idx.partition("_")
        try:
            idx = int(idx_s)
        except:
            await query.answer("Invalid index.", show_alert=True)
            return

        user_data_key = f"pending_posts_{platform}_{account}"
        pending = context.user_data.get(user_data_key)
        if not pending or pending.get("index", 0) != idx:
            await query.answer("Post expired or out of order.", show_alert=True)
            await send_next_post_with_confirmation(query, context, platform, account)
            return

        try:
            await context.bot.edit_message_caption(
                chat_id=query.message.chat.id,
                message_id=query.message.message_id,
                caption=(query.message.caption or "") + "\n\n⏭️ <b>Skipped!</b>",
                parse_mode="HTML",
                reply_markup=None
            )
        except Exception:
            await context.bot.edit_message_text(
                chat_id=query.message.chat.id,
                message_id=query.message.message_id,
                text=(query.message.text or "") + "\n\n⏭️ <b>Skipped!</b>",
                parse_mode="HTML",
                reply_markup=None
            )

        pending["index"] += 1
        context.user_data[user_data_key] = pending

        await send_next_post_with_confirmation(query, context, platform, account)
        return

    # Cancel remaining posts
    if data.startswith("cancel_posts_"):
        _, _, plat_acc = data.partition("cancel_posts_")
        platform, _, account = plat_acc.partition("_")

        user_data_key = f"pending_posts_{platform}_{account}"
        pending = context.user_data.pop(user_data_key, None)

        sent_count = pending["index"] if pending else 0
        total = pending["total"] if pending else 0

        await safe_edit(query, text=(query.message.caption or "") + f"\n\n❌ Cancelled. Sent {sent_count}/{total} posts.", parse_mode="HTML", reply_markup=None)
        await query.message.reply_text(f"❌ Sending cancelled. Sent {sent_count}/{total} posts.")

        if pending and pending["index"] > 0:
            badge = get_user_badge(uid)
            await send_ai_button(query.message, pending["index"], platform, account, badge)
        return

    # Admin confirmations
    if data.startswith("confirm_ban_"):
        if not is_admin(uid):
            await query.edit_message_text("❌ Admins only.")
            return
        _, _, tid_s = data.partition("confirm_ban_")
        try:
            tid = int(tid_s)
        except:
            await query.edit_message_text("Invalid id.")
            return
        ban_tg_user(tid)
        await query.edit_message_text(f"User {tid} has been banned.", reply_markup=build_admin_menu())
        return

    if data.startswith("confirm_reset_cooldown_"):
        if not is_admin(uid):
            await query.edit_message_text("❌ Admins only.")
            return
        _, _, tid_s = data.partition("confirm_reset_cooldown_")
        try:
            tid = int(tid_s)
        except:
            await query.edit_message_text("Invalid id.")
            return
        reset_cooldown(tid)
        await query.edit_message_text(f"Cooldown reset for user {tid}.", reply_markup=build_admin_menu())
        return

    if data.startswith("confirm_unban_"):
        if not is_admin(uid):
            await query.edit_message_text("❌ Admins only.")
            return
        _, _, tid_s = data.partition("confirm_unban_")
        try:
            tid = int(tid_s)
        except:
            await query.edit_message_text("Invalid id.")
            return
        unban_tg_user(tid)
        await query.edit_message_text(f"User {tid} unbanned.", reply_markup=build_admin_menu())
        return

    if data == "confirm_export_csv":
        if not is_admin(uid):
            await query.edit_message_text("❌ Admins only.")
            return
        await query.edit_message_text("Preparing CSV...")
        users = list_all_tg_users(limit=10000)
        csv_bytes = users_to_csv_bytes(users)
        bio = io.BytesIO(csv_bytes)
        bio.name = "tg_users.csv"
        try:
            await context.bot.send_document(chat_id=uid, document=InputFile(bio))
            await query.edit_message_text("CSV sent.")
        except Exception as e:
            await query.edit_message_text(f"Failed to send CSV: {e}")
        return

    # Menu navigation
    if data == "menu_main":
        await query.edit_message_text("Main menu:", reply_markup=build_main_menu())
        return
    if data == "dashboard":
        # Import dashboard_command from commands to avoid circular import
        from Bot.commands import dashboard_command
        await dashboard_command(update, context)
        return
    if data == "menu_x":
        context.user_data["platform"] = "x"
        context.user_data["awaiting_username"] = True
        await query.edit_message_text("Send the X/Twitter <b>user ID</b> (the long numeric ID, not username).\n\n"
        "How to get it:\n"
        "Go to <a href='https://tweethunter.io/twitter-id-converter'>Twitter Username Converter</a>\n"
        "Enter the @username there → it gives you the numeric ID.\n"
        "Copy and send that number here.\n\n",
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=build_back_markup("menu_main"))
        return
    if data == "menu_fb":
        context.user_data["platform"] = "fb"
        context.user_data["awaiting_username"] = True
        await query.edit_message_text(
            "Send the Facebook page username or name (e.g. nike, coca-cola):\n\n"
            "Note: Only public pages and send direct page link for accuracy(recommended)! only the posted pictures is fetched",
            reply_markup=build_back_markup("menu_main")
        )
        return
    if data == "menu_ig":
        context.user_data["platform"] = "ig"
        context.user_data["awaiting_username"] = True
        await query.edit_message_text("Send the Instagram username (without @):", reply_markup=build_back_markup("menu_main"))
        return
    if data =="help":
        # Import help_command
        from Bot.commands import help_command
        await help_command(update, context)
        return

    if data == "menu_yt":
        context.user_data["platform"] = "yt"
        context.user_data["awaiting_username"] = True
        await query.edit_message_text("Send YouTube channel username (e.g. Seyivibe) or search query:", reply_markup=build_back_markup("menu_main"))
        return

    if data == "saved_menu":
        await query.edit_message_text("Saved usernames:", reply_markup=build_saved_menu())
        return
    if data == "saved_add_start":
        context.user_data["awaiting_save"] = True
        await query.edit_message_text("Send: <platform> <username or ID> [label]\nExample: `x elonmusk fav`", reply_markup=build_back_markup("saved_menu"))
        return
    if data == "saved_list" or data.startswith("saved_page_"):
        page = 0
        if data.startswith("saved_page_"):
            page = int(data[len("saved_page_"):])

        items = list_saved_accounts(uid)
        if not items:
            await query.edit_message_text("You no get any saved account. Save page link when saving in fb", reply_markup=build_saved_menu())
            return

        per_page = 4
        start = page * per_page
        end = start + per_page
        page_items = items[start:end]
        total_pages = (len(items) + per_page - 1) // per_page

        text = f"Your saved accounts ({page+1}/{total_pages}):\n\n"
        rows = []
        for it in page_items:
            sid = it["id"]
            plat = it["platform"].upper()
            acc = it["account_name"]
            lbl = it.get("label") or ""
            display = f"{sid}. [{plat}] @{acc}"
            if lbl:
                display += f" — {lbl}"
            text += display + "\n"

            rows.append([
                InlineKeyboardButton("Send", callback_data=f"saved_sendcb_{sid}"),
                InlineKeyboardButton("Rename", callback_data=f"saved_rename_start_{sid}"),
                InlineKeyboardButton("Remove", callback_data=f"saved_removecb_{sid}")
            ])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"saved_page_{page-1}"))
        if end < len(items):
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"saved_page_{page+1}"))
        if nav:
            rows.append(nav)

        rows.append([InlineKeyboardButton("↩️ Back", callback_data="saved_menu")])

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))
        return
    if data.startswith("saved_removecb_"):
        _, _, sid_s = data.partition("saved_removecb_")
        try:
            sid = int(sid_s)
        except:
            await query.edit_message_text("Invalid id.")
            return
        ok = remove_saved_account(uid, sid)
        if ok:
            await query.edit_message_text(f"Removed saved account {sid}.", reply_markup=build_saved_menu())
        else:
            await query.edit_message_text("Could not remove saved account.", reply_markup=build_saved_menu())
        return

    if data.startswith("saved_rename_start_"):
        _, _, sid_s = data.partition("saved_rename_start_")
        try:
            sid = int(sid_s)
        except:
            await query.edit_message_text("Invalid id.")
            return
        context.user_data["awaiting_rename_id"] = sid
        await query.edit_message_text("Send the new label for this saved account (single message):", reply_markup=build_back_markup("saved_list"))
        return

    # Admin panel callbacks
    if data.startswith("admin_"):
        if not is_admin(uid):
            await query.edit_message_text("❌ Admins only.")
            return

        if data.startswith("admin_list_users_"):
            _, _, page_s = data.partition("admin_list_users_")
            page = int(page_s or "0")
            users = list_all_tg_users(limit=10000)
            total = len(users)
            start = page * PAGE_SIZE_USERS
            end = start + PAGE_SIZE_USERS
            page_users = users[start:end]
            text = f"Users (page {page+1}):\n\n"
            rows = []
            for u in page_users:
                tid = u.get('telegram_id')
                text += f"- {u.get('first_name') or ''} ({tid}) banned={u.get('is_banned')} reqs={u.get('request_count')} invites={u.get('invite_count')}\n"
                rows.append([
                    InlineKeyboardButton(f"Stats {tid}", callback_data=f"admin_user_stats_{tid}"),
                    InlineKeyboardButton(f"Reset CD {tid}", callback_data=f"admin_reset_cooldown_start_{tid}"),
                    InlineKeyboardButton(f"Ban {tid}" if not u.get('is_banned') else f"Unban {tid}", callback_data=f"admin_{'ban' if not u.get('is_banned') else 'unban'}_start_{tid}")
                ])
            nav_row = []
            if page > 0:
                nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin_list_users_{page-1}"))
            if end < total:
                nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_list_users_{page+1}"))
            if nav_row:
                rows.append(nav_row)
            rows.append([InlineKeyboardButton("↩️ Back", callback_data="admin_back")])
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))
            return

        if data.startswith("admin_user_stats_"):
            _, _, tid_s = data.partition("admin_user_stats_")
            try:
                tid = int(tid_s)
            except:
                await query.edit_message_text("Invalid id.")
                return
            stats = get_user_stats(tid)
            if not stats:
                await query.edit_message_text("User not found.")
                return
            user = stats['user']
            badge = stats['badge']
            rl = stats['rate_limits']
            saves = stats['save_count']
            text = f"Stats for {user.get('first_name', 'User')} ({tid})\n\n"
            text += f"Joined: {user.get('joined_at')}\nRequests: {user.get('request_count', 0)}\nInvites: {user.get('invite_count', 0)}\nBanned: {bool(user.get('is_banned'))}\n"
            text += f"Badge: {badge['emoji']} {badge['name']}\nSaves: {saves}/{badge['save_slots'] if isinstance(badge['save_slots'], int) else '∞'}\n\n"
            text += "Cooldowns:\n"
            text += f"Minute: {rl.get('minute_count',0)}/{badge['limits'].get('min','∞')} (reset: {rl.get('minute_reset')})\n"
            text += f"Hour: {rl.get('hour_count',0)}/{badge['limits'].get('hour','∞')} (reset: {rl.get('hour_reset')})\n"
            text += f"Day: {rl.get('day_count',0)}/{badge['limits'].get('day','∞')} (reset: {rl.get('day_reset')})\n"
            await query.edit_message_text(text, reply_markup=build_back_markup("admin_list_users_0"))
            return

        if data.startswith("admin_reset_cooldown_start_"):
            _, _, tid_s = data.partition("admin_reset_cooldown_start_")
            try:
                tid = int(tid_s)
            except:
                await query.edit_message_text("Invalid id.")
                return
            await query.edit_message_text(f"Confirm reset cooldown for {tid}?", reply_markup=build_confirm_markup("reset_cooldown", tid))
            return

        if data == "admin_leaderboard":
            await query.edit_message_text("Loading leaderboard...", reply_markup=build_back_markup("admin_back"))
            return

        if data == "admin_back":
            await query.edit_message_text("Admin panel:", reply_markup=build_admin_menu())
            return

        if data == "admin_export_csv":
            await query.edit_message_text("Export users to CSV? Confirm to proceed.", reply_markup=build_confirm_markup("export_csv"))
            return

        if data == "admin_broadcast_start":
            context.user_data["admin_broadcast"] = True
            await query.edit_message_text("Send the message to broadcast. Use /cancel to abort.", reply_markup=build_cancel_and_back("admin_broadcast_cancel", "admin_back"))
            return

        if data == "admin_broadcast_cancel":
            context.user_data.pop("admin_broadcast", None)
            await query.edit_message_text("Broadcast cancelled.", reply_markup=build_admin_menu())
            return

        if data == "admin_ai_start":
            context.user_data["awaiting_manual_ai"] = True
            await query.edit_message_text(
                "🧠 <b>Manual AI Analysis</b>\n\n"
                "Send the text/post/caption you want me to analyze with Groq AI.\n"
                "If you just fetched posts, I can auto-use them if you send nothing.\n\n"
                "/cancel to abort.",
                parse_mode="HTML",
                reply_markup=build_back_markup("admin_back")
            )
            return

    if data.startswith("page_"):
        parts = data.split("_", 3)
        if len(parts) < 4:
            await query.edit_message_text("Invalid page data.")
            return
        page = int(parts[1])
        platform = parts[2]
        account = parts[3]
        posts = fetch_latest_urls(platform, account) if platform == "x" else fetch_ig_urls(account)
        start = page * POSTS_PER_PAGE
        end = start + POSTS_PER_PAGE
        page_posts = posts[start:end]
        total_pages = max(1, (len(posts) + POSTS_PER_PAGE - 1) // POSTS_PER_PAGE)
        msg = f"Page {page+1} of {total_pages}\n\n"
        for p in page_posts:
            if isinstance(p, dict):
                msg += f"{p.get('url')}\n"
            else:
                msg += f"{p}\n"
        keyboard = []
        if page > 0:
            keyboard.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"page_{page-1}_{platform}_{account}"))
        if page < total_pages - 1:
            keyboard.append(InlineKeyboardButton("Next ➡️", callback_data=f"page_{page+1}_{platform}_{account}"))
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup([keyboard]) if keyboard else None)
        return

    await query.edit_message_text("Unknown action or handled elsewhere.")