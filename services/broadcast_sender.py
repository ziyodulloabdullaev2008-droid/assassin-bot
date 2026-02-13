import asyncio
import random
from datetime import datetime, timedelta, timezone

from core.state import app_state
from services.broadcast_config_service import get_broadcast_config
from services.broadcast_service import update_progress, set_status, mark_error
from services.join_service import enqueue_join, should_enqueue_from_error


async def schedule_broadcast_send(
    user_id: int,
    account_number: int,
    chat_ids: list,
    texts: list = None,
    text: str = None,
    interval_minutes: int = 1,
    count: int = 1,
    broadcast_id: int = None,
    parse_mode: str = "HTML",
    text_mode: str = "random",
):
    """Schedule a broadcast with per-chat pacing and per-round interval."""
    if texts is None:
        if text is None:
            text = "No text provided"
        texts = [text]

    text_index = 0

    try:
        async with app_state.user_authenticated_lock:
            client = app_state.user_authenticated.get(user_id, {}).get(account_number)

        if not client:
            if broadcast_id is not None and broadcast_id in app_state.active_broadcasts:
                await mark_error(broadcast_id, "Account disconnected", "account_disconnected")
            return

        try:
            await client.get_me()
            current_time = datetime.now(timezone.utc)
        except Exception:
            current_time = datetime.now()

        total_chats = len(chat_ids)
        total_messages = total_chats * count

        config = get_broadcast_config(user_id)
        interval_config = config.get("interval", interval_minutes)

        if isinstance(interval_config, int):
            interval_min = interval_max = interval_config
        elif "-" in str(interval_config):
            try:
                parts = str(interval_config).split("-")
                interval_min = int(parts[0].strip())
                interval_max = int(parts[1].strip())
            except Exception:
                interval_min = interval_max = int(interval_minutes)
        else:
            interval_min = interval_max = int(interval_config)

        chat_pause_config = config.get("chat_pause", "1-3")
        if "-" in str(chat_pause_config):
            try:
                base_min_pause, base_max_pause = map(float, str(chat_pause_config).split("-"))
            except Exception:
                base_min_pause = base_max_pause = 2.0
        else:
            base_min_pause = base_max_pause = float(chat_pause_config)

        limit_count = int(config.get("plan_limit_count", 0) or 0)
        limit_rest = float(config.get("plan_limit_rest", 0) or 0)
        scheduled_since_rest = 0

        # Initial schedule time per chat (staggered by chat_pause)
        base_cursor = current_time + timedelta(seconds=10)
        chat_next = {}
        for chat_id in chat_ids:
            chat_next[chat_id] = base_cursor
            base_cursor += timedelta(seconds=random.uniform(base_min_pause, base_max_pause))

        # Fixed random interval per chat (minutes between messages in that chat)
        chat_interval = {}
        for chat_id in chat_ids:
            chat_interval[chat_id] = random.uniform(interval_min, interval_max)

        sent_count = 0
        failed_count = 0
        cancelled_by_user = False

        for msg_index in range(count):
            if cancelled_by_user:
                break

            for chat_id in chat_ids:
                if broadcast_id is not None and broadcast_id in app_state.active_broadcasts:
                    status = app_state.active_broadcasts[broadcast_id]["status"]
                    if status == "cancelled":
                        cancelled_by_user = True
                        break
                    if status == "paused":
                        return

                if text_mode == "random":
                    current_text = random.choice(texts)
                else:
                    current_text = texts[text_index % len(texts)]
                    text_index += 1

                try:
                    await client.send_message(
                        chat_id,
                        current_text,
                        schedule=chat_next[chat_id],
                        parse_mode=parse_mode,
                    )
                    sent_count += 1
                    if broadcast_id is not None and broadcast_id in app_state.active_broadcasts:
                        await update_progress(broadcast_id, sent_count, sent_count)
                except Exception as e:
                    error_str = str(e).lower()
                    if "floodwait" in error_str or "too many requests" in error_str or "420" in error_str:
                        pass
                    if should_enqueue_from_error(error_str):
                        await enqueue_join(user_id, chat_id=chat_id)
                    failed_count += 1

                scheduled_since_rest += 1
                if limit_count > 0 and limit_rest > 0 and scheduled_since_rest >= limit_count:
                    await asyncio.sleep(limit_rest * 60)
                    scheduled_since_rest = 0

                # Advance next schedule time for this chat by its own interval
                chat_next[chat_id] = chat_next[chat_id] + timedelta(minutes=chat_interval[chat_id])

        if broadcast_id is not None and broadcast_id in app_state.active_broadcasts:
            if cancelled_by_user:
                await set_status(broadcast_id, "stopped")
            else:
                await set_status(broadcast_id, "completed")

    except Exception as e:
        if broadcast_id is not None and broadcast_id in app_state.active_broadcasts:
            await mark_error(broadcast_id, str(e), "send_failed")
