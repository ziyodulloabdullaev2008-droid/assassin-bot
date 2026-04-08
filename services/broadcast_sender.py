import asyncio
import random
import time

from telethon.errors import FloodWaitError

from core.config import API_HASH, API_ID
from core.state import app_state
from services.channel_post_service import resolve_entity_reference
from services.broadcast_service import (
    discard_broadcast_task,
    get_broadcast,
    mark_error,
    set_status,
    update_broadcast_fields,
)
from services.session_service import ensure_connected_client


def _parse_int_range(value, default_min: int, default_max: int) -> tuple[int, int]:
    if isinstance(value, int):
        return value, value

    text = str(value).strip()
    if "-" in text:
        try:
            left, right = text.split("-", 1)
            low = int(left.strip())
            high = int(right.strip())
            if low > 0 and high >= low:
                return low, high
        except Exception:
            pass
    else:
        try:
            parsed = int(text)
            if parsed > 0:
                return parsed, parsed
        except Exception:
            pass

    return default_min, default_max


def _parse_float_range(value, default_min: float, default_max: float) -> tuple[float, float]:
    text = str(value).strip()
    if "-" in text:
        try:
            left, right = text.split("-", 1)
            low = float(left.strip())
            high = float(right.strip())
            if low >= 0 and high >= low:
                return low, high
        except Exception:
            pass
    else:
        try:
            parsed = float(text)
            if parsed >= 0:
                return parsed, parsed
        except Exception:
            pass

    return default_min, default_max


def _pick_text(texts: list[str], text_mode: str, text_index: int) -> tuple[str, int]:
    if not texts:
        return "No text provided", text_index

    if text_mode == "random":
        return random.choice(texts), text_index

    current_text = texts[text_index % len(texts)]
    return current_text, text_index + 1


def _pick_content_item(items: list[dict], text_mode: str, text_index: int) -> tuple[dict, int]:
    if not items:
        return {"kind": "text", "text": "No text provided"}, text_index

    if text_mode == "random":
        return random.choice(items), text_index

    current_item = items[text_index % len(items)]
    return current_item, text_index + 1


def _normalize_chat_runtime(
    chat_ids: list,
    runtime_items: list[dict] | None,
) -> list[dict]:
    now = time.time()
    normalized_items: list[dict] = []
    existing_by_chat: dict[str, dict] = {}

    for item in runtime_items or []:
        if not isinstance(item, dict) or "chat_id" not in item:
            continue
        existing_by_chat[str(item["chat_id"])] = item

    for index, chat_id in enumerate(chat_ids):
        raw_item = existing_by_chat.get(str(chat_id), {})
        try:
            next_send_at = float(raw_item.get("next_send_at", now))
        except (TypeError, ValueError):
            next_send_at = now

        normalized_items.append(
            {
                "chat_id": chat_id,
                "next_send_at": next_send_at,
                "sent_count": int(raw_item.get("sent_count", 0) or 0),
                "failed_count": int(raw_item.get("failed_count", 0) or 0),
                "order": int(raw_item.get("order", index) or index),
            }
        )

    return normalized_items


def _pick_next_chat_entry(chat_runtime: list[dict]) -> dict | None:
    if not chat_runtime:
        return None

    earliest_at = min(float(item.get("next_send_at", 0.0) or 0.0) for item in chat_runtime)
    ready_items = [
        item
        for item in chat_runtime
        if abs(float(item.get("next_send_at", 0.0) or 0.0) - earliest_at) < 0.001
    ]
    if len(ready_items) == 1:
        return ready_items[0]

    return random.choice(ready_items)


async def _wait_while_paused(broadcast_id: int) -> str:
    while True:
        broadcast = get_broadcast(broadcast_id)
        if not broadcast:
            return "missing"

        status = broadcast.get("status")
        if status == "cancelled":
            return "cancelled"
        if status != "paused":
            return status

        await asyncio.sleep(1)


async def _sleep_with_controls(broadcast_id: int, seconds: float) -> str:
    finish_at = time.monotonic() + max(0.0, seconds)

    while True:
        status = await _wait_while_paused(broadcast_id)
        if status in {"cancelled", "missing"}:
            return status

        remaining = finish_at - time.monotonic()
        if remaining <= 0:
            return "running"

        await asyncio.sleep(min(1.0, remaining))


async def _notify_floodwait(user_id: int, account_name: str, wait_seconds: int) -> None:
    bot = app_state.bot
    if not bot:
        return

    try:
        await bot.send_message(
            user_id,
            (
                "\u26a0\ufe0f \u0423 \u0442\u0435\u0431\u044f FloodWait\n\n"
                f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442: <b>{account_name}</b>\n"
                f"\u0416\u0434\u0430\u0442\u044c: <b>{max(int(wait_seconds), 1)}</b> \u0441\u0435\u043a"
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass


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
    """Send a broadcast with real pacing, pause/resume support, and floodwait handling."""
    if texts is None:
        texts = [text or "No text provided"]

    if broadcast_id is None:
        return

    try:
        client = await ensure_connected_client(
            user_id,
            account_number,
            api_id=API_ID,
            api_hash=API_HASH,
        )
        if not client:
            await mark_error(
                broadcast_id,
                "Account disconnected or session is no longer authorized",
                "account_disconnected",
            )
            return

        broadcast = get_broadcast(broadcast_id)
        if not broadcast:
            return

        chat_ids = list(broadcast.get("chat_ids") or chat_ids or [])
        texts = list(broadcast.get("texts") or texts or ["No text provided"])
        content_items = list(broadcast.get("content_items") or [])
        text_mode = broadcast.get("text_mode", text_mode)
        parse_mode = broadcast.get("parse_mode", parse_mode)
        account_name = str(
            broadcast.get("account_name") or f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442 {account_number}"
        )

        if not content_items:
            content_items = [
                {"kind": "text", "text": item_text}
                for item_text in texts
            ]

        sent_count = int(broadcast.get("sent_chats", 0) or 0)
        failed_count = int(broadcast.get("failed_count", 0) or 0)
        processed_count = int(
            broadcast.get("processed_count", sent_count + failed_count) or 0
        )
        text_index = int(broadcast.get("text_index", 0) or 0)
        next_global_send_at = float(broadcast.get("next_global_send_at", 0.0) or 0.0)
        chat_runtime = _normalize_chat_runtime(
            chat_ids,
            broadcast.get("chat_runtime"),
        )
        if not chat_ids:
            await mark_error(broadcast_id, "No chats configured for broadcast", "no_chats")
            return

        while True:
            broadcast = get_broadcast(broadcast_id)
            if not broadcast:
                return

            status = broadcast.get("status")
            if status == "cancelled":
                return

            if status == "paused":
                resumed_status = await _wait_while_paused(broadcast_id)
                if resumed_status in {"cancelled", "missing"}:
                    return
                continue

            count = int(broadcast.get("count", count) or count or 1)
            interval_value = broadcast.get("interval_value", broadcast.get("interval_minutes", interval_minutes))
            chat_pause_value = broadcast.get("chat_pause", "1-3")
            chat_runtime = _normalize_chat_runtime(
                chat_ids,
                broadcast.get("chat_runtime") or chat_runtime,
            )
            next_global_send_at = float(
                broadcast.get("next_global_send_at", next_global_send_at) or 0.0
            )

            if processed_count >= count:
                await set_status(broadcast_id, "completed")
                return

            chat_entry = _pick_next_chat_entry(chat_runtime)
            if not chat_entry:
                await mark_error(broadcast_id, "No chats configured for broadcast", "no_chats")
                return

            wait_until = max(
                float(chat_entry.get("next_send_at", 0.0) or 0.0),
                next_global_send_at,
            )
            wait_seconds = max(0.0, wait_until - time.time())
            if wait_seconds > 0:
                sleep_status = await _sleep_with_controls(broadcast_id, wait_seconds)
                if sleep_status in {"cancelled", "missing"}:
                    return
                continue

            chat_id = chat_entry["chat_id"]
            current_item, next_text_index = _pick_content_item(
                content_items,
                text_mode,
                text_index,
            )
            last_error = None
            send_succeeded = False

            try:
                if current_item.get("kind") == "forward":
                    source_entity = await resolve_entity_reference(
                        client,
                        current_item["source_ref"],
                    )
                    source_message = await client.get_messages(
                        source_entity,
                        ids=int(current_item["message_id"]),
                    )
                    if not source_message:
                        raise ValueError("Source channel message not found")
                    # Resend the source post as a fresh message so Telegram
                    # does not show the original channel in the recipient chat.
                    await client.send_message(chat_id, source_message)
                else:
                    await client.send_message(
                        chat_id,
                        current_item.get("text", "No text provided"),
                        parse_mode=parse_mode,
                    )
                sent_count += 1
                text_index = next_text_index
                send_succeeded = True
            except FloodWaitError as exc:
                wait_seconds = max(int(exc.seconds), 1)
                await update_broadcast_fields(
                    broadcast_id,
                    last_wait_seconds=wait_seconds,
                    last_error="FloodWait",
                )
                await _notify_floodwait(user_id, account_name, wait_seconds)
                sleep_status = await _sleep_with_controls(
                    broadcast_id,
                    wait_seconds,
                )
                if sleep_status in {"cancelled", "missing"}:
                    return
                continue
            except Exception as exc:
                error_text = str(exc).lower()
                if (
                    "floodwait" in error_text
                    or "too many requests" in error_text
                    or "420" in error_text
                ):
                    await update_broadcast_fields(
                        broadcast_id,
                        last_error=str(exc),
                    )
                    await _notify_floodwait(user_id, account_name, 30)
                    sleep_status = await _sleep_with_controls(broadcast_id, 30)
                    if sleep_status in {"cancelled", "missing"}:
                        return
                    continue

                failed_count += 1
                text_index = next_text_index
                last_error = str(exc)
                chat_entry["failed_count"] = int(chat_entry.get("failed_count", 0) or 0) + 1

            processed_count += 1
            interval_min, interval_max = _parse_int_range(interval_value, 1, 1)
            wait_minutes = random.randint(interval_min, interval_max)
            chat_entry["next_send_at"] = time.time() + (wait_minutes * 60)
            if send_succeeded:
                chat_entry["sent_count"] = int(chat_entry.get("sent_count", 0) or 0) + 1

            pause_min, pause_max = _parse_float_range(chat_pause_value, 1.0, 3.0)
            next_global_send_at = time.time() + random.uniform(pause_min, pause_max)

            await update_broadcast_fields(
                broadcast_id,
                sent_chats=sent_count,
                planned_count=count,
                failed_count=failed_count,
                processed_count=processed_count,
                text_index=text_index,
                chat_runtime=chat_runtime,
                next_global_send_at=next_global_send_at,
                last_interval_minutes=wait_minutes,
                current_chat_id=chat_id,
                last_error=last_error,
            )

    except Exception as exc:
        await mark_error(broadcast_id, str(exc), "send_failed")
    finally:
        discard_broadcast_task(broadcast_id)
