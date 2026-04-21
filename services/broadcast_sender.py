import asyncio
import html
import random
import time
from datetime import datetime, timezone

from telethon.errors import FloodWaitError
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

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


def _distribute_chat_targets(chat_ids: list, total_count: int) -> dict[str, int]:
    if not chat_ids:
        return {}

    safe_total = max(int(total_count or 0), 0)
    base = safe_total // len(chat_ids)
    extra = safe_total % len(chat_ids)
    targets: dict[str, int] = {}
    for index, chat_id in enumerate(chat_ids):
        targets[str(chat_id)] = base + (1 if index < extra else 0)
    return targets


def _chat_attempts(item: dict) -> int:
    return int(item.get("sent_count", 0) or 0) + int(item.get("failed_count", 0) or 0)


def _chat_has_quota(item: dict) -> bool:
    target_count = max(int(item.get("target_count", 0) or 0), 0)
    return _chat_attempts(item) < target_count


def _normalize_chat_runtime(
    chat_ids: list,
    runtime_items: list[dict] | None,
    total_count: int,
) -> list[dict]:
    now = time.time()
    normalized_items: list[dict] = []
    existing_by_chat: dict[str, dict] = {}
    targets_by_chat = _distribute_chat_targets(chat_ids, total_count)

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
                "name": str(raw_item.get("name") or raw_item.get("chat_name") or chat_id),
                "status": str(raw_item.get("status") or "active"),
                "last_error": str(raw_item.get("last_error") or ""),
                "last_error_at": float(raw_item.get("last_error_at", 0.0) or 0.0),
                "target_count": int(targets_by_chat.get(str(chat_id), 0)),
            }
        )

    return normalized_items


def _pick_next_chat_entry(chat_runtime: list[dict]) -> dict | None:
    active_items = [
        item
        for item in chat_runtime
        if str(item.get("status") or "active") == "active" and _chat_has_quota(item)
    ]
    if not active_items:
        return None

    earliest_at = min(float(item.get("next_send_at", 0.0) or 0.0) for item in active_items)
    ready_items = [
        item
        for item in active_items
        if abs(float(item.get("next_send_at", 0.0) or 0.0) - earliest_at) < 0.001
    ]
    if len(ready_items) == 1:
        return ready_items[0]

    return random.choice(ready_items)


def _should_apply_global_pace(
    chat_entry: dict,
    chat_runtime: list[dict],
    next_global_send_at: float,
    now: float,
) -> bool:
    if next_global_send_at <= now:
        return False

    # During the first pass through chats, the global pace spaces out
    # the initial messages so they do not go out in a burst.
    if _chat_attempts(chat_entry) == 0:
        return True

    # After the first pass, pace only matters when multiple chats are due
    # within the same pace window and need to be separated.
    for item in chat_runtime:
        if item is chat_entry:
            continue
        if str(item.get("status") or "active") != "active" or not _chat_has_quota(item):
            continue
        try:
            other_next_send_at = float(item.get("next_send_at", 0.0) or 0.0)
        except (TypeError, ValueError):
            other_next_send_at = 0.0
        if other_next_send_at <= next_global_send_at:
            return True

    return False


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


async def _sleep_with_controls(
    broadcast_id: int,
    seconds: float,
    *,
    expected_runtime_revision: int | None = None,
) -> str:
    finish_at = time.monotonic() + max(0.0, seconds)

    while True:
        broadcast = get_broadcast(broadcast_id)
        if not broadcast:
            return "missing"
        if (
            expected_runtime_revision is not None
            and int(broadcast.get("runtime_revision", 0) or 0) != expected_runtime_revision
        ):
            return "changed"

        status = broadcast.get("status")
        if status == "paused":
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


async def _notify_broadcast_error(
    user_id: int,
    broadcast_id: int,
    account_name: str,
    error_text: str,
    *,
    chat_id=None,
    chat_name: str | None = None,
) -> None:
    bot = app_state.bot
    if not bot:
        return

    lines = [
        "\u26a0\ufe0f \u041e\u0448\u0438\u0431\u043a\u0430 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438",
        "",
        f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442: <b>{html.escape(str(account_name))}</b>",
    ]
    if chat_name or chat_id is not None:
        lines.append(
            f"\u0427\u0430\u0442: <b>{html.escape(str(chat_name or chat_id))}</b>"
            + (f" (<code>{chat_id}</code>)" if chat_id is not None else "")
        )
    lines.append(
        f"\u041e\u0448\u0438\u0431\u043a\u0430: <code>{html.escape(str(error_text)[:800])}</code>"
    )

    reply_markup = None
    if chat_id is not None:
        reply_markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="\u23f8\ufe0f \u041e\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u044c \u0434\u043b\u044f \u044d\u0442\u043e\u0433\u043e \u0447\u0430\u0442\u0430",
                        callback_data=f"bc_err_pause_{broadcast_id}_{chat_id}",
                    )
                ]
            ]
        )

    try:
        await bot.send_message(
            user_id,
            "\n".join(lines),
            parse_mode="HTML",
            reply_markup=reply_markup,
        )
    except Exception:
        pass


async def _notify_broadcast_debug(user_id: int, text: str) -> None:
    if user_id not in app_state.broadcast_debug_users:
        return

    bot = app_state.bot
    if not bot:
        return

    try:
        await bot.send_message(user_id, text, parse_mode="HTML")
    except Exception:
        pass


def _format_duration_brief(total_seconds: float) -> str:
    seconds = max(int(total_seconds), 0)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    if hours > 0:
        return f"{hours}ч {minutes}м"
    if minutes > 0:
        return f"{minutes}м {secs}с"
    return f"{secs}с"


def _broadcast_display_number(user_id: int, broadcast_id: int) -> int:
    visible_broadcast_ids = sorted(
        bid
        for bid, payload in app_state.active_broadcasts.items()
        if payload.get("user_id") == user_id
        and payload.get("status") in ("running", "paused")
    )
    for index, bid in enumerate(visible_broadcast_ids, start=1):
        if bid == broadcast_id:
            return index
    return 1


async def _notify_broadcast_finished(
    user_id: int,
    broadcast_id: int,
    broadcast: dict,
    *,
    sent_count: int,
    failed_count: int,
    processed_count: int,
    chat_runtime: list[dict],
) -> None:
    bot = app_state.bot
    if not bot:
        return

    account_name = str(
        broadcast.get("account_name")
        or f"Аккаунт {broadcast.get('account')}"
    )
    total_chats = int(broadcast.get("total_chats", len(chat_runtime)) or len(chat_runtime))
    successful_chats = sum(1 for item in chat_runtime if int(item.get("sent_count", 0) or 0) > 0)
    error_items = [
        item for item in chat_runtime if str(item.get("last_error") or "").strip()
    ]
    interval_value = broadcast.get("interval_value", broadcast.get("interval_minutes", "-"))
    chat_pause_value = broadcast.get("chat_pause", "-")
    start_time = broadcast.get("start_time")
    if isinstance(start_time, datetime):
        duration_text = _format_duration_brief(
            datetime.now(timezone.utc).timestamp() - start_time.timestamp()
        )
    else:
        duration_text = "-"

    lines = [
        "✅ <b>Рассылка завершена</b>",
        "",
        f"ID: <code>{broadcast_id}</code>",
        f"Аккаунт: <b>{html.escape(account_name)}</b>",
        f"Успешно отправлено: <b>{sent_count}</b>",
        f"Ошибок: <b>{failed_count}</b>",
        f"Обработано шагов: <b>{processed_count}</b>",
        f"Чатов с успешной отправкой: <b>{successful_chats}/{total_chats}</b>",
        f"Интервал: <b>{html.escape(str(interval_value))}</b> мин на чат",
        f"Темп: <b>{html.escape(str(chat_pause_value))}</b> сек",
        f"Длительность: <b>{duration_text}</b>",
    ]

    if error_items:
        lines.extend(["", "<b>Чаты с ошибками:</b>"])
        for item in error_items[:10]:
            chat_name = html.escape(str(item.get("name") or item.get("chat_id")))
            chat_error = html.escape(str(item.get("last_error") or "")[:120])
            lines.append(f"• {chat_name}: <code>{chat_error}</code>")
        if len(error_items) > 10:
            lines.append(f"• и ещё {len(error_items) - 10}")

    try:
        await bot.send_message(user_id, "\n".join(lines), parse_mode="HTML")
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
            await _notify_broadcast_error(
                user_id,
                broadcast_id,
                f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442 {account_number}",
                "Account disconnected or session is no longer authorized",
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
        current_count = int(broadcast.get("count", count) or count or 1)
        chat_runtime = _normalize_chat_runtime(
            chat_ids,
            broadcast.get("chat_runtime"),
            current_count,
        )
        if not chat_ids:
            await mark_error(broadcast_id, "No chats configured for broadcast", "no_chats")
            await _notify_broadcast_error(
                user_id,
                broadcast_id,
                account_name,
                "No chats configured for broadcast",
            )
            return

        source_entity_cache: dict[str, object] = {}
        source_message_cache: dict[tuple[str, int], object] = {}

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
            chat_pause_value = broadcast.get("chat_pause", "20-60")
            runtime_revision = int(broadcast.get("runtime_revision", 0) or 0)
            chat_runtime = _normalize_chat_runtime(
                chat_ids,
                broadcast.get("chat_runtime") or chat_runtime,
                count,
            )
            next_global_send_at = float(
                broadcast.get("next_global_send_at", next_global_send_at) or 0.0
            )

            if processed_count >= count:
                await update_broadcast_fields(
                    broadcast_id,
                    sent_chats=sent_count,
                    planned_count=count,
                    failed_count=failed_count,
                    processed_count=processed_count,
                    text_index=text_index,
                    chat_runtime=chat_runtime,
                    next_global_send_at=next_global_send_at,
                )
                await set_status(broadcast_id, "completed")
                final_broadcast = get_broadcast(broadcast_id) or broadcast
                await _notify_broadcast_finished(
                    user_id,
                    broadcast_id,
                    final_broadcast,
                    sent_count=sent_count,
                    failed_count=failed_count,
                    processed_count=processed_count,
                    chat_runtime=chat_runtime,
                )
                return

            chat_entry = _pick_next_chat_entry(chat_runtime)
            if not chat_entry:
                if any(
                    str(item.get("status") or "active") == "paused" and _chat_has_quota(item)
                    for item in chat_runtime
                ):
                    sleep_status = await _sleep_with_controls(
                        broadcast_id,
                        1,
                        expected_runtime_revision=runtime_revision,
                    )
                    if sleep_status in {"cancelled", "missing"}:
                        return
                    continue
                await update_broadcast_fields(
                    broadcast_id,
                    sent_chats=sent_count,
                    planned_count=count,
                    failed_count=failed_count,
                    processed_count=processed_count,
                    text_index=text_index,
                    chat_runtime=chat_runtime,
                    next_global_send_at=next_global_send_at,
                )
                await set_status(broadcast_id, "completed")
                final_broadcast = get_broadcast(broadcast_id) or broadcast
                await _notify_broadcast_finished(
                    user_id,
                    broadcast_id,
                    final_broadcast,
                    sent_count=sent_count,
                    failed_count=failed_count,
                    processed_count=processed_count,
                    chat_runtime=chat_runtime,
                )
                return

            now_ts = time.time()
            chat_ready_at = float(chat_entry.get("next_send_at", 0.0) or 0.0)
            wait_until = chat_ready_at
            pace_applied = _should_apply_global_pace(
                chat_entry,
                chat_runtime,
                next_global_send_at,
                now_ts,
            )
            if pace_applied:
                wait_until = max(wait_until, next_global_send_at)
            wait_seconds = max(0.0, wait_until - time.time())
            if wait_seconds > 0:
                sleep_status = await _sleep_with_controls(
                    broadcast_id,
                    wait_seconds,
                    expected_runtime_revision=runtime_revision,
                )
                if sleep_status in {"cancelled", "missing"}:
                    return
                continue

            chat_id = chat_entry["chat_id"]
            was_first_attempt = _chat_attempts(chat_entry) == 0
            current_item, next_text_index = _pick_content_item(
                content_items,
                text_mode,
                text_index,
            )
            last_error = None
            send_succeeded = False

            try:
                if current_item.get("kind") == "forward":
                    source_ref = str(current_item["source_ref"])
                    source_message_id = int(current_item["message_id"])
                    source_entity = source_entity_cache.get(source_ref)
                    if source_entity is None:
                        source_entity = await resolve_entity_reference(client, source_ref)
                        source_entity_cache[source_ref] = source_entity

                    source_cache_key = (source_ref, source_message_id)
                    source_message = source_message_cache.get(source_cache_key)
                    if source_message is None:
                        source_message = await client.get_messages(
                            source_entity,
                            ids=source_message_id,
                        )
                        source_message_cache[source_cache_key] = source_message
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
                chat_entry["last_error"] = f"FloodWait {wait_seconds} сек"
                chat_entry["last_error_at"] = time.time()
                await update_broadcast_fields(
                    broadcast_id,
                    last_wait_seconds=wait_seconds,
                    last_error="FloodWait",
                    chat_runtime=chat_runtime,
                )
                await _notify_floodwait(user_id, account_name, wait_seconds)
                sleep_status = await _sleep_with_controls(
                    broadcast_id,
                    wait_seconds,
                    expected_runtime_revision=runtime_revision,
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
                    chat_entry["last_error"] = str(exc)
                    chat_entry["last_error_at"] = time.time()
                    await update_broadcast_fields(
                        broadcast_id,
                        last_error=str(exc),
                        chat_runtime=chat_runtime,
                    )
                    await _notify_floodwait(user_id, account_name, 30)
                    sleep_status = await _sleep_with_controls(
                        broadcast_id,
                        30,
                        expected_runtime_revision=runtime_revision,
                    )
                    if sleep_status in {"cancelled", "missing"}:
                        return
                    continue

                failed_count += 1
                text_index = next_text_index
                last_error = str(exc)
                chat_entry["failed_count"] = int(chat_entry.get("failed_count", 0) or 0) + 1
                chat_entry["last_error"] = last_error
                chat_entry["last_error_at"] = time.time()
                await _notify_broadcast_error(
                    user_id,
                    broadcast_id,
                    account_name,
                    last_error,
                    chat_id=chat_id,
                    chat_name=str(chat_entry.get("name") or ""),
                )

            processed_count += 1
            interval_min, interval_max = _parse_int_range(interval_value, 1, 1)
            wait_minutes = random.randint(interval_min, interval_max)
            chat_entry["next_send_at"] = time.time() + (wait_minutes * 60)
            if send_succeeded:
                chat_entry["sent_count"] = int(chat_entry.get("sent_count", 0) or 0) + 1
                chat_entry["last_error"] = ""
                chat_entry["last_error_at"] = 0.0

            pause_min, pause_max = _parse_float_range(chat_pause_value, 1.0, 3.0)
            pace_seconds = random.uniform(pause_min, pause_max)
            next_global_send_at = time.time() + pace_seconds

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
            display_number = _broadcast_display_number(user_id, broadcast_id)
            config_name = str(broadcast.get("config_name") or "\u041f\u043e \u0443\u043c\u043e\u043b\u0447\u0430\u043d\u0438\u044e")
            await _notify_broadcast_debug(
                user_id,
                (
                    "\U0001f9ea <b>\u041b\u043e\u0433\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438</b>\n\n"
                    f"\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430: <b>#{display_number}</b> (<code>{broadcast_id}</code>)\n"
                    f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442: <b>{html.escape(account_name)}</b>\n"
                    f"\u041a\u043e\u043d\u0444\u0438\u0433: <b>{html.escape(config_name)}</b>\n"
                    f"\u0427\u0430\u0442: <b>{html.escape(str(chat_entry.get('name') or chat_id))}</b>\n"
                    f"\u041f\u043e\u043f\u044b\u0442\u043e\u043a \u0432 \u0447\u0430\u0442: <b>{_chat_attempts(chat_entry)}/{int(chat_entry.get('target_count', 0) or 0)}</b>\n"
                    f"\u041f\u0435\u0440\u0432\u044b\u0439 \u0437\u0430\u0445\u043e\u0434: <b>{'\u0434\u0430' if was_first_attempt else '\u043d\u0435\u0442'}</b>\n"
                    f"\u0422\u0435\u043c\u043f \u043f\u0440\u0438\u043c\u0435\u043d\u044f\u043b\u0441\u044f: <b>{'\u0434\u0430' if pace_applied else '\u043d\u0435\u0442'}</b>\n"
                    f"\u0421\u043b\u0435\u0434\u0443\u044e\u0449\u0435\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u0432 \u044d\u0442\u043e\u0442 \u0447\u0430\u0442: <b>~ {_format_duration_brief(wait_minutes * 60)}</b>\n"
                    f"\u0421\u043b\u0435\u0434\u0443\u044e\u0449\u0438\u0439 \u0433\u043b\u043e\u0431\u0430\u043b\u044c\u043d\u044b\u0439 \u0442\u0435\u043c\u043f: <b>{pace_seconds:.1f}</b> \u0441\u0435\u043a"
                ),
            )

    except Exception as exc:
        await mark_error(broadcast_id, str(exc), "send_failed")
        await _notify_broadcast_error(
            user_id,
            broadcast_id,
            account_name if "account_name" in locals() else f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442 {account_number}",
            str(exc),
        )
    finally:
        discard_broadcast_task(broadcast_id)
