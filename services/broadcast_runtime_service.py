from datetime import datetime, timezone

from database import add_broadcast_chat, get_user_accounts, remove_broadcast_chat
from services.broadcast_profiles_service import (
    ensure_active_config,
    sync_active_config_from_db,
)


def add_broadcast_chat_with_profile(
    user_id: int,
    chat_id: int,
    chat_name: str,
    chat_link: str | None = None,
) -> bool:
    ensure_active_config(user_id)
    added = add_broadcast_chat(user_id, chat_id, chat_name, chat_link=chat_link)
    sync_active_config_from_db(user_id)
    return added


def remove_broadcast_chat_with_profile(user_id: int, chat_id: int) -> None:
    ensure_active_config(user_id)
    remove_broadcast_chat(user_id, chat_id)
    sync_active_config_from_db(user_id)


def iter_connected_account_numbers(user_id: int) -> list[int]:
    active_accounts = [
        acc_num
        for acc_num, _, _, _, is_active in get_user_accounts(user_id)
        if is_active
    ]
    if active_accounts:
        return sorted(active_accounts)

    fallback_accounts = [acc_num for acc_num, _, _, _, _ in get_user_accounts(user_id)]
    fallback_accounts.sort()
    return fallback_accounts


def account_label(
    account_number: int,
    username: str | None,
    first_name: str | None,
) -> str:
    return (first_name or username or f"Аккаунт {account_number}").strip()


def broadcast_chat_runtime_items(broadcast: dict) -> list[dict]:
    items = list(broadcast.get("chat_runtime") or [])
    return sorted(
        [item for item in items if isinstance(item, dict)],
        key=lambda item: int(item.get("order", 0) or 0),
    )


def broadcast_chat_status_label(chat_item: dict) -> str:
    status = str(chat_item.get("status") or "active")
    if status == "paused":
        return "⏸️ Пауза"
    if status == "disabled":
        return "⛔ Отключен"
    return "▶️ Активен"


def broadcast_chat_short_name(chat_item: dict) -> str:
    return str(chat_item.get("name") or chat_item.get("chat_id") or "?")


def chat_attempts(chat_item: dict) -> int:
    return int(chat_item.get("sent_count", 0) or 0) + int(chat_item.get("failed_count", 0) or 0)


def chat_deliveries(chat_item: dict) -> int:
    return int(chat_item.get("sent_count", 0) or 0)


def chat_has_delivery_quota(chat_item: dict) -> bool:
    target_count = max(int(chat_item.get("target_count", 0) or 0), 0)
    return chat_deliveries(chat_item) < target_count


def broadcast_phase(chat_items: list[dict]) -> str:
    active_items = [
        item
        for item in chat_items
        if str(item.get("status") or "active") == "active" and chat_has_delivery_quota(item)
    ]
    if any(chat_deliveries(item) == 0 for item in active_items):
        return "first_pass"
    return "repeats"


def rebalance_chat_targets(items: list[dict], total_count: int) -> list[dict]:
    runtime_items = [item for item in items if isinstance(item, dict)]
    if not runtime_items:
        return runtime_items

    delivered_total = sum(chat_deliveries(item) for item in runtime_items)
    effective_total = max(int(total_count or 0), delivered_total)
    active_items = [
        item for item in runtime_items if str(item.get("status") or "active") == "active"
    ]

    for item in runtime_items:
        if str(item.get("status") or "active") != "active":
            item["target_count"] = chat_deliveries(item)

    if not active_items:
        return runtime_items

    extra_total = max(
        effective_total - sum(chat_deliveries(item) for item in runtime_items),
        0,
    )
    base_extra = extra_total // len(active_items)
    remainder = extra_total % len(active_items)

    for index, item in enumerate(active_items):
        item["target_count"] = chat_deliveries(item) + base_extra + (1 if index < remainder else 0)

    return runtime_items


def format_chat_error_line(chat_item: dict) -> str:
    error_text = str(chat_item.get("last_error") or "").strip()
    if not error_text:
        return ""
    return error_text if len(error_text) <= 120 else f"{error_text[:117]}..."


def format_chat_error_log(chat_item: dict) -> str:
    number = int(chat_item.get("order", 0) or 0) + 1
    chat_id = chat_item.get("chat_id")
    error_text = str(chat_item.get("last_error") or "").strip() or "-"
    error_time = float(chat_item.get("last_error_at", 0.0) or 0.0)
    if error_time > 0:
        timestamp = datetime.fromtimestamp(error_time, tz=timezone.utc).astimezone()
        timestamp_text = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    else:
        timestamp_text = "-"

    return (
        f"[{number}] {broadcast_chat_short_name(chat_item)}\n"
        f"id: {chat_id}\n"
        f"time: {timestamp_text}\n"
        f"error: {error_text}"
    )


def find_chat_runtime_item(broadcast: dict, order: int) -> dict | None:
    for item in broadcast_chat_runtime_items(broadcast):
        item_order = item.get("order", -1)
        if int(item_order if item_order is not None else -1) == order:
            return item
    return None


def active_chat_counts(broadcast: dict) -> tuple[int, int, int]:
    active = paused = disabled = 0
    for item in broadcast_chat_runtime_items(broadcast):
        status = str(item.get("status") or "active")
        if status == "paused":
            paused += 1
        elif status == "disabled":
            disabled += 1
        else:
            active += 1
    return active, paused, disabled


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


def _parse_float_range(
    value, default_min: float, default_max: float
) -> tuple[float, float]:
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


def interval_unit_label(unit: str | None) -> str:
    return "сек" if str(unit or "minutes") == "seconds" else "мин"


def interval_unit_display(unit: str | None) -> str:
    return "сек на чат" if str(unit or "minutes") == "seconds" else "мин на чат"


def interval_range_seconds(broadcast: dict) -> tuple[float, float]:
    interval_min, interval_max = _parse_int_range(
        broadcast.get("interval_value", broadcast.get("interval_minutes", 30)),
        30,
        90,
    )
    if str(broadcast.get("interval_unit") or "minutes") == "seconds":
        return float(interval_min), float(interval_max)
    return float(interval_min) * 60.0, float(interval_max) * 60.0


def estimate_broadcast_finish_timestamp(
    broadcast: dict,
    *,
    now_ts: float | None = None,
) -> float | None:
    now_ts = float(now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp())
    count = int(broadcast.get("count", 0) or 0)
    sent = int(broadcast.get("sent_chats", 0) or 0)
    failed = int(broadcast.get("failed_count", 0) or 0)
    processed = int(broadcast.get("processed_count", sent + failed) or 0)
    remaining = max(count - processed, 0)
    if remaining <= 0:
        return now_ts

    active_items = [
        item
        for item in broadcast_chat_runtime_items(broadcast)
        if str(item.get("status") or "active") == "active"
    ]
    if not active_items:
        return None

    interval_min_seconds, interval_max_seconds = interval_range_seconds(broadcast)
    pause_min, pause_max = _parse_float_range(
        broadcast.get("chat_pause", "20-60"),
        20.0,
        60.0,
    )
    average_interval_seconds = (interval_min_seconds + interval_max_seconds) / 2.0
    average_pause_seconds = (pause_min + pause_max) / 2.0

    chat_next_times = []
    for item in active_items:
        next_send_at = float(item.get("next_send_at", 0.0) or 0.0)
        chat_next_times.append(max(next_send_at, now_ts))

    global_next_at = max(float(broadcast.get("next_global_send_at", 0.0) or 0.0), now_ts)
    last_send_at = now_ts

    for _ in range(remaining):
        index = min(range(len(chat_next_times)), key=lambda idx: chat_next_times[idx])
        send_at = max(chat_next_times[index], global_next_at)
        last_send_at = send_at
        chat_next_times[index] = send_at + average_interval_seconds
        global_next_at = send_at + average_pause_seconds

    return last_send_at


def estimate_next_send_timestamp(
    broadcast: dict,
    *,
    now_ts: float | None = None,
) -> float | None:
    now_ts = float(now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp())
    active_items = [
        item
        for item in broadcast_chat_runtime_items(broadcast)
        if str(item.get("status") or "active") == "active"
    ]
    if not active_items:
        return None

    next_chat_ready_at = min(
        max(float(item.get("next_send_at", 0.0) or 0.0), now_ts) for item in active_items
    )
    global_next_at = max(float(broadcast.get("next_global_send_at", 0.0) or 0.0), now_ts)
    return max(next_chat_ready_at, global_next_at)


def estimate_group_next_send_timestamp(
    items: list[tuple[int, dict]],
    *,
    now_ts: float | None = None,
) -> float | None:
    now_ts = float(now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp())
    timestamps = [
        ts
        for _, broadcast in items
        for ts in [estimate_next_send_timestamp(broadcast, now_ts=now_ts)]
        if ts is not None
    ]
    if not timestamps:
        return None
    return min(timestamps)


def estimate_group_finish_timestamp(
    items: list[tuple[int, dict]],
    *,
    now_ts: float | None = None,
) -> float | None:
    now_ts = float(now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp())
    timestamps = [
        ts
        for _, broadcast in items
        for ts in [estimate_broadcast_finish_timestamp(broadcast, now_ts=now_ts)]
        if ts is not None
    ]
    if not timestamps:
        return None
    return max(timestamps)


def format_eta_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"

    total_seconds = max(int(round(seconds)), 0)
    if total_seconds < 60:
        return f"~ {total_seconds} сек"

    minutes, seconds_left = divmod(total_seconds, 60)
    hours, minutes_left = divmod(minutes, 60)
    days, hours_left = divmod(hours, 24)

    parts: list[str] = []
    if days:
        parts.append(f"{days}д")
    if hours_left:
        parts.append(f"{hours_left}ч")
    if minutes_left:
        parts.append(f"{minutes_left}м")
    elif not parts and seconds_left:
        parts.append("1м")

    return "~ " + " ".join(parts[:2])


def format_finish_time(timestamp: float | None) -> str:
    if timestamp is None:
        return "-"
    finish_time = datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone()
    return finish_time.strftime("%H:%M")
