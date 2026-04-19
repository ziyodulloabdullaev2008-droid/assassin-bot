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
