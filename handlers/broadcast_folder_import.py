import asyncio
import html

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.utils import get_peer_id

from core.config import API_HASH, API_ID
from database import get_user_accounts
from services.broadcast_runtime_service import account_label
from services.session_service import ensure_connected_client


def folder_title(dialog_filter) -> str:
    title = getattr(dialog_filter, "title", None)
    text = getattr(title, "text", None)
    if text:
        return str(text)
    if isinstance(title, str) and title.strip():
        return title.strip()
    return f"Папка {getattr(dialog_filter, 'id', '?')}"


def folder_peer_ids(peers) -> set[int]:
    result: set[int] = set()
    for peer in peers or []:
        try:
            result.add(int(get_peer_id(peer)))
        except Exception:
            continue
    return result


def dialog_matches_folder(dialog, dialog_filter) -> bool:
    entity = getattr(dialog, "entity", None)
    if entity is None:
        return False

    try:
        peer_id = int(get_peer_id(entity))
    except Exception:
        return False

    include_ids = folder_peer_ids(getattr(dialog_filter, "include_peers", None))
    exclude_ids = folder_peer_ids(getattr(dialog_filter, "exclude_peers", None))
    pinned_ids = folder_peer_ids(getattr(dialog_filter, "pinned_peers", None))

    if peer_id in exclude_ids:
        return False

    if include_ids or pinned_ids:
        return peer_id in include_ids or peer_id in pinned_ids

    if getattr(dialog_filter, "exclude_archived", False) and getattr(dialog, "archived", False):
        return False
    if getattr(dialog_filter, "exclude_muted", False):
        notify_settings = getattr(dialog, "notify_settings", None)
        if getattr(notify_settings, "mute_until", 0):
            return False
    if getattr(dialog_filter, "exclude_read", False) and getattr(dialog, "unread_count", 0) == 0:
        return False
    if getattr(dialog_filter, "groups", False) and not getattr(dialog, "is_group", False):
        return False
    if getattr(dialog_filter, "broadcasts", False):
        if not getattr(dialog, "is_channel", False) or getattr(dialog, "is_group", False):
            return False
    if getattr(dialog_filter, "bots", False) and not bool(getattr(entity, "bot", False)):
        return False
    if getattr(dialog_filter, "contacts", False) and not bool(getattr(entity, "contact", False)):
        return False
    if getattr(dialog_filter, "non_contacts", False) and bool(getattr(entity, "contact", False)):
        return False

    return True


async def load_account_folders(user_id: int, account_number: int) -> tuple[object, list]:
    client = await ensure_connected_client(
        user_id,
        account_number,
        api_id=API_ID,
        api_hash=API_HASH,
    )
    if not client:
        raise RuntimeError("Не удалось подключить аккаунт")

    try:
        result = await asyncio.wait_for(client(GetDialogFiltersRequest()), timeout=8.0)
    except asyncio.TimeoutError as exc:
        raise RuntimeError("Telegram слишком долго отвечает при загрузке папок") from exc

    filters = list(getattr(result, "filters", result or []) or [])
    folders = [
        item
        for item in filters
        if getattr(item, "id", None) is not None and hasattr(item, "include_peers")
    ]
    return client, folders


async def load_folder_chats(
    user_id: int,
    account_number: int,
    folder_id: int,
) -> tuple[object, object, list[dict]]:
    client, folders = await load_account_folders(user_id, account_number)
    folder = next((item for item in folders if int(getattr(item, "id", 0) or 0) == folder_id), None)
    if not folder:
        raise RuntimeError("Папка не найдена")

    try:
        dialogs = await asyncio.wait_for(client.get_dialogs(limit=None), timeout=20.0)
    except asyncio.TimeoutError as exc:
        raise RuntimeError("Telegram слишком долго отвечает при загрузке чатов папки") from exc
    items: list[dict] = []
    seen_ids: set[int] = set()
    for dialog in dialogs:
        if not dialog_matches_folder(dialog, folder):
            continue

        entity = getattr(dialog, "entity", None)
        if entity is None:
            continue

        try:
            chat_id = int(get_peer_id(entity))
        except Exception:
            chat_id = int(getattr(entity, "id", 0) or 0)
        if not chat_id or chat_id in seen_ids:
            continue

        title = getattr(entity, "title", None) or getattr(entity, "first_name", None)
        if not title and hasattr(entity, "username") and entity.username:
            title = f"@{entity.username}"
        if not title:
            title = f"Чат {chat_id}"

        chat_link = None
        username = getattr(entity, "username", None)
        if username:
            chat_link = f"https://t.me/{username}"

        seen_ids.add(chat_id)
        items.append(
            {
                "chat_id": chat_id,
                "chat_name": str(title),
                "chat_link": chat_link,
            }
        )

    items.sort(key=lambda item: item["chat_name"].lower())
    return client, folder, items


def build_folder_account_picker(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    accounts = get_user_accounts(user_id)
    available_accounts = [
        (acc_num, username, first_name)
        for acc_num, _, username, first_name, is_active in accounts
        if is_active
    ] or [
        (acc_num, username, first_name)
        for acc_num, _, username, first_name, _ in accounts
    ]

    text = "📂 <b>ИМПОРТ ЧАТОВ ИЗ ПАПКИ</b>\n\nВыбери аккаунт, с которого читать папки Telegram."
    keyboard_rows = [
        [
            InlineKeyboardButton(
                text=f"👤 {account_label(acc_num, username, first_name)}",
                callback_data=f"bc_folder_acc_{acc_num}",
            )
        ]
        for acc_num, username, first_name in available_accounts
    ]
    keyboard_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="bc_chats_back")])
    return text, InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


def build_folder_list_view(account_number: int, folders: list) -> tuple[str, InlineKeyboardMarkup]:
    lines = [
        "📂 <b>ПАПКИ TELEGRAM</b>",
        "",
        f"Аккаунт: <b>{account_number}</b>",
        "",
    ]
    keyboard_rows = []
    for folder in folders:
        folder_id = int(getattr(folder, "id", 0))
        title = folder_title(folder)
        lines.append(f"• <b>{html.escape(title)}</b> — ID {folder_id}")
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    text=title[:32],
                    callback_data=f"bc_folder_pick_{account_number}_{folder_id}",
                )
            ]
        )

    if not folders:
        lines.append("У этого аккаунта нет пользовательских папок.")

    keyboard_rows.append([InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="bc_chats_import")])
    keyboard_rows.append([InlineKeyboardButton(text="⬅️ К чатам", callback_data="bc_chats_back")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


def build_folder_preview_view(
    account_number: int,
    folder,
    folder_chats: list[dict],
) -> tuple[str, InlineKeyboardMarkup]:
    title = folder_title(folder)
    lines = [
        "📂 <b>ПРЕДПРОСМОТР ПАПКИ</b>",
        "",
        f"Аккаунт: <b>{account_number}</b>",
        f"Папка: <b>{html.escape(title)}</b>",
        f"Найдено чатов: <b>{len(folder_chats)}</b>",
        "",
    ]

    preview_items = folder_chats[:12]
    for idx, item in enumerate(preview_items, 1):
        name = html.escape(str(item.get("chat_name") or item.get("chat_id")))
        lines.append(f"{idx}. {name} <code>{item['chat_id']}</code>")
    if len(folder_chats) > len(preview_items):
        lines.append(f"... ещё {len(folder_chats) - len(preview_items)}")

    keyboard_rows = []
    if folder_chats:
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    text="➕ Добавить",
                    callback_data=f"bc_folder_add_{account_number}_{int(folder.id)}",
                ),
                InlineKeyboardButton(
                    text="♻️ Заменить",
                    callback_data=f"bc_folder_replace_{account_number}_{int(folder.id)}",
                ),
            ]
        )
    keyboard_rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ К папкам",
                callback_data=f"bc_folder_acc_{account_number}",
            )
        ]
    )
    keyboard_rows.append([InlineKeyboardButton(text="⬅️ К чатам", callback_data="bc_chats_back")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


def parse_folder_callback(data: str, action: str) -> tuple[int, int]:
    prefix = f"bc_folder_{action}_"
    if not data.startswith(prefix):
        raise ValueError("Неверный callback папки")
    tail = data[len(prefix) :]
    account_text, folder_text = tail.split("_", 1)
    return int(account_text), int(folder_text)
