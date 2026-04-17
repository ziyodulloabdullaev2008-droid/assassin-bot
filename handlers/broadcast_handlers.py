from aiogram import Router, F

from aiogram.filters.command import Command

from aiogram.fsm.context import FSMContext

from aiogram.fsm.state import State, StatesGroup

from aiogram.types import (
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
)

import asyncio
import html

from datetime import datetime, timezone
from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.utils import get_peer_id

from core.state import app_state

from database import (
    add_broadcast_chat,
    remove_broadcast_chat,
    get_broadcast_chats,
    get_user_accounts,
    save_broadcast_config,
)

from services.broadcast_service import (
    next_broadcast_id,
    create_broadcast,
    get_broadcast,
    get_broadcast_task,
    register_broadcast_task,
    set_status as set_broadcast_status,
    cleanup_old_broadcasts as cleanup_old_broadcasts_service,
    update_broadcast_fields,
)

from services.broadcast_config_service import get_broadcast_config

from services.broadcast_sender import schedule_broadcast_send

from services.broadcast_profiles_service import (
    ensure_active_config,
    sync_active_config_from_db,
)
from services.channel_post_service import (
    build_text_source_label,
    count_source_items,
    fetch_channel_posts,
    format_source_channel_link,
    normalize_channel_reference,
    parse_numeric_reference,
    post_preview_text,
    resolve_entity_reference,
    source_channel_title,
)
from services.session_service import ensure_connected_client
from core.config import API_HASH, API_ID

from services.mention_utils import delete_message_after_delay

from ui.broadcast_ui import build_broadcast_keyboard, build_broadcast_menu_text

from ui.texts_ui import build_texts_keyboard, build_text_settings_keyboard

from ui.main_menu_ui import get_main_menu_keyboard

router = Router()

user_authenticated = app_state.user_authenticated

broadcast_update_lock = app_state.broadcast_update_lock

active_broadcasts = app_state.active_broadcasts
LOGIN_REQUIRED_TEXT = "\u274c \u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u0432\u043e\u0439\u0434\u0438 \u0447\u0435\u0440\u0435\u0437 /login"
CANCEL_TEXT = "\u274c \u041e\u0442\u043c\u0435\u043d\u0438\u0442\u044c"
COUNT_BUTTON_TEXT = "\u041a\u043e\u043b-\u0432\u043e"
INTERVAL_BUTTON_TEXT = "\u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b"
CHAT_PAUSE_MAX_SECONDS = 3600


def save_broadcast_config_with_profile(user_id: int, config: dict) -> None:

    ensure_active_config(user_id)

    save_broadcast_config(user_id, config)

    sync_active_config_from_db(user_id)


def _is_channel_source(config: dict) -> bool:
    return config.get("text_source_type", "manual") == "channel"


def _build_text_settings_info(config: dict) -> str:
    info = "📝 <b>НАСТРОЙКИ КОНТЕНТА</b>\n\n"
    info += f"Источник текста: {build_text_source_label(config)}\n"
    info += f"Вариантов: {count_source_items(config)}\n"
    info += (
        f"Режим: {'Random ✅' if config.get('text_mode') == 'random' else 'No Random ❌'}\n"
    )
    if _is_channel_source(config):
        info += f"Канал: {source_channel_title(config)}\n"
    else:
        info += f"Формат: {config.get('parse_mode', 'HTML')}\n"
    return info


def _build_text_list_info(config: dict) -> str:
    if _is_channel_source(config):
        count = len(config.get("source_posts") or [])
        info = "📚 <b>ПОСТЫ ИЗ КАНАЛА</b>\n\n"
        info += f"Канал: {source_channel_title(config)}\n"
        info += f"Постов доступно: {count}\n\n"
        if not count:
            info += "Сначала укажи канал-источник и загрузи посты."
        else:
            info += "Выбери пост для просмотра."
        return info

    count = len(config.get("texts") or [])
    info = "📚 <b>СПИСОК ТЕКСТОВ</b>\n\n"
    info += f"Всего текстов: {count}\n\n"
    if not count:
        info += "Еще не добавлено ни одного текста."
    else:
        info += "Выбери текст для просмотра или редактирования."
    return info


async def _load_channel_source_for_user(
    user_id: int,
    channel_ref: str,
    *,
    preferred_account_number: int | None = None,
) -> dict:
    normalized_ref = normalize_channel_reference(channel_ref)
    if not normalized_ref:
        raise ValueError("Укажи ссылку, @username или ID канала")

    account_numbers = []
    if preferred_account_number is not None:
        account_numbers.append(preferred_account_number)

    for acc_num, _, _, _, is_active in get_user_accounts(user_id):
        if not is_active or acc_num in account_numbers:
            continue
        account_numbers.append(acc_num)

    for acc_num in account_numbers:
        client = await ensure_connected_client(
            user_id,
            acc_num,
            api_id=API_ID,
            api_hash=API_HASH,
        )
        if not client:
            continue
        try:
            source_data = await fetch_channel_posts(client, normalized_ref)
            source_data["source_account"] = acc_num
            return source_data
        except Exception:
            continue

    raise RuntimeError("Не удалось загрузить посты канала ни с одного подключенного аккаунта")


def _iter_connected_account_numbers(user_id: int) -> list[int]:
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


async def _ensure_account_ready(user_id: int, account_number: int):
    return await ensure_connected_client(
        user_id,
        account_number,
        api_id=API_ID,
        api_hash=API_HASH,
    )


def _account_label(account_number: int, username: str | None, first_name: str | None) -> str:
    title = (first_name or username or f"Аккаунт {account_number}").strip()
    return title


def _folder_title(dialog_filter) -> str:
    title = getattr(dialog_filter, "title", None)
    text = getattr(title, "text", None)
    if text:
        return str(text)
    if isinstance(title, str) and title.strip():
        return title.strip()
    return f"Папка {getattr(dialog_filter, 'id', '?')}"


def _folder_peer_ids(peers) -> set[int]:
    result: set[int] = set()
    for peer in peers or []:
        try:
            result.add(int(get_peer_id(peer)))
        except Exception:
            continue
    return result


def _dialog_matches_folder(dialog, dialog_filter) -> bool:
    entity = getattr(dialog, "entity", None)
    if entity is None:
        return False

    try:
        dialog_peer_id = int(get_peer_id(entity))
    except Exception:
        return False

    include_ids = _folder_peer_ids(getattr(dialog_filter, "include_peers", None))
    exclude_ids = _folder_peer_ids(getattr(dialog_filter, "exclude_peers", None))
    pinned_ids = _folder_peer_ids(getattr(dialog_filter, "pinned_peers", None))

    if dialog_peer_id in exclude_ids:
        return False

    if include_ids or pinned_ids:
        return dialog_peer_id in include_ids or dialog_peer_id in pinned_ids

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


async def _load_account_folders(user_id: int, account_number: int) -> tuple[object, list]:
    client = await _ensure_account_ready(user_id, account_number)
    if not client:
        raise RuntimeError("Не удалось подключить аккаунт")

    try:
        filters = await asyncio.wait_for(client(GetDialogFiltersRequest()), timeout=8.0)
    except asyncio.TimeoutError as exc:
        raise RuntimeError("Telegram слишком долго отвечает при загрузке папок") from exc
    folders = [
        item
        for item in (filters or [])
        if getattr(item, "id", None) is not None and hasattr(item, "include_peers")
    ]
    return client, folders


async def _load_folder_chats(user_id: int, account_number: int, folder_id: int) -> tuple[object, object, list[dict]]:
    client, folders = await _load_account_folders(user_id, account_number)
    folder = next((item for item in folders if int(getattr(item, "id", -1)) == folder_id), None)
    if folder is None:
        raise RuntimeError("Папка не найдена")

    try:
        dialogs = await asyncio.wait_for(client.get_dialogs(limit=None), timeout=20.0)
    except asyncio.TimeoutError as exc:
        raise RuntimeError("Telegram слишком долго отвечает при загрузке чатов папки") from exc
    result: list[dict] = []
    seen_chat_ids: set[int] = set()

    for dialog in dialogs:
        if not _dialog_matches_folder(dialog, folder):
            continue

        entity = getattr(dialog, "entity", None)
        if entity is None:
            continue

        try:
            chat_id = int(get_peer_id(entity))
        except Exception:
            chat_id = int(getattr(entity, "id", 0) or 0)
        if not chat_id or chat_id in seen_chat_ids:
            continue

        title = getattr(entity, "title", None) or getattr(entity, "first_name", None)
        if not title and hasattr(entity, "username") and entity.username:
            title = f"@{entity.username}"
        if not title:
            title = f"Чат {chat_id}"

        seen_chat_ids.add(chat_id)
        result.append(
            {
                "chat_id": chat_id,
                "chat_name": str(title),
                "chat_link": _detect_chat_link(None, entity),
            }
        )

    result.sort(key=lambda item: item["chat_name"].lower())
    return client, folder, result


def _preferred_account_number(user_id: int) -> int | None:
    accounts = get_user_accounts(user_id)
    for acc_num, _, _, _, is_active in accounts:
        if is_active:
            return acc_num
    return accounts[0][0] if accounts else None


async def _load_channel_preview_message(user_id: int, config: dict, text_index: int):
    items = config.get("source_posts") or []
    if text_index < 0 or text_index >= len(items):
        raise IndexError("Post not found")

    account_number = _preferred_account_number(user_id)
    if account_number is None:
        raise RuntimeError("Нет подключенных аккаунтов")

    client = await ensure_connected_client(
        user_id,
        account_number,
        api_id=API_ID,
        api_hash=API_HASH,
    )
    if not client:
        raise RuntimeError("Не удалось подключить аккаунт")

    source_ref = str(config.get("source_channel_ref") or "")
    if not source_ref:
        raise RuntimeError("Источник канала не задан")

    source_entity = await resolve_entity_reference(client, source_ref)
    message_id = int(items[text_index]["message_id"])
    source_message = await client.get_messages(source_entity, ids=message_id)
    if not source_message:
        raise RuntimeError("Пост канала не найден")

    return client, source_message, account_number


async def _resolve_chat_for_user(
    user_id: int,
    chat_reference: str,
) -> tuple[object, int]:
    account_numbers = _iter_connected_account_numbers(user_id)
    if not account_numbers:
        raise RuntimeError("Нет подключенных аккаунтов")

    last_error = None
    for account_number in account_numbers:
        client = await ensure_connected_client(
            user_id,
            account_number,
            api_id=API_ID,
            api_hash=API_HASH,
        )
        if not client:
            continue

        try:
            entity = await resolve_entity_reference(client, chat_reference)
            return entity, account_number
        except Exception as exc:
            last_error = exc
            continue

    if last_error:
        raise last_error
    raise RuntimeError("Не удалось получить доступ к чату")


def _build_manual_content_items(config: dict) -> list[dict]:
    return [{"kind": "text", "text": text} for text in (config.get("texts") or [])]


def _build_channel_content_items(config: dict) -> list[dict]:
    source_ref = config.get("source_channel_ref")
    return [
        {
            "kind": "forward",
            "message_id": int(item["message_id"]),
            "preview": str(item.get("preview") or ""),
            "source_ref": source_ref,
        }
        for item in (config.get("source_posts") or [])
        if item.get("message_id")
    ]


def _broadcast_content_ready(config: dict) -> bool:
    if _is_channel_source(config):
        return bool(config.get("source_channel_ref") and config.get("source_posts"))
    return bool(config.get("texts"))


def _build_missing_content_notice(config: dict) -> str:
    if _is_channel_source(config):
        if not config.get("source_channel_ref"):
            return (
                "❌ Канал-источник не указан.\n\n"
                "Открой 'Настройки контента' и выбери канал с готовыми постами."
            )
        return (
            "❌ Посты из канала не загружены.\n\n"
            "Нажми 'Обновить посты' или заново укажи канал-источник."
        )
    return (
        "❌ Текст рассылки не установлен.\n\n"
        "Нажми '📝 Выбрать текст', чтобы добавить текст вручную."
    )


def _detect_chat_link(chat_input: str | None = None, chat_entity=None) -> str | None:
    if chat_entity is not None:
        username = getattr(chat_entity, "username", None)
        if username:
            return f"https://t.me/{username}"

    value = (chat_input or "").strip()
    if not value:
        return None

    if value.startswith("@") and len(value) > 1:
        return f"https://t.me/{value[1:]}"

    lower = value.lower()
    if lower.startswith("https://t.me/"):
        return value
    if lower.startswith("http://t.me/"):
        return "https://" + value[len("http://") :]

    return None


def add_broadcast_chat_with_profile(
    user_id: int, chat_id: int, chat_name: str, chat_link: str | None = None
) -> bool:

    ensure_active_config(user_id)

    added = add_broadcast_chat(user_id, chat_id, chat_name, chat_link=chat_link)

    sync_active_config_from_db(user_id)
    return added


def remove_broadcast_chat_with_profile(user_id: int, chat_id: int) -> None:

    ensure_active_config(user_id)

    remove_broadcast_chat(user_id, chat_id)

    sync_active_config_from_db(user_id)


def cleanup_old_broadcasts(max_age_minutes: int = 120):
    """Remove completed/errored broadcasts from memory to prevent leaks."""

    deleted = cleanup_old_broadcasts_service(max_age_minutes=max_age_minutes)

    if deleted:
        print(f"Cleanup removed {deleted} old broadcasts from memory")

    return deleted


def _broadcast_chat_runtime_items(broadcast: dict) -> list[dict]:
    items = list(broadcast.get("chat_runtime") or [])
    return sorted(
        [item for item in items if isinstance(item, dict)],
        key=lambda item: int(item.get("order", 0) or 0),
    )


def _broadcast_chat_status_label(chat_item: dict) -> str:
    status = str(chat_item.get("status") or "active")
    if status == "paused":
        return "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
    if status == "disabled":
        return "\u26d4 \u041e\u0442\u043a\u043b\u044e\u0447\u0435\u043d"
    return "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u0435\u043d"


def _broadcast_chat_short_name(chat_item: dict) -> str:
    return str(chat_item.get("name") or chat_item.get("chat_id") or "?")


def _format_chat_error_line(chat_item: dict) -> str:
    error_text = str(chat_item.get("last_error") or "").strip()
    if not error_text:
        return ""
    trimmed = error_text if len(error_text) <= 120 else f"{error_text[:117]}..."
    return trimmed


def _format_chat_error_log(chat_item: dict) -> str:
    number = int(chat_item.get("order", 0) or 0) + 1
    name = _broadcast_chat_short_name(chat_item)
    chat_id = chat_item.get("chat_id")
    error_text = str(chat_item.get("last_error") or "").strip() or "-"
    error_time = float(chat_item.get("last_error_at", 0.0) or 0.0)
    if error_time > 0:
        timestamp = datetime.fromtimestamp(error_time, tz=timezone.utc).astimezone()
        timestamp_text = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    else:
        timestamp_text = "-"
    return (
        f"[{number}] {name}\n"
        f"id: {chat_id}\n"
        f"time: {timestamp_text}\n"
        f"error: {error_text}"
    )


def _find_chat_runtime_item(broadcast: dict, order: int) -> dict | None:
    for item in _broadcast_chat_runtime_items(broadcast):
        item_order = item.get("order", -1)
        if int(item_order if item_order is not None else -1) == order:
            return item
    return None


def _active_chat_counts(broadcast: dict) -> tuple[int, int, int]:
    active = paused = disabled = 0
    for item in _broadcast_chat_runtime_items(broadcast):
        status = str(item.get("status") or "active")
        if status == "paused":
            paused += 1
        elif status == "disabled":
            disabled += 1
        else:
            active += 1
    return active, paused, disabled


class BroadcastConfigState(StatesGroup):
    waiting_for_count = State()

    waiting_for_interval = State()

    waiting_for_chat_pause = State()

    waiting_for_text = State()
    waiting_for_source_channel = State()

    waiting_for_chat_id = (
        State()
    )  # Р вЂќР В»РЎРЏ Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р С‘РЎРЏ РЎвЂЎР В°РЎвЂљР В°

    waiting_for_chat_name = State()  # Р вЂќР В»РЎРЏ Р Р†Р Р†Р С•Р Т‘Р В° Р С‘Р СР ВµР Р…Р С‘ РЎвЂЎР В°РЎвЂљР В° Р ВµРЎРѓР В»Р С‘ ID Р Р…Р ВµР Т‘Р С•РЎРѓРЎвЂљРЎС“Р С—Р ВµР Р…

    waiting_for_chat_delete = (
        State()
    )  # Р вЂќР В»РЎРЏ РЎС“Р Т‘Р В°Р В»Р ВµР Р…Р С‘РЎРЏ РЎвЂЎР В°РЎвЂљР В°

    viewing_active_broadcast = State()  # Р вЂќР В»РЎРЏ Р С—РЎР‚Р С•РЎРѓР СР С•РЎвЂљРЎР‚Р В° Р В°Р С”РЎвЂљР С‘Р Р†Р Р…Р С•Р в„– РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘

    waiting_for_text_add = State()  # Р вЂќР В»РЎРЏ Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р С‘РЎРЏ Р Р…Р С•Р Р†Р С•Р С–Р С• РЎвЂљР ВµР С”РЎРѓРЎвЂљР В°

    waiting_for_text_edit = State()  # Р вЂќР В»РЎРЏ РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘РЎРЏ РЎвЂљР ВµР С”РЎРѓРЎвЂљР В°


class FakeMessage:
    """Р вЂ™РЎРѓР С—Р С•Р СР С•Р С–Р В°РЎвЂљР ВµР В»РЎРЉР Р…РЎвЂ№Р в„– Р С”Р В»Р В°РЎРѓРЎРѓ Р Т‘Р В»РЎРЏ РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘РЎРЏ РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р в„– РЎвЂЎР ВµРЎР‚Р ВµР В· callback"""

    def __init__(self, user_id, query=None):

        self.from_user = type("obj", (object,), {"id": user_id})()

        self.query = query

    async def answer(self, text, **kwargs):
        """\u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u0443\u0435\u0442 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u0438\u043b\u0438 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u0442 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435."""

        if not self.query:
            return

        try:
            reply_markup = kwargs.get("reply_markup")

            if reply_markup and isinstance(reply_markup, InlineKeyboardMarkup):
                await self.query.message.edit_text(
                    text,
                    reply_markup=reply_markup,
                    parse_mode=kwargs.get("parse_mode", "HTML"),
                )

            else:
                await self.query.message.answer(
                    text,
                    reply_markup=reply_markup,
                    parse_mode=kwargs.get("parse_mode", "HTML"),
                )

        except Exception as e:
            # Р вЂўРЎРѓР В»Р С‘ РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ Р Р…Р Вµ Р С‘Р В·Р СР ВµР Р…Р С‘Р В»Р С•РЎРѓРЎРЉ, Р С—РЎР‚Р С•РЎРѓРЎвЂљР С• Р С•РЎвЂљР С—РЎР‚Р В°Р Р†Р В»РЎРЏР ВµР С РЎС“Р Р†Р ВµР Т‘Р С•Р СР В»Р ВµР Р…Р С‘Р Вµ

            if "not modified" in str(e).lower():
                await self.query.answer("РІСљвЂ¦", show_alert=False)

            else:
                print(
                    f"РІС™В РїС‘РЏ  Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р С—РЎР‚Р С‘ РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘Р С‘ РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘РЎРЏ: {str(e)}"
                )


async def show_broadcast_menu(message_or_query, user_id: int, is_edit: bool = False):
    """Р СџР С•Р С”Р В°Р В·РЎвЂ№Р Р†Р В°Р ВµРЎвЂљ Р СР ВµР Р…РЎР‹ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘ (Р С•РЎвЂљР С—РЎР‚Р В°Р Р†Р В»РЎРЏР ВµРЎвЂљ Р С‘Р В»Р С‘ РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚РЎС“Р ВµРЎвЂљ РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ)"""

    config = get_broadcast_config(user_id)

    chats = get_broadcast_chats(user_id)

    info = build_broadcast_menu_text(config, chats, active_broadcasts, user_id)

    kb = build_broadcast_keyboard(
        include_active=False,
        user_id=user_id,
        active_broadcasts=active_broadcasts,
        back_callback="delete_bc_menu",
    )

    if is_edit:
        if isinstance(message_or_query, CallbackQuery):
            await _edit_or_notice(message_or_query, info, kb)
        else:
            try:
                await message_or_query.message.edit_text(
                    text=info, reply_markup=kb, parse_mode="HTML"
                )
            except Exception:
                await message_or_query.message.answer(
                    info, reply_markup=kb, parse_mode="HTML"
                )

    else:
        await message_or_query.answer(info, reply_markup=kb, parse_mode="HTML")


async def _edit_or_notice(
    query: CallbackQuery,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    *,
    parse_mode: str = "HTML",
    fallback_to_answer: bool = False,
) -> bool:
    try:
        await query.message.edit_text(
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
        return True
    except Exception as exc:
        error_text = str(exc).lower()
        if "not modified" in error_text:
            try:
                await query.answer("\u0423\u0436\u0435 \u0430\u043a\u0442\u0443\u0430\u043b\u044c\u043d\u043e", show_alert=False)
            except Exception:
                pass
            return False

        if fallback_to_answer:
            try:
                await query.message.answer(
                    text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                )
                return True
            except Exception:
                pass

        try:
            await query.answer(
                "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u044d\u043a\u0440\u0430\u043d",
                show_alert=False,
            )
        except Exception:
            pass
        return False


def _build_broadcast_chats_view(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    chats = get_broadcast_chats(user_id)
    info = "\U0001f4ac <b>\u0427\u0410\u0422\u042b \u0414\u041b\u042f \u0420\u0410\u0421\u0421\u042b\u041b\u041a\u0418</b>\n\n"

    if chats:
        for idx, (chat_id, chat_name) in enumerate(chats, 1):
            info += f"{idx}\ufe0f\u20e3 {chat_name}\n   ID: {chat_id}\n\n"
    else:
        info += "\U0001f4ed \u041d\u0435\u0442 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438\n\n"

    info += "\u041d\u0430\u0436\u043c\u0438 \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u0435:"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="\u2795 \u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c",
                    callback_data="bc_chats_add",
                ),
                InlineKeyboardButton(
                    text="\U0001f4c2 \u0418\u0437 \u043f\u0430\u043f\u043a\u0438",
                    callback_data="bc_chats_import",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="\U0001f5d1\ufe0f \u0423\u0434\u0430\u043b\u0438\u0442\u044c",
                    callback_data="bc_chats_delete",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                    callback_data="bc_back",
                )
            ],
        ]
    )
    return info, kb


async def show_broadcast_chats_menu(
    message_or_query, user_id: int, menu_message_id: int | None = None
) -> None:
    info, kb = _build_broadcast_chats_view(user_id)

    if menu_message_id is not None and isinstance(message_or_query, Message):
        try:
            await message_or_query.bot.edit_message_text(
                chat_id=message_or_query.chat.id,
                message_id=menu_message_id,
                text=info,
                reply_markup=kb,
                parse_mode="HTML",
            )
            return
        except Exception:
            pass

    target = (
        message_or_query.message
        if hasattr(message_or_query, "message")
        else message_or_query
    )
    try:
        await target.edit_text(info, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await target.answer(info, reply_markup=kb, parse_mode="HTML")


async def broadcast_chats_menu(message: Message):
    """Backward-compatible wrapper for old calls."""
    await show_broadcast_chats_menu(message, message.from_user.id)


@router.callback_query(F.data == "bc_chats_back")
async def bc_chats_back_callback(query: CallbackQuery):
    await query.answer()
    await show_broadcast_chats_menu(query, query.from_user.id)


def _build_folder_account_picker(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
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
                text=f"👤 {_account_label(acc_num, username, first_name)}",
                callback_data=f"bc_folder_acc_{acc_num}",
            )
        ]
        for acc_num, username, first_name in available_accounts
    ]
    keyboard_rows.append(
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="bc_chats_back")]
    )
    return text, InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


def _build_folder_list_view(account_number: int, folders: list) -> tuple[str, InlineKeyboardMarkup]:
    lines = [
        "📂 <b>ПАПКИ TELEGRAM</b>",
        "",
        f"Аккаунт: <b>{account_number}</b>",
        "",
    ]

    keyboard_rows = []
    for folder in folders:
        folder_id = int(getattr(folder, "id", 0))
        title = _folder_title(folder)
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

    keyboard_rows.append(
        [InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="bc_chats_import")]
    )
    keyboard_rows.append(
        [InlineKeyboardButton(text="⬅️ К чатам", callback_data="bc_chats_back")]
    )
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


def _build_folder_preview_view(
    account_number: int,
    folder,
    folder_chats: list[dict],
) -> tuple[str, InlineKeyboardMarkup]:
    title = _folder_title(folder)
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
    keyboard_rows.append(
        [InlineKeyboardButton(text="⬅️ К чатам", callback_data="bc_chats_back")]
    )
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


def _parse_folder_callback(data: str, action: str) -> tuple[int, int]:
    prefix = f"bc_folder_{action}_"
    if not data.startswith(prefix):
        raise ValueError("Неверный callback папки")

    tail = data[len(prefix):]
    account_text, folder_text = tail.split("_", 1)
    return int(account_text), int(folder_text)


@router.message(Command("broadcast"))
@router.message(F.text.contains("Рассылка"))
async def cmd_broadcast_menu(message: Message):
    """Р вЂњР В»Р В°Р Р†Р Р…Р С•Р Вµ Р СР ВµР Р…РЎР‹ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘ - Р С‘Р Р…РЎвЂћР С•РЎР‚Р СР В°РЎвЂ Р С‘РЎРЏ Р С‘ РЎС“Р С—РЎР‚Р В°Р Р†Р В»Р ВµР Р…Р С‘Р Вµ"""

    user_id = message.from_user.id

    if not _iter_connected_account_numbers(user_id):
        await message.answer(LOGIN_REQUIRED_TEXT)

        return

    await show_broadcast_menu(message, user_id, is_edit=False)


@router.callback_query(F.data == "close_bc_menu")
async def close_bc_menu_callback(query: CallbackQuery):
    """Return to broadcast chats menu."""

    await query.answer()
    user_id = query.from_user.id
    try:
        await show_broadcast_chats_menu(
            query, user_id, menu_message_id=query.message.message_id
        )
    except Exception:
        pass


@router.callback_query(F.data.in_(["delete_bc_menu", "delete_bs_menu"]))
async def delete_bc_menu_callback(query: CallbackQuery):
    """Close broadcast menu message (legacy callbacks supported)."""
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass


@router.callback_query(F.data == "bc_text")
async def bc_text_callback(query: CallbackQuery, state: FSMContext):
    """Open content source settings for broadcast."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    kb = build_text_settings_keyboard(
        config.get("text_source_type", "manual"),
        config.get("text_mode", "random"),
        config.get("parse_mode", "HTML"),
    )

    await state.update_data(
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
    )

    await query.message.edit_text(
        _build_text_settings_info(config),
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.callback_query(F.data == "text_source_toggle")
async def text_source_toggle_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    config["text_source_type"] = "channel" if not _is_channel_source(config) else "manual"
    config["text_index"] = 0
    save_broadcast_config_with_profile(user_id, config)

    kb = build_text_settings_keyboard(
        config.get("text_source_type", "manual"),
        config.get("text_mode", "random"),
        config.get("parse_mode", "HTML"),
    )
    await query.message.edit_text(
        _build_text_settings_info(config),
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.callback_query(F.data == "text_channel_source")
async def text_channel_source_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    config = get_broadcast_config(query.from_user.id)

    await state.set_state(BroadcastConfigState.waiting_for_source_channel)
    await state.update_data(
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
    )

    current_ref = html.escape(config.get("source_channel_ref") or "\u043d\u0435 \u0432\u044b\u0431\u0440\u0430\u043d")
    text = (
        "\U0001f4e1 <b>\u041a\u0430\u043d\u0430\u043b-\u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a</b>\n\n"
        f"\u0422\u0435\u043a\u0443\u0449\u0438\u0439 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a: <code>{current_ref}</code>\n\n"
        "\u041e\u0442\u043f\u0440\u0430\u0432\u044c \u0441\u0441\u044b\u043b\u043a\u0443 \u043d\u0430 \u043a\u0430\u043d\u0430\u043b, @username \u0438\u043b\u0438 ID \u043a\u0430\u043d\u0430\u043b\u0430.\n"
        "\u0411\u043e\u0442 \u0437\u0430\u0433\u0440\u0443\u0437\u0438\u0442 \u043f\u043e\u0441\u0442\u044b \u0438 \u0431\u0443\u0434\u0435\u0442 \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u044c \u0438\u0445 \u043a\u0430\u043a \u0432\u0430\u0440\u0438\u0430\u043d\u0442\u044b \u0442\u0435\u043a\u0441\u0442\u0430."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="\u041d\u0430\u0437\u0430\u0434", callback_data="bc_text")]]
    )
    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "text_channel_refresh")
async def text_channel_refresh_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    source_ref = config.get("source_channel_ref")
    if not source_ref:
        await query.answer("\u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u0443\u043a\u0430\u0436\u0438 \u043a\u0430\u043d\u0430\u043b-\u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a", show_alert=True)
        return

    try:
        source_data = await _load_channel_source_for_user(user_id, source_ref)
    except Exception as exc:
        await query.answer(str(exc), show_alert=True)
        return

    config.update(source_data)
    save_broadcast_config_with_profile(user_id, config)

    kb = build_texts_keyboard(
        config.get("source_posts") or [],
        back_callback="bc_text",
        item_prefix="Post",
        allow_add=False,
        extra_buttons=[
            [
                InlineKeyboardButton(
                    text="\u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043f\u043e\u0441\u0442\u044b",
                    callback_data="text_channel_refresh",
                )
            ]
        ],
    )
    await query.message.edit_text(
        _build_text_list_info(config),
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.callback_query(F.data == "text_list")
async def text_list_callback(query: CallbackQuery, state: FSMContext):
    """Show either manual texts or loaded channel posts."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    is_channel = _is_channel_source(config)
    items = config.get("source_posts") if is_channel else config.get("texts", [])
    extra_buttons = (
        [
            [
                InlineKeyboardButton(
                    text="\u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043f\u043e\u0441\u0442\u044b",
                    callback_data="text_channel_refresh",
                )
            ]
        ]
        if is_channel
        else None
    )
    kb = build_texts_keyboard(
        items or [],
        back_callback="bc_text",
        item_prefix="Пост" if is_channel else "Text",
        allow_add=not is_channel,
        extra_buttons=extra_buttons,
    )

    await state.update_data(
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
    )

    await query.message.edit_text(
        _build_text_list_info(config),
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("text_view_"))
async def text_view_callback(query: CallbackQuery, state: FSMContext):
    """Open a single manual text or a channel post preview."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    is_channel = _is_channel_source(config)

    try:
        text_index = int(query.data.split("_")[2])
        items = config.get("source_posts") if is_channel else config.get("texts", [])
        if text_index >= len(items):
            await query.answer("\u042d\u043b\u0435\u043c\u0435\u043d\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d", show_alert=True)
            return

        await state.update_data(
            edit_message_id=query.message.message_id,
            chat_id=query.message.chat.id,
        )

        if is_channel:
            post_item = items[text_index]
            info = (
                f"\U0001f4e8 <b>\u041f\u043e\u0441\u0442 #{text_index + 1}</b>\n\n"
                f"\u041a\u0430\u043d\u0430\u043b: {html.escape(source_channel_title(config))}\n"
                f"Message ID: <code>{int(post_item['message_id'])}</code>\n"
                f"\u041f\u0440\u0435\u0432\u044c\u044e: <code>{html.escape(post_preview_text(post_item.get('preview', '')))}</code>\n"
            )
            post_link = format_source_channel_link(config, int(post_item["message_id"]))
            buttons = []
            if post_link:
                buttons.append([
                    InlineKeyboardButton(
                        text="\u041e\u0442\u043a\u0440\u044b\u0442\u044c \u043f\u043e\u0441\u0442",
                        url=post_link,
                    )
                ])
            nav_row = []
            if text_index > 0:
                nav_row.append(
                    InlineKeyboardButton(
                        text="\u2b05\ufe0f \u041f\u0440\u0435\u0434",
                        callback_data=f"text_view_{text_index - 1}",
                    )
                )
            if text_index + 1 < len(items):
                nav_row.append(
                    InlineKeyboardButton(
                        text="\u0414\u0430\u043b\u0435\u0435 \u27a1\ufe0f",
                        callback_data=f"text_view_{text_index + 1}",
                    )
                )
            if nav_row:
                buttons.append(nav_row)
            buttons.append(
                [
                    InlineKeyboardButton(
                        text="\U0001f9ea \u0422\u0435\u0441\u0442 \u0441\u0435\u0431\u0435",
                        callback_data=f"text_channel_test_{text_index}",
                    )
                ]
            )
            buttons.append([InlineKeyboardButton(text="\u041d\u0430\u0437\u0430\u0434", callback_data="text_list")])
            kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        else:
            current_text = str(items[text_index])
            parse_mode = config.get("parse_mode", "HTML")
            preview_text = current_text
            suffix = ""
            if len(preview_text) > 3500:
                preview_text = preview_text[:3500]
                suffix = f"\n<i>... \u043e\u0431\u0440\u0435\u0437\u0430\u043d\u043e, \u0432\u0441\u0435\u0433\u043e {len(current_text)} \u0441\u0438\u043c\u0432\u043e\u043b\u043e\u0432</i>"

            info = (
                f"\U0001f4dd <b>\u0422\u0435\u043a\u0441\u0442 #{text_index + 1}</b>\n\n"
                f"\u0424\u043e\u0440\u043c\u0430\u0442: <b>{html.escape(parse_mode)}</b>\n"
                f"<code>{html.escape(preview_text)}</code>{suffix}"
            )
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="\u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u0442\u044c",
                            callback_data=f"text_edit_{text_index}",
                        ),
                        InlineKeyboardButton(
                            text="\u0423\u0434\u0430\u043b\u0438\u0442\u044c",
                            callback_data=f"text_delete_{text_index}",
                        ),
                    ],
                    [InlineKeyboardButton(text="\u041d\u0430\u0437\u0430\u0434", callback_data="text_list")],
                ]
            )

        await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")
    except (ValueError, IndexError):
        await query.answer("\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0442\u043a\u0440\u044b\u0442\u044c \u044d\u043b\u0435\u043c\u0435\u043d\u0442", show_alert=True)


@router.callback_query(F.data.startswith("text_channel_test_"))
async def text_channel_test_callback(query: CallbackQuery):
    await query.answer("Отправляю тест...")

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    if not _is_channel_source(config):
        await query.answer("Этот тест работает только для канального режима", show_alert=True)
        return

    try:
        text_index = int(query.data.split("_")[3])
        client, source_message, account_number = await _load_channel_preview_message(
            user_id,
            config,
            text_index,
        )
        await client.send_message("me", source_message)
        await query.answer(
            f"Тестовый пост отправлен в Избранное через аккаунт {account_number}",
            show_alert=True,
        )
    except Exception as exc:
        await query.answer(
            f"Не удалось отправить тестовый пост: {str(exc)}",
            show_alert=True,
        )


@router.callback_query(F.data == "text_add_new")
async def text_add_new_callback(query: CallbackQuery, state: FSMContext):
    """Ask user for a new manual broadcast text."""

    await query.answer()

    config = get_broadcast_config(query.from_user.id)
    if _is_channel_source(config):
        await query.answer(
            "\u0412 \u0440\u0435\u0436\u0438\u043c\u0435 \u043a\u0430\u043d\u0430\u043b\u0430 \u0442\u0435\u043a\u0441\u0442\u044b \u0432\u0440\u0443\u0447\u043d\u0443\u044e \u043d\u0435 \u0440\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u0443\u044e\u0442\u0441\u044f",
            show_alert=True,
        )
        return

    await state.set_state(BroadcastConfigState.waiting_for_text_add)
    await state.update_data(
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
    )

    text = (
        "\u270d\ufe0f <b>\u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u043d\u043e\u0432\u044b\u0439 \u0442\u0435\u043a\u0441\u0442</b>\n\n"
        "\u041e\u0442\u043f\u0440\u0430\u0432\u044c \u043e\u0434\u043d\u0438\u043c \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435\u043c \u0442\u0435\u043a\u0441\u0442 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438.\n\n"
        f"\u0422\u0435\u043a\u0443\u0449\u0438\u0439 \u0444\u043e\u0440\u043c\u0430\u0442: <b>{html.escape(config.get('parse_mode', 'HTML'))}</b>."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="\u041d\u0430\u0437\u0430\u0434", callback_data="text_list")]]
    )
    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data.startswith("text_edit_"))
async def text_edit_callback(query: CallbackQuery, state: FSMContext):
    """Ask user for a new body for an existing manual text."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    if _is_channel_source(config):
        await query.answer(
            "\u041f\u043e\u0441\u0442\u044b \u043a\u0430\u043d\u0430\u043b\u0430 \u0440\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u0443\u044e\u0442\u0441\u044f \u0442\u043e\u043b\u044c\u043a\u043e \u0432 \u0441\u0430\u043c\u043e\u043c \u043a\u0430\u043d\u0430\u043b\u0435",
            show_alert=True,
        )
        return

    try:
        text_index = int(query.data.split("_")[2])
        texts = config.get("texts") or []
        if text_index >= len(texts):
            await query.answer("\u0422\u0435\u043a\u0441\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d", show_alert=True)
            return

        current_text = str(texts[text_index])
        parse_mode = config.get("parse_mode", "HTML")

        await state.set_state(BroadcastConfigState.waiting_for_text_edit)
        await state.update_data(
            edit_message_id=query.message.message_id,
            chat_id=query.message.chat.id,
            text_index=text_index,
        )

        preview_text = current_text
        suffix = ""
        if len(preview_text) > 3500:
            preview_text = preview_text[:3500]
            suffix = f"\n<i>... \u043e\u0431\u0440\u0435\u0437\u0430\u043d\u043e, \u0432\u0441\u0435\u0433\u043e {len(current_text)} \u0441\u0438\u043c\u0432\u043e\u043b\u043e\u0432</i>"

        text = (
            f"\u270f\ufe0f <b>\u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435 \u0442\u0435\u043a\u0441\u0442\u0430 #{text_index + 1}</b>\n\n"
            f"\u0424\u043e\u0440\u043c\u0430\u0442: <b>{html.escape(parse_mode)}</b>\n\n"
            f"<code>{html.escape(preview_text)}</code>{suffix}\n\n"
            "\u041e\u0442\u043f\u0440\u0430\u0432\u044c \u043d\u043e\u0432\u044b\u0439 \u0432\u0430\u0440\u0438\u0430\u043d\u0442 \u0442\u0435\u043a\u0441\u0442\u0430 \u043e\u0434\u043d\u0438\u043c \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435\u043c."
        )

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="\u041d\u0430\u0437\u0430\u0434",
                        callback_data=f"text_view_{text_index}",
                    )
                ]
            ]
        )
        await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except (ValueError, IndexError):
        await query.answer("\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0442\u043a\u0440\u044b\u0442\u044c \u0442\u0435\u043a\u0441\u0442", show_alert=True)


@router.callback_query(F.data.startswith("text_delete_"))
async def text_delete_callback(query: CallbackQuery, state: FSMContext):
    """Delete a manual broadcast text."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)

    if _is_channel_source(config):
        await query.answer(
            "\u041f\u043e\u0441\u0442\u044b \u043a\u0430\u043d\u0430\u043b\u0430 \u0443\u0434\u0430\u043b\u044f\u044e\u0442\u0441\u044f \u0442\u043e\u043b\u044c\u043a\u043e \u0432 \u0441\u0430\u043c\u043e\u043c \u043a\u0430\u043d\u0430\u043b\u0435",
            show_alert=True,
        )
        return

    try:
        text_index = int(query.data.split("_")[2])
        texts = list(config.get("texts") or [])
        if text_index >= len(texts):
            await query.answer("\u0422\u0435\u043a\u0441\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d", show_alert=True)
            return

        texts.pop(text_index)
        config["texts"] = texts
        if text_index >= len(texts):
            config["text_index"] = max(len(texts) - 1, 0)

        save_broadcast_config_with_profile(user_id, config)
        await query.answer("\u0422\u0435\u043a\u0441\u0442 \u0443\u0434\u0430\u043b\u0435\u043d")

        kb = build_texts_keyboard(config["texts"], back_callback="bc_text")
        await query.message.edit_text(
            _build_text_list_info(config),
            reply_markup=kb,
            parse_mode="HTML",
        )
    except (ValueError, IndexError):
        await query.answer("\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0443\u0434\u0430\u043b\u0438\u0442\u044c \u0442\u0435\u043a\u0441\u0442", show_alert=True)


@router.callback_query(F.data == "text_mode_toggle")
async def text_mode_toggle_callback(query: CallbackQuery, state: FSMContext):
    """Toggle random/sequential selection for current content source."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    if count_source_items(config) == 0:
        await query.answer("\u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u0434\u043e\u0431\u0430\u0432\u044c \u0432\u0430\u0440\u0438\u0430\u043d\u0442\u044b \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438", show_alert=True)
        return

    config["text_mode"] = "sequence" if config.get("text_mode") == "random" else "random"
    config["text_index"] = 0
    save_broadcast_config_with_profile(user_id, config)

    kb = build_text_settings_keyboard(
        config.get("text_source_type", "manual"),
        config.get("text_mode", "random"),
        config.get("parse_mode", "HTML"),
    )
    await query.message.edit_text(
        _build_text_settings_info(config),
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.callback_query(F.data == "text_format_toggle")
async def text_format_toggle_callback(query: CallbackQuery, state: FSMContext):
    """Toggle parse mode for manual texts only."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    if _is_channel_source(config):
        await query.answer(
            "\u0414\u043b\u044f \u043f\u043e\u0441\u0442\u043e\u0432 \u0438\u0437 \u043a\u0430\u043d\u0430\u043b\u0430 \u0444\u043e\u0440\u043c\u0430\u0442 \u043f\u0435\u0440\u0435\u043a\u043b\u044e\u0447\u0430\u0442\u044c \u043d\u0435 \u043d\u0443\u0436\u043d\u043e",
            show_alert=True,
        )
        return

    config["parse_mode"] = "Markdown" if config.get("parse_mode") == "HTML" else "HTML"
    save_broadcast_config_with_profile(user_id, config)

    kb = build_text_settings_keyboard(
        config.get("text_source_type", "manual"),
        config.get("text_mode", "random"),
        config.get("parse_mode", "HTML"),
    )
    await query.message.edit_text(
        _build_text_settings_info(config),
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.callback_query(F.data == "bc_quantity")
async def bc_quantity_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    await state.set_state(BroadcastConfigState.waiting_for_count)

    await state.update_data(
        edit_message_id=query.message.message_id, chat_id=query.message.chat.id
    )

    config = get_broadcast_config(query.from_user.id)

    text = (
        "\U0001f522 <b>\u041e\u0411\u0429\u0415\u0415 \u041a\u041e\u041b-\u0412\u041e \u0421\u041e\u041e\u0411\u0429\u0415\u041d\u0418\u0419</b>\n\n"
        f"\u0422\u0435\u043a\u0443\u0449\u0435\u0435: {config.get('count', 0)}\n\n"
        "\u0412\u0432\u0435\u0434\u0438 \u043d\u043e\u0432\u043e\u0435 \u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u043e\u0442 1 \u0434\u043e 1000:"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=CANCEL_TEXT,
                    callback_data="bc_cancel",
                )
            ]
        ]
    )

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "bc_interval")
async def bc_interval_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    await state.set_state(BroadcastConfigState.waiting_for_interval)

    await state.update_data(
        edit_message_id=query.message.message_id, chat_id=query.message.chat.id
    )

    config = get_broadcast_config(query.from_user.id)

    current_interval = config.get("interval", "30-90")

    text = (
        "\u23f1\ufe0f <b>\u0418\u041d\u0422\u0415\u0420\u0412\u0410\u041b \u0414\u041b\u042f \u041a\u0410\u0416\u0414\u041e\u0413\u041e \u0427\u0410\u0422\u0410</b>\n\n"
        f"\u0422\u0435\u043a\u0443\u0449\u0438\u0439: {current_interval} \u043c\u0438\u043d\n\n"
        "\u041f\u043e\u0441\u043b\u0435 \u043a\u0430\u0436\u0434\u043e\u0439 \u043e\u0442\u043f\u0440\u0430\u0432\u043a\u0438 \u0431\u043e\u0442 \u0437\u0430\u043d\u043e\u0432\u043e \u043d\u0430\u0437\u043d\u0430\u0447\u0430\u0435\u0442 \u044d\u0442\u043e\u0442 \u0438\u043d\u0442\u0435\u0440\u0432\u0430\u043b \u0438\u043c\u0435\u043d\u043d\u043e \u0434\u043b\u044f \u0442\u043e\u0433\u043e \u0447\u0430\u0442\u0430, \u043a\u0443\u0434\u0430 \u0442\u043e\u043b\u044c\u043a\u043e \u0447\u0442\u043e \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u043b.\n"
        "\u0412\u0432\u0435\u0434\u0438 \u043e\u0434\u043d\u043e \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d.\n"
        "\u041f\u0440\u0438\u043c\u0435\u0440\u044b: <code>15</code> \u0438\u043b\u0438 <code>10-30</code>"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=CANCEL_TEXT,
                    callback_data="bc_cancel",
                )
            ]
        ]
    )

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "bc_batch_pause")
async def bc_batch_pause_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    await state.set_state(BroadcastConfigState.waiting_for_chat_pause)

    await state.update_data(
        edit_message_id=query.message.message_id, chat_id=query.message.chat.id
    )

    config = get_broadcast_config(query.from_user.id)

    current_pause = config.get("chat_pause", "20-60")

    text = (
        "\u26a1 <b>\u0422\u0415\u041c\u041f</b>\n\n"
        "\u042d\u0442\u043e \u043c\u0438\u043d\u0438\u043c\u0430\u043b\u044c\u043d\u0430\u044f \u043f\u0430\u0443\u0437\u0430 \u043c\u0435\u0436\u0434\u0443 \u043b\u044e\u0431\u044b\u043c\u0438 \u0434\u0432\u0443\u043c\u044f \u043e\u0442\u043f\u0440\u0430\u0432\u043a\u0430\u043c\u0438.\n"
        "\u0415\u0441\u043b\u0438 \u0434\u0432\u0430 \u0447\u0430\u0442\u0430 \u0433\u043e\u0442\u043e\u0432\u044b \u043f\u043e\u0447\u0442\u0438 \u043e\u0434\u043d\u043e\u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e, \u0438\u043c\u0435\u043d\u043d\u043e \u0442\u0435\u043c\u043f \u0440\u0430\u0437\u0434\u0432\u0438\u043d\u0435\u0442 \u0438\u0445 \u043f\u043e \u0432\u0440\u0435\u043c\u0435\u043d\u0438.\n\n"
        f"\u0422\u0435\u043a\u0443\u0449\u0438\u0439: <b>{current_pause}</b> \u0441\u0435\u043a\n\n"
        "\u0412\u0432\u0435\u0434\u0438 \u043e\u0434\u043d\u043e \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d:\n"
        "\u2022 <code>2</code>\n"
        "\u2022 <code>1-3</code>\n"
        f"\u041c\u0430\u043a\u0441\u0438\u043c\u0443\u043c: <code>{CHAT_PAUSE_MAX_SECONDS}</code> \u0441\u0435\u043a"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=CANCEL_TEXT,
                    callback_data="bc_cancel_tempo",
                )
            ]
        ]
    )

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "bc_cancel_tempo")
async def bc_cancel_tempo_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    await state.clear()

    user_id = query.from_user.id

    await show_broadcast_menu(query, user_id, is_edit=True)


@router.callback_query(F.data == "bc_back")
async def bc_back_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id
    await show_broadcast_menu(query, user_id, is_edit=True)


@router.callback_query(F.data == "bc_cancel")
async def bc_cancel_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id
    fake = FakeMessage(user_id, query)
    await return_to_previous_menu(fake, state)


@router.callback_query(F.data == "bc_chats")
async def bc_chats_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    user_id = query.from_user.id
    await state.update_data(
        previous_menu="broadcast", menu_message_id=query.message.message_id
    )
    await show_broadcast_chats_menu(
        query, user_id, menu_message_id=query.message.message_id
    )


@router.callback_query(F.data == "bc_active")
async def bc_active_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    user_broadcasts = {
        bid: b
        for bid, b in active_broadcasts.items()
        if b["user_id"] == user_id and b["status"] in ("running", "paused")
    }

    if not user_broadcasts:
        text = "\U0001f4ed \u041d\u0435\u0442 \u0430\u043a\u0442\u0438\u0432\u043d\u044b\u0445 \u0440\u0430\u0441\u0441\u044b\u043b\u043e\u043a"

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                        callback_data="bc_back",
                    )
                ]
            ]
        )

        await _edit_or_notice(query, text, kb)
        return

    groups = {}

    singles = []

    for bid, b in user_broadcasts.items():
        gid = b.get("group_id")

        if gid is None:
            singles.append((bid, b))

        else:
            groups.setdefault(gid, []).append((bid, b))

    total_running = sum(1 for _, b in user_broadcasts.items() if b["status"] == "running")
    total_paused = sum(1 for _, b in user_broadcasts.items() if b["status"] == "paused")
    info = (
        "\U0001f4e4 <b>\u0410\u041a\u0422\u0418\u0412\u041d\u042b\u0415 \u0420\u0410\u0421\u0421\u042b\u041b\u041a\u0418</b>\n\n"
        f"\u0412\u0441\u0435\u0433\u043e: {len(user_broadcasts)} | "
        f"\u25b6\ufe0f {total_running} | \u23f8\ufe0f {total_paused}\n\n"
    )

    buttons = []

    for gid, items in sorted(groups.items()):
        status = (
            "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
            if any(b["status"] == "running" for _, b in items)
            else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
        )

        sent = sum(int(b.get("sent_chats", 0) or 0) for _, b in items)
        plan = sum(int(b.get("planned_count", 0) or 0) for _, b in items)
        info += (
            f"{status} <b>\u0413\u0440\u0443\u043f\u043f\u0430 #{gid}</b>\n"
            f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432: {len(items)} | "
            f"\u041f\u0440\u043e\u0433\u0440\u0435\u0441\u0441: {sent}/{plan}\n\n"
        )

        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"{status.split()[0]} \u0413\u0440\u0443\u043f\u043f\u0430 #{gid}",
                    callback_data=f"view_group_{gid}",
                )
            ]
        )

    for bid, b in sorted(singles):
        status = (
            "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
            if b["status"] == "running"
            else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
        )

        account_name = b.get(
            "account_name",
            f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442 {b.get('account', '?')}",
        )

        info += (
            f"{status} <b>\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 #{bid}</b>\n"
            f"{account_name} | {b.get('sent_chats', 0)}/{b.get('planned_count', 0)}\n\n"
        )

        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"{status.split()[0]} \u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 #{bid}",
                    callback_data=f"view_bc_{bid}",
                )
            ]
        )

    buttons.append(
        [
            InlineKeyboardButton(
                text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                callback_data="bc_back",
            )
        ]
    )

    await _edit_or_notice(
        query,
        info.strip(),
        InlineKeyboardMarkup(inline_keyboard=buttons),
    )


async def _render_group_detail(query: CallbackQuery, user_id: int, gid: int) -> None:
    payload = _build_group_detail_payload(user_id, gid)
    if not payload:
        await query.answer(
            "\u0413\u0440\u0443\u043f\u043f\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )
        return

    info, keyboard = payload
    await _edit_or_notice(query, info, keyboard)


def _group_runtime_items(user_id: int, gid: int) -> list[tuple[int, dict]]:
    return [
        (bid, broadcast)
        for bid, broadcast in active_broadcasts.items()
        if broadcast.get("group_id") == gid
        and broadcast.get("user_id") == user_id
        and broadcast.get("status") in ("running", "paused")
    ]


def _group_error_runtime_items(items: list[tuple[int, dict]]) -> list[tuple[int, dict, dict]]:
    error_items: list[tuple[int, dict, dict]] = []
    for bid, broadcast in items:
        for chat_item in _broadcast_chat_runtime_items(broadcast):
            if str(chat_item.get("last_error") or "").strip():
                error_items.append((bid, broadcast, chat_item))
    error_items.sort(
        key=lambda item: float(item[2].get("last_error_at", 0.0) or 0.0),
        reverse=True,
    )
    return error_items


def _build_group_detail_payload(
    user_id: int, gid: int
) -> tuple[str, InlineKeyboardMarkup] | None:
    items = _group_runtime_items(user_id, gid)
    if not items:
        return None

    total_accounts = len(items)
    total_chats = sum(int(broadcast.get("total_chats", 0) or 0) for _, broadcast in items)
    total_count = sum(int(broadcast.get("count", 0) or 0) for _, broadcast in items)
    sent = sum(int(broadcast.get("sent_chats", 0) or 0) for _, broadcast in items)
    failed = sum(int(broadcast.get("failed_count", 0) or 0) for _, broadcast in items)
    error_items = _group_error_runtime_items(items)

    status = (
        "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
        if any(broadcast["status"] == "running" for _, broadcast in items)
        else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
    )

    interval_values = {
        str(broadcast.get("interval_value", broadcast.get("interval_minutes", "?")))
        for _, broadcast in items
    }
    pause_values = {str(broadcast.get("chat_pause", "20-60")) for _, broadcast in items}
    interval_text = ", ".join(sorted(interval_values)) if interval_values else "-"
    pause_text = ", ".join(sorted(pause_values)) if pause_values else "-"

    info = f"\U0001f4e6 <b>\u0413\u0440\u0443\u043f\u043f\u0430 #{gid}</b>\n\n"
    info += f"\u0421\u0442\u0430\u0442\u0443\u0441: {status}\n"
    info += f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432: {total_accounts}\n"
    info += f"\u0427\u0430\u0442\u043e\u0432: {total_chats}\n"
    info += f"\u041f\u043b\u0430\u043d: {total_count}\n"
    info += f"\u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e: {sent}\n"
    info += f"\u041e\u0448\u0438\u0431\u043e\u043a: {failed}\n"
    info += f"\u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b: {html.escape(interval_text)} \u043c\u0438\u043d\n"
    info += f"\u0422\u0435\u043c\u043f: {html.escape(pause_text)} \u0441\u0435\u043a\n"

    buttons = [
        [
            InlineKeyboardButton(
                text="\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430",
                callback_data=f"bc_group_pause_{gid}",
            ),
            InlineKeyboardButton(
                text="\u25b6\ufe0f \u041f\u0440\u043e\u0434\u043e\u043b\u0436\u0438\u0442\u044c",
                callback_data=f"bc_group_resume_{gid}",
            ),
        ],
        [
            InlineKeyboardButton(
                text="\u26d4 \u041e\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u044c",
                callback_data=f"bc_group_cancel_{gid}",
            )
        ],
        [
            InlineKeyboardButton(
                text="\u270f\ufe0f \u041a\u043e\u043b-\u0432\u043e",
                callback_data=f"bc_group_edit_count_{gid}",
            ),
            InlineKeyboardButton(
                text="\u23f1\ufe0f \u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b",
                callback_data=f"bc_group_edit_interval_{gid}",
            ),
        ],
        [
            InlineKeyboardButton(
                text="\u26a1 \u0422\u0435\u043c\u043f",
                callback_data=f"bc_group_edit_pause_{gid}",
            ),
            InlineKeyboardButton(
                text="\U0001f504 \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c",
                callback_data=f"view_group_{gid}",
            ),
        ],
    ]
    if error_items:
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"\u26a0\ufe0f \u041e\u0448\u0438\u0431\u043a\u0438 ({len(error_items)})",
                    callback_data=f"bc_group_errors_{gid}",
                )
            ]
        )
    buttons.append(
        [
            InlineKeyboardButton(
                text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                callback_data="bc_active",
            )
        ]
    )
    return info, InlineKeyboardMarkup(inline_keyboard=buttons)


async def _edit_group_detail_message(
    message: Message, user_id: int, gid: int, *, chat_id: int, message_id: int
) -> bool:
    payload = _build_group_detail_payload(user_id, gid)
    if not payload:
        return False
    info, keyboard = payload
    await message.bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=info,
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    return True


async def _render_group_error_log(query: CallbackQuery, gid: int) -> None:
    user_id = query.from_user.id
    items = _group_runtime_items(user_id, gid)
    if not items:
        await query.answer(
            "\u0413\u0440\u0443\u043f\u043f\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )
        return

    error_items = _group_error_runtime_items(items)
    lines = [f"\u26a0\ufe0f <b>\u041e\u0448\u0438\u0431\u043a\u0438 \u0433\u0440\u0443\u043f\u043f\u044b #{gid}</b>", ""]
    if not error_items:
        lines.append("\u0421\u0435\u0439\u0447\u0430\u0441 \u043e\u0448\u0438\u0431\u043e\u043a \u043d\u0435\u0442.")
    else:
        for _, broadcast, chat_item in error_items[:15]:
            account_name = html.escape(
                str(
                    broadcast.get(
                        "account_name",
                        f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442 {broadcast.get('account', '?')}",
                    )
                )
            )
            lines.append(f"<b>{account_name}</b>")
            lines.append(f"<pre>{html.escape(_format_chat_error_log(chat_item))}</pre>")

    buttons = [
        [
            InlineKeyboardButton(
                text="\u2b05\ufe0f \u041a \u0433\u0440\u0443\u043f\u043f\u0435",
                callback_data=f"view_group_{gid}",
            )
        ]
    ]
    if error_items:
        buttons.append(
            [
                InlineKeyboardButton(
                    text="\U0001f504 \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c",
                    callback_data=f"bc_group_errors_{gid}",
                )
            ]
        )

    await _edit_or_notice(
        query,
        "\n".join(lines),
        InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("view_group_"))
async def view_group_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    try:
        gid = int(query.data.split("_")[2])

    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)

        return

    await _render_group_detail(query, user_id, gid)


@router.callback_query(F.data.startswith("bc_group_errors_"))
async def bc_group_errors_callback(query: CallbackQuery):
    await query.answer()
    try:
        gid = int(query.data.split("_")[3])
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    await _render_group_error_log(query, gid)


@router.callback_query(F.data.startswith("bc_group_edit_count_"))
async def bc_group_edit_count_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id
    try:
        gid = int(query.data.split("_")[4])
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    if not _group_runtime_items(user_id, gid):
        await query.answer(
            "\u0413\u0440\u0443\u043f\u043f\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )
        return

    await state.set_state(BroadcastConfigState.waiting_for_count)
    await state.update_data(
        edit_group_id=gid,
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
        previous_menu="group_detail",
    )
    await query.message.edit_text(
        "\U0001f522 <b>\u041a\u041e\u041b-\u0412\u041e \u0414\u041b\u042f \u0413\u0420\u0423\u041f\u041f\u042b</b>\n\n"
        "\u041d\u043e\u0432\u043e\u0435 \u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u043f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u0441\u044f \u043a \u043a\u0430\u0436\u0434\u043e\u0439 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0435 \u0432 \u0433\u0440\u0443\u043f\u043f\u0435.\n\n"
        "\u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u043e \u043e\u0442 1 \u0434\u043e 1000:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=CANCEL_TEXT,
                        callback_data=f"view_group_{gid}",
                    )
                ]
            ]
        ),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("bc_group_edit_interval_"))
async def bc_group_edit_interval_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id
    try:
        gid = int(query.data.split("_")[4])
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    if not _group_runtime_items(user_id, gid):
        await query.answer(
            "\u0413\u0440\u0443\u043f\u043f\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )
        return

    await state.set_state(BroadcastConfigState.waiting_for_interval)
    await state.update_data(
        edit_group_id=gid,
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
        previous_menu="group_detail",
    )
    await query.message.edit_text(
        "\u23f1\ufe0f <b>\u0418\u041d\u0422\u0415\u0420\u0412\u0410\u041b \u0414\u041b\u042f \u0413\u0420\u0423\u041f\u041f\u042b</b>\n\n"
        "\u041d\u043e\u0432\u044b\u0439 \u0438\u043d\u0442\u0435\u0440\u0432\u0430\u043b \u043f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u0441\u044f \u043a \u043a\u0430\u0436\u0434\u043e\u0439 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0435 \u0432 \u0433\u0440\u0443\u043f\u043f\u0435.\n"
        "\u041e\u0442\u043f\u0440\u0430\u0432\u044c \u043e\u0434\u043d\u043e \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d.\n"
        "\u041f\u0440\u0438\u043c\u0435\u0440\u044b: <code>15</code> \u0438\u043b\u0438 <code>10-30</code>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=CANCEL_TEXT,
                        callback_data=f"view_group_{gid}",
                    )
                ]
            ]
        ),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("bc_group_edit_pause_"))
async def bc_group_edit_pause_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id
    try:
        gid = int(query.data.split("_")[4])
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    if not _group_runtime_items(user_id, gid):
        await query.answer(
            "\u0413\u0440\u0443\u043f\u043f\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )
        return

    await state.set_state(BroadcastConfigState.waiting_for_chat_pause)
    await state.update_data(
        edit_group_id=gid,
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
        previous_menu="group_detail",
    )
    await query.message.edit_text(
        "\u26a1 <b>\u0422\u0415\u041c\u041f \u0414\u041b\u042f \u0413\u0420\u0423\u041f\u041f\u042b</b>\n\n"
        "\u041d\u043e\u0432\u044b\u0439 \u0442\u0435\u043c\u043f \u043f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u0441\u044f \u043a\u043e \u0432\u0441\u0435\u043c \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0430\u043c \u0432 \u0433\u0440\u0443\u043f\u043f\u0435.\n\n"
        "\u0412\u0432\u0435\u0434\u0438 \u043e\u0434\u043d\u043e \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d:\n"
        "\u2022 <code>2</code>\n"
        "\u2022 <code>1-3</code>\n"
        f"\u041c\u0430\u043a\u0441\u0438\u043c\u0443\u043c: <code>{CHAT_PAUSE_MAX_SECONDS}</code> \u0441\u0435\u043a",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=CANCEL_TEXT,
                        callback_data=f"view_group_{gid}",
                    )
                ]
            ]
        ),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("bc_group_pause_"))
async def bc_group_pause_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    gid = int(query.data.split("_")[3])

    for bid, b in list(active_broadcasts.items()):
        if b.get("group_id") == gid and b.get("user_id") == user_id:
            await set_broadcast_status(bid, "paused")

    await _render_group_detail(query, user_id, gid)


@router.callback_query(F.data.startswith("bc_group_resume_"))
async def bc_group_resume_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    gid = int(query.data.split("_")[3])

    for bid, b in list(active_broadcasts.items()):
        if b.get("group_id") == gid and b.get("user_id") == user_id:
            await set_broadcast_status(bid, "running")
            _start_or_resume_broadcast_task(bid)

    await _render_group_detail(query, user_id, gid)


@router.callback_query(F.data.startswith("bc_group_cancel_"))
async def bc_group_cancel_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    gid = int(query.data.split("_")[3])

    for bid, b in list(active_broadcasts.items()):
        if b.get("group_id") == gid and b.get("user_id") == user_id:
            await set_broadcast_status(bid, "cancelled")

    await bc_active_callback(query)


@router.callback_query(F.data.startswith("view_bc_"))
async def view_bc_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    try:
        bid = int(query.data.split("_")[2])

    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)

        return

    if bid not in active_broadcasts or active_broadcasts[bid]["user_id"] != user_id:
        await query.answer(
            "\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )

        return

    b = active_broadcasts[bid]
    chat_items = _broadcast_chat_runtime_items(b)
    active_chats, paused_chats, disabled_chats = _active_chat_counts(b)

    status = (
        "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
        if b["status"] == "running"
        else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
        if b["status"] == "paused"
        else "\u26d4 \u041e\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d\u0430"
        if b["status"] == "cancelled"
        else "\u274c \u041e\u0448\u0438\u0431\u043a\u0430"
        if b["status"] == "error"
        else "\u2705 \u0417\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0430"
    )

    account_name = b.get(
        "account_name",
        f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442 {b.get('account', '?')}",
    )

    info = (
        f"\U0001f4e4 <b>\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 #{bid}</b>\n\n"
    )

    info += f"\u0421\u0442\u0430\u0442\u0443\u0441: {status}\n"

    info += f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442: {account_name}\n"

    info += f"\u0427\u0430\u0442\u043e\u0432: {b.get('total_chats', 0)}\n"
    info += (
        f"\u0410\u043a\u0442\u0438\u0432\u043d\u044b: {active_chats} | "
        f"\u041f\u0430\u0443\u0437\u0430: {paused_chats} | "
        f"\u041e\u0442\u043a\u043b: {disabled_chats}\n"
    )

    info += (
        f"\u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e: "
        f"{b.get('sent_chats', 0)}/{b.get('planned_count', 0)}\n"
    )
    info += f"\u041e\u0448\u0438\u0431\u043e\u043a: {b.get('failed_count', 0)}\n"

    info += f"\u041a\u043e\u043b-\u0432\u043e: {b.get('count', 0)}\n"

    info += f"\u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b: {b.get('interval_minutes', '?')} \u043c\u0438\u043d \u043d\u0430 \u0447\u0430\u0442\n"

    error_items = [item for item in chat_items if item.get("last_error")]

    buttons = [
        [
            InlineKeyboardButton(
                text="\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430",
                callback_data=f"pause_bc_{bid}",
            ),
            InlineKeyboardButton(
                text="\u25b6\ufe0f \u041f\u0440\u043e\u0434\u043e\u043b\u0436\u0438\u0442\u044c",
                callback_data=f"resume_bc_{bid}",
            ),
            InlineKeyboardButton(
                text="\u26d4 \u0421\u0442\u043e\u043f",
                callback_data=f"cancel_bc_{bid}",
            ),
        ],
        [
            InlineKeyboardButton(
                text="\u270f\ufe0f \u041a\u043e\u043b-\u0432\u043e",
                callback_data=f"bc_edit_count_{bid}",
            ),
            InlineKeyboardButton(
                text="\u23f1\ufe0f \u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b",
                callback_data=f"bc_edit_interval_{bid}",
            ),
        ],
    ]

    action_row = [
        InlineKeyboardButton(
            text="\U0001f4dd \u0427\u0430\u0442\u044b",
            callback_data=f"bc_chat_list_{bid}",
        )
    ]
    if error_items:
        action_row.append(
            InlineKeyboardButton(
                text=f"\u26a0\ufe0f \u041e\u0448\u0438\u0431\u043a\u0438 ({len(error_items)})",
                callback_data=f"bc_errors_{bid}",
            )
        )
    buttons.append(action_row)

    buttons.append(
        [
            InlineKeyboardButton(
                text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                callback_data="bc_active",
            ),
            InlineKeyboardButton(
                text="\U0001f504 \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c",
                callback_data=f"view_bc_{bid}",
            ),
        ]
    )

    await _edit_or_notice(
        query,
        info,
        InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("pause_bc_"))
async def pause_bc_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    bid = int(query.data.split("_")[2])

    if bid in active_broadcasts and active_broadcasts[bid]["user_id"] == user_id:
        await set_broadcast_status(bid, "paused")

    await view_bc_callback(query)


@router.callback_query(F.data.startswith("resume_bc_"))
async def resume_bc_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    bid = int(query.data.split("_")[2])

    if bid in active_broadcasts and active_broadcasts[bid]["user_id"] == user_id:
        await set_broadcast_status(bid, "running")
        _start_or_resume_broadcast_task(bid)

    await view_bc_callback(query)


@router.callback_query(F.data.startswith("cancel_bc_"))
async def cancel_bc_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    bid = int(query.data.split("_")[2])

    if bid in active_broadcasts and active_broadcasts[bid]["user_id"] == user_id:
        await set_broadcast_status(bid, "cancelled")

    await bc_active_callback(query)


async def _render_broadcast_error_log(query: CallbackQuery, bid: int) -> None:
    user_id = query.from_user.id
    broadcast = active_broadcasts.get(bid)
    if not broadcast or broadcast.get("user_id") != user_id:
        await query.answer(
            "\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )
        return

    error_items = [
        item
        for item in _broadcast_chat_runtime_items(broadcast)
        if str(item.get("last_error") or "").strip()
    ]
    error_items.sort(
        key=lambda item: float(item.get("last_error_at", 0.0) or 0.0),
        reverse=True,
    )

    info = [f"\u26a0\ufe0f <b>\u041e\u0448\u0438\u0431\u043a\u0438 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438 #{bid}</b>", ""]
    if not error_items:
        info.append("\u0421\u0435\u0439\u0447\u0430\u0441 \u043e\u0448\u0438\u0431\u043e\u043a \u043d\u0435\u0442.")
    else:
        for item in error_items[:15]:
            info.append(f"<pre>{html.escape(_format_chat_error_log(item))}</pre>")

    buttons = [
        [
            InlineKeyboardButton(
                text="\u2b05\ufe0f \u041a \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0435",
                callback_data=f"view_bc_{bid}",
            )
        ]
    ]
    if error_items:
        buttons.append(
            [
                InlineKeyboardButton(
                    text="\U0001f504 \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c",
                    callback_data=f"bc_errors_{bid}",
                )
            ]
        )

    await _edit_or_notice(
        query,
        "\n".join(info),
        InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("bc_errors_"))
async def bc_errors_callback(query: CallbackQuery):
    await query.answer()
    try:
        bid = int(query.data.split("_")[2])
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    await _render_broadcast_error_log(query, bid)


async def _render_broadcast_chat_list(query: CallbackQuery, bid: int) -> None:
    user_id = query.from_user.id
    broadcast = active_broadcasts.get(bid)
    if not broadcast or broadcast.get("user_id") != user_id:
        await query.answer(
            "\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )
        return

    items = _broadcast_chat_runtime_items(broadcast)
    lines = [f"\U0001f4dd <b>\u0427\u0430\u0442\u044b \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438 #{bid}</b>", ""]
    buttons: list[list[InlineKeyboardButton]] = []

    if not items:
        lines.append("\u0421\u043f\u0438\u0441\u043e\u043a \u0447\u0430\u0442\u043e\u0432 \u043f\u0443\u0441\u0442.")
    else:
        for item in items:
            order = int(item.get("order", 0) or 0)
            number = order + 1
            name = html.escape(_broadcast_chat_short_name(item))
            sent = int(item.get("sent_count", 0) or 0)
            failed = int(item.get("failed_count", 0) or 0)
            lines.append(
                f"{number}. {_broadcast_chat_status_label(item)} | {name} | \u2705 {sent} | \u274c {failed}"
            )
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=f"{number}. {_broadcast_chat_short_name(item)[:24]}",
                        callback_data=f"bc_chat_view_{bid}_{order}",
                    )
                ]
            )

    buttons.append(
        [
            InlineKeyboardButton(
                text="\U0001f504 \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c",
                callback_data=f"bc_chat_list_{bid}",
            )
        ]
    )

    buttons.append(
        [
            InlineKeyboardButton(
                text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                callback_data=f"view_bc_{bid}",
            )
        ]
    )

    text = "\n".join(lines)
    markup = InlineKeyboardMarkup(inline_keyboard=buttons)
    await _edit_or_notice(query, text, markup)


async def _render_broadcast_chat_detail(query: CallbackQuery, bid: int, order: int) -> None:
    user_id = query.from_user.id
    broadcast = active_broadcasts.get(bid)
    if not broadcast or broadcast.get("user_id") != user_id:
        await query.answer(
            "\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )
        return

    item = _find_chat_runtime_item(broadcast, order)
    if not item:
        await query.answer("\u0427\u0430\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d", show_alert=True)
        return

    number = order + 1
    info = [
        f"\U0001f4ac <b>\u0427\u0430\u0442 #{number}</b>",
        "",
        f"\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435: {html.escape(_broadcast_chat_short_name(item))}",
        f"ID: <code>{item.get('chat_id')}</code>",
        f"\u0421\u0442\u0430\u0442\u0443\u0441: {_broadcast_chat_status_label(item)}",
        f"\u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e: {int(item.get('sent_count', 0) or 0)}",
        f"\u041e\u0448\u0438\u0431\u043e\u043a: {int(item.get('failed_count', 0) or 0)}",
    ]

    next_send_at = float(item.get("next_send_at", 0.0) or 0.0)
    if next_send_at > 0:
        eta = max(0, int(next_send_at - datetime.now(timezone.utc).timestamp()))
        info.append(f"\u0421\u043b\u0435\u0434. \u043e\u0442\u043f\u0440\u0430\u0432\u043a\u0430: \u0447\u0435\u0440\u0435\u0437 {eta} \u0441\u0435\u043a")

    error_line = _format_chat_error_line(item)
    if error_line:
        info.extend(
            [
                "",
                f"<b>\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u044f\u044f \u043e\u0448\u0438\u0431\u043a\u0430</b>",
                html.escape(error_line),
            ]
        )

    buttons = [
        [
            InlineKeyboardButton(
                text="\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430",
                callback_data=f"bc_chat_pause_{bid}_{order}",
            ),
            InlineKeyboardButton(
                text="\u25b6\ufe0f \u0412\u043a\u043b\u044e\u0447\u0438\u0442\u044c",
                callback_data=f"bc_chat_resume_{bid}_{order}",
            ),
        ],
        [
            InlineKeyboardButton(
                text="\u26d4 \u0423\u0431\u0440\u0430\u0442\u044c \u0438\u0437 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438",
                callback_data=f"bc_chat_disable_{bid}_{order}",
            ),
            InlineKeyboardButton(
                text="\U0001f504 \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c",
                callback_data=f"bc_chat_view_{bid}_{order}",
            )
        ],
        [
            InlineKeyboardButton(
                text="\u2b05\ufe0f \u041a \u0441\u043f\u0438\u0441\u043a\u0443 \u0447\u0430\u0442\u043e\u0432",
                callback_data=f"bc_chat_list_{bid}",
            )
        ],
    ]

    text = "\n".join(info)
    markup = InlineKeyboardMarkup(inline_keyboard=buttons)
    await _edit_or_notice(query, text, markup)


@router.callback_query(F.data.startswith("bc_chat_list_"))
async def bc_chat_list_callback(query: CallbackQuery):
    await query.answer()
    try:
        bid = int(query.data.split("_")[3])
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    await _render_broadcast_chat_list(query, bid)


@router.callback_query(F.data.startswith("bc_chat_view_"))
async def bc_chat_view_callback(query: CallbackQuery):
    await query.answer()
    try:
        _, _, _, bid_text, order_text = query.data.split("_")
        bid = int(bid_text)
        order = int(order_text)
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    await _render_broadcast_chat_detail(query, bid, order)


async def _set_broadcast_chat_status(
    user_id: int,
    bid: int,
    order: int,
    status: str,
) -> bool:
    broadcast = active_broadcasts.get(bid)
    if not broadcast or broadcast.get("user_id") != user_id:
        return False

    items = _broadcast_chat_runtime_items(broadcast)
    found = False
    for item in items:
        item_order = item.get("order", -1)
        if int(item_order if item_order is not None else -1) != order:
            continue
        item["status"] = status
        found = True
        if status == "active" and float(item.get("next_send_at", 0.0) or 0.0) <= 0:
            item["next_send_at"] = datetime.now(timezone.utc).timestamp()
        break

    if not found:
        return False

    await update_broadcast_fields(bid, chat_runtime=items)
    return True


@router.callback_query(F.data.startswith("bc_chat_pause_"))
async def bc_chat_pause_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id
    try:
        _, _, _, bid_text, order_text = query.data.split("_")
        bid = int(bid_text)
        order = int(order_text)
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    await _set_broadcast_chat_status(user_id, bid, order, "paused")
    await _render_broadcast_chat_detail(query, bid, order)


@router.callback_query(F.data.startswith("bc_chat_resume_"))
async def bc_chat_resume_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id
    try:
        _, _, _, bid_text, order_text = query.data.split("_")
        bid = int(bid_text)
        order = int(order_text)
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    changed = await _set_broadcast_chat_status(user_id, bid, order, "active")
    if changed:
        broadcast = active_broadcasts.get(bid)
        if broadcast and broadcast.get("status") == "running":
            _start_or_resume_broadcast_task(bid)
    await _render_broadcast_chat_detail(query, bid, order)


@router.callback_query(F.data.startswith("bc_chat_disable_"))
async def bc_chat_disable_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id
    try:
        _, _, _, bid_text, order_text = query.data.split("_")
        bid = int(bid_text)
        order = int(order_text)
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    await _set_broadcast_chat_status(user_id, bid, order, "disabled")
    await _render_broadcast_chat_detail(query, bid, order)


@router.callback_query(F.data == "back_to_broadcast_menu")
async def back_to_broadcast_menu_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    await show_broadcast_menu(query, user_id, is_edit=True)


@router.callback_query(F.data.startswith("bc_edit_count_"))
async def bc_edit_count_callback(query: CallbackQuery, state: FSMContext):
    """Р ВР В·Р СР ВµР Р…Р С‘РЎвЂљРЎРЉ Р С”Р С•Р В»-Р Р†Р С• Р В°Р С”РЎвЂљР С‘Р Р†Р Р…Р С•Р в„– РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘"""

    await query.answer()

    user_id = query.from_user.id

    try:
        bid = int(query.data.split("_")[3])

    except Exception:
        await query.answer("Р С›РЎв‚¬Р С‘Р В±Р С”Р В°", show_alert=True)

        return

    if bid not in active_broadcasts or active_broadcasts[bid]["user_id"] != user_id:
        await query.answer(
            "Р В Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р В° Р Р…Р Вµ Р Р…Р В°Р в„–Р Т‘Р ВµР Р…Р В°",
            show_alert=True,
        )

        return

    await state.set_state(BroadcastConfigState.waiting_for_count)

    await state.update_data(
        edit_broadcast_id=bid,
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
    )

    info = "Р вЂ™Р Р†Р ВµР Т‘Р С‘ Р Р…Р С•Р Р†Р С•Р Вµ Р С”Р С•Р В»-Р Р†Р С• РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р в„– (1-1000, Р С‘Р В»Р С‘ Р Р…Р В°Р В¶Р СР С‘ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ):"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ",
                    callback_data=f"view_bc_{bid}",
                )
            ]
        ]
    )

    await query.message.edit_text(info, reply_markup=kb)


@router.callback_query(F.data.startswith("bc_edit_interval_"))
async def bc_edit_interval_callback(query: CallbackQuery, state: FSMContext):
    """Р ВР В·Р СР ВµР Р…Р С‘РЎвЂљРЎРЉ Р С‘Р Р…РЎвЂљР ВµРЎР‚Р Р†Р В°Р В» Р В°Р С”РЎвЂљР С‘Р Р†Р Р…Р С•Р в„– РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘"""

    await query.answer()

    user_id = query.from_user.id

    try:
        bid = int(query.data.split("_")[3])

    except Exception:
        await query.answer("Р С›РЎв‚¬Р С‘Р В±Р С”Р В°", show_alert=True)

        return

    if bid not in active_broadcasts or active_broadcasts[bid]["user_id"] != user_id:
        await query.answer(
            "Р В Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р В° Р Р…Р Вµ Р Р…Р В°Р в„–Р Т‘Р ВµР Р…Р В°",
            show_alert=True,
        )

        return

    await state.set_state(BroadcastConfigState.waiting_for_interval)

    await state.update_data(
        edit_broadcast_id=bid,
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
    )

    info = "Р вЂ™Р Р†Р ВµР Т‘Р С‘ Р Р…Р С•Р Р†РЎвЂ№Р в„– Р С‘Р Р…РЎвЂљР ВµРЎР‚Р Р†Р В°Р В» Р Р† Р СР С‘Р Р…РЎС“РЎвЂљР В°РЎвЂ¦ (1-60, Р С‘Р В»Р С‘ Р Р…Р В°Р В¶Р СР С‘ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ):"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ",
                    callback_data=f"view_bc_{bid}",
                )
            ]
        ]
    )

    await query.message.edit_text(info, reply_markup=kb)


@router.message(BroadcastConfigState.waiting_for_source_channel)
async def process_source_channel_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    source_ref = normalize_channel_reference(message.text)
    if not source_ref:
        await message.answer("Укажи ссылку, @username или ID канала")
        return

    try:
        source_data = await _load_channel_source_for_user(user_id, source_ref)
    except Exception as exc:
        await message.answer(f"❌ Не удалось загрузить посты: {exc}")
        return

    config = get_broadcast_config(user_id)
    config["text_source_type"] = "channel"
    config.update(source_data)
    config["text_index"] = 0
    save_broadcast_config_with_profile(user_id, config)

    data = await state.get_data()
    chat_id = data.get("chat_id")
    edit_message_id = data.get("edit_message_id")
    await state.clear()

    try:
        await message.delete()
    except Exception:
        pass

    kb = build_text_settings_keyboard(
        config.get("text_source_type", "manual"),
        config.get("text_mode", "random"),
        config.get("parse_mode", "HTML"),
    )
    info = _build_text_settings_info(config)
    if edit_message_id and chat_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id,
                message_id=edit_message_id,
                text=info,
                reply_markup=kb,
                parse_mode="HTML",
            )
            return
        except Exception:
            pass

    await message.answer(info, reply_markup=kb, parse_mode="HTML")


@router.message(BroadcastConfigState.waiting_for_text_add)
async def process_text_add(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р С‘РЎРЏ Р Р…Р С•Р Р†Р С•Р С–Р С• РЎвЂљР ВµР С”РЎРѓРЎвЂљР В° Р Р† РЎРѓР С—Р С‘РЎРѓР С•Р С”"""

    user_id = message.from_user.id

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С РЎвЂЎРЎвЂљР С• РЎРЊРЎвЂљР С• Р Р…Р Вµ Р С•РЎвЂљР СР ВµР Р…Р В°

    if message.text and message.text.startswith("РІвЂ В©РїС‘РЏ"):
        await state.clear()

        # Р СџР С•Р С”Р В°Р В·РЎвЂ№Р Р†Р В°Р ВµР С РЎРѓР С—Р С‘РЎРѓР С•Р С” РЎвЂљР ВµР С”РЎРѓРЎвЂљР С•Р Р†

        config = get_broadcast_config(user_id)

        if not config["texts"]:
            info = (
                "СЂСџвЂњвЂћ Р РЋР СџР ВР РЋР С›Р С™ Р СћР вЂўР С™Р РЋР СћР С›Р вЂ™\n\n"
            )

            info += "Р СњР ВµРЎвЂљ Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р Р…РЎвЂ№РЎвЂ¦ РЎвЂљР ВµР С”РЎРѓРЎвЂљР С•Р Р†.\n\n"

            info += "Р СњР В°Р В¶Р СР С‘ 'Р вЂќР С•Р В±Р В°Р Р†Р С‘РЎвЂљРЎРЉ Р Р…Р С•Р Р†РЎвЂ№Р в„–' РЎвЂЎРЎвЂљР С•Р В±РЎвЂ№ Р Т‘Р С•Р В±Р В°Р Р†Р С‘РЎвЂљРЎРЉ РЎвЂљР ВµР С”РЎРѓРЎвЂљ Р Т‘Р В»РЎРЏ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘."

        else:
            info = (
                "СЂСџвЂњвЂћ Р РЋР СџР ВР РЋР С›Р С™ Р СћР вЂўР С™Р РЋР СћР С›Р вЂ™\n\n"
            )

            info += f"Р вЂ™РЎРѓР ВµР С–Р С• РЎвЂљР ВµР С”РЎРѓРЎвЂљР С•Р Р†: {len(config['texts'])}\n"

            info += "Р вЂ™РЎвЂ№Р В±Р ВµРЎР‚Р С‘ РЎвЂљР ВµР С”РЎРѓРЎвЂљ Р Т‘Р В»РЎРЏ Р С—РЎР‚Р С•РЎРѓР СР С•РЎвЂљРЎР‚Р В° Р С‘Р В»Р С‘ РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘РЎРЏ.\n"

        kb = build_texts_keyboard(config["texts"], back_callback="bc_text")

        data = await state.get_data()

        chat_id = data.get("chat_id")

        edit_message_id = data.get("edit_message_id")

        if edit_message_id and chat_id:
            try:
                await message.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=edit_message_id,
                    text=info,
                    reply_markup=kb,
                    parse_mode="HTML",
                )

            except Exception:
                await message.answer(info, reply_markup=kb, parse_mode="HTML")

        return

    # Р вЂќР С•Р В±Р В°Р Р†Р В»РЎРЏР ВµР С Р Р…Р С•Р Р†РЎвЂ№Р в„– РЎвЂљР ВµР С”РЎРѓРЎвЂљ

    config = get_broadcast_config(user_id)

    config["texts"].append(message.text)

    save_broadcast_config_with_profile(user_id, config)

    await state.clear()

    await message.delete()

    # Р СџР С•Р С”Р В°Р В·РЎвЂ№Р Р†Р В°Р ВµР С Р С•Р В±Р Р…Р С•Р Р†Р В»Р ВµР Р…Р Р…РЎвЂ№Р в„– РЎРѓР С—Р С‘РЎРѓР С•Р С”

    if not config["texts"]:
        info = "СЂСџвЂњвЂћ Р РЋР СџР ВР РЋР С›Р С™ Р СћР вЂўР С™Р РЋР СћР С›Р вЂ™\n\n"

        info += "Р СњР ВµРЎвЂљ Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р Р…РЎвЂ№РЎвЂ¦ РЎвЂљР ВµР С”РЎРѓРЎвЂљР С•Р Р†.\n\n"

        info += "Р СњР В°Р В¶Р СР С‘ 'Р вЂќР С•Р В±Р В°Р Р†Р С‘РЎвЂљРЎРЉ Р Р…Р С•Р Р†РЎвЂ№Р в„–' РЎвЂЎРЎвЂљР С•Р В±РЎвЂ№ Р Т‘Р С•Р В±Р В°Р Р†Р С‘РЎвЂљРЎРЉ РЎвЂљР ВµР С”РЎРѓРЎвЂљ Р Т‘Р В»РЎРЏ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘."

    else:
        info = "СЂСџвЂњвЂћ Р РЋР СџР ВР РЋР С›Р С™ Р СћР вЂўР С™Р РЋР СћР С›Р вЂ™\n\n"

        info += f"Р вЂ™РЎРѓР ВµР С–Р С• РЎвЂљР ВµР С”РЎРѓРЎвЂљР С•Р Р†: {len(config['texts'])}\n"

        info += "Р вЂ™РЎвЂ№Р В±Р ВµРЎР‚Р С‘ РЎвЂљР ВµР С”РЎРѓРЎвЂљ Р Т‘Р В»РЎРЏ Р С—РЎР‚Р С•РЎРѓР СР С•РЎвЂљРЎР‚Р В° Р С‘Р В»Р С‘ РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘РЎРЏ.\n"

    kb = build_texts_keyboard(config["texts"], back_callback="bc_text")

    data = await state.get_data()

    chat_id = data.get("chat_id")

    edit_message_id = data.get("edit_message_id")

    if edit_message_id and chat_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id,
                message_id=edit_message_id,
                text=info,
                reply_markup=kb,
                parse_mode="HTML",
            )

        except Exception:
            await message.answer(info, reply_markup=kb, parse_mode="HTML")

    else:
        await message.answer(info, reply_markup=kb, parse_mode="HTML")


@router.message(BroadcastConfigState.waiting_for_text_edit)
async def process_text_edit(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘РЎРЏ РЎвЂљР ВµР С”РЎРѓРЎвЂљР В°"""

    user_id = message.from_user.id

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С РЎвЂЎРЎвЂљР С• РЎРЊРЎвЂљР С• Р Р…Р Вµ Р С•РЎвЂљР СР ВµР Р…Р В°

    if message.text and message.text.startswith("РІвЂ В©РїС‘РЏ"):
        data = await state.get_data()

        text_index = data.get("text_index", 0)

        await state.clear()

        # Р СџР С•Р С”Р В°Р В·РЎвЂ№Р Р†Р В°Р ВµР С Р С‘Р В·Р СР ВµР Р…Р ВµР Р…Р Р…РЎвЂ№Р в„– РЎвЂљР ВµР С”РЎРѓРЎвЂљ

        config = get_broadcast_config(user_id)

        if text_index >= len(config["texts"]):
            text_index = len(config["texts"]) - 1

        current_text = config["texts"][text_index]

        parse_mode = config.get("parse_mode", "HTML")

        info = f"СЂСџвЂњвЂ№ Р СћР вЂўР С™Р РЋР Сћ #{text_index + 1}\n\n"

        info += f"СЂСџвЂњСњ <b>Р В¤Р С•РЎР‚Р СР В°РЎвЂљ:</b> {parse_mode}\n"

        info += "РІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓ\n"

        max_text_length = 3500

        if len(current_text) > max_text_length:
            display_text = current_text[:max_text_length]

            info += f"<code>{display_text}</code>\n"

            info += f"<i>... (РЎвЂљР ВµР С”РЎРѓРЎвЂљ Р С•Р В±РЎР‚Р ВµР В·Р В°Р Р…, Р Р†РЎРѓР ВµР С–Р С• {len(current_text)} РЎРѓР С‘Р СР Р†Р С•Р В»Р С•Р Р†)</i>\n"

        else:
            info += f"<code>{current_text}</code>\n"

        info += "РІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓ\n"

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Р ВР В·Р СР ВµР Р…Р С‘РЎвЂљРЎРЉ",
                        callback_data=f"text_edit_{text_index}",
                    ),
                    InlineKeyboardButton(
                        text="Р Р€Р Т‘Р В°Р В»Р С‘РЎвЂљРЎРЉ",
                        callback_data=f"text_delete_{text_index}",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        text="Р СњР В°Р В·Р В°Р Т‘", callback_data="text_list"
                    )
                ],
            ]
        )

        data = await state.get_data()

        chat_id = data.get("chat_id")

        edit_message_id = data.get("edit_message_id")

        if edit_message_id and chat_id:
            try:
                await message.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=edit_message_id,
                    text=info,
                    reply_markup=kb,
                    parse_mode="HTML",
                )

            except Exception:
                await message.answer(info, reply_markup=kb, parse_mode="HTML")

        return

    # Р В Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚РЎС“Р ВµР С РЎвЂљР ВµР С”РЎРѓРЎвЂљ

    data = await state.get_data()

    text_index = data.get("text_index", 0)

    config = get_broadcast_config(user_id)

    if text_index < len(config["texts"]):
        config["texts"][text_index] = message.text

        save_broadcast_config_with_profile(user_id, config)

    await state.clear()

    await message.delete()

    # Р СџР С•Р С”Р В°Р В·РЎвЂ№Р Р†Р В°Р ВµР С Р С•Р В±Р Р…Р С•Р Р†Р В»Р ВµР Р…Р Р…РЎвЂ№Р в„– РЎвЂљР ВµР С”РЎРѓРЎвЂљ

    if text_index >= len(config["texts"]):
        text_index = len(config["texts"]) - 1

    current_text = config["texts"][text_index]

    parse_mode = config.get("parse_mode", "HTML")

    info = f"СЂСџвЂњвЂ№ Р СћР вЂўР С™Р РЋР Сћ #{text_index + 1}\n\n"

    info += f"СЂСџвЂњСњ <b>Р В¤Р С•РЎР‚Р СР В°РЎвЂљ:</b> {parse_mode}\n"

    info += "РІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓ\n"

    max_text_length = 3500

    if len(current_text) > max_text_length:
        display_text = current_text[:max_text_length]

        info += f"<code>{display_text}</code>\n"

        info += f"<i>... (РЎвЂљР ВµР С”РЎРѓРЎвЂљ Р С•Р В±РЎР‚Р ВµР В·Р В°Р Р…, Р Р†РЎРѓР ВµР С–Р С• {len(current_text)} РЎРѓР С‘Р СР Р†Р С•Р В»Р С•Р Р†)</i>\n"

    else:
        info += f"<code>{current_text}</code>\n"

    info += "РІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓ\n"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Р ВР В·Р СР ВµР Р…Р С‘РЎвЂљРЎРЉ",
                    callback_data=f"text_edit_{text_index}",
                ),
                InlineKeyboardButton(
                    text="Р Р€Р Т‘Р В°Р В»Р С‘РЎвЂљРЎРЉ",
                    callback_data=f"text_delete_{text_index}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="Р СњР В°Р В·Р В°Р Т‘", callback_data="text_list"
                )
            ],
        ]
    )

    chat_id = data.get("chat_id")

    edit_message_id = data.get("edit_message_id")

    if edit_message_id and chat_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id,
                message_id=edit_message_id,
                text=info,
                reply_markup=kb,
                parse_mode="HTML",
            )

        except Exception:
            await message.answer(info, reply_markup=kb, parse_mode="HTML")

    else:
        await message.answer(info, reply_markup=kb, parse_mode="HTML")


@router.message(F.text == COUNT_BUTTON_TEXT)
async def broadcast_count_button(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С”Р Р…Р С•Р С—Р С”Р С‘ Р Р†РЎвЂ№Р В±Р С•РЎР‚Р В° Р С”Р С•Р В»Р С‘РЎвЂЎР ВµРЎРѓРЎвЂљР Р†Р В° РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р в„–"""

    user_id = message.from_user.id

    config = get_broadcast_config(user_id)

    await state.update_data(previous_menu="broadcast")

    await state.set_state(BroadcastConfigState.waiting_for_count)

    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=CANCEL_TEXT)]],
        resize_keyboard=True,
    )

    await message.answer(
        (
            "\U0001f522 <b>\u041e\u0411\u0429\u0415\u0415 \u041a\u041e\u041b-\u0412\u041e \u0421\u041e\u041e\u0411\u0429\u0415\u041d\u0418\u0419</b>\n\n"
            f"\u0422\u0435\u043a\u0443\u0449\u0435\u0435: {config.get('count', 0)}\n\n"
            "\u041e\u0442\u043f\u0440\u0430\u0432\u044c \u0447\u0438\u0441\u043b\u043e \u043e\u0442 1 \u0434\u043e 1000"
        ),
        reply_markup=keyboard,
        parse_mode="HTML",
    )


@router.message(BroadcastConfigState.waiting_for_count)
async def process_broadcast_count(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С—Р С•Р В»РЎС“РЎвЂЎР ВµР Р…Р С‘РЎРЏ Р С”Р С•Р В»Р С‘РЎвЂЎР ВµРЎРѓРЎвЂљР Р†Р В° РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р в„–"""

    user_id = message.from_user.id

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С РЎвЂЎРЎвЂљР С• РЎРЊРЎвЂљР С• Р Р…Р Вµ Р С”Р Р…Р С•Р С—Р С”Р В° Р С•РЎвЂљР СР ВµР Р…РЎвЂ№

    if message.text == CANCEL_TEXT:
        await return_to_previous_menu(message, state)

        return

    try:
        count = int(message.text)

        if count < 1 or count > 1000:
            await message.answer("\u274c \u041a\u043e\u043b-\u0432\u043e \u0434\u043e\u043b\u0436\u043d\u043e \u0431\u044b\u0442\u044c \u043e\u0442 1 \u0434\u043e 1000")

            return

        config = get_broadcast_config(user_id)

        config["count"] = count

        save_broadcast_config_with_profile(user_id, config)

        data = await state.get_data()

        edit_broadcast_id = data.get("edit_broadcast_id")
        edit_group_id = data.get("edit_group_id")
        edit_message_id = data.get("edit_message_id")

        chat_id = data.get("chat_id")

        if edit_broadcast_id in active_broadcasts:
            await update_broadcast_fields(
                edit_broadcast_id,
                count=count,
                planned_count=count,
            )
        elif edit_group_id is not None:
            for bid, broadcast in list(active_broadcasts.items()):
                if (
                    broadcast.get("group_id") == edit_group_id
                    and broadcast.get("user_id") == user_id
                    and broadcast.get("status") in ("running", "paused")
                ):
                    await update_broadcast_fields(
                        bid,
                        count=count,
                        planned_count=count,
                    )

        await state.clear()

        # Р Р€Р Т‘Р В°Р В»РЎРЏР ВµР С РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ Р С—Р С•Р В»РЎРЉР В·Р С•Р Р†Р В°РЎвЂљР ВµР В»РЎРЏ

        try:
            await message.delete()

        except Exception:
            pass

        # Р В Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚РЎС“Р ВµР С РЎвЂљР С• Р В¶Р Вµ РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ РЎРѓ Р С‘Р Р…РЎвЂћР С•РЎР‚Р СР В°РЎвЂ Р С‘Р ВµР в„– Р С• РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р Вµ Р С‘Р В»Р С‘ Р С•РЎвЂљР С—РЎР‚Р В°Р Р†Р В»РЎРЏР ВµР С Р Р…Р С•Р Р†Р С•Р Вµ

        chats = get_broadcast_chats(user_id)

        if edit_message_id and chat_id and edit_group_id is not None:
            try:
                if await _edit_group_detail_message(
                    message,
                    user_id,
                    edit_group_id,
                    chat_id=chat_id,
                    message_id=edit_message_id,
                ):
                    return
            except Exception as e:
                print(f"Group detail refresh failed after count update: {e}")
                await message.answer(
                    "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043c\u0435\u043d\u044e \u0433\u0440\u0443\u043f\u043f\u044b"
                )
                return

        if edit_message_id and chat_id:
            try:
                info = build_broadcast_menu_text(
                    config, chats, active_broadcasts, user_id
                )

                kb = build_broadcast_keyboard(
                    include_active=False,
                    user_id=user_id,
                    active_broadcasts=active_broadcasts,
                    back_callback="delete_bc_menu",
                )

                await message.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=edit_message_id,
                    text=info,
                    reply_markup=kb,
                    parse_mode="HTML",
                )

            except Exception as e:
                print(
                    f"Р С›РЎв‚¬Р С‘Р В±Р С”Р В° РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘РЎРЏ: {e}"
                )

                import traceback

                traceback.print_exc()

                await message.answer(
                    "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043c\u0435\u043d\u044e"
                )

        else:
            await cmd_broadcast_menu(message)

    except ValueError:
        await message.answer("\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u043e")


@router.message(F.text == INTERVAL_BUTTON_TEXT)
async def broadcast_interval_button(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С”Р Р…Р С•Р С—Р С”Р С‘ Р Р†РЎвЂ№Р В±Р С•РЎР‚Р В° Р С‘Р Р…РЎвЂљР ВµРЎР‚Р Р†Р В°Р В»Р В°"""

    user_id = message.from_user.id

    config = get_broadcast_config(user_id)

    await state.update_data(previous_menu="broadcast")

    await state.set_state(BroadcastConfigState.waiting_for_interval)

    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=CANCEL_TEXT)]],
        resize_keyboard=True,
    )

    await message.answer(
        (
            "\u23f1\ufe0f <b>\u0418\u041d\u0422\u0415\u0420\u0412\u0410\u041b \u0414\u041b\u042f \u041a\u0410\u0416\u0414\u041e\u0413\u041e \u0427\u0410\u0422\u0410</b>\n\n"
            f"\u0422\u0435\u043a\u0443\u0449\u0438\u0439: {config.get('interval', 0)} \u043c\u0438\u043d\n\n"
            "\u041f\u043e\u0441\u043b\u0435 \u043a\u0430\u0436\u0434\u043e\u0439 \u043e\u0442\u043f\u0440\u0430\u0432\u043a\u0438 \u0431\u043e\u0442 \u0437\u0430\u043d\u043e\u0432\u043e \u0432\u044b\u0431\u0438\u0440\u0430\u0435\u0442 \u044d\u0442\u043e\u0442 \u0438\u043d\u0442\u0435\u0440\u0432\u0430\u043b \u0434\u043b\u044f \u043a\u043e\u043d\u043a\u0440\u0435\u0442\u043d\u043e\u0433\u043e \u0447\u0430\u0442\u0430.\n"
            "\u041e\u0442\u043f\u0440\u0430\u0432\u044c \u043e\u0434\u043d\u043e \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d.\n"
            "\u041f\u0440\u0438\u043c\u0435\u0440\u044b: <code>15</code> \u0438\u043b\u0438 <code>10-30</code>"
        ),
        reply_markup=keyboard,
        parse_mode="HTML",
    )


@router.message(BroadcastConfigState.waiting_for_interval)
async def process_broadcast_interval(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С—Р С•Р В»РЎС“РЎвЂЎР ВµР Р…Р С‘РЎРЏ Р С‘Р Р…РЎвЂљР ВµРЎР‚Р Р†Р В°Р В»Р В°"""

    user_id = message.from_user.id

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С РЎвЂЎРЎвЂљР С• РЎРЊРЎвЂљР С• Р Р…Р Вµ Р С”Р Р…Р С•Р С—Р С”Р В° Р С•РЎвЂљР СР ВµР Р…РЎвЂ№

    if message.text == CANCEL_TEXT:
        await return_to_previous_menu(message, state)

        return

    try:
        text = message.text.strip()

        # Р СџР В°РЎР‚РЎРѓР С‘Р С РЎвЂћР С•РЎР‚Р СР В°РЎвЂљ: Р СР С•Р В¶Р ВµРЎвЂљ Р В±РЎвЂ№РЎвЂљРЎРЉ РЎвЂЎР С‘РЎРѓР В»Р С• Р С‘Р В»Р С‘ Р Т‘Р С‘Р В°Р С—Р В°Р В·Р С•Р Р… Р СР С‘Р Р…-Р СР В°Р С”РЎРѓ

        if "-" in text:
            # Р В¤Р С•РЎР‚Р СР В°РЎвЂљ: Р СР С‘Р Р…-Р СР В°Р С”РЎРѓ

            parts = text.split("-")

            if len(parts) != 2:
                await message.answer(
                    "\u274c \u041d\u0435\u0432\u0435\u0440\u043d\u044b\u0439 \u0444\u043e\u0440\u043c\u0430\u0442. \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439: 10-30 \u0438\u043b\u0438 15"
                )

                return

            try:
                min_interval = int(parts[0].strip())

                max_interval = int(parts[1].strip())

                if min_interval < 1 or max_interval < 1 or min_interval > max_interval:
                    await message.answer(
                        "\u274c \u0417\u043d\u0430\u0447\u0435\u043d\u0438\u044f \u0434\u043e\u043b\u0436\u043d\u044b \u0431\u044b\u0442\u044c \u043f\u043e\u043b\u043e\u0436\u0438\u0442\u0435\u043b\u044c\u043d\u044b\u043c\u0438, \u0438 min \u043d\u0435 \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u0431\u043e\u043b\u044c\u0448\u0435 max"
                    )

                    return

                if min_interval > 480 or max_interval > 480:
                    await message.answer(
                        "\u274c \u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b \u043d\u0435 \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u0431\u043e\u043b\u044c\u0448\u0435 480 \u043c\u0438\u043d\u0443\u0442 (8 \u0447\u0430\u0441\u043e\u0432)"
                    )

                    return

                interval_value = text  # Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµР С Р С”Р В°Р С” РЎРѓРЎвЂљРЎР‚Р С•Р С”РЎС“ "Р СР С‘Р Р…-Р СР В°Р С”РЎРѓ"

            except ValueError:
                await message.answer(
                    "\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u0430 \u0432 \u0444\u043e\u0440\u043c\u0430\u0442\u0435: 10-30"
                )

                return

        else:
            # Р С›Р Т‘Р Р…Р С• РЎвЂЎР С‘РЎРѓР В»Р С•

            try:
                interval_int = int(text)

                if interval_int < 1 or interval_int > 480:
                    await message.answer(
                        "\u274c \u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u043e\u0442 1 \u0434\u043e 480 \u043c\u0438\u043d\u0443\u0442"
                    )

                    return

                interval_value = text

            except ValueError:
                await message.answer(
                    "\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d (\u043d\u0430\u043f\u0440\u0438\u043c\u0435\u0440 10-30)"
                )

                return

        # Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµР С Р С”Р С•Р Р…РЎвЂћР С‘Р С–

        config = get_broadcast_config(user_id)

        config["interval"] = interval_value

        save_broadcast_config_with_profile(user_id, config)

        data = await state.get_data()

        edit_broadcast_id = data.get("edit_broadcast_id")
        edit_group_id = data.get("edit_group_id")
        edit_message_id = data.get("edit_message_id")

        chat_id = data.get("chat_id")

        if edit_broadcast_id in active_broadcasts:
            await update_broadcast_fields(
                edit_broadcast_id,
                interval_minutes=interval_value,
                interval_value=interval_value,
            )
        elif edit_group_id is not None:
            for bid, broadcast in list(active_broadcasts.items()):
                if (
                    broadcast.get("group_id") == edit_group_id
                    and broadcast.get("user_id") == user_id
                    and broadcast.get("status") in ("running", "paused")
                ):
                    await update_broadcast_fields(
                        bid,
                        interval_minutes=interval_value,
                        interval_value=interval_value,
                    )

        await state.clear()

        # Р Р€Р Т‘Р В°Р В»РЎРЏР ВµР С РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ Р С—Р С•Р В»РЎРЉР В·Р С•Р Р†Р В°РЎвЂљР ВµР В»РЎРЏ

        try:
            await message.delete()

        except Exception:
            pass

        # Р В Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚РЎС“Р ВµР С РЎвЂљР С• Р В¶Р Вµ РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ РЎРѓ Р С‘Р Р…РЎвЂћР С•РЎР‚Р СР В°РЎвЂ Р С‘Р ВµР в„– Р С• РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р Вµ

        chats = get_broadcast_chats(user_id)

        if edit_message_id and chat_id and edit_group_id is not None:
            try:
                if await _edit_group_detail_message(
                    message,
                    user_id,
                    edit_group_id,
                    chat_id=chat_id,
                    message_id=edit_message_id,
                ):
                    return
            except Exception as e:
                print(f"Group detail refresh failed after interval update: {e}")
                await message.answer(
                    "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043c\u0435\u043d\u044e \u0433\u0440\u0443\u043f\u043f\u044b"
                )
                return

        if edit_message_id and chat_id:
            try:
                info = build_broadcast_menu_text(
                    config, chats, active_broadcasts, user_id
                )

                kb = build_broadcast_keyboard(
                    include_active=False,
                    user_id=user_id,
                    active_broadcasts=active_broadcasts,
                    back_callback="delete_bc_menu",
                )

                await message.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=edit_message_id,
                    text=info,
                    reply_markup=kb,
                    parse_mode="HTML",
                )

            except Exception as e:
                print(
                    f"Р С›РЎв‚¬Р С‘Р В±Р С”Р В° РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘РЎРЏ: {e}"
                )

                import traceback

                traceback.print_exc()

                await message.answer(
                    "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043c\u0435\u043d\u044e"
                )

        else:
            await cmd_broadcast_menu(message)

    except ValueError:
        await message.answer("\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u043e")


@router.message(BroadcastConfigState.waiting_for_chat_pause)
async def process_broadcast_chat_pause(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С—Р С•Р В»РЎС“РЎвЂЎР ВµР Р…Р С‘РЎРЏ Р В·Р В°Р Т‘Р ВµРЎР‚Р В¶Р С”Р С‘ Р СР ВµР В¶Р Т‘РЎС“ РЎвЂЎР В°РЎвЂљР В°Р СР С‘"""

    user_id = message.from_user.id

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С РЎвЂЎРЎвЂљР С• РЎРЊРЎвЂљР С• Р Р…Р Вµ Р С”Р Р…Р С•Р С—Р С”Р В° Р С•РЎвЂљР СР ВµР Р…РЎвЂ№

    if message.text == CANCEL_TEXT:
        await return_to_previous_menu(message, state)

        return

    try:
        text = message.text.strip()

        # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С РЎвЂћР С•РЎР‚Р СР В°РЎвЂљ: Р СР С•Р В¶Р ВµРЎвЂљ Р В±РЎвЂ№РЎвЂљРЎРЉ РЎвЂЎР С‘РЎРѓР В»Р С• Р С‘Р В»Р С‘ Р Т‘Р С‘Р В°Р С—Р В°Р В·Р С•Р Р… Р СР С‘Р Р…-Р СР В°Р С”РЎРѓ

        if "-" in text:
            # Р В¤Р С•РЎР‚Р СР В°РЎвЂљ: Р СР С‘Р Р…-Р СР В°Р С”РЎРѓ

            parts = text.split("-")

            if len(parts) != 2:
                await message.answer(
                    "\u274c \u041d\u0435\u0432\u0435\u0440\u043d\u044b\u0439 \u0444\u043e\u0440\u043c\u0430\u0442. \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439: 1-3 \u0438\u043b\u0438 2"
                )

                return

            try:
                min_pause = int(parts[0].strip())

                max_pause = int(parts[1].strip())

                if min_pause < 1 or max_pause < 1 or min_pause > max_pause:
                    await message.answer(
                        "\u274c \u0417\u043d\u0430\u0447\u0435\u043d\u0438\u044f \u0434\u043e\u043b\u0436\u043d\u044b \u0431\u044b\u0442\u044c \u043f\u043e\u043b\u043e\u0436\u0438\u0442\u0435\u043b\u044c\u043d\u044b\u043c\u0438, \u0438 min \u043d\u0435 \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u0431\u043e\u043b\u044c\u0448\u0435 max"
                    )

                    return

                if min_pause > CHAT_PAUSE_MAX_SECONDS or max_pause > CHAT_PAUSE_MAX_SECONDS:
                    await message.answer(
                        f"\u274c \u0422\u0435\u043c\u043f \u043d\u0435 \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u0431\u043e\u043b\u044c\u0448\u0435 {CHAT_PAUSE_MAX_SECONDS} \u0441\u0435\u043a\u0443\u043d\u0434"
                    )

                    return

                pause_value = text  # Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµР С Р С”Р В°Р С” РЎРѓРЎвЂљРЎР‚Р С•Р С”РЎС“ "Р СР С‘Р Р…-Р СР В°Р С”РЎРѓ"

            except ValueError:
                await message.answer(
                    "\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u0430 \u0432 \u0444\u043e\u0440\u043c\u0430\u0442\u0435: 1-3"
                )

                return

        else:
            # Р С›Р Т‘Р Р…Р С• РЎвЂЎР С‘РЎРѓР В»Р С•

            try:
                pause_int = int(text)

                if pause_int < 1 or pause_int > CHAT_PAUSE_MAX_SECONDS:
                    await message.answer(
                        f"\u274c \u0422\u0435\u043c\u043f \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u043e\u0442 1 \u0434\u043e {CHAT_PAUSE_MAX_SECONDS} \u0441\u0435\u043a\u0443\u043d\u0434"
                    )

                    return

                pause_value = text

            except ValueError:
                await message.answer(
                    "\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d (\u043d\u0430\u043f\u0440\u0438\u043c\u0435\u0440 1-3)"
                )

                return

        # Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµР С Р С”Р С•Р Р…РЎвЂћР С‘Р С–

        config = get_broadcast_config(user_id)

        config["chat_pause"] = pause_value

        save_broadcast_config_with_profile(user_id, config)

        data = await state.get_data()

        edit_group_id = data.get("edit_group_id")
        edit_message_id = data.get("edit_message_id")

        chat_id = data.get("chat_id")

        await state.clear()

        # Р Р€Р Т‘Р В°Р В»РЎРЏР ВµР С РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ Р С—Р С•Р В»РЎРЉР В·Р С•Р Р†Р В°РЎвЂљР ВµР В»РЎРЏ

        try:
            await message.delete()

        except Exception:
            pass

        # Р В Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚РЎС“Р ВµР С Р СР ВµР Р…РЎР‹ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘

        if edit_group_id is not None:
            for bid, broadcast in list(active_broadcasts.items()):
                if (
                    broadcast.get("group_id") == edit_group_id
                    and broadcast.get("user_id") == user_id
                    and broadcast.get("status") in ("running", "paused")
                ):
                    await update_broadcast_fields(bid, chat_pause=pause_value)

        if edit_message_id and chat_id and edit_group_id is not None:
            try:
                if await _edit_group_detail_message(
                    message,
                    user_id,
                    edit_group_id,
                    chat_id=chat_id,
                    message_id=edit_message_id,
                ):
                    return
            except Exception as e:
                print(f"Group detail refresh failed after pace update: {e}")
                await message.answer(
                    "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043c\u0435\u043d\u044e \u0433\u0440\u0443\u043f\u043f\u044b"
                )
                return

        if edit_message_id and chat_id:
            try:
                chats = get_broadcast_chats(user_id)

                info = build_broadcast_menu_text(
                    config, chats, active_broadcasts, user_id
                )

                kb = build_broadcast_keyboard(
                    include_active=False,
                    user_id=user_id,
                    active_broadcasts=active_broadcasts,
                    back_callback="delete_bc_menu",
                )

                await message.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=edit_message_id,
                    text=info,
                    reply_markup=kb,
                    parse_mode="HTML",
                )

            except Exception as e:
                print(
                    f"Р С›РЎв‚¬Р С‘Р В±Р С”Р В° РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘РЎРЏ: {e}"
                )

                await message.answer(
                    "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043c\u0435\u043d\u044e"
                )

        else:
            await cmd_broadcast_menu(message)

    except Exception as e:
        print(
            f"Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р С•Р В±РЎР‚Р В°Р В±Р С•РЎвЂљР С”Р С‘ Р В·Р В°Р Т‘Р ВµРЎР‚Р В¶Р С”Р С‘ Р СР ВµР В¶Р Т‘РЎС“ РЎвЂЎР В°РЎвЂљР В°Р СР С‘: {e}"
        )

        await message.answer(
            "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0441\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c \u0442\u0435\u043c\u043f"
        )


async def _send_broadcast_notice(message_or_query, text: str) -> None:

    try:
        if hasattr(message_or_query, "message"):
            await message_or_query.message.answer(text)

        else:
            await message_or_query.answer(text)

    except Exception:
        try:
            await message_or_query.answer(text)

        except Exception:
            pass


def _start_or_resume_broadcast_task(broadcast_id: int) -> bool:
    broadcast = get_broadcast(broadcast_id)
    if not broadcast:
        return False

    existing_task = get_broadcast_task(broadcast_id)
    if existing_task and not existing_task.done():
        return True

    interval_value = broadcast.get("interval_value", broadcast.get("interval_minutes", 1))
    interval_minutes = int(interval_value) if str(interval_value).isdigit() else 1

    task = asyncio.create_task(
        schedule_broadcast_send(
            user_id=broadcast["user_id"],
            account_number=broadcast["account"],
            chat_ids=list(broadcast.get("chat_ids") or []),
            texts=list(broadcast.get("texts") or []),
            interval_minutes=interval_minutes,
            count=int(broadcast.get("count", 1) or 1),
            broadcast_id=broadcast_id,
            parse_mode=broadcast.get("parse_mode", "HTML"),
            text_mode=broadcast.get("text_mode", "random"),
        )
    )
    register_broadcast_task(broadcast_id, task)
    return True


async def execute_broadcast(
    message_or_query,
    user_id: int,
    account_number: int,
    config: dict,
    chats: list,
    group_id: int = None,
) -> None:
    client = await _ensure_account_ready(user_id, account_number)
    if not client:
        await _send_broadcast_notice(
            message_or_query,
            f"❌ Не удалось подключить аккаунт {account_number}. Проверь сессию и прокси.",
        )
        return

    chat_ids = [cid for cid, _ in chats]
    broadcast_id = next_broadcast_id()

    account_name = None
    for acc_num, telegram_id, username, first_name, is_active in get_user_accounts(user_id):
        if acc_num == account_number:
            account_name = first_name or username or f"Аккаунт {acc_num}"
            break

    source_type = config.get("text_source_type", "manual")
    runtime_config = dict(config)
    if source_type == "channel":
        source_ref = runtime_config.get("source_channel_ref")
        if not source_ref:
            await _send_broadcast_notice(
                message_or_query,
                "❌ Не указан канал-источник",
            )
            return

        try:
            source_data = await _load_channel_source_for_user(
                user_id,
                source_ref,
                preferred_account_number=account_number,
            )
        except Exception as exc:
            if not runtime_config.get("source_posts"):
                await _send_broadcast_notice(
                    message_or_query,
                    f"❌ Не удалось загрузить посты канала: {exc}",
                )
                return
        else:
            runtime_config.update(source_data)
            save_broadcast_config_with_profile(user_id, runtime_config)

    content_items = (
        _build_channel_content_items(runtime_config)
        if source_type == "channel"
        else _build_manual_content_items(runtime_config)
    )
    if not content_items:
        await _send_broadcast_notice(
            message_or_query,
            "❌ Нет контента для рассылки",
        )
        return

    payload = {
        "user_id": user_id,
        "account": account_number,
        "account_name": account_name or f"Аккаунт {account_number}",
        "chat_ids": chat_ids,
        "texts": list(runtime_config.get("texts") or []),
        "content_items": content_items,
        "text_source_type": source_type,
        "text_mode": runtime_config.get("text_mode", "random"),
        "parse_mode": runtime_config.get("parse_mode", "HTML"),
        "source_channel_ref": runtime_config.get("source_channel_ref", ""),
        "source_channel_title": runtime_config.get("source_channel_title", ""),
        "chat_pause": runtime_config.get("chat_pause", "20-60"),
        "total_chats": len(chat_ids),
        "sent_chats": 0,
        "planned_count": int(runtime_config.get("count", 1)),
        "count": int(runtime_config.get("count", 1)),
        "interval_minutes": runtime_config.get("interval", 1),
        "interval_value": runtime_config.get("interval", 1),
        "start_time": datetime.now(timezone.utc),
        "status": "running",
        "failed_count": 0,
        "processed_count": 0,
        "chat_runtime": [
            {
                "chat_id": chat_id,
                "name": chat_name,
                "next_send_at": 0,
                "sent_count": 0,
                "failed_count": 0,
                "status": "active",
                "last_error": "",
                "last_error_at": 0,
                "order": index,
            }
            for index, (chat_id, chat_name) in enumerate(chats)
        ],
        "next_global_send_at": 0,
        "text_index": 0,
    }

    if group_id is not None:
        payload["group_id"] = group_id

    create_broadcast(broadcast_id, payload)
    _start_or_resume_broadcast_task(broadcast_id)

    await _send_broadcast_notice(
        message_or_query,
        f"✅ Рассылка #{broadcast_id} запущена",
    )


@router.callback_query(F.data == "bc_launch")
async def bc_launch_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id

    accounts = get_user_accounts(user_id)
    available_accounts = [
        (acc_num, username, first_name)
        for acc_num, _, username, first_name, is_active in accounts
        if is_active
    ] or [
        (acc_num, username, first_name)
        for acc_num, _, username, first_name, _ in accounts
    ]

    if not available_accounts:
        await _send_broadcast_notice(
            query,
            "\u274c \u0422\u044b \u043d\u0435 \u0437\u0430\u043b\u043e\u0433\u0438\u0440\u043e\u0432\u0430\u043d!",
        )
        return

    config = get_broadcast_config(user_id)
    chats = get_broadcast_chats(user_id)

    if not _broadcast_content_ready(config):
        await _send_broadcast_notice(query, _build_missing_content_notice(config))
        return

    if not chats:
        await _send_broadcast_notice(
            query,
            "\u274c \u041d\u0435\u0442 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438!\n\n\u0414\u043e\u0431\u0430\u0432\u044c \u0447\u0430\u0442\u044b \u0447\u0435\u0440\u0435\u0437 '\U0001f4ac \u0427\u0430\u0442\u044b \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438'",
        )
        return

    if len(available_accounts) == 1:
        account_number = available_accounts[0][0]
        await execute_broadcast(query, user_id, account_number, config, chats)
        return

    buttons = []
    for acc_num, username, first_name in available_accounts:
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"\U0001f464 {_account_label(acc_num, username, first_name)}",
                    callback_data=f"start_bc_{acc_num}",
                )
            ]
        )

    if len(buttons) > 1:
        buttons.insert(
            0,
            [
                InlineKeyboardButton(
                    text="\U0001f7e2 \u0412\u0441\u0435 \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u044b",
                    callback_data="start_bc_all",
                )
            ],
        )

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await query.message.answer(
        "\u0412\u044b\u0431\u0435\u0440\u0438 \u0430\u043a\u043a\u0430\u0443\u043d\u0442 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438:",
        reply_markup=keyboard,
    )


@router.callback_query(F.data.startswith("start_bc_"))
async def start_bc_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    chats = get_broadcast_chats(user_id)

    if not _broadcast_content_ready(config):
        await _send_broadcast_notice(query, _build_missing_content_notice(config))
        return

    if not chats:
        await _send_broadcast_notice(
            query,
            "\u274c \u041d\u0435\u0442 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438",
        )

        return

    try:
        await query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    if query.data == "start_bc_all":
        connected_accounts = []
        for acc_num in _iter_connected_account_numbers(user_id):
            client = await _ensure_account_ready(user_id, acc_num)
            if client:
                connected_accounts.append(acc_num)

        if not connected_accounts:
            await _send_broadcast_notice(
                query,
                "\u274c \u041d\u0435\u0442 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043d\u044b\u0445 \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432",
            )

            return

        group_id = next_broadcast_id()

        for acc_num in connected_accounts:
            await execute_broadcast(
                query, user_id, acc_num, config, chats, group_id=group_id
            )

        try:
            await query.message.delete()
        except Exception:
            pass

        await _send_broadcast_notice(
            query,
            f"\u2705 \u0417\u0430\u043f\u0443\u0449\u0435\u043d\u043e \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432: {len(connected_accounts)}",
        )

        return

    try:
        account_number = int(query.data.split("_")[2])

    except Exception:
        await _send_broadcast_notice(query, "\u041e\u0448\u0438\u0431\u043a\u0430")

        return

    await execute_broadcast(query, user_id, account_number, config, chats)
    try:
        await query.message.delete()
    except Exception:
        pass


@router.message(
    F.text.in_(
        [
            "\U0001f680 \u0417\u0430\u043f\u0443\u0441\u0442\u0438\u0442\u044c",
            "\U0001f680 \u0417\u0430\u043f\u0443\u0441\u0442\u0438\u0442\u044c \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0443",
        ]
    )
)
async def start_broadcast_button(message: Message):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С”Р Р…Р С•Р С—Р С”Р С‘ Р В·Р В°Р С—РЎС“РЎРѓР С”Р В° РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘"""

    user_id = message.from_user.id

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С Р В·Р В°Р В»Р С•Р С–Р С‘РЎР‚Р С•Р Р†Р В°Р Р… Р В»Р С‘

    accounts = get_user_accounts(user_id)
    available_accounts = [
        (acc_num, username, first_name)
        for acc_num, _, username, first_name, is_active in accounts
        if is_active
    ] or [
        (acc_num, username, first_name)
        for acc_num, _, username, first_name, _ in accounts
    ]

    if not available_accounts:
        await message.answer(LOGIN_REQUIRED_TEXT)

        return

    # Р СџР С•Р В»РЎС“РЎвЂЎР В°Р ВµР С Р С”Р С•Р Р…РЎвЂћР С‘Р С– РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘

    config = get_broadcast_config(user_id)

    chats = get_broadcast_chats(user_id)

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С РЎвЂЎРЎвЂљР С• Р ВµРЎРѓРЎвЂљРЎРЉ РЎвЂљР ВµР С”РЎРѓРЎвЂљ

    if not _broadcast_content_ready(config):
        await message.answer(_build_missing_content_notice(config))
        return

    if not chats:
        await message.answer(
            "\u274c \u041d\u0435\u0442 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438!\n\n"
            "\u0414\u043e\u0431\u0430\u0432\u044c \u0447\u0430\u0442\u044b \u0447\u0435\u0440\u0435\u0437 '\U0001f4ac \u0427\u0430\u0442\u044b'."
        )

        return

    # Р вЂўРЎРѓР В»Р С‘ РЎвЂљР С•Р В»РЎРЉР С”Р С• Р С•Р Т‘Р С‘Р Р… Р В°Р С”Р С”Р В°РЎС“Р Р…РЎвЂљ - Р С‘РЎРѓР С—Р С•Р В»РЎРЉР В·РЎС“Р ВµР С Р ВµР С–Р С•

    if len(available_accounts) == 1:
        account_number = available_accounts[0][0]

    else:
        # Р вЂўРЎРѓР В»Р С‘ Р Р…Р ВµРЎРѓР С”Р С•Р В»РЎРЉР С”Р С• - Р С—Р С•Р С”Р В°Р В·РЎвЂ№Р Р†Р В°Р ВµР С Р Р†РЎвЂ№Р В±Р С•РЎР‚

        buttons = []

        for acc_num, username, first_name in available_accounts:
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=f"\U0001f464 {_account_label(acc_num, username, first_name)}",
                        callback_data=f"start_bc_{acc_num}",
                    )
                ]
            )

        if len(buttons) > 1:
            buttons.insert(
                0,
                [
                    InlineKeyboardButton(
                        text="\U0001f7e2 \u0412\u0441\u0435 \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u044b",
                        callback_data="start_bc_all",
                    )
                ],
            )

        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

        await message.answer(
            "\u0412\u044b\u0431\u0435\u0440\u0438 \u0430\u043a\u043a\u0430\u0443\u043d\u0442 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438:",
            reply_markup=keyboard,
        )

        return

    # Р вЂ”Р В°Р С—РЎС“РЎРѓР С”Р В°Р ВµР С РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”РЎС“

    await execute_broadcast(message, user_id, account_number, config, chats)


# Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С”Р Р…Р С•Р С—Р С”Р С‘ "Р С’Р С”РЎвЂљР С‘Р Р†Р Р…РЎвЂ№Р Вµ" Р Т‘Р В»РЎРЏ Р С—РЎР‚Р С•РЎРѓР СР С•РЎвЂљРЎР‚Р В° Р В°Р С”РЎвЂљР С‘Р Р†Р Р…РЎвЂ№РЎвЂ¦ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С•Р С”


@router.message(F.text == "\U0001f4e4 \u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0435")
async def active_broadcasts_button(message: Message):

    user_id = message.from_user.id

    user_broadcasts = {
        bid: b
        for bid, b in active_broadcasts.items()
        if b["user_id"] == user_id and b["status"] in ("running", "paused")
    }

    if not user_broadcasts:
        await message.answer(
            "\u274c \u041d\u0435\u0442 \u0430\u043a\u0442\u0438\u0432\u043d\u044b\u0445 \u0440\u0430\u0441\u0441\u044b\u043b\u043e\u043a"
        )

        return

    groups = {}

    singles = []

    for bid, b in user_broadcasts.items():
        gid = b.get("group_id")

        if gid is None:
            singles.append((bid, b))

        else:
            groups.setdefault(gid, []).append((bid, b))

    info = "\U0001f4e4 <b>\u0410\u041a\u0422\u0418\u0412\u041d\u042b\u0415 \u0420\u0410\u0421\u0421\u042b\u041b\u041a\u0418</b>\n\n"

    for gid, items in sorted(groups.items()):
        status = (
            "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
            if any(b["status"] == "running" for _, b in items)
            else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
        )

        info += f"\u0413\u0440\u0443\u043f\u043f\u0430 #{gid} {status} | \u0410\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432: {len(items)}\n"

    for bid, b in sorted(singles):
        status = (
            "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
            if b["status"] == "running"
            else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
        )

        account_name = b.get(
            "account_name",
            f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442 {b.get('account', '?')}",
        )

        info += f"\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 #{bid} {status} | {account_name}\n"

    await message.answer(info, parse_mode="HTML")

    inline_buttons = []

    for gid, items in sorted(groups.items()):
        inline_buttons.append(
            [
                InlineKeyboardButton(
                    text=f"\u0413\u0440\u0443\u043f\u043f\u0430 #{gid}",
                    callback_data=f"view_group_{gid}",
                )
            ]
        )

    for bid, b in sorted(singles):
        inline_buttons.append(
            [
                InlineKeyboardButton(
                    text=f"\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 #{bid}",
                    callback_data=f"view_bc_{bid}",
                )
            ]
        )

    inline_buttons.append(
        [
            InlineKeyboardButton(
                text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434 \u0432 \u043c\u0435\u043d\u044e",
                callback_data="back_to_broadcast_menu",
            )
        ]
    )

    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=inline_buttons)

    await message.answer(
        "\u0412\u044b\u0431\u0435\u0440\u0438 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0443 \u0434\u043b\u044f \u0443\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u044f:",
        reply_markup=inline_keyboard,
    )


@router.callback_query(F.data == "bc_chats_add")
async def bc_chats_add_callback(query: CallbackQuery, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С”Р Р…Р С•Р С—Р С”Р С‘ Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р С‘РЎРЏ РЎвЂЎР В°РЎвЂљР В° Р С‘Р В· Р СР ВµР Р…РЎР‹"""

    await query.answer()

    await state.update_data(
        previous_menu="broadcast_chats", menu_message_id=query.message.message_id
    )

    await state.set_state(BroadcastConfigState.waiting_for_chat_id)

    text = (
        "\U0001f4ec <b>\u0414\u041e\u0411\u0410\u0412\u041b\u0415\u041d\u0418\u0415 \u0427\u0410\u0422\u0410</b>\n\n"
        "\u041e\u0442\u043f\u0440\u0430\u0432\u044c ID \u0447\u0430\u0442\u0430 \u0438\u043b\u0438 \u0441\u0441\u044b\u043b\u043a\u0443/\u044e\u0437\u0435\u0440\u043d\u0435\u0439\u043c \u043a\u0430\u043d\u0430\u043b\u0430:\n"
        "\u041c\u043e\u0436\u043d\u043e \u0441\u0440\u0430\u0437\u0443 \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u043e: \u043a\u0430\u0436\u0434\u044b\u0439 ID/\u0441\u0441\u044b\u043b\u043a\u0443 \u0441 \u043d\u043e\u0432\u043e\u0439 \u0441\u0442\u0440\u043e\u043a\u0438.\n\n"
        "\u041f\u0440\u0438\u043c\u0435\u0440\u044b:\n"
        "  \u2022 <code>-1001234567890</code>\n"
        "  \u2022 <code>@mychannel</code>\n\n"
        "\u0427\u0430\u0442 \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u043e\u0442\u043a\u0440\u044b\u0442 \u0438\u043b\u0438 \u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d \u0442\u0432\u043e\u0435\u043c\u0443 Telegram-\u0430\u043a\u043a\u0430\u0443\u043d\u0442\u0443."
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=CANCEL_TEXT,
                    callback_data="bc_cancel",
                )
            ]
        ]
    )

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "bc_chats_import")
async def bc_chats_import_callback(query: CallbackQuery):
    await query.answer()

    user_id = query.from_user.id
    accounts = _iter_connected_account_numbers(user_id)
    if not accounts:
        await query.answer(LOGIN_REQUIRED_TEXT, show_alert=True)
        return

    text, kb = _build_folder_account_picker(user_id)
    await _edit_or_notice(query, text, kb, fallback_to_answer=True)


@router.callback_query(F.data.startswith("bc_folder_acc_"))
async def bc_folder_account_callback(query: CallbackQuery):
    await query.answer()

    user_id = query.from_user.id
    try:
        account_number = int(query.data.rsplit("_", 1)[1])
    except Exception:
        await query.answer("Ошибка аккаунта", show_alert=True)
        return

    try:
        await query.message.edit_text(
            f"⏳ <b>Загружаю папки аккаунта {account_number}</b>",
            parse_mode="HTML",
        )
    except Exception:
        pass

    try:
        _, folders = await _load_account_folders(user_id, account_number)
    except Exception as exc:
        await query.message.answer(
            f"❌ Не удалось загрузить папки: {html.escape(str(exc))}",
            parse_mode="HTML",
        )
        return

    text, kb = _build_folder_list_view(account_number, folders)
    await _edit_or_notice(query, text, kb, fallback_to_answer=True)


@router.callback_query(F.data.startswith("bc_folder_pick_"))
async def bc_folder_pick_callback(query: CallbackQuery):
    await query.answer()

    user_id = query.from_user.id
    try:
        account_number, folder_id = _parse_folder_callback(query.data, "pick")
    except Exception:
        await query.answer("Ошибка папки", show_alert=True)
        return

    try:
        await query.message.edit_text(
            f"⏳ <b>Загружаю чаты из папки {folder_id}</b>",
            parse_mode="HTML",
        )
    except Exception:
        pass

    try:
        _, folder, folder_chats = await _load_folder_chats(
            user_id,
            account_number,
            folder_id,
        )
    except Exception as exc:
        await query.message.answer(
            f"❌ Не удалось загрузить чаты папки: {html.escape(str(exc))}",
            parse_mode="HTML",
        )
        return

    text, kb = _build_folder_preview_view(account_number, folder, folder_chats)
    await _edit_or_notice(query, text, kb, fallback_to_answer=True)


async def _apply_folder_import(
    query: CallbackQuery,
    user_id: int,
    account_number: int,
    folder_id: int,
    *,
    replace_existing: bool,
) -> None:
    try:
        await query.message.edit_text(
            f"⏳ <b>Импортирую чаты из папки {folder_id}</b>",
            parse_mode="HTML",
        )
    except Exception:
        pass

    try:
        _, folder, folder_chats = await _load_folder_chats(
            user_id,
            account_number,
            folder_id,
        )
    except Exception as exc:
        await query.message.answer(
            f"❌ Ошибка импорта из папки: {html.escape(str(exc))}",
            parse_mode="HTML",
        )
        return

    if not folder_chats:
        await query.answer("В папке нет чатов для импорта", show_alert=True)
        return

    if replace_existing:
        for chat_id, _chat_name in list(get_broadcast_chats(user_id)):
            remove_broadcast_chat_with_profile(user_id, chat_id)

    added = 0
    duplicates = 0
    for item in folder_chats:
        is_added = add_broadcast_chat_with_profile(
            user_id,
            item["chat_id"],
            item["chat_name"],
            chat_link=item.get("chat_link"),
        )
        if is_added:
            added += 1
        else:
            duplicates += 1

    title = _folder_title(folder)
    mode_label = "заменён" if replace_existing else "обновлён"
    await show_broadcast_chats_menu(query, user_id)
    await query.message.answer(
        "\n".join(
            [
                "📂 <b>Импорт из папки завершён</b>",
                "",
                f"Папка: <b>{html.escape(title)}</b>",
                f"Аккаунт-источник: <b>{account_number}</b>",
                f"Список чатов {mode_label}.",
                f"✅ Добавлено: <b>{added}</b>",
                f"⚠️ Уже были: <b>{duplicates}</b>",
                f"📊 Всего в папке: <b>{len(folder_chats)}</b>",
            ]
        ),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("bc_folder_add_"))
async def bc_folder_add_callback(query: CallbackQuery):
    await query.answer()

    try:
        account_number, folder_id = _parse_folder_callback(query.data, "add")
        await _apply_folder_import(
            query,
            query.from_user.id,
            account_number,
            folder_id,
            replace_existing=False,
        )
    except Exception as exc:
        await query.message.answer(
            f"❌ Ошибка импорта из папки: {html.escape(str(exc))}",
            parse_mode="HTML",
        )


@router.callback_query(F.data.startswith("bc_folder_replace_"))
async def bc_folder_replace_callback(query: CallbackQuery):
    await query.answer()

    try:
        account_number, folder_id = _parse_folder_callback(query.data, "replace")
        await _apply_folder_import(
            query,
            query.from_user.id,
            account_number,
            folder_id,
            replace_existing=True,
        )
    except Exception as exc:
        await query.message.answer(
            f"❌ Ошибка замены чатов из папки: {html.escape(str(exc))}",
            parse_mode="HTML",
        )


@router.message(BroadcastConfigState.waiting_for_chat_id)
async def process_add_broadcast_chat_with_profile(message: Message, state: FSMContext):
    """Add one or many chats to the broadcast list."""

    user_id = message.from_user.id
    raw_input = message.text or ""
    chat_inputs = [line.strip() for line in raw_input.splitlines() if line.strip()]

    if not chat_inputs:
        await message.answer(
            "\u274c \u041e\u0442\u043f\u0440\u0430\u0432\u044c ID \u0447\u0430\u0442\u0430, \u0441\u0441\u044b\u043b\u043a\u0443 \u0438\u043b\u0438 \u044e\u0437\u0435\u0440\u043d\u0435\u0439\u043c. \u041c\u043e\u0436\u043d\u043e \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u043e \u0441\u0442\u0440\u043e\u043a \u0441\u0440\u0430\u0437\u0443.",
            parse_mode="HTML",
        )
        return

    if len(chat_inputs) == 1 and chat_inputs[0] == CANCEL_TEXT:
        await return_to_previous_menu(message, state)
        return

    try:
        await message.delete()
    except Exception:
        pass

    loading_text = (
        "\u23f3 \u0417\u0430\u0433\u0440\u0443\u0436\u0430\u044e \u0438\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044e \u043e \u0447\u0430\u0442\u0430\u0445..."
        if len(chat_inputs) > 1
        else "\u23f3 \u0417\u0430\u0433\u0440\u0443\u0436\u0430\u044e \u0438\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044e \u043e \u0447\u0430\u0442\u0435..."
    )
    loading_msg = await message.answer(loading_text)

    async def _delete_loading():
        try:
            await loading_msg.delete()
        except Exception:
            pass

    async def _resolve_chat_input(chat_input: str) -> dict:
        chat_reference = parse_numeric_reference(chat_input)
        if chat_reference is None:
            chat_reference = chat_input

        chat, resolved_account = await _resolve_chat_for_user(user_id, chat_reference)
        _ = resolved_account  # kept for future diagnostics

        try:
            chat_id = int(get_peer_id(chat))
        except Exception:
            chat_id = int(getattr(chat, "id"))

        title = getattr(chat, "title", None) or getattr(chat, "first_name", None)
        if not title and hasattr(chat, "id"):
            title = f"user{chat.id}"

        chat_name = str(title) if title else f"\u0427\u0430\u0442 {chat_id}"
        chat_link = _detect_chat_link(chat_input, chat) or _detect_chat_link(chat_input, None)
        return {
            "input": chat_input,
            "chat_id": chat_id,
            "chat_name": chat_name,
            "chat_link": chat_link,
        }

    def _item_line(item: dict) -> str:
        name = html.escape(str(item.get("chat_name") or item.get("chat_id") or item.get("input")))
        chat_id = item.get("chat_id")
        if chat_id is None:
            return f"\u2022 {name}"
        return f"\u2022 {name} <code>{chat_id}</code>"

    def _build_add_summary(added: list[dict], duplicates: list[dict], failed: list[dict]) -> str:
        lines = [
            "\U0001f4ec <b>\u0418\u0442\u043e\u0433 \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u0438\u044f \u0447\u0430\u0442\u043e\u0432</b>",
            "",
            f"\u2705 \u0414\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u043e: <b>{len(added)}</b>",
            f"\u26a0\ufe0f \u0423\u0436\u0435 \u0431\u044b\u043b\u0438: <b>{len(duplicates)}</b>",
            f"\u274c \u041e\u0448\u0438\u0431\u043e\u043a: <b>{len(failed)}</b>",
        ]

        def append_items(title: str, items: list[dict]) -> None:
            if not items:
                return
            lines.extend(["", title])
            for item in items[:8]:
                lines.append(_item_line(item))
            if len(items) > 8:
                lines.append(f"... \u0435\u0449\u0435 {len(items) - 8}")

        append_items("\u2705 <b>\u0414\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u044b:</b>", added)
        append_items("\u26a0\ufe0f <b>\u0423\u0436\u0435 \u0432 \u0441\u043f\u0438\u0441\u043a\u0435:</b>", duplicates)

        if failed:
            lines.extend(["", "\u274c <b>\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0434\u043e\u0431\u0430\u0432\u0438\u0442\u044c:</b>"])
            for item in failed[:8]:
                source = html.escape(str(item.get("input", "")))
                error = html.escape(str(item.get("error", ""))[:180])
                lines.append(f"\u2022 <code>{source}</code> - {error}")
            if len(failed) > 8:
                lines.append(f"... \u0435\u0449\u0435 {len(failed) - 8}")

        return "\n".join(lines)

    try:
        if not _iter_connected_account_numbers(user_id):
            await message.answer(LOGIN_REQUIRED_TEXT)
            await state.clear()
            await _delete_loading()
            return

        added_chats: list[dict] = []
        duplicate_chats: list[dict] = []
        failed_chats: list[dict] = []

        for chat_input in chat_inputs:
            if chat_input == CANCEL_TEXT:
                continue

            try:
                chat_data = await _resolve_chat_input(chat_input)
                chat_id = chat_data.get("chat_id")
                chat_name = chat_data.get("chat_name") or f"\u0427\u0430\u0442 {chat_id}"
                if chat_id is None:
                    raise RuntimeError("\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u043f\u0440\u0435\u0434\u0435\u043b\u0438\u0442\u044c ID \u0447\u0430\u0442\u0430")

                added = add_broadcast_chat_with_profile(
                    user_id,
                    chat_id,
                    chat_name,
                    chat_link=chat_data.get("chat_link"),
                )

                if added:
                    added_chats.append(chat_data)
                else:
                    duplicate_chats.append(chat_data)

            except Exception as e:
                print(f"Broadcast chat add failed for {chat_input}: {str(e)}")
                failed_chats.append({"input": chat_input, "error": str(e)})

        await _delete_loading()

        if not added_chats and not duplicate_chats:
            if len(failed_chats) == 1:
                error_text = str(failed_chats[0].get("error", "")).lower()
                if "timed out" in error_text or "timeout" in error_text:
                    await message.answer(
                        "\u274c Telegram \u0441\u043b\u0438\u0448\u043a\u043e\u043c \u0434\u043e\u043b\u0433\u043e \u043e\u0442\u0432\u0435\u0447\u0430\u0435\u0442 \u043f\u0440\u0438 \u043f\u043e\u0438\u0441\u043a\u0435 \u0447\u0430\u0442\u0430. "
                        "\u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439 \u0435\u0449\u0435 \u0440\u0430\u0437 \u0438\u043b\u0438 \u0434\u0440\u0443\u0433\u0443\u044e "
                        "\u0441\u0441\u044b\u043b\u043a\u0443/\u0430\u0439\u0434\u0438.",
                        parse_mode="HTML",
                    )
                    return
                await message.answer(
                    "\u274c \u0427\u0430\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d. \u0412\u0432\u0435\u0434\u0438 ID \u0447\u0430\u0442\u0430 "
                    "(<code>-1003880811528</code>), \u0441\u0441\u044b\u043b\u043a\u0443 \u0438\u043b\u0438 \u044e\u0437\u0435\u0440\u043d\u0435\u0439\u043c "
                    "(<code>@mychannel</code>). \u0410\u043a\u043a\u0430\u0443\u043d\u0442, \u0447\u0435\u0440\u0435\u0437 \u043a\u043e\u0442\u043e\u0440\u044b\u0439 "
                    "\u0438\u0434\u0435\u0442 \u043f\u043e\u0438\u0441\u043a, \u0434\u043e\u043b\u0436\u0435\u043d \u0432\u0438\u0434\u0435\u0442\u044c \u044d\u0442\u043e\u0442 \u0447\u0430\u0442.",
                    parse_mode="HTML",
                )
            else:
                await message.answer(
                    _build_add_summary(added_chats, duplicate_chats, failed_chats),
                    parse_mode="HTML",
                )
            return

        notify_msg = await message.answer(
            _build_add_summary(added_chats, duplicate_chats, failed_chats),
            parse_mode="HTML",
        )

        if not failed_chats:
            asyncio.create_task(delete_message_after_delay(notify_msg, 7))

        state_data = await state.get_data()
        await state.clear()
        await show_broadcast_chats_menu(
            message, user_id, menu_message_id=state_data.get("menu_message_id")
        )

    except Exception as e:
        await _delete_loading()
        print(f"Error in process_add_broadcast_chat: {str(e)}")
        await message.answer(f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {html.escape(str(e))}", parse_mode="HTML")


@router.callback_query(F.data.startswith("select_chat_"))
async def select_chat_callback(query: CallbackQuery, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р Р†РЎвЂ№Р В±Р С•РЎР‚Р В° РЎвЂЎР В°РЎвЂљР В° Р С‘Р В· Р С—Р С•РЎвЂ¦Р С•Р В¶Р С‘РЎвЂ¦"""

    user_id = query.from_user.id

    try:
        chat_id = int(query.data.split("_")[2])

        available_accounts = _iter_connected_account_numbers(user_id)
        if not available_accounts:
            await query.answer(LOGIN_REQUIRED_TEXT, show_alert=True)

            return

        account_number = available_accounts[0]
        client = await _ensure_account_ready(user_id, account_number)
        if not client:
            await query.answer(
                "❌ Не удалось подключить аккаунт. Проверь сессию и прокси.",
                show_alert=True,
            )
            return

        # Р СџР С•Р В»РЎС“РЎвЂЎР В°Р ВµР С Р С‘Р Р…РЎвЂћР С•РЎР‚Р СР В°РЎвЂ Р С‘РЎР‹ Р С• Р Р†РЎвЂ№Р В±РЎР‚Р В°Р Р…Р Р…Р С•Р С РЎвЂЎР В°РЎвЂљР Вµ

        dialogs = await client.get_dialogs(limit=None)

        for dialog in dialogs:
            if dialog.entity.id == chat_id:
                entity = dialog.entity

                chat_name = (
                    entity.title
                    if hasattr(entity, "title")
                    else (entity.first_name or str(chat_id))
                )
                chat_link = _detect_chat_link(None, entity)

                # Р вЂќР С•Р В±Р В°Р Р†Р В»РЎРЏР ВµР С РЎвЂЎР В°РЎвЂљ

                add_broadcast_chat_with_profile(
                    user_id, chat_id, chat_name, chat_link=chat_link
                )

                state_data = await state.get_data()

                await state.clear()

                await show_broadcast_chats_menu(
                    query,
                    user_id,
                    menu_message_id=state_data.get("menu_message_id")
                    or query.message.message_id,
                )

                return

        await query.answer(
            "\u274c \u0427\u0430\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d", show_alert=True
        )

    except Exception as e:
        await query.answer(
            f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {str(e)}", show_alert=True
        )


@router.callback_query(F.data.startswith("manual_chat_"))
async def manual_chat_callback(query: CallbackQuery, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р Р†Р Р†Р С•Р Т‘Р В° Р С‘Р СР ВµР Р…Р С‘ РЎвЂЎР В°РЎвЂљР В° Р Р†РЎР‚РЎС“РЎвЂЎР Р…РЎС“РЎР‹"""

    try:
        chat_id = int(query.data.split("_")[2])

        await state.update_data(chat_id=chat_id, previous_menu="broadcast_chats")

        await state.set_state(BroadcastConfigState.waiting_for_chat_name)

        await query.answer()

        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=CANCEL_TEXT)]],
            resize_keyboard=True,
        )

        await query.message.delete()

        await query.message.answer(
            f"\u270f\ufe0f \u0412\u0432\u0435\u0434\u0438 \u0438\u043c\u044f \u0438\u043b\u0438 \u043e\u043f\u0438\u0441\u0430\u043d\u0438\u0435 \u0434\u043b\u044f \u0447\u0430\u0442\u0430 \u0441 ID {chat_id}:",
            reply_markup=keyboard,
        )

    except Exception as e:
        await query.answer(f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {str(e)}", show_alert=True)


@router.message(BroadcastConfigState.waiting_for_chat_name)
async def process_broadcast_chat_name(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р Р†Р Р†Р С•Р Т‘Р В° Р С‘Р СР ВµР Р…Р С‘ РЎвЂЎР В°РЎвЂљР В° Р С—РЎР‚Р С‘ Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р С‘Р С‘"""

    user_id = message.from_user.id

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С Р С•РЎвЂљР СР ВµР Р…РЎС“

    if message.text == CANCEL_TEXT:
        await return_to_previous_menu(message, state)

        return

    try:
        data = await state.get_data()

        chat_id = data.get("chat_id")

        chat_name = message.text.strip()

        if not chat_id:
            await message.answer(
                "\u274c \u041e\u0448\u0438\u0431\u043a\u0430: Chat ID \u043d\u0435 \u0441\u043e\u0445\u0440\u0430\u043d\u0435\u043d. \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439 \u0441\u043d\u043e\u0432\u0430"
            )

            await state.clear()

            await show_broadcast_chats_menu(
                message,
                message.from_user.id,
                menu_message_id=data.get("menu_message_id"),
            )

            return

        # Р вЂќР С•Р В±Р В°Р Р†Р В»РЎРЏР ВµР С РЎвЂЎР В°РЎвЂљ РЎРѓ Р Р†Р Р†Р ВµР Т‘РЎвЂР Р…Р Р…РЎвЂ№Р С Р С‘Р СР ВµР Р…Р ВµР С

        added = add_broadcast_chat_with_profile(user_id, chat_id, chat_name)

        # Р С›РЎвЂљР С—РЎР‚Р В°Р Р†Р В»РЎРЏР ВµР С РЎС“Р Р†Р ВµР Т‘Р С•Р СР В»Р ВµР Р…Р С‘Р Вµ Р С‘ РЎРѓРЎР‚Р В°Р В·РЎС“ РЎС“Р Т‘Р В°Р В»РЎРЏР ВµР С (Р В±РЎвЂ№РЎРѓРЎвЂљРЎР‚Р С•Р Вµ Р Р†РЎРѓР С—Р В»РЎвЂ№Р Р†Р В°РЎР‹РЎвЂ°Р ВµР Вµ РЎС“Р Р†Р ВµР Т‘Р С•Р СР В»Р ВµР Р…Р С‘Р Вµ)

        if added:
            notify_msg = await message.answer(
                f"\u2705 \u0427\u0430\u0442 '{chat_name}' \u0443\u0441\u043f\u0435\u0448\u043d\u043e \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d!"
            )

        else:
            notify_msg = await message.answer(
                "\u26a0\ufe0f \u0427\u0430\u0442 \u0441 \u044d\u0442\u0438\u043c ID \u0443\u0436\u0435 \u0435\u0441\u0442\u044c \u0432 \u0441\u043f\u0438\u0441\u043a\u0435"
            )

        # Р Р€Р Т‘Р В°Р В»РЎРЏР ВµР С РЎС“Р Р†Р ВµР Т‘Р С•Р СР В»Р ВµР Р…Р С‘Р Вµ Р С—Р С•РЎвЂЎРЎвЂљР С‘ РЎРѓРЎР‚Р В°Р В·РЎС“ (500Р СРЎРѓ) Р Т‘Р В»РЎРЏ РЎРЊРЎвЂћРЎвЂћР ВµР С”РЎвЂљР В° Р Р†РЎРѓР С—Р В»РЎвЂ№Р Р†Р В°РЎР‹РЎвЂ°Р ВµР С–Р С• РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘РЎРЏ

        import asyncio

        asyncio.create_task(delete_message_after_delay(notify_msg, 0.5))

        await state.clear()

        await show_broadcast_chats_menu(
            message, message.from_user.id, menu_message_id=data.get("menu_message_id")
        )

    except Exception as e:
        print(f"Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р Р† process_broadcast_chat_name: {str(e)}")

        await message.answer(f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {str(e)}")

        await state.clear()


@router.callback_query(F.data == "bc_chats_delete")
async def bc_chats_delete_callback(query: CallbackQuery, state: FSMContext):
    """Show broadcast chat removal UI with multi-delete and clear-all."""

    await query.answer()

    user_id = query.from_user.id

    chats = get_broadcast_chats(user_id)

    if not chats:
        text = "\U0001f6ab \u041d\u0435\u0442 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0443\u0434\u0430\u043b\u0435\u043d\u0438\u044f!"

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                        callback_data="close_bc_menu",
                    )
                ]
            ]
        )

        await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

        return

    await state.update_data(
        previous_menu="broadcast_chats", menu_message_id=query.message.message_id
    )

    await state.set_state(BroadcastConfigState.waiting_for_chat_delete)

    text = "\U0001f5d1\ufe0f <b>\u0423\u0414\u0410\u041b\u0415\u041d\u0418\u0415 \u0427\u0410\u0422\u041e\u0412</b>\n\n"

    for idx, (chat_id, chat_name) in enumerate(chats, 1):
        text += f"{idx}. {chat_name}\n"

    text += (
        f"\n\u0412\u0432\u0435\u0434\u0438 \u043d\u043e\u043c\u0435\u0440\u0430 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0443\u0434\u0430\u043b\u0435\u043d\u0438\u044f (\u043e\u0442 1 \u0434\u043e {len(chats)}).\n"
        "\u041c\u043e\u0436\u043d\u043e \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u043e \u0447\u0438\u0441\u0435\u043b \u0447\u0435\u0440\u0435\u0437 \u043f\u0440\u043e\u0431\u0435\u043b \u0438\u043b\u0438 \u0437\u0430\u043f\u044f\u0442\u0443\u044e, \u043d\u0430\u043f\u0440\u0438\u043c\u0435\u0440: 1 4"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="\U0001f9f9 \u041e\u0447\u0438\u0441\u0442\u0438\u0442\u044c \u0432\u0441\u0435",
                    callback_data="bc_chats_delete_all",
                )
            ],
            [
                InlineKeyboardButton(
                    text=CANCEL_TEXT,
                    callback_data="bc_cancel",
                )
            ],
        ]
    )

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "bc_chats_delete_all")
async def bc_chats_delete_all_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id

    chats = get_broadcast_chats(user_id)
    for chat_id, _ in chats:
        remove_broadcast_chat_with_profile(user_id, chat_id)

    await state.clear()
    await show_broadcast_chats_menu(
        query, user_id, menu_message_id=query.message.message_id
    )


@router.message(F.text == "\U0001f5d1\ufe0f \u0423\u0434\u0430\u043b\u0438\u0442\u044c")
async def delete_broadcast_chat_button(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С”Р Р…Р С•Р С—Р С”Р С‘ РЎС“Р Т‘Р В°Р В»Р ВµР Р…Р С‘РЎРЏ РЎвЂЎР В°РЎвЂљР В° Р С‘Р В· РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘ - Р РЋР СћР С’Р В Р В«Р в„ў Р С›Р вЂР В Р С’Р вЂР С›Р СћР В§Р ВР С™ (Р Р€Р вЂР В Р С’Р СћР В¬)"""

    # Р В­РЎвЂљР С•РЎвЂљ Р С•Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р В±Р С•Р В»РЎРЉРЎв‚¬Р Вµ Р Р…Р Вµ Р С‘РЎРѓР С—Р С•Р В»РЎРЉР В·РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ

    pass


@router.message(BroadcastConfigState.waiting_for_chat_delete)
async def process_delete_broadcast_chat(message: Message, state: FSMContext):
    """Delete one or many broadcast chats by numeric indexes."""

    user_id = message.from_user.id

    if message.text in {
        CANCEL_TEXT,
    }:
        await return_to_previous_menu(message, state)
        return

    data = await state.get_data()
    menu_message_id = data.get("menu_message_id")

    try:
        chats = get_broadcast_chats(user_id)

        if not chats:
            await state.clear()
            await show_broadcast_chats_menu(
                message, user_id, menu_message_id=menu_message_id
            )
            return

        raw = (message.text or "").replace(",", " ")
        tokens = [token for token in raw.split() if token]
        if not tokens:
            await message.answer(
                f"\u274c \u0412\u0432\u0435\u0434\u0438 \u043d\u043e\u043c\u0435\u0440\u0430 \u043e\u0442 1 \u0434\u043e {len(chats)}"
            )
            return

        indexes = []
        for token in tokens:
            value = int(token) - 1
            if value < 0 or value >= len(chats):
                await message.answer(
                    f"\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u0430 \u043e\u0442 1 \u0434\u043e {len(chats)}"
                )
                return
            indexes.append(value)

        for idx in sorted(set(indexes), reverse=True):
            chat_id, _ = chats[idx]
            remove_broadcast_chat_with_profile(user_id, chat_id)

        await state.clear()

        try:
            await message.delete()
        except Exception:
            pass

        await show_broadcast_chats_menu(
            message, user_id, menu_message_id=menu_message_id
        )

    except ValueError:
        await message.answer(
            "\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u0430 \u0447\u0435\u0440\u0435\u0437 \u043f\u0440\u043e\u0431\u0435\u043b \u0438\u043b\u0438 \u0437\u0430\u043f\u044f\u0442\u0443\u044e"
        )


async def return_to_previous_menu(message: Message, state: FSMContext):
    """\u0412\u043e\u0437\u0432\u0440\u0430\u0449\u0430\u0435\u0442 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f \u0432 \u043f\u0440\u0435\u0434\u044b\u0434\u0443\u0449\u0435\u0435 \u043c\u0435\u043d\u044e \u0431\u0435\u0437 \u043b\u0438\u0448\u043d\u0435\u0433\u043e \u0442\u0435\u043a\u0441\u0442\u0430."""

    data = await state.get_data()
    previous_menu = data.get("previous_menu", "broadcast")
    await state.clear()

    if previous_menu == "broadcast":
        await cmd_broadcast_menu(message)
        return

    if previous_menu == "broadcast_chats":
        await show_broadcast_chats_menu(
            message, message.from_user.id, menu_message_id=data.get("menu_message_id")
        )
        return

    await message.answer(
        "\u0413\u043b\u0430\u0432\u043d\u043e\u0435 \u043c\u0435\u043d\u044e",
        reply_markup=get_main_menu_keyboard(),
    )


# Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С”Р С•Р СР В°Р Р…Р Т‘РЎвЂ№ /se - РЎС“Р С—РЎР‚Р В°Р Р†Р В»Р ВµР Р…Р С‘Р Вµ Р Р†РЎРѓР ВµР СР С‘ РЎРѓР ВµРЎРѓРЎРѓР С‘РЎРЏР СР С‘
