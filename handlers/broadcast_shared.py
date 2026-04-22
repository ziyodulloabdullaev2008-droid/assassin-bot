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

from core.state import app_state

from database import (
    add_broadcast_chat,
    get_broadcast_chats,
    get_broadcast_chats_with_links,
    get_user_accounts,
    remove_broadcast_chat,
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
from services.broadcast_runtime_service import (
    account_label as _account_label,
    active_chat_counts as _active_chat_counts,
    add_broadcast_chat_with_profile,
    broadcast_chat_runtime_items as _broadcast_chat_runtime_items,
    broadcast_chat_short_name as _broadcast_chat_short_name,
    broadcast_chat_status_label as _broadcast_chat_status_label,
    estimate_broadcast_finish_timestamp as _estimate_broadcast_finish_timestamp,
    estimate_group_finish_timestamp as _estimate_group_finish_timestamp,
    estimate_group_next_send_timestamp as _estimate_group_next_send_timestamp,
    estimate_next_send_timestamp as _estimate_next_send_timestamp,
    find_chat_runtime_item as _find_chat_runtime_item,
    format_eta_duration as _format_eta_duration,
    format_chat_error_line as _format_chat_error_line,
    format_chat_error_log as _format_chat_error_log,
    rebalance_chat_targets as _rebalance_chat_targets,
    interval_unit_display as _interval_unit_display,
    interval_unit_label as _interval_unit_label,
    iter_connected_account_numbers as _iter_connected_account_numbers,
    remove_broadcast_chat_with_profile,
)

from services.broadcast_profiles_service import (
    ensure_active_config,
    get_active_config_id,
    get_config_detail,
    sync_active_config_from_db,
)
from services.channel_post_service import (
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
from telethon.utils import get_peer_id

from services.mention_utils import delete_message_after_delay

from ui.broadcast_ui import build_broadcast_keyboard, build_broadcast_menu_text
from ui.broadcast_texts import (
    build_text_list_info as _build_text_list_info,
    build_text_settings_info as _build_text_settings_info,
    CANCEL_TEXT,
    COUNT_BUTTON_TEXT,
    INTERVAL_BUTTON_TEXT,
    is_channel_source as _is_channel_source,
    LOGIN_REQUIRED_TEXT,
)

from ui.texts_ui import build_texts_keyboard, build_text_settings_keyboard

from ui.main_menu_ui import get_main_menu_keyboard
from handlers.broadcast_folder_import import (
    build_folder_account_picker as _build_folder_account_picker,
    build_folder_list_view as _build_folder_list_view,
    build_folder_preview_view as _build_folder_preview_view,
    folder_title as _folder_title,
    load_account_folders as _load_account_folders,
    load_folder_chats as _load_folder_chats,
    parse_folder_callback as _parse_folder_callback,
)

router = Router()

user_authenticated = app_state.user_authenticated

broadcast_update_lock = app_state.broadcast_update_lock

active_broadcasts = app_state.active_broadcasts
CHAT_PAUSE_MAX_SECONDS = 3600


def _broadcast_display_numbers(user_id: int) -> dict[int, int]:
    user_ids = sorted(
        bid
        for bid, broadcast in active_broadcasts.items()
        if broadcast.get("user_id") == user_id
        and broadcast.get("status") in ("running", "paused")
    )
    return {bid: index for index, bid in enumerate(user_ids, start=1)}


def _broadcast_chat_has_quota(item: dict) -> bool:
    target_count = max(int(item.get("target_count", 0) or 0), 0)
    attempts = int(item.get("sent_count", 0) or 0) + int(item.get("failed_count", 0) or 0)
    return attempts < target_count


def _next_runtime_revision(broadcast: dict) -> int:
    return int(broadcast.get("runtime_revision", 0) or 0) + 1


def _current_interval_unit(data: dict | None) -> str:
    return "seconds" if str((data or {}).get("interval_unit") or "minutes") == "seconds" else "minutes"


def _build_interval_input_text(current_value, interval_unit: str) -> str:
    unit_word = "\u0441\u0435\u043a\u0443\u043d\u0434\u0430\u0445" if interval_unit == "seconds" else "\u043c\u0438\u043d\u0443\u0442\u0430\u0445"
    unit_examples = "15 \u0438\u043b\u0438 10-30"
    unit_short = _interval_unit_display(interval_unit)
    max_value = 3600 if interval_unit == "seconds" else 480
    unit_suffix = "\u0441\u0435\u043a" if interval_unit == "seconds" else "\u043c\u0438\u043d"
    return (
        "\u23f1\ufe0f <b>\u0418\u041d\u0422\u0415\u0420\u0412\u0410\u041b \u0414\u041b\u042f \u041a\u0410\u0416\u0414\u041e\u0413\u041e \u0427\u0410\u0422\u0410</b>\n\n"
        f"\u0422\u0435\u043a\u0443\u0449\u0438\u0439: {current_value} {unit_short}\n\n"
        "\u041f\u043e\u0441\u043b\u0435 \u043a\u0430\u0436\u0434\u043e\u0439 \u043e\u0442\u043f\u0440\u0430\u0432\u043a\u0438 \u0431\u043e\u0442 \u0437\u0430\u043d\u043e\u0432\u043e \u0432\u044b\u0431\u0438\u0440\u0430\u0435\u0442 \u044d\u0442\u043e\u0442 \u0438\u043d\u0442\u0435\u0440\u0432\u0430\u043b "
        "\u0434\u043b\u044f \u043a\u043e\u043d\u043a\u0440\u0435\u0442\u043d\u043e\u0433\u043e \u0447\u0430\u0442\u0430.\n"
        f"\u0421\u0435\u0439\u0447\u0430\u0441 \u0432\u0432\u043e\u0434 \u0432 {unit_word}.\n"
        "\u041e\u0442\u043f\u0440\u0430\u0432\u044c \u043e\u0434\u043d\u043e \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d.\n"
        f"\u041f\u0440\u0438\u043c\u0435\u0440\u044b: <code>{unit_examples}</code>\n"
        f"\u041c\u0430\u043a\u0441\u0438\u043c\u0443\u043c: <code>{max_value}</code> {unit_suffix}"
    )


def _build_interval_input_keyboard(interval_unit: str, cancel_callback: str) -> InlineKeyboardMarkup:
    toggle_unit = "seconds" if interval_unit != "seconds" else "minutes"
    toggle_text = "\U0001f501 \u041f\u0435\u0440\u0435\u0432\u0435\u0441\u0442\u0438 \u0432 \u0441\u0435\u043a\u0443\u043d\u0434\u044b" if toggle_unit == "seconds" else "\U0001f501 \u041f\u0435\u0440\u0435\u0432\u0435\u0441\u0442\u0438 \u0432 \u043c\u0438\u043d\u0443\u0442\u044b"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=toggle_text,
                    callback_data=f"bc_interval_unit_{toggle_unit}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="\u2b05\ufe0f \u041e\u0442\u043c\u0435\u043d\u0438\u0442\u044c",
                    callback_data=cancel_callback,
                )
            ],
        ]
    )


def save_broadcast_config_with_profile(user_id: int, config: dict) -> None:

    ensure_active_config(user_id)

    save_broadcast_config(user_id, config)

    sync_active_config_from_db(user_id)

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

    fallback_numbers = []
    for acc_num, _, _, _, is_active in get_user_accounts(user_id):
        if acc_num in account_numbers or acc_num in fallback_numbers:
            continue
        if is_active:
            account_numbers.append(acc_num)
        else:
            fallback_numbers.append(acc_num)

    account_numbers.extend(fallback_numbers)
    if not account_numbers:
        raise RuntimeError("Нет доступных аккаунтов для загрузки постов канала")

    errors: list[str] = []
    for acc_num in account_numbers:
        client = await ensure_connected_client(
            user_id,
            acc_num,
            api_id=API_ID,
            api_hash=API_HASH,
        )
        if not client:
            errors.append(f"Аккаунт {acc_num}: не удалось подключить сессию")
            continue
        try:
            source_data = await fetch_channel_posts(client, normalized_ref)
            source_data["source_account"] = acc_num
            return source_data
        except Exception as exc:
            errors.append(f"Аккаунт {acc_num}: {str(exc)}")
            continue

    reason = errors[0] if errors else "нет аккаунтов с доступом к каналу"
    raise RuntimeError(
        "Не удалось загрузить посты канала ни с одного аккаунта. "
        "Проверь, что указана ссылка на сам канал, а не на отдельный пост, "
        f"и что хотя бы один аккаунт видит этот канал. Пример: {reason}"
    )

async def _ensure_account_ready(user_id: int, account_number: int):
    return await ensure_connected_client(
        user_id,
        account_number,
        api_id=API_ID,
        api_hash=API_HASH,
    )

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

    source_account = config.get("source_account")
    account_number = int(source_account) if str(source_account or "").isdigit() else None
    if account_number is None:
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

class BroadcastConfigState(StatesGroup):
    waiting_for_count = State()

    waiting_for_interval = State()

    waiting_for_chat_pause = State()

    waiting_for_text = State()
    waiting_for_source_channel = State()

    waiting_for_chat_id = State()  # cleaned comment

    waiting_for_chat_name = State()  # cleaned comment

    waiting_for_chat_delete = State()  # cleaned comment

    viewing_active_broadcast = State()  # cleaned comment

    waiting_for_text_add = State()  # cleaned comment

    waiting_for_text_edit = State()  # cleaned comment

class FakeMessage:
    """Helper class FakeMessage."""

    def __init__(self, user_id, query=None):

        self.from_user = type("obj", (object,), {"id": user_id})()

        self.query = query

    async def answer(self, text, **kwargs):
        """Handle answer."""

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
  # cleaned comment
            if "not modified" in str(e).lower():
                await self.query.answer("Уже открыто", show_alert=False)

            else:
                print(
                    f"Ошибка при обработке callback-сообщения: {str(e)}"
                )

async def show_broadcast_menu(message_or_query, user_id: int, is_edit: bool = False):
    """Handle show broadcast menu."""

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
    interval_units = {
        _interval_unit_label(broadcast.get("interval_unit"))
        for _, broadcast in items
    }
    interval_unit_text = ", ".join(sorted(interval_units)) if interval_units else _interval_unit_label("minutes")
    pause_text = ", ".join(sorted(pause_values)) if pause_values else "-"
    finish_ts = _estimate_group_finish_timestamp(items)
    next_send_ts = _estimate_group_next_send_timestamp(items)
    eta_text = _format_eta_duration(
        None
        if finish_ts is None
        else finish_ts - datetime.now(timezone.utc).timestamp()
    )

    info = f"\U0001f4e6 <b>\u0413\u0440\u0443\u043f\u043f\u0430 #{gid}</b>\n\n"
    info += f"\u25fc\ufe0f \u0421\u0442\u0430\u0442\u0443\u0441: {status}\n"
    info += f"\U0001f465 \u0410\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432: {total_accounts}\n"
    info += f"\U0001f4ad \u0427\u0430\u0442\u043e\u0432: {total_chats}\n"
    info += f"\U0001f522 \u041f\u043b\u0430\u043d: {total_count}\n"
    info += f"\U0001f4ec \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e: {sent}\n"
    info += f"\u26a0\ufe0f \u041e\u0448\u0438\u0431\u043e\u043a: {failed}\n"
    info += f"\u23f1\ufe0f \u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b: {html.escape(interval_text)} {html.escape(interval_unit_text)}\n"
    info += f"\u26a1\ufe0f \u0422\u0435\u043c\u043f: {html.escape(pause_text)} \u0441\u0435\u043a\n"
    if next_send_ts is not None:
        info += (
            f"\u23ed\ufe0f \u0421\u043b\u0435\u0434. \u0448\u0430\u0433 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438: "
            f"{_format_eta_duration(next_send_ts - datetime.now(timezone.utc).timestamp())}\n"
        )
    if finish_ts is not None:
        info += f"\u23f3 \u0414\u043e \u043a\u043e\u043d\u0446\u0430: {eta_text}\n"

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
    display_number = _broadcast_display_numbers(user_id).get(bid, bid)
    now_ts = datetime.now(timezone.utc).timestamp()
    lines = [f"\U0001f4dd <b>\u0427\u0430\u0442\u044b \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438 #{display_number}</b>", ""]
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
            target = int(item.get("target_count", 0) or 0)
            next_send_at = float(item.get("next_send_at", 0.0) or 0.0)
            eta_text = (
                _format_eta_duration(max(next_send_at - now_ts, 0.0))
                if next_send_at > 0 and str(item.get("status") or "active") == "active"
                else "-"
            )
            lines.append(
                f"{number}. {_broadcast_chat_status_label(item)} | {name} | "
                f"\U0001f4ec {sent}/{target} | \u274c {failed} | \u23f1\ufe0f {eta_text}"
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
        (
            f"\u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e: "
            f"{int(item.get('sent_count', 0) or 0)}/{int(item.get('target_count', 0) or 0)}"
        ),
        f"\u041e\u0448\u0438\u0431\u043e\u043a: {int(item.get('failed_count', 0) or 0)}",
    ]

    next_send_at = float(item.get("next_send_at", 0.0) or 0.0)
    if next_send_at > 0:
        eta = max(0, int(next_send_at - datetime.now(timezone.utc).timestamp()))
        info.append(f"\u041f\u043e\u0432\u0442\u043e\u0440 \u0432 \u044d\u0442\u043e\u0442 \u0447\u0430\u0442: \u0447\u0435\u0440\u0435\u0437 {eta} \u0441\u0435\u043a")

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
    now_ts = datetime.now(timezone.utc).timestamp()
    for item in items:
        item_order = item.get("order", -1)
        if int(item_order if item_order is not None else -1) != order:
            continue
        previous_status = str(item.get("status") or "active")
        item["status"] = status
        found = True
        if status == "active" and previous_status != "active":
            item["next_send_at"] = now_ts
        break

    if not found:
        return False

    items = _rebalance_chat_targets(items, int(broadcast.get("count", 0) or 0))
    await update_broadcast_fields(
        bid,
        chat_runtime=items,
        runtime_revision=_next_runtime_revision(broadcast),
    )
    return True


async def _set_broadcast_chat_status_by_chat_id(
    user_id: int,
    bid: int,
    chat_id,
    status: str,
) -> bool:
    broadcast = active_broadcasts.get(bid)
    if not broadcast or broadcast.get("user_id") != user_id:
        return False

    items = _broadcast_chat_runtime_items(broadcast)
    found = False
    chat_id_text = str(chat_id)
    now_ts = datetime.now(timezone.utc).timestamp()
    for item in items:
        if str(item.get("chat_id")) != chat_id_text:
            continue
        previous_status = str(item.get("status") or "active")
        item["status"] = status
        found = True
        if status == "active" and previous_status != "active":
            item["next_send_at"] = now_ts
        break

    if not found:
        return False

    items = _rebalance_chat_targets(items, int(broadcast.get("count", 0) or 0))
    await update_broadcast_fields(
        bid,
        chat_runtime=items,
        runtime_revision=_next_runtime_revision(broadcast),
    )
    return True

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
    chat_link_map = {
        int(chat_id): chat_link
        for chat_id, _chat_name, chat_link in get_broadcast_chats_with_links(user_id)
    }
    broadcast_id = next_broadcast_id()

    account_name = None
    for acc_num, telegram_id, username, first_name, is_active in get_user_accounts(user_id):
        if acc_num == account_number:
            account_name = first_name or username or f"Аккаунт {acc_num}"
            break

    source_type = config.get("text_source_type", "manual")
    runtime_config = dict(config)
    active_config_id = get_active_config_id(user_id)
    config_detail = get_config_detail(user_id, active_config_id) if active_config_id is not None else None
    config_name = (
        str((config_detail or {}).get("name") or "")
        if config_detail
        else ("По умолчанию" if active_config_id == 0 else "")
    )
    if not config_name:
        config_name = "По умолчанию" if active_config_id == 0 else f"Конфиг {active_config_id}"
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
        "config_id": active_config_id,
        "config_name": config_name,
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
        "interval_unit": str(runtime_config.get("interval_unit") or "minutes"),
        "start_time": datetime.now(timezone.utc),
        "status": "running",
        "failed_count": 0,
        "processed_count": 0,
        "chat_runtime": [
            {
                "chat_id": chat_id,
                "name": chat_name,
                "chat_link": chat_link_map.get(int(chat_id)),
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
        "runtime_revision": 0,
        "text_index": 0,
    }

    if group_id is not None:
        payload["group_id"] = group_id

    create_broadcast(broadcast_id, payload)
    _start_or_resume_broadcast_task(broadcast_id)
    display_number = _broadcast_display_numbers(user_id).get(broadcast_id, 1)

    await _send_broadcast_notice(
        message_or_query,
        (
            "✅ Рассылка запущена\n\n"
            f"Активная: #{display_number}\n"
            f"Аккаунт: {payload['account_name']}\n"
            f"Конфиг: {payload['config_name']}\n"
            f"Чатов: {len(chat_ids)}\n"
            f"Сообщений: {payload['planned_count']}"
        ),
    )

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


__all__ = [name for name in globals() if not name.startswith('__')]

