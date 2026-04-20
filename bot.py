# -*- coding: utf-8 -*-

import asyncio

import html

import sys

from datetime import datetime

from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramUnauthorizedError

from aiogram.types import (
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    Update,
)

from aiogram.dispatcher.middlewares.base import BaseMiddleware

from aiogram.filters.command import Command

from aiogram.fsm.context import FSMContext

from aiogram.fsm.state import State, StatesGroup

from telethon import TelegramClient

from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError

from core.state import app_state

from core.config import TOKEN, ADMIN_ID, API_ID, API_HASH, DEV_LOG_CHAT_ID

from core.logging import (
    setup_logging,
    start_telegram_log_forwarding,
    stop_telegram_log_forwarding,
)

from services.proxy_service import (
    build_session_candidates,
    build_telegram_client,
    format_proxy_summary,
    parse_proxy_input,
    test_session_proxy,
)
from services.session_service import (
    drop_cached_client,
    ensure_connected_client,
    load_saved_sessions,
)
from services.user_paths import (
    session_base_path,
    temp_session_base_path,
)

from services.mention_service import (
    start_mention_monitoring as start_mention_monitoring_service,
    stop_mention_monitoring,
)

from services.mention_utils import normalize_chat_id
from services.operation_guard_service import (
    end_operation,
    get_active_operation,
    try_begin_operation,
)
from services.account_events_service import (
    append_account_event,
    format_recent_account_events,
    get_recent_account_events,
)

from services.broadcast_config_service import load_broadcast_configs
from services.broadcast_service import delete_broadcast, load_persisted_broadcasts

from ui.main_menu_ui import get_main_menu_keyboard

from handlers.basic_handlers import router as basic_router

from handlers.vip_handlers import router as vip_router

from handlers.account_handlers import router as account_router

from handlers.config_handlers import router as config_router

from handlers.broadcast_handlers import router as broadcast_router
from handlers.joins_handlers import router as joins_router

from services.vip_service import get_vip_cache_size, is_vip_user_cached, update_vip_cache

from database import (
    add_user_account_with_number,
    get_user_account_created_at,
    init_db,
    add_or_update_user,
    get_account_proxy_check_result,
    clear_account_proxy,
    get_account_proxy,
    save_account_proxy_check_result,
    set_user_logged_in,
    start_login_session,
    get_login_session,
    update_login_step,
    save_phone_number,
    delete_login_session,
    add_user_account,
    get_user_accounts,
    set_account_proxy,
    get_tracked_chats,
    get_broadcast_chats,
    get_vip_session_limit,
)

# Ensure console can print Unicode logs (emoji/cyrillic) on Windows terminals.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# Create bot and dispatcher.

bot = Bot(token=TOKEN)

dp = Dispatcher()

init_db()

user_authenticated_lock = app_state.user_authenticated_lock

user_clients = app_state.user_clients

user_hashes = app_state.user_hashes

user_code_input = app_state.user_code_input

user_authenticated = app_state.user_authenticated

user_last_dialogs = app_state.user_last_dialogs

user_chats_files = app_state.user_chats_files

active_broadcasts = app_state.active_broadcasts

ACTIVE_BROADCAST_GUARD_TEXT = (
    "❌ Сейчас у тебя идет активная рассылка.\n\n"
    "Сначала останови её в разделе активных рассылок, а уже потом меняй "
    "сессии, логин, прокси или состояние аккаунтов."
)
ACTIVE_OPERATION_GUARD_TEXT = (
    "⏳ Сейчас уже выполняется другая операция.\n\n"
    "Дождись её завершения и попробуй ещё раз."
)

mention_monitors = app_state.mention_monitors


async def retry_with_backoff(async_func, max_retries=3, base_delay=0.5):
    """Выполнить async функцию с retry при database locked ошибке"""

    for attempt in range(max_retries):
        try:
            return await async_func()

        except Exception as e:
            error_str = str(e).lower()

            if (
                "database is locked" in error_str
                or "database locked" in error_str
                or "connection" in error_str
                and "reset" in error_str
            ):
                if attempt < max_retries - 1:
                    delay = base_delay * (2**attempt)

                    print(
                        f"⚠️  Database locked, retry {attempt + 1}/{max_retries} через {delay}s..."
                    )

                    await asyncio.sleep(delay)

                    continue

            raise


def cleanup_user_session(user_id: int, account_number: int = None):
    """Очистить данные пользователя из памяти при logout"""

    if user_id in user_hashes:
        del user_hashes[user_id]

    if user_id in user_code_input:
        del user_code_input[user_id]

    if user_id in user_last_dialogs:
        del user_last_dialogs[user_id]

    if user_id in user_chats_files:
        if account_number is None:
            del user_chats_files[user_id]

        elif account_number in user_chats_files[user_id]:
            del user_chats_files[user_id][account_number]

    completed_ids = [
        bid
        for bid, b in active_broadcasts.items()
        if b["user_id"] == user_id
        and b["status"] in ("completed", "error", "cancelled")
    ]

    for bid in completed_ids:
        delete_broadcast(bid)

    now = datetime.now().timestamp()

    old_denial_users = [
        uid for uid, ts in vip_denial_messages.items() if now - ts > 3600
    ]

    for uid in old_denial_users:
        del vip_denial_messages[uid]

    stop_mention_monitoring(user_id, account_number)


def _format_account_added_age(user_id: int, account_number: int) -> str:
    created_at = get_user_account_created_at(user_id, account_number)
    if created_at is None:
        return "\u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u043e"

    created_at_dt = datetime.fromtimestamp(float(created_at))
    age_days = max((datetime.now() - created_at_dt).days, 0)
    if age_days == 0:
        return "\u0441\u0435\u0433\u043e\u0434\u043d\u044f"
    return f"{age_days} \u0434\u043d."


async def start_mention_monitoring(user_id: int):
    """Запускает мониторинг упоминаний для всех аккаунтов пользователя"""

    await start_mention_monitoring_service(
        bot,
        user_id,
        get_tracked_chats=get_tracked_chats,
        get_broadcast_chats=get_broadcast_chats,
        get_user_accounts=get_user_accounts,
        normalize_chat_id=normalize_chat_id,
        InlineKeyboardButton=InlineKeyboardButton,
        InlineKeyboardMarkup=InlineKeyboardMarkup,
    )


class LoginStates(StatesGroup):
    waiting_proxy_choice = State()

    waiting_proxy_input = State()

    waiting_phone = State()

    waiting_code = State()

    waiting_password = State()

    adding_chat = State()


class DeleteChatState(StatesGroup):
    waiting_for_number = State()


class ProxyStates(StatesGroup):
    waiting_proxy_value = State()


vip_denial_messages = {}
subscription_check_cache = {}


def _has_running_broadcasts(user_id: int) -> bool:
    return any(
        broadcast.get("user_id") == user_id and broadcast.get("status") == "running"
        for broadcast in active_broadcasts.values()
    )


async def _guard_user_operation(target) -> bool:
    user = getattr(target, "from_user", None)
    user_id = getattr(user, "id", None)
    if not user_id:
        return True

    active_operation = get_active_operation(user_id)
    if not active_operation:
        return True

    text = ACTIVE_OPERATION_GUARD_TEXT
    if isinstance(target, CallbackQuery):
        try:
            await target.answer(text, show_alert=True)
        except Exception:
            try:
                await target.message.answer(text)
            except Exception:
                pass
        return False

    try:
        await target.answer(text)
    except Exception:
        pass
    return False


async def _guard_running_broadcast(target) -> bool:
    user = getattr(target, "from_user", None)
    user_id = getattr(user, "id", None)
    if not user_id:
        return True
    if not _has_running_broadcasts(user_id):
        return True

    text = ACTIVE_BROADCAST_GUARD_TEXT
    if isinstance(target, CallbackQuery):
        try:
            await target.answer(text, show_alert=True)
        except Exception:
            try:
                await target.message.answer(text)
            except Exception:
                pass
        return False

    try:
        await target.answer(text)
    except Exception:
        pass
    return False


async def _guard_broadcast_sensitive_action(target) -> bool:
    if not await _guard_user_operation(target):
        return False
    return await _guard_running_broadcast(target)
REQUIRED_CHANNELS = [
    {
        "chat_id": "@stryxxss",
        "title": "stryxxss",
        "url": "https://t.me/stryxxss",
    },
    {
        "chat_id": "@stryxxs",
        "title": "stryxxs",
        "url": "https://t.me/stryxxs",
    },
    {
        "chat_id": "@otzivstryxxs",
        "title": "otzivstryxxs",
        "url": "https://t.me/otzivstryxxs",
    },
    {
        "chat_id": "@stryxxsuz",
        "title": "stryxxsuz",
        "url": "https://t.me/stryxxsuz",
    },
    {
        "chat_id": "@chatxso",
        "title": "chatxso",
        "url": "https://t.me/chatxso",
    },
]


def build_required_subscription_keyboard(
    channels: list[dict] | None = None,
) -> InlineKeyboardMarkup:
    channels = channels if channels is not None else REQUIRED_CHANNELS
    rows = [
        [
            InlineKeyboardButton(
                text=f"📢 {channel['title']}",
                url=channel["url"],
            )
        ]
        for channel in channels
    ]
    rows.append(
        [
            InlineKeyboardButton(
                text="✅ Проверить подписку",
                callback_data="check_required_subscription",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_required_subscription_text(missing_titles: list[str] | None = None) -> str:
    channels_count = len(missing_titles) if missing_titles else len(REQUIRED_CHANNELS)
    text = (
        "🔒 <b>Для работы с ботом нужна подписка</b>\n\n"
        f"Подпишись на недостающие каналы/чаты ({channels_count}) "
        "и нажми кнопку проверки."
    )
    if missing_titles:
        text += "\n\nНе хватает подписки на:\n" + "\n".join(
            f"• {title}" for title in missing_titles
        )
    return text


async def get_missing_required_channels(bot: Bot, user_id: int) -> list[dict]:
    missing = []
    for channel in REQUIRED_CHANNELS:
        try:
            member = await bot.get_chat_member(channel["chat_id"], user_id)
            status = getattr(member, "status", "")
            if status in {"left", "kicked"}:
                missing.append(channel)
        except Exception:
            missing.append(channel)
    return missing


def _has_fresh_subscription_cache(user_id: int, ttl_seconds: int = 300) -> bool:
    cached_at = float(subscription_check_cache.get(user_id, 0.0) or 0.0)
    if cached_at <= 0:
        return False
    return (datetime.now().timestamp() - cached_at) <= ttl_seconds


class PrivateOnlyMiddleware(BaseMiddleware):
    """Блокирует обработку апдейтов вне личных чатов."""

    async def __call__(self, handler, event: Update, data: dict):
        if event.message:
            chat = event.message.chat
            if chat and getattr(chat, "type", None) != "private":
                return
        elif event.callback_query:
            msg = event.callback_query.message
            if not msg:
                return
            chat = msg.chat
            if chat and getattr(chat, "type", None) != "private":
                return
        return await handler(event, data)


class VIPCheckMiddleware(BaseMiddleware):
    """Middleware для проверки VIP статуса пользователя"""

    PUBLIC_COMMANDS = {"/start", "/help", "/restart", "/logout"}

    VIP_ONLY_COMMANDS = {
        "/login",
        "/proxy",
        "/health",
        "/menu",
        "/sa",
        "/se",
        "/broadcast",
        "/settings",
        "/config",
        "/chats",
    }

    VIP_ONLY_TEXT_MARKERS = {
        "рассылка",
        "мой аккаунт",
    }

    PUBLIC_CALLBACKS = {
        "check_required_subscription",
        "bc_launch",
        "bc_pause",
        "bc_resume",
        "bc_cancel_broadcast",
        "start_broadcast_with_account",
        "monitor_start",
        "monitor_stop",
        "monitor_toggle",
    }

    async def __call__(self, handler, event: Update, data: dict):
        """Проверяет VIP статус перед обработкой сообщения или callback'а"""

        user_id = None

        if event.message:
            user_id = event.message.from_user.id

            message = event.message

            if message.text:
                text_lower = message.text.lower()

                for cmd in self.PUBLIC_COMMANDS:
                    if text_lower.startswith(cmd):
                        return await handler(event, data)

            if user_id and not is_vip_user_cached(user_id) and user_id != ADMIN_ID:
                if message.text:
                    text_lower = message.text.lower()
                    restricted = any(
                        text_lower.startswith(cmd) for cmd in self.VIP_ONLY_COMMANDS
                    )
                    if not restricted:
                        restricted = any(
                            marker in text_lower
                            for marker in self.VIP_ONLY_TEXT_MARKERS
                        )

                    if restricted:
                        now = datetime.now().timestamp()
                        last_denial = vip_denial_messages.get(user_id, 0)

                        if now - last_denial > 5:
                            try:
                                await message.answer(
                                    "❌ Доступ ограничен.\n\n"
                                    "Для получения доступа обратитесь к @stryyx"
                                )
                            except Exception as e:
                                print(
                                    f"⚠️ Ошибка при отправке сообщения об отказе в доступе: {str(e)}"
                                )

                            vip_denial_messages[user_id] = now

                        return

                return await handler(event, data)

        elif event.callback_query:
            user_id = event.callback_query.from_user.id

            callback_data = event.callback_query.data

            if user_id and not is_vip_user_cached(user_id) and user_id != ADMIN_ID:
                is_public = False

                for public_cb in self.PUBLIC_CALLBACKS:
                    if callback_data.startswith(public_cb):
                        is_public = True

                        break

                if not is_public:
                    try:
                        await event.callback_query.answer(
                            "❌ Доступ ограничен. Для получения доступа обратитесь к @stryyx",
                            show_alert=True,
                        )

                    except Exception as e:
                        print(f"⚠️ Ошибка при ответе на callback: {str(e)}")

                    return

        return await handler(event, data)


class SubscriptionRequiredMiddleware(BaseMiddleware):
    """Blocks bot usage until the user is subscribed to both required channels."""

    PUBLIC_CALLBACKS = {"check_required_subscription"}

    async def __call__(self, handler, event: Update, data: dict):
        user_id = None
        message = None
        callback_query = None

        if event.message:
            message = event.message
            user_id = message.from_user.id
        elif event.callback_query:
            callback_query = event.callback_query
            user_id = callback_query.from_user.id
            if callback_query.data and any(
                callback_query.data.startswith(cb) for cb in self.PUBLIC_CALLBACKS
            ):
                return await handler(event, data)
        else:
            return await handler(event, data)

        if user_id in {ADMIN_ID, 777000}:
            return await handler(event, data)

        if _has_fresh_subscription_cache(user_id):
            return await handler(event, data)

        missing_channels = await get_missing_required_channels(bot, user_id)
        if not missing_channels:
            subscription_check_cache[user_id] = datetime.now().timestamp()
            return await handler(event, data)

        subscription_check_cache.pop(user_id, None)

        missing_titles = [channel["title"] for channel in missing_channels]
        text = build_required_subscription_text(missing_titles)
        kb = build_required_subscription_keyboard(missing_channels)

        if message:
            await message.answer(text, reply_markup=kb, parse_mode="HTML")
            return

        if callback_query:
            await callback_query.answer(
                "Сначала подпишись на оба канала",
                show_alert=True,
            )
            try:
                await callback_query.message.answer(text, reply_markup=kb, parse_mode="HTML")
            except Exception:
                pass
            return

        return await handler(event, data)


dp.update.outer_middleware(PrivateOnlyMiddleware())
dp.update.outer_middleware(VIPCheckMiddleware())
dp.update.outer_middleware(SubscriptionRequiredMiddleware())

LOGIN_CANCEL_TEXT = "↩️ Отменить действие"


def _build_login_cancel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=LOGIN_CANCEL_TEXT)]],
        resize_keyboard=True,
    )


def _build_login_proxy_choice_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🌐 Добавить прокси", callback_data="login_proxy_yes"
                ),
                InlineKeyboardButton(
                    text="⏭️ Без прокси", callback_data="login_proxy_skip"
                ),
            ],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="login_proxy_cancel")],
        ]
    )


def _build_proxy_accounts_keyboard(accounts) -> InlineKeyboardMarkup:
    keyboard = []
    for account_number, _, username, first_name, is_active in accounts:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=_format_account_list_label(
                        account_number,
                        username,
                        first_name,
                        bool(is_active),
                    ),
                    callback_data=f"proxy_account_{account_number}",
                )
            ]
        )
    keyboard.append([InlineKeyboardButton(text="⬅️ Закрыть", callback_data="close_proxy_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def _build_proxy_account_keyboard(
    account_number: int,
    has_proxy: bool,
    source: str = "proxy",
) -> InlineKeyboardMarkup:
    if source == "se":
        menu_callback = f"se_proxy_menu_{account_number}"
        test_callback = f"se_proxy_test_{account_number}"
        reconnect_callback = f"se_proxy_reconnect_{account_number}"
        set_callback = f"se_proxy_set_{account_number}"
        delete_callback = f"se_proxy_delete_{account_number}"
        back_callback = f"se_account_{account_number}"
    else:
        menu_callback = f"proxy_account_{account_number}"
        test_callback = f"proxy_test_{account_number}"
        reconnect_callback = f"proxy_reconnect_{account_number}"
        set_callback = f"proxy_set_{account_number}"
        delete_callback = f"proxy_delete_{account_number}"
        back_callback = "proxy_back_accounts"

    keyboard = [
        [
            InlineKeyboardButton(
                text="📡 Проверить", callback_data=test_callback
            ),
            InlineKeyboardButton(
                text="🔌 Переподключить", callback_data=reconnect_callback
            ),
        ],
        [
            InlineKeyboardButton(
                text="✏️ Изменить", callback_data=set_callback
            ),
        ]
    ]
    if has_proxy:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text="🗑 Удалить", callback_data=delete_callback
                )
            ]
        )
    keyboard.append(
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)]
    )
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def _build_proxy_delete_confirm_keyboard(
    account_number: int,
    source: str = "proxy",
) -> InlineKeyboardMarkup:
    confirm_callback = (
        f"se_proxy_remove_confirm_{account_number}"
        if source == "se"
        else f"proxy_remove_confirm_{account_number}"
    )
    back_callback = (
        f"se_proxy_menu_{account_number}"
        if source == "se"
        else f"proxy_account_{account_number}"
    )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, удалить", callback_data=confirm_callback),
                InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback),
            ]
        ]
    )


def _build_proxy_input_cancel_keyboard(
    account_number: int,
    source: str = "proxy",
) -> InlineKeyboardMarkup:
    cancel_callback = (
        f"se_proxy_menu_{account_number}"
        if source == "se"
        else f"proxy_account_{account_number}"
    )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="❌ Отмена", callback_data=cancel_callback
                )
            ]
        ]
    )


async def _send_phone_prompt(target_message):
    guide_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🎥 Видеогайд", url="https://youtu.be/YS8nkKR7C38?si=VKu6-6qsSBFOJwAB"
                )
            ]
        ]
    )

    await target_message.answer(
        "🔐 <b>ВХОД В АККАУНТ</b>\n\n"
        "Для работы бота нужно дать доступ к твоему Telegram аккаунту.\n\n"
        "📱 <b>Введи свой номер телефона в формате:</b>\n"
        "+7XXXXXXXXXX или +1XXXXXXXXX",
        parse_mode="HTML",
        reply_markup=_build_login_cancel_keyboard(),
    )

    await target_message.answer(
        "Если нужна помощь по входу, открой видеоинструкцию:",
        reply_markup=guide_keyboard,
    )


def _get_user_session_limit_status(user_id: int) -> tuple[int | None, int, bool]:
    session_limit = get_vip_session_limit(user_id)
    current_accounts = len(get_user_accounts(user_id))
    limit_reached = (
        session_limit is not None
        and session_limit > 0
        and current_accounts >= session_limit
    )
    return session_limit, current_accounts, limit_reached


async def _notify_session_limit_reached(target_message: Message, user_id: int) -> bool:
    session_limit, current_accounts, limit_reached = _get_user_session_limit_status(user_id)
    if not limit_reached:
        return False

    await target_message.answer(
        "❌ Лимит сессий исчерпан.\n\n"
        f"Сейчас подключено: <b>{current_accounts}</b>\n"
        f"Лимит: <b>{session_limit}</b>\n\n"
        "Удалите лишний аккаунт или увеличьте лимит через VIP-настройки.",
        parse_mode="HTML",
    )
    return True


async def _start_login_with_optional_proxy(
    target_message: Message,
    state: FSMContext,
    user_id: int,
    username: str | None,
    first_name: str | None,
):
    if await _notify_session_limit_reached(target_message, user_id):
        await state.clear()
        return

    add_or_update_user(user_id, username or "unknown", first_name)
    await state.clear()
    await state.set_state(LoginStates.waiting_proxy_choice)
    await target_message.answer(
        "🌐 <b>Прокси перед входом</b>\n\n"
        "Если хочешь, можешь сразу задать прокси для этого аккаунта.\n"
        "Поддерживаются:\n"
        "• <code>host:port</code>\n"
        "• <code>host:port:login:password</code>\n"
        "• <code>host:port:secret</code>\n"
        "• ссылка <code>t.me/proxy?server=...</code>",
        parse_mode="HTML",
        reply_markup=_build_login_proxy_choice_keyboard(),
    )


def _format_relative_datetime(timestamp: float | None) -> str:
    if not timestamp:
        return "не было"
    try:
        dt = datetime.fromtimestamp(float(timestamp)).astimezone()
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "неизвестно"


def _format_proxy_check_block(check_result: dict | None) -> str:
    if not check_result:
        return "Проверка: не запускалась"

    status = check_result.get("ok")
    status_text = (
        "успешно"
        if status is True
        else ("ошибка" if status is False else "неизвестно")
    )
    ping_ms = check_result.get("ping_ms")
    ping_text = f"{ping_ms} ms" if ping_ms is not None else "-"
    error_text = str(check_result.get("error") or "-")
    success_at = _format_relative_datetime(check_result.get("success_at"))
    checked_at = _format_relative_datetime(check_result.get("checked_at"))
    return (
        f"Проверка: {status_text}\n"
        f"Последняя проверка: {checked_at}\n"
        f"Последний успех: {success_at}\n"
        f"Пинг: {ping_text}\n"
        f"Ошибка: {error_text}"
    )


def _build_proxy_account_text(account_number: int, proxy_settings, check_result=None) -> str:
    status = "Настроен" if proxy_settings else "Не задан"
    check_block = html.escape(_format_proxy_check_block(check_result))
    proxy_block = html.escape(format_proxy_summary(proxy_settings))
    return (
        f"🌐 <b>Прокси аккаунта {account_number}</b>\n\n"
        f"Статус: <b>{status}</b>\n\n"
        f"<code>{proxy_block}</code>\n\n"
        f"<code>{check_block}</code>"
    )


def _build_empty_sessions_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="➕ Добавить аккаунт", callback_data="add_new_account"
                )
            ],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_sessions_menu")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="close_sessions_menu")],
        ]
    )


def _format_account_list_label(
    account_number: int,
    username: str,
    first_name: str,
    is_active: bool,
) -> str:
    label = f"{'🟢' if is_active else '🔴'} {account_number}"
    if username:
        label += f" • @{str(username)[:20]}"
    elif first_name:
        label += f" • {str(first_name)[:20]}"
    else:
        label += " • Без ника"
    return label


def _build_sessions_accounts_keyboard(accounts) -> InlineKeyboardMarkup:
    keyboard = []
    for account_number, _, username, first_name, is_active in accounts:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=_format_account_list_label(
                        account_number,
                        username,
                        first_name,
                        bool(is_active),
                    ),
                    callback_data=f"se_account_{account_number}",
                )
            ]
        )

    keyboard.extend(
        [
            [InlineKeyboardButton(text="➕ Добавить аккаунт", callback_data="add_new_account")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_sessions_menu")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="close_sessions_menu")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def _build_sessions_list_text(accounts) -> str:
    return (
        "📱 <b>АККАУНТЫ</b>\n\n"
        "Выбери аккаунт из списка ниже.\n"
        f"Всего аккаунтов: <b>{len(accounts)}</b>"
    )


async def _sync_account_profiles(
    user_id: int,
    account_numbers: list[int] | None = None,
) -> tuple[int, int]:
    accounts = get_user_accounts(user_id)
    if account_numbers is not None:
        allowed = set(account_numbers)
        accounts = [account for account in accounts if account[0] in allowed]

    synced = 0
    total = 0

    for account_number, _, _, _, _ in accounts:
        total += 1
        client = None
        disconnect_after = False

        try:
            existing = user_authenticated.get(user_id, {}).get(account_number)
            if existing:
                try:
                    if existing.is_connected():
                        client = existing
                except Exception:
                    client = None

            if client is None:
                candidates = build_session_candidates(user_id, account_number)
                if not candidates:
                    continue
                client = build_telegram_client(
                    candidates[0],
                    API_ID,
                    API_HASH,
                    get_account_proxy(user_id, account_number),
                )
                disconnect_after = True
                await asyncio.wait_for(client.connect(), timeout=8.0)
                if not await client.is_user_authorized():
                    continue

            me = await asyncio.wait_for(client.get_me(), timeout=8.0)
            if not me:
                continue

            add_user_account_with_number(
                user_id,
                account_number,
                me.id,
                me.username or "",
                me.first_name or "User",
                me.phone or "",
            )
            synced += 1
        except Exception:
            continue
        finally:
            if disconnect_after and client:
                try:
                    await client.disconnect()
                except Exception:
                    pass

    return synced, total


async def _get_account_health_snapshot(user_id: int, account_number: int) -> dict:
    accounts = get_user_accounts(user_id)
    account_info = next((acc for acc in accounts if acc[0] == account_number), None)
    if not account_info:
        return {
            "exists": False,
            "session_exists": False,
            "proxy_settings": None,
            "proxy_check": None,
            "authorized": False,
            "connected": False,
        }

    session_candidates = build_session_candidates(user_id, account_number)
    session_exists = bool(session_candidates)
    proxy_settings = get_account_proxy(user_id, account_number)
    proxy_check = get_account_proxy_check_result(user_id, account_number)

    client = None
    authorized = False
    connected = False
    try:
        client = await ensure_connected_client(
            user_id,
            account_number,
            api_id=API_ID,
            api_hash=API_HASH,
        )
        if client:
            connected = bool(client.is_connected())
            authorized = bool(await client.is_user_authorized())
    except Exception:
        pass

    return {
        "exists": True,
        "session_exists": session_exists,
        "proxy_settings": proxy_settings,
        "proxy_check": proxy_check,
        "authorized": authorized,
        "connected": connected,
    }


def _build_account_health_summary_lines(health_snapshot: dict) -> list[str]:
    session_exists = bool(health_snapshot.get("session_exists"))
    proxy_settings = health_snapshot.get("proxy_settings")
    proxy_check = health_snapshot.get("proxy_check")
    authorized = bool(health_snapshot.get("authorized"))
    connected = bool(health_snapshot.get("connected"))

    proxy_check_status = "не запускалась"
    if proxy_check:
        if proxy_check.get("ok") is True:
            proxy_check_status = "успешно"
        elif proxy_check.get("ok") is False:
            proxy_check_status = "ошибка"

    return [
        "<b>Health</b>",
        f"Сессия: <b>{'есть' if session_exists else 'нет'}</b>",
        f"Авторизация: <b>{'ok' if authorized else 'нет'}</b>",
        f"Подключение: <b>{'активно' if connected else 'lazy'}</b>",
        f"Прокси: <b>{'есть' if proxy_settings else 'нет'}</b>",
        f"Проверка прокси: <b>{proxy_check_status}</b>",
    ]


async def _build_session_account_detail_text(user_id: int, account_number: int) -> str:
    accounts = get_user_accounts(user_id)
    account_info = next((acc for acc in accounts if acc[0] == account_number), None)
    if not account_info:
        return "❌ Аккаунт не найден"

    _, telegram_id, username, first_name, is_active = account_info
    health_snapshot = await _get_account_health_snapshot(user_id, account_number)
    session_exists = bool(health_snapshot.get("session_exists"))
    proxy_settings = health_snapshot.get("proxy_settings")
    proxy_check = health_snapshot.get("proxy_check")
    client_connected = bool(health_snapshot.get("connected"))

    if client_connected:
        connection_text = "подключен"
    elif session_exists:
        connection_text = "готов к запуску"
    else:
        connection_text = "сессия не найдена"

    proxy_status = "есть" if proxy_settings else "нет"
    proxy_check_status = "не запускалась"
    if proxy_check:
        if proxy_check.get("ok") is True:
            proxy_check_status = "успешно"
        elif proxy_check.get("ok") is False:
            proxy_check_status = "ошибка"

    active_for_account = sum(
        1
        for broadcast in active_broadcasts.values()
        if broadcast.get("user_id") == user_id
        and broadcast.get("account") == account_number
        and broadcast.get("status") in ("running", "paused")
    )

    lines = [
        f"📌 <b>Аккаунт {account_number}</b>",
        "",
        f"Имя: <b>{html.escape(str(first_name or 'Неизвестно'))}</b>",
        (
            f"Username: <code>@{html.escape(str(username))}</code>"
            if username
            else "Username: -"
        ),
        f"Telegram ID: <code>{telegram_id}</code>",
        f"Статус: <b>{'в работе' if is_active else 'выключен'}</b>",
        f"Подключение: <b>{connection_text}</b>",
        f"Сессия в боте: <b>{_format_account_added_age(user_id, account_number)}</b>",
        f"Прокси: <b>{proxy_status}</b>",
        f"Проверка прокси: <b>{proxy_check_status}</b>",
        f"Активных рассылок: <b>{active_for_account}</b>",
    ]

    lines.extend([""] + _build_account_health_summary_lines(health_snapshot))

    if proxy_settings:
        lines.extend(
            [
                "",
                "<b>Прокси</b>",
                f"<code>{html.escape(format_proxy_summary(proxy_settings))}</code>",
            ]
        )

    recent_events = format_recent_account_events(
        get_recent_account_events(user_id, account_number=account_number, limit=4)
    )
    if recent_events:
        lines.extend(["", "<b>Последние события</b>"])
        lines.extend([html.escape(event_line) for event_line in recent_events])

    return "\n".join(lines)


def _build_session_account_detail_keyboard(
    account_number: int,
    is_active: bool,
    ) -> InlineKeyboardMarkup:
    toggle_text = "⏸️ Отключить" if is_active else "▶️ Включить"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=toggle_text,
                    callback_data=f"se_toggle_account_{account_number}",
                ),
                InlineKeyboardButton(
                    text="🌐 Прокси",
                    callback_data=f"se_proxy_menu_{account_number}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Обновить",
                    callback_data=f"se_refresh_account_{account_number}",
                ),
                InlineKeyboardButton(
                    text="🩺 Health",
                    callback_data=f"health_account_{account_number}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🗑️ Удалить",
                    callback_data=f"se_delete_account_{account_number}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="se_back_accounts")],
        ]
    )


def _build_session_delete_confirm_keyboard(account_number: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Да, удалить",
                    callback_data=f"se_account_delete_confirm_{account_number}",
                ),
                InlineKeyboardButton(
                    text="⬅️ Назад",
                    callback_data=f"se_account_{account_number}",
                ),
            ]
        ]
    )


async def _show_session_account_detail(query: CallbackQuery, account_number: int) -> None:
    accounts = get_user_accounts(query.from_user.id)
    account_info = next((acc for acc in accounts if acc[0] == account_number), None)
    if not account_info:
        await query.answer("❌ Аккаунт не найден", show_alert=True)
        return

    text = await _build_session_account_detail_text(query.from_user.id, account_number)
    keyboard = _build_session_account_detail_keyboard(account_number, bool(account_info[4]))
    await query.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)


def _build_health_accounts_keyboard(accounts) -> InlineKeyboardMarkup:
    keyboard = []
    for account_number, _, username, first_name, is_active in accounts:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=_format_account_list_label(
                        account_number,
                        username,
                        first_name,
                        bool(is_active),
                    ),
                    callback_data=f"health_account_{account_number}",
                )
            ]
        )
    keyboard.append(
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="health_refresh")]
    )
    keyboard.append(
        [InlineKeyboardButton(text="⬅️ Закрыть", callback_data="health_close")]
    )
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def _build_account_health_text(user_id: int, account_number: int) -> str:
    accounts = get_user_accounts(user_id)
    account_info = next((acc for acc in accounts if acc[0] == account_number), None)
    if not account_info:
        return "❌ Аккаунт не найден"

    _, telegram_id, username, first_name, is_active = account_info
    health_snapshot = await _get_account_health_snapshot(user_id, account_number)
    session_exists = bool(health_snapshot.get("session_exists"))
    proxy_settings = health_snapshot.get("proxy_settings")
    proxy_check = health_snapshot.get("proxy_check")
    authorized = bool(health_snapshot.get("authorized"))
    connected = bool(health_snapshot.get("connected"))

    lines = [
        f"🩺 <b>Health аккаунта {account_number}</b>",
        "",
        f"Имя: <b>{html.escape(str(first_name or 'Неизвестно'))}</b>",
        f"Username: <code>@{html.escape(str(username))}</code>" if username else "Username: -",
        f"Telegram ID: <code>{telegram_id}</code>",
        f"Статус: <b>{'в работе' if is_active else 'выключен'}</b>",
        "",
        f"Файл сессии: <b>{'есть' if session_exists else 'нет'}</b>",
        f"Авторизация: <b>{'ok' if authorized else 'нет'}</b>",
        f"Подключение: <b>{'активно' if connected else 'ленивый режим'}</b>",
        f"Прокси: <b>{'есть' if proxy_settings else 'нет'}</b>",
        "",
        f"<code>{html.escape(_format_proxy_check_block(proxy_check))}</code>",
    ]
    recent_events = format_recent_account_events(
        get_recent_account_events(user_id, account_number=account_number, limit=5)
    )
    if recent_events:
        lines.extend(["", "<b>Последние события</b>"])
        lines.extend([html.escape(event_line) for event_line in recent_events])
    return "\n".join(lines)


def _build_health_detail_keyboard(account_number: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔄 Проверить ещё раз",
                    callback_data=f"health_account_{account_number}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="health_refresh")],
        ]
    )


@dp.callback_query(F.data == "check_required_subscription")
async def check_required_subscription_callback(query: CallbackQuery):
    missing_channels = await get_missing_required_channels(bot, query.from_user.id)
    if missing_channels:
        titles = [channel["title"] for channel in missing_channels]
        await query.answer("Подписка еще не подтверждена", show_alert=True)
        try:
            await query.message.edit_text(
                build_required_subscription_text(titles),
                reply_markup=build_required_subscription_keyboard(missing_channels),
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    await query.answer("Подписка подтверждена ✅", show_alert=True)
    try:
        await query.message.edit_text(
            "✅ <b>Подписка подтверждена</b>\n\nТеперь бот доступен.",
            parse_mode="HTML",
        )
    except Exception:
        pass
    await query.message.answer("Главное меню:", reply_markup=get_main_menu_keyboard())


@dp.message(Command("se"))
async def cmd_sessions(message: Message):
    bot_user_id = message.from_user.id
    info, inline_keyboard = await get_sessions_text_and_keyboard(bot_user_id)
    if not info:
        await message.answer(
            "📱 <b>АККАУНТЫ</b>\n\nПока что тут пусто. Добавь первый аккаунт.",
            parse_mode="HTML",
            reply_markup=_build_empty_sessions_keyboard(),
        )
        return

    await message.answer(info, reply_markup=inline_keyboard, parse_mode="HTML")


@dp.message(Command("proxy"))
async def cmd_proxy(message: Message):
    accounts = get_user_accounts(message.from_user.id)
    if not accounts:
        await message.answer("❌ Сначала добавь аккаунт через /login")
        return

    await message.answer(
        "🌐 <b>Прокси аккаунтов</b>\n\nВыбери аккаунт:",
        parse_mode="HTML",
        reply_markup=_build_proxy_accounts_keyboard(accounts),
    )


@dp.message(Command("health"))
async def cmd_health(message: Message):
    accounts = get_user_accounts(message.from_user.id)
    if not accounts:
        await message.answer("❌ Сначала добавь аккаунт через /login")
        return

    await message.answer(
        "🩺 <b>Проверка аккаунтов</b>\n\nВыбери аккаунт для диагностики:",
        parse_mode="HTML",
        reply_markup=_build_health_accounts_keyboard(accounts),
    )


@dp.callback_query(F.data == "health_close")
async def health_close_callback(query: CallbackQuery):
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass


@dp.callback_query(F.data == "health_refresh")
async def health_refresh_callback(query: CallbackQuery):
    await query.answer()
    accounts = get_user_accounts(query.from_user.id)
    await query.message.edit_text(
        "🩺 <b>Проверка аккаунтов</b>\n\nВыбери аккаунт для диагностики:",
        parse_mode="HTML",
        reply_markup=_build_health_accounts_keyboard(accounts),
    )


@dp.callback_query(F.data.startswith("health_account_"))
async def health_account_callback(query: CallbackQuery):
    await query.answer("Собираю состояние...")
    account_number = int(query.data.rsplit("_", 1)[1])
    text = await _build_account_health_text(query.from_user.id, account_number)
    await query.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=_build_health_detail_keyboard(account_number),
    )


@dp.callback_query(F.data == "close_proxy_menu")
async def close_proxy_menu_callback(query: CallbackQuery):
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass


@dp.callback_query(F.data == "proxy_back_accounts")
async def proxy_back_accounts_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    await state.clear()
    accounts = get_user_accounts(query.from_user.id)
    if not accounts:
        await query.message.answer("❌ Аккаунты не найдены")
        return
    text = "🌐 <b>Прокси аккаунтов</b>\n\nВыбери аккаунт:"
    kb = _build_proxy_accounts_keyboard(accounts)
    try:
        await query.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=kb,
        )
    except Exception:
        await query.message.answer(text, parse_mode="HTML", reply_markup=kb)


@dp.callback_query(F.data.startswith("proxy_account_"))
@dp.callback_query(F.data.startswith("se_proxy_menu_"))
async def proxy_account_callback(query: CallbackQuery, state: FSMContext):
    try:
        await query.answer()
        await state.clear()
        account_number = int(query.data.rsplit("_", 1)[1])
        source = "se" if query.data.startswith("se_proxy_menu_") else "proxy"
        proxy_settings = get_account_proxy(query.from_user.id, account_number)
        check_result = get_account_proxy_check_result(query.from_user.id, account_number)
        text = _build_proxy_account_text(account_number, proxy_settings, check_result)
        kb = _build_proxy_account_keyboard(
            account_number,
            bool(proxy_settings),
            source=source,
        )
        try:
            await query.message.edit_text(
                text,
                parse_mode="HTML",
                reply_markup=kb,
            )
        except Exception:
            await query.message.answer(
                text,
                parse_mode="HTML",
                reply_markup=kb,
            )
    except Exception as exc:
        print(f"❌ Ошибка в proxy_account_callback: {exc}")
        try:
            await query.message.answer(f"❌ Ошибка открытия прокси: {str(exc)}")
        except Exception:
            pass


@dp.callback_query(F.data.startswith("proxy_set_"))
@dp.callback_query(F.data.startswith("se_proxy_set_"))
async def proxy_set_callback(query: CallbackQuery, state: FSMContext):
    if not await _guard_broadcast_sensitive_action(query):
        return
    await query.answer()
    account_number = int(query.data.rsplit("_", 1)[1])
    source = "se" if query.data.startswith("se_proxy_set_") else "proxy"
    await state.set_state(ProxyStates.waiting_proxy_value)
    await state.update_data(proxy_account_number=account_number, proxy_source=source)
    await query.message.edit_text(
        "🌐 <b>Новый прокси</b>\n\n"
        "Отправь прокси одним сообщением.\n"
        "Форматы:\n"
        "<code>host:port</code>\n"
        "<code>host:port:login:password</code>\n"
        "<code>host:port:secret</code>\n"
        "<code>https://t.me/proxy?server=...</code>",
        parse_mode="HTML",
        reply_markup=_build_proxy_input_cancel_keyboard(account_number, source=source),
    )


@dp.message(ProxyStates.waiting_proxy_value, ~F.text.startswith("/"))
async def process_proxy_value(message: Message, state: FSMContext):
    if not await _guard_broadcast_sensitive_action(message):
        return
    data = await state.get_data()
    account_number = data.get("proxy_account_number")
    source = data.get("proxy_source", "proxy")
    if not account_number:
        await state.clear()
        await message.answer("❌ Потерян аккаунт для настройки прокси.")
        return

    try:
        proxy_settings = parse_proxy_input(message.text)
        set_account_proxy(message.from_user.id, account_number, proxy_settings)
        await drop_cached_client(message.from_user.id, account_number)
        append_account_event(
            message.from_user.id,
            account_number=account_number,
            kind="proxy_updated",
            text="Прокси обновлен.",
        )
        await state.clear()
        await message.answer(
            "✅ Прокси сохранён.\n\n"
            f"<code>{html.escape(format_proxy_summary(proxy_settings))}</code>",
            parse_mode="HTML",
            reply_markup=get_main_menu_keyboard(),
        )
        await message.answer(
            _build_proxy_account_text(
                account_number,
                proxy_settings,
                get_account_proxy_check_result(message.from_user.id, account_number),
            ),
            parse_mode="HTML",
            reply_markup=_build_proxy_account_keyboard(
                account_number,
                True,
                source=source,
            ),
        )
    except Exception as exc:
        await message.answer(
            f"❌ Не удалось сохранить прокси: {exc}\n\nПопробуй ещё раз.",
        )


@dp.callback_query(F.data.startswith("proxy_delete_"))
@dp.callback_query(F.data.startswith("se_proxy_delete_"))
async def proxy_delete_callback(query: CallbackQuery, state: FSMContext):
    if not await _guard_broadcast_sensitive_action(query):
        return
    await query.answer()
    account_number = int(query.data.rsplit("_", 1)[1])
    source = "se" if query.data.startswith("se_proxy_delete_") else "proxy"
    await query.message.edit_text(
        "⚠️ <b>Подтвердить удаление прокси</b>\n\n"
        f"Аккаунт: <b>{account_number}</b>\n\n"
        "Прокси будет удалён из настроек аккаунта.",
        parse_mode="HTML",
        reply_markup=_build_proxy_delete_confirm_keyboard(
            account_number,
            source=source,
        ),
    )


@dp.callback_query(F.data.startswith("proxy_remove_confirm_"))
@dp.callback_query(F.data.startswith("se_proxy_remove_confirm_"))
async def proxy_delete_confirm_callback(query: CallbackQuery, state: FSMContext):
    if not await _guard_broadcast_sensitive_action(query):
        return
    await query.answer()
    await state.clear()
    account_number = int(query.data.rsplit("_", 1)[1])
    source = "se" if query.data.startswith("se_proxy_remove_confirm_") else "proxy"
    clear_account_proxy(query.from_user.id, account_number)
    await drop_cached_client(query.from_user.id, account_number)
    append_account_event(
        query.from_user.id,
        account_number=account_number,
        kind="proxy_deleted",
        text="Прокси удален.",
    )
    await query.message.edit_text(
        _build_proxy_account_text(account_number, None, None),
        parse_mode="HTML",
        reply_markup=_build_proxy_account_keyboard(
            account_number,
            False,
            source=source,
        ),
    )


@dp.callback_query(F.data.startswith("proxy_test_"))
@dp.callback_query(F.data.startswith("se_proxy_test_"))
async def proxy_test_callback(query: CallbackQuery):
    if not await _guard_broadcast_sensitive_action(query):
        return
    await query.answer("Проверяю прокси...")
    account_number = int(query.data.rsplit("_", 1)[1])
    source = "se" if query.data.startswith("se_proxy_test_") else "proxy"
    user_id = query.from_user.id
    if not try_begin_operation(user_id, "proxy_test"):
        return
    proxy_settings = get_account_proxy(query.from_user.id, account_number)
    if not proxy_settings:
        end_operation(user_id, "proxy_test")
        await query.answer("❌ У этого аккаунта прокси не задан", show_alert=True)
        return

    try:
        ok, details, ping_ms = await test_session_proxy(
            query.from_user.id,
            account_number,
            API_ID,
            API_HASH,
            proxy_settings,
        )
        save_account_proxy_check_result(
            query.from_user.id,
            account_number,
            ok=ok,
            error=None if ok else details,
            ping_ms=ping_ms,
        )
        append_account_event(
            query.from_user.id,
            account_number=account_number,
            kind="proxy_test",
            level="info" if ok else "warning",
            text=(
                f"Проверка прокси: {'успешно' if ok else 'ошибка'}"
                + (f", задержка {ping_ms} ms." if ping_ms is not None else ".")
            ),
        )
        status_line = "✅ Прокси рабочий" if ok else "❌ Прокси не отвечает"
        await query.message.edit_text(
            _build_proxy_account_text(
                account_number,
                proxy_settings,
                get_account_proxy_check_result(query.from_user.id, account_number),
            )
            + f"\n\n<b>Проверка:</b>\n<code>{html.escape(status_line)}\n{html.escape(details)}</code>",
            parse_mode="HTML",
            reply_markup=_build_proxy_account_keyboard(
                account_number,
                True,
                source=source,
            ),
        )
    finally:
        end_operation(user_id, "proxy_test")


@dp.callback_query(F.data.startswith("proxy_reconnect_"))
@dp.callback_query(F.data.startswith("se_proxy_reconnect_"))
async def proxy_reconnect_callback(query: CallbackQuery):
    if not await _guard_broadcast_sensitive_action(query):
        return
    await query.answer("Переподключаю аккаунт...")
    account_number = int(query.data.rsplit("_", 1)[1])
    source = "se" if query.data.startswith("se_proxy_reconnect_") else "proxy"
    user_id = query.from_user.id
    if not try_begin_operation(user_id, "proxy_reconnect"):
        return
    proxy_settings = get_account_proxy(query.from_user.id, account_number)

    try:
        await drop_cached_client(query.from_user.id, account_number)
        client = await ensure_connected_client(
            query.from_user.id,
            account_number,
            api_id=API_ID,
            api_hash=API_HASH,
        )
        if not client:
            append_account_event(
                query.from_user.id,
                account_number=account_number,
                kind="proxy_reconnect_failed",
                level="warning",
                text="Переподключение аккаунта не удалось.",
            )
            await query.message.edit_text(
                _build_proxy_account_text(
                    account_number,
                    proxy_settings,
                    get_account_proxy_check_result(query.from_user.id, account_number),
                )
                + "\n\n<b>Переподключение:</b>\n<code>❌ Не удалось подключить аккаунт.</code>",
                parse_mode="HTML",
                reply_markup=_build_proxy_account_keyboard(
                    account_number,
                    bool(proxy_settings),
                    source=source,
                ),
            )
            return

        append_account_event(
            query.from_user.id,
            account_number=account_number,
            kind="proxy_reconnect",
            text="Аккаунт переподключен вручную.",
        )
        await query.message.edit_text(
            _build_proxy_account_text(
                account_number,
                proxy_settings,
                get_account_proxy_check_result(query.from_user.id, account_number),
            )
            + "\n\n<b>Переподключение:</b>\n<code>✅ Аккаунт снова в памяти и готов к работе.</code>",
            parse_mode="HTML",
            reply_markup=_build_proxy_account_keyboard(
                account_number,
                bool(proxy_settings),
                source=source,
            ),
        )
    finally:
        end_operation(user_id, "proxy_reconnect")


@dp.callback_query(F.data == "add_new_account")
async def add_new_account_callback(query: CallbackQuery, state: FSMContext):
    if not await _guard_broadcast_sensitive_action(query):
        return

    await query.answer()

    await _start_login_with_optional_proxy(
        query.message,
        state,
        query.from_user.id,
        query.from_user.username,
        query.from_user.first_name,
    )


@dp.callback_query(F.data == "refresh_sessions_menu")
async def refresh_sessions_menu_callback(query: CallbackQuery, state: FSMContext):
    if not await _guard_user_operation(query):
        return
    await query.answer("Обновляю...")
    await state.clear()
    user_id = query.from_user.id
    if not try_begin_operation(user_id, "refresh_sessions"):
        return
    try:
        synced, total = await _sync_account_profiles(user_id)

        info, inline_keyboard = await get_sessions_text_and_keyboard(user_id)

        refresh_text = "🔄 <b>Список аккаунтов обновлён</b>"
        if total > 0:
            refresh_text += f"\nСинхронизировано: <b>{synced}/{total}</b>"

        if not info:
            await query.message.edit_text(
                refresh_text + "\n\nПока нет добавленных аккаунтов.",
                reply_markup=_build_empty_sessions_keyboard(),
                parse_mode="HTML",
            )
            return

        await query.message.edit_text(
            refresh_text + "\n\n" + info,
            reply_markup=inline_keyboard,
            parse_mode="HTML",
        )
    finally:
        end_operation(user_id, "refresh_sessions")


async def get_sessions_text_and_keyboard(user_id):
    """Build account menu text and keyboard."""
    accounts = get_user_accounts(user_id)

    stale_accounts = []
    for account_number, *_ in accounts:
        session_candidates = [
            Path(f"{session_base_path(user_id, account_number)}.session"),
            Path(__file__).resolve().parent
            / f"session_{user_id}_{account_number}.session",
        ]
        if not any(p.exists() for p in session_candidates):
            stale_accounts.append(account_number)

    if stale_accounts:
        for account_number in stale_accounts:
            if (
                user_id in user_authenticated
                and account_number in user_authenticated[user_id]
            ):
                try:
                    await asyncio.wait_for(
                        user_authenticated[user_id][account_number].disconnect(),
                        timeout=5.0,
                    )
                except Exception:
                    pass
                del user_authenticated[user_id][account_number]

            if (
                user_id in mention_monitors
                and account_number in mention_monitors[user_id]
            ):
                task = mention_monitors[user_id][account_number]
                if task and not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=2)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass
                del mention_monitors[user_id][account_number]

        from database import sqlite3, DB_PATH

        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        cursor = conn.cursor()
        for account_number in stale_accounts:
            cursor.execute(
                "DELETE FROM user_accounts WHERE bot_user_id = ? AND account_number = ?",
                (user_id, account_number),
            )
        conn.commit()
        conn.close()

        accounts = get_user_accounts(user_id)

    if not accounts:
        return None, None
    return _build_sessions_list_text(accounts), _build_sessions_accounts_keyboard(accounts)


@dp.callback_query(F.data.startswith("se_account_"))
async def se_account_callback(query: CallbackQuery):
    await query.answer("Открываю аккаунт...")
    account_number = int(query.data.rsplit("_", 1)[1])
    await _show_session_account_detail(query, account_number)


@dp.callback_query(F.data.startswith("se_refresh_account_"))
async def se_refresh_account_callback(query: CallbackQuery):
    if not await _guard_user_operation(query):
        return
    await query.answer("Синхронизирую аккаунт...")
    account_number = int(query.data.rsplit("_", 1)[1])
    user_id = query.from_user.id
    if not try_begin_operation(user_id, "refresh_account"):
        return
    try:
        synced, _ = await _sync_account_profiles(user_id, [account_number])
        if synced:
            append_account_event(
                user_id,
                account_number=account_number,
                kind="account_synced",
                text="Профиль аккаунта синхронизирован.",
            )
        await _show_session_account_detail(query, account_number)
    finally:
        end_operation(user_id, "refresh_account")


@dp.callback_query(F.data == "se_back_accounts")
async def se_back_accounts_callback(query: CallbackQuery):
    await query.answer()
    info, inline_keyboard = await get_sessions_text_and_keyboard(query.from_user.id)
    if not info:
        await query.message.edit_text(
            "📱 <b>АККАУНТЫ</b>\n\nПока что тут пусто. Добавь первый аккаунт.",
            parse_mode="HTML",
            reply_markup=_build_empty_sessions_keyboard(),
        )
        return
    await query.message.edit_text(
        info,
        parse_mode="HTML",
        reply_markup=inline_keyboard,
    )


@dp.callback_query(F.data.startswith("toggle_account_"))
@dp.callback_query(F.data.startswith("se_toggle_account_"))
async def toggle_account(query: CallbackQuery):
    if not await _guard_broadcast_sensitive_action(query):
        return

    bot_user_id = query.from_user.id

    account_number = int(query.data.rsplit("_", 1)[1])
    from_se_detail = query.data.startswith("se_toggle_account_")
    if not try_begin_operation(bot_user_id, "toggle_account"):
        return

    try:
        accounts = get_user_accounts(bot_user_id)

        current_status = None

        for acc_num, telegram_id, username, first_name, is_active in accounts:
            if acc_num == account_number:
                current_status = is_active

                break

        if current_status is None:
            await query.answer("❌ Аккаунт не найден", show_alert=True)

            return

        from database import sqlite3, DB_PATH

        if current_status:
            conn = sqlite3.connect(DB_PATH, timeout=30.0)

            cursor = conn.cursor()

            cursor.execute(
                "UPDATE user_accounts SET is_active = 0 WHERE bot_user_id = ? AND account_number = ?",
                (bot_user_id, account_number),
            )

            conn.commit()

            conn.close()

            if (
                bot_user_id in user_authenticated
                and account_number in user_authenticated[bot_user_id]
            ):
                try:
                    await asyncio.wait_for(
                        user_authenticated[bot_user_id][account_number].disconnect(),
                        timeout=5.0,
                    )

                except Exception:
                    pass

                del user_authenticated[bot_user_id][account_number]

            append_account_event(
                bot_user_id,
                account_number=account_number,
                kind="account_disabled",
                text="Аккаунт выключен вручную.",
            )
            await query.answer("🔴 Аккаунт отключен от рассылки", show_alert=False)

        else:
            try:
                session_candidates = build_session_candidates(bot_user_id, account_number)
                if not session_candidates:
                    await query.answer(
                        "❌ Файл сессии не найден. Используй /login для переподключения",
                        show_alert=True,
                    )

                    return

                session_file = session_candidates[0]

                client = build_telegram_client(
                    session_file,
                    API_ID,
                    API_HASH,
                    get_account_proxy(bot_user_id, account_number),
                )

                for attempt in range(3):
                    try:
                        await client.connect()

                        await asyncio.sleep(1)

                        if client.is_connected():
                            break

                    except Exception:
                        if attempt == 2:
                            raise

                        await asyncio.sleep(1)

                if await client.is_user_authorized():
                    conn = sqlite3.connect(DB_PATH, timeout=30.0)

                    cursor = conn.cursor()

                    cursor.execute(
                        "UPDATE user_accounts SET is_active = 1 WHERE bot_user_id = ? AND account_number = ?",
                        (bot_user_id, account_number),
                    )

                    conn.commit()

                    conn.close()

                    if bot_user_id not in user_authenticated:
                        user_authenticated[bot_user_id] = {}

                    user_authenticated[bot_user_id][account_number] = client

                    append_account_event(
                        bot_user_id,
                        account_number=account_number,
                        kind="account_enabled",
                        text="Аккаунт снова включен и подключен.",
                    )
                    await query.answer("🟢 Аккаунт включен", show_alert=False)

                else:
                    await client.disconnect()

                    await query.answer(
                        "❌ Аккаунт не авторизован. Используй /login", show_alert=True
                    )

                    return

            except Exception as e:
                await query.answer(f"❌ Ошибка подключения: {str(e)}", show_alert=True)

                return

        if from_se_detail:
            await _show_session_account_detail(query, account_number)
        else:
            info, inline_keyboard = await get_sessions_text_and_keyboard(bot_user_id)
            if info and inline_keyboard:
                try:
                    await query.message.edit_text(
                        info, reply_markup=inline_keyboard, parse_mode="HTML"
                    )
                except Exception:
                    pass
            else:
                await query.message.edit_text(
                    "📱 <b>АККАУНТЫ</b>\n\nПока нет добавленных аккаунтов.",
                    parse_mode="HTML",
                    reply_markup=_build_empty_sessions_keyboard(),
                )

    except Exception as e:
        print(f"❌ Ошибка в toggle_account: {str(e)}")

        await query.answer(f"❌ Ошибка: {str(e)}", show_alert=True)
    finally:
        end_operation(bot_user_id, "toggle_account")


@dp.callback_query(F.data == "close_sessions_menu")
async def close_sessions_menu_callback(query: CallbackQuery):
    """Удалить меню сессий"""

    await query.answer()

    try:
        await query.message.delete()

    except Exception as e:
        print(f"⚠️ Ошибка при удалении меню сессий: {str(e)}")


@dp.callback_query(F.data.startswith("delete_account_"))
@dp.callback_query(F.data.startswith("se_delete_account_"))
async def delete_account(query: CallbackQuery):
    if not await _guard_broadcast_sensitive_action(query):
        return

    account_number = int(query.data.rsplit("_", 1)[1])
    await query.answer()
    await query.message.edit_text(
        "⚠️ <b>Подтвердить удаление аккаунта</b>\n\n"
        f"Аккаунт: <b>{account_number}</b>\n\n"
        "Будут удалены сессия, прокси и локальные данные этого аккаунта.",
        parse_mode="HTML",
        reply_markup=_build_session_delete_confirm_keyboard(account_number),
    )


@dp.callback_query(F.data.startswith("account_delete_confirm_"))
@dp.callback_query(F.data.startswith("se_account_delete_confirm_"))
async def delete_account_confirm(query: CallbackQuery):
    if not await _guard_broadcast_sensitive_action(query):
        return

    bot_user_id = query.from_user.id

    account_number = int(query.data.rsplit("_", 1)[1])
    if not try_begin_operation(bot_user_id, "delete_account"):
        return

    try:
        if (
            bot_user_id in mention_monitors
            and account_number in mention_monitors[bot_user_id]
        ):
            task = mention_monitors[bot_user_id][account_number]

            if task and not task.done():
                task.cancel()

                try:
                    await asyncio.wait_for(task, timeout=2)

                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

            del mention_monitors[bot_user_id][account_number]

        if (
            bot_user_id in user_authenticated
            and account_number in user_authenticated[bot_user_id]
        ):
            try:
                client = user_authenticated[bot_user_id][account_number]

                await client.disconnect()

            except Exception:
                pass

            del user_authenticated[bot_user_id][account_number]

        await asyncio.sleep(2)

        import gc

        gc.collect()

        await asyncio.sleep(1)

        from pathlib import Path
        import os

        session_extensions = [
            ".session",
            ".session-journal",
            ".session-wal",
            ".session-shm",
        ]
        session_bases = [
            session_base_path(bot_user_id, account_number),
            Path(__file__).resolve().parent / f"session_{bot_user_id}_{account_number}",
        ]

        for session_file in session_bases:
            for ext in session_extensions:
                file_to_delete = Path(str(session_file) + ext)
                if file_to_delete.exists():
                    for attempt in range(10):
                        try:
                            temp_name = Path(str(session_file) + ext + ".delete")
                            try:
                                os.rename(file_to_delete, temp_name)
                                os.remove(temp_name)
                            except Exception:
                                os.remove(file_to_delete)

                            print(f"✅ Удален файл сессии: {file_to_delete}")
                            break
                        except OSError as e:
                            if attempt < 9:
                                await asyncio.sleep(0.5)
                            else:
                                print(f"⚠️  Не удалось удалить {file_to_delete}: {e}")

        from database import sqlite3, DB_PATH

        conn = sqlite3.connect(DB_PATH, timeout=30.0)

        cursor = conn.cursor()

        cursor.execute(
            "DELETE FROM user_accounts WHERE bot_user_id = ? AND account_number = ?",
            (bot_user_id, account_number),
        )

        conn.commit()

        await query.answer("❌ Аккаунт удален", show_alert=False)

        cursor.execute(
            "SELECT account_number FROM user_accounts WHERE bot_user_id = ? ORDER BY account_number",
            (bot_user_id,),
        )

        remaining = cursor.fetchall()

        for new_idx, (old_idx,) in enumerate(remaining, 1):
            if old_idx != new_idx:
                cursor.execute(
                    "UPDATE user_accounts SET account_number = ? WHERE bot_user_id = ? AND account_number = ?",
                    (new_idx, bot_user_id, old_idx),
                )

                old_session_base = session_base_path(bot_user_id, old_idx)
                new_session_base = session_base_path(bot_user_id, new_idx)
                for ext in session_extensions:
                    old_path = Path(str(old_session_base) + ext)
                    new_path = Path(str(new_session_base) + ext)
                    if not old_path.exists():
                        continue
                    try:
                        if new_path.exists():
                            os.remove(new_path)
                        old_path.rename(new_path)
                    except Exception as rename_error:
                        print(
                            f"⚠️  Не удалось переименовать {old_path} -> {new_path}: {rename_error}"
                        )

                if (
                    bot_user_id in user_authenticated
                    and old_idx in user_authenticated[bot_user_id]
                ):
                    user_authenticated[bot_user_id][new_idx] = user_authenticated[
                        bot_user_id
                    ][old_idx]

                    del user_authenticated[bot_user_id][old_idx]

        conn.commit()

        conn.close()

        info, inline_keyboard = await get_sessions_text_and_keyboard(bot_user_id)
        if info and inline_keyboard:
            await query.message.edit_text(
                info, reply_markup=inline_keyboard, parse_mode="HTML"
            )
        else:
            await query.message.edit_text(
                "📱 <b>АККАУНТЫ</b>\n\nПока нет добавленных аккаунтов.",
                parse_mode="HTML",
                reply_markup=_build_empty_sessions_keyboard(),
            )

    except Exception as e:
        print(f"❌ Ошибка в delete_account: {str(e)}")

        await query.answer(f"❌ Ошибка: {str(e)}", show_alert=True)
    finally:
        end_operation(bot_user_id, "delete_account")


@dp.message(Command("login"))
async def cmd_login(message: Message, state: FSMContext):
    if not await _guard_broadcast_sensitive_action(message):
        return

    if await _notify_session_limit_reached(message, message.from_user.id):
        return

    user = message.from_user

    print(f"📱 ЛОГИН: Пользователь {user.id} ({user.first_name}) нажал /login")

    await _start_login_with_optional_proxy(
        message,
        state,
        user.id,
        user.username,
        user.first_name,
    )


@dp.callback_query(F.data == "login_proxy_yes")
async def login_proxy_yes_callback(query: CallbackQuery, state: FSMContext):
    if not await _guard_broadcast_sensitive_action(query):
        return

    await query.answer()

    await state.set_state(LoginStates.waiting_proxy_input)

    await query.message.answer(
        "🌐 Отправь прокси одним сообщением.\n\n"
        "Форматы:\n"
        "host:port\n"
        "host:port:login:password\n"
        "host:port:secret\n"
        "https://t.me/proxy?server=...",
        reply_markup=_build_login_cancel_keyboard(),
    )


@dp.callback_query(F.data == "login_proxy_skip")
async def login_proxy_skip_callback(query: CallbackQuery, state: FSMContext):
    if not await _guard_broadcast_sensitive_action(query):
        return

    await query.answer()

    await state.update_data(login_proxy=None)
    await state.set_state(LoginStates.waiting_phone)

    await _send_phone_prompt(query.message)


@dp.callback_query(F.data == "login_proxy_cancel")
async def login_proxy_cancel_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    await state.clear()

    await query.message.answer("✅ Вход отменен", reply_markup=get_main_menu_keyboard())


@dp.message(
    LoginStates.waiting_proxy_input,
    ~F.text.startswith("/"),
    ~(F.text == LOGIN_CANCEL_TEXT),
)
async def process_login_proxy(message: Message, state: FSMContext):
    if not await _guard_broadcast_sensitive_action(message):
        return

    try:
        proxy_settings = parse_proxy_input(message.text)
    except Exception as exc:
        await message.answer(f"❌ Не удалось распознать прокси: {exc}")
        return

    await state.update_data(login_proxy=proxy_settings)
    await state.set_state(LoginStates.waiting_phone)

    await message.answer(
        "✅ Прокси принят.\n\n"
        f"<code>{html.escape(format_proxy_summary(proxy_settings))}</code>",
        parse_mode="HTML",
    )

    await _send_phone_prompt(message)


@dp.message(
    LoginStates.waiting_phone,
    ~F.text.startswith("/"),
    ~(F.text == "↩️ Отменить действие"),
)
async def process_phone(message: Message, state: FSMContext):
    if not await _guard_broadcast_sensitive_action(message):
        return

    phone_input = (message.text or "").strip()
    digits_only = "".join(ch for ch in phone_input if ch.isdigit())
    phone = f"+{digits_only}" if digits_only else phone_input

    user_id = message.from_user.id

    if await _notify_session_limit_reached(message, user_id):
        await state.clear()
        return

    print(f"PHONE INPUT: user {user_id} entered {phone}")

    if not phone.startswith("+") or not phone[1:].isdigit() or len(phone) < 10:
        print("   Invalid phone number format")

        await message.answer(
            "❌ Неверный номер телефона! Используй формат +7XXXXXXXXXX или 7XXXXXXXXXX"
        )

        return

    save_phone_number(user_id, phone)

    import time

    login_id = int(time.time() * 1000) % 1000000

    temp_session_file = temp_session_base_path(user_id, login_id)

    data = await state.get_data()
    login_proxy = data.get("login_proxy")

    try:
        client = build_telegram_client(temp_session_file, API_ID, API_HASH, login_proxy)
        await message.answer("⏳ Подключение к Telegram...")

        print("   ⏳ Подключаюсь к Telegram...")

        connected = False

        for attempt in range(3):
            try:
                await client.connect()

                await asyncio.sleep(2)

                if client.is_connected():
                    print(f"   ✓ Подключение к серверу (попытка {attempt + 1})")

                    connected = True

                    break

            except Exception as e:
                print(f" удалась: {str(e)}")

                if attempt < 2:
                    await asyncio.sleep(2)

                else:
                    raise

        if not connected:
            raise Exception("Не удалось подключиться к Telegram после 3 попыток")

        await asyncio.sleep(1)

        print("   ⏳ Запрашиваю код...")

        try:
            sent_code = await client.send_code_request(phone)

            phone_code_hash = sent_code.phone_code_hash

            print(f"   ✅ Код отправлен на номер {phone}")

        except Exception as e:
            print(f"   ❌ Ошибка при запросе кода: {str(e)}")

            await client.disconnect()

            await message.answer(
                f"❌ Ошибка при запросе кода: {str(e)}\n\nПопробуй /login снова"
            )

            return

        user_hashes[user_id] = phone_code_hash

        user_clients[user_id] = client

        start_login_session(user_id, phone)

        await state.set_state(LoginStates.waiting_code)

        await state.update_data(
            login_id=login_id, temp_session_file=str(temp_session_file)
        )

        user_code_input[user_id] = ""

        print("   ✅ Состояние установлено на waiting_code")

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="1", callback_data="digit_1"),
                    InlineKeyboardButton(text="2", callback_data="digit_2"),
                    InlineKeyboardButton(text="3", callback_data="digit_3"),
                ],
                [
                    InlineKeyboardButton(text="4", callback_data="digit_4"),
                    InlineKeyboardButton(text="5", callback_data="digit_5"),
                    InlineKeyboardButton(text="6", callback_data="digit_6"),
                ],
                [
                    InlineKeyboardButton(text="7", callback_data="digit_7"),
                    InlineKeyboardButton(text="8", callback_data="digit_8"),
                    InlineKeyboardButton(text="9", callback_data="digit_9"),
                ],
                [InlineKeyboardButton(text="0", callback_data="digit_0")],
                [
                    InlineKeyboardButton(
                        text="❌ Очистить", callback_data="clear_code"
                    ),
                    InlineKeyboardButton(
                        text="✅ Отправить", callback_data="submit_code"
                    ),
                ],
            ]
        )

        await message.answer(
            "✅ Код отправлен на твой номер!\n\n📝 Нажимай кнопки для ввода 5-значного кода:",
            reply_markup=keyboard,
        )

    except Exception as e:
        print(f"   ❌ ОШИБКА: {type(e).__name__}: {str(e)}")

        import traceback

        traceback.print_exc()

        try:
            if user_id in user_clients:
                await user_clients[user_id].disconnect()

                del user_clients[user_id]

        except Exception:
            pass

        await message.answer(
            f"❌ Ошибка подключения: {str(e)}\n\nПопробуй /login снова"
        )

        await state.clear()


@dp.message(
    LoginStates.waiting_proxy_choice,
    F.text.in_({LOGIN_CANCEL_TEXT, "❌ Отменить", "Отменить"}),
)
@dp.message(
    LoginStates.waiting_proxy_input,
    F.text.in_({LOGIN_CANCEL_TEXT, "❌ Отменить", "Отменить"}),
)
@dp.message(
    LoginStates.waiting_phone,
    F.text.in_({LOGIN_CANCEL_TEXT, "❌ Отменить", "Отменить"}),
)
@dp.message(
    LoginStates.waiting_code,
    F.text.in_({LOGIN_CANCEL_TEXT, "❌ Отменить", "Отменить"}),
)
@dp.message(
    LoginStates.waiting_password,
    F.text.in_({LOGIN_CANCEL_TEXT, "❌ Отменить", "Отменить"}),
)
async def cancel_login_flow(message: Message, state: FSMContext):
    user_id = message.from_user.id

    try:
        if user_id in user_clients:
            try:
                await user_clients[user_id].disconnect()
            except Exception:
                pass
            del user_clients[user_id]
    except Exception:
        pass

    user_code_input.pop(user_id, None)
    delete_login_session(user_id)
    await state.clear()

    await message.answer(
        "\u2705 \u0412\u0445\u043e\u0434 \u043e\u0442\u043c\u0435\u043d\u0435\u043d",
        reply_markup=get_main_menu_keyboard(),
    )


@dp.callback_query(F.data.startswith("digit_"))
async def process_digit(query: CallbackQuery, state: FSMContext):

    user_id = query.from_user.id

    digit = query.data.split("_")[1]

    print(f"🔢 КОД: Введена цифра {digit} от {user_id}")

    if user_id not in user_code_input:
        print("   ❌ Пользователь не в user_code_input")

        await query.answer("❌ Начните с /login")

        return

    current_state = await state.get_state()

    if current_state != LoginStates.waiting_code:
        print(f"   ❌ Неправильное состояние: {current_state}")

        await query.answer("❌ Начните с /login")

        return

    if len(user_code_input[user_id]) < 5:
        user_code_input[user_id] += digit

    display = "•" * len(user_code_input[user_id])

    print(f"   Введено: {display} ({len(user_code_input[user_id])}/5)")

    await query.answer()

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1", callback_data="digit_1"),
                InlineKeyboardButton(text="2", callback_data="digit_2"),
                InlineKeyboardButton(text="3", callback_data="digit_3"),
            ],
            [
                InlineKeyboardButton(text="4", callback_data="digit_4"),
                InlineKeyboardButton(text="5", callback_data="digit_5"),
                InlineKeyboardButton(text="6", callback_data="digit_6"),
            ],
            [
                InlineKeyboardButton(text="7", callback_data="digit_7"),
                InlineKeyboardButton(text="8", callback_data="digit_8"),
                InlineKeyboardButton(text="9", callback_data="digit_9"),
            ],
            [InlineKeyboardButton(text="0", callback_data="digit_0")],
            [
                InlineKeyboardButton(text="❌ Очистить", callback_data="clear_code"),
                InlineKeyboardButton(text="✅ Отправить", callback_data="submit_code"),
            ],
        ]
    )

    await query.message.edit_text(
        f"📝 Введено: {display}\n\nНажимай кнопки для ввода 5-значного кода:",
        reply_markup=keyboard,
    )


@dp.callback_query(F.data == "clear_code")
async def clear_code(query: CallbackQuery, state: FSMContext):

    user_id = query.from_user.id

    user_code_input[user_id] = ""

    await query.answer()

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1", callback_data="digit_1"),
                InlineKeyboardButton(text="2", callback_data="digit_2"),
                InlineKeyboardButton(text="3", callback_data="digit_3"),
            ],
            [
                InlineKeyboardButton(text="4", callback_data="digit_4"),
                InlineKeyboardButton(text="5", callback_data="digit_5"),
                InlineKeyboardButton(text="6", callback_data="digit_6"),
            ],
            [
                InlineKeyboardButton(text="7", callback_data="digit_7"),
                InlineKeyboardButton(text="8", callback_data="digit_8"),
                InlineKeyboardButton(text="9", callback_data="digit_9"),
            ],
            [InlineKeyboardButton(text="0", callback_data="digit_0")],
            [
                InlineKeyboardButton(text="❌ Очистить", callback_data="clear_code"),
                InlineKeyboardButton(text="✅ Отправить", callback_data="submit_code"),
            ],
        ]
    )

    await query.message.edit_text(
        "📝 Введено: \n\nНажимай кнопки для ввода 5-значного кода:",
        reply_markup=keyboard,
    )


@dp.callback_query(F.data == "submit_code")
async def submit_code(query: CallbackQuery, state: FSMContext):

    user_id = query.from_user.id

    code = user_code_input.get(user_id, "")

    print(f"✅ КОД: Отправка кода {code} от {user_id}")

    if len(code) != 5:
        print(f"   ❌ Код неверной длины: {len(code)}")

        await query.answer("❌ Код должен быть 5 цифр!", show_alert=True)

        return

    await query.answer()

    await process_code_login(query.message, code, user_id, state)


async def process_code_login(
    message: Message, code: str, user_id: int, state: FSMContext
):
    if not await _guard_broadcast_sensitive_action(message):
        return

    if user_id not in user_clients:
        await message.answer("❌ Сессия истекла. Попробуй /login снова")

        await state.clear()

        return

    print(f"🔐 Проверка кода для пользователя {user_id}")

    client = user_clients[user_id]

    phone_number = get_login_session(user_id)[0] if get_login_session(user_id) else None

    phone_code_hash = user_hashes.get(user_id)

    if not phone_code_hash:
        await message.answer("❌ Ошибка: потерян хэш кода. Попробуй /login снова")

        await state.clear()

        return

    try:
        await message.answer("⏳ Проверка кода...")

        print(f"   Вход с номером {phone_number}, кодом {code}")

        await client.sign_in(
            phone=phone_number, code=code, phone_code_hash=phone_code_hash
        )

        print("   ✅ Код принят, требуется пароль или готово")

        await state.set_state(LoginStates.waiting_password)

        update_login_step(user_id, "logged_in")

        user_code_input[user_id] = ""

        await message.answer(
            "✅ Код верный!\n\n🔐 Нужен ли пароль двухэтапной аутентификации? (напиши пароль или 'нет')"
        )

    except SessionPasswordNeededError:
        print("   ⚠️  Требуется пароль двухэтапной аутентификации")

        await state.set_state(LoginStates.waiting_password)

        user_code_input[user_id] = ""

        await message.answer(
            "🔐 Требуется пароль двухэтапной аутентификации.\n\n📝 Введи пароль:"
        )

    except PhoneCodeInvalidError:
        print("   ❌ Неверный код")

        user_code_input[user_id] = ""

        await message.answer(
            "❌ Неверный код! Попробуй снова (или напиши /login для нового кода):"
        )

    except Exception as e:
        print(f"   ❌ Ошибка логина: {str(e)}")

        if user_id in user_clients:
            try:
                await user_clients[user_id].disconnect()

            except Exception:
                pass

            del user_clients[user_id]

        if user_id in user_hashes:
            del user_hashes[user_id]

        if user_id in user_code_input:
            del user_code_input[user_id]

        delete_login_session(user_id)

        await message.answer(f"❌ Ошибка: {str(e)}\n\nПопробуй /login снова")

        await state.clear()


@dp.message(
    LoginStates.waiting_password,
    ~F.text.startswith("/"),
    ~(F.text == "↩️ Отменить действие"),
)
async def process_password(message: Message, state: FSMContext):
    if not await _guard_broadcast_sensitive_action(message):
        return

    user_id = message.from_user.id

    password_input = message.text.strip()

    print(f"🔐 Получен ввод пароля для пользователя {user_id}")

    if user_id not in user_clients:
        await message.answer("❌ Сессия истекла. Попробуй /login снова")

        await state.clear()

        return

    client = user_clients[user_id]

    try:
        if password_input.lower() == "нет":
            me = await client.get_me()

            print(
                f"✅ Получена информация об аккаунте (без пароля): {me.first_name} (ID: {me.id})"
            )

            session_limit, current_accounts, limit_reached = _get_user_session_limit_status(user_id)
            if limit_reached:
                await message.answer(
                    "❌ Лимит сессий исчерпан.\n\n"
                    f"Сейчас подключено: <b>{current_accounts}</b>\n"
                    f"Лимит: <b>{session_limit}</b>\n\n"
                    "Удалите лишний аккаунт или увеличьте лимит через VIP-настройки.",
                    parse_mode="HTML",
                )
                await state.clear()
                try:
                    await client.disconnect()
                except Exception:
                    pass
                delete_login_session(user_id)
                return

            account_number = add_user_account(
                user_id, me.id, me.username or "", me.first_name or "User", ""
            )

            print(f"✅ Аккаунт добавлен в БД с номером: {account_number}")

            data = await state.get_data()
            login_proxy = data.get("login_proxy")
            if login_proxy:
                set_account_proxy(user_id, account_number, login_proxy)

            temp_session_file_str = data.get(
                "temp_session_file", f"temp_session_{user_id}_unknown"
            )

            try:
                await asyncio.sleep(0.5)

                await client.disconnect()

                await asyncio.sleep(0.5)

                print("✅ Клиент отключен перед переименованием сессии")

            except Exception as e:
                print(f"⚠️  Ошибка отключения клиента: {str(e)}")

            from pathlib import Path

            old_session_file = Path(temp_session_file_str)

            new_session_file = session_base_path(user_id, account_number)

            import os

            import shutil

            old_session_with_ext = Path(f"{old_session_file}.session")

            new_session_with_ext = Path(f"{new_session_file}.session")

            print("   🔍 Проверка файла сессии:")

            print(f"      Существует: {old_session_with_ext.exists()}")

            if old_session_with_ext.exists():
                try:
                    if new_session_with_ext.exists():
                        print("   ⚠️  Целевой файл уже существует, удаляю старый")

                        os.remove(str(new_session_with_ext))

                    shutil.copy2(str(old_session_with_ext), str(new_session_with_ext))

                    print(
                        f"   ✅ Файл сессии скопирован: {old_session_with_ext.name} -> {new_session_with_ext.name}"
                    )

                    os.remove(str(old_session_with_ext))

                    print(f"   ✅ Временный файл удален: {old_session_with_ext.name}")

                    if not new_session_with_ext.exists():
                        print("   ❌ ОШИБКА: Файл не был скопирован правильно!")

                    else:
                        print(
                            f"   ✅ Проверка: новый файл существует - {new_session_with_ext.exists()}"
                        )

                except Exception as e:
                    print(f"   ❌ Ошибка копирования файла: {str(e)}")

                    import traceback

                    traceback.print_exc()

            else:
                print(f"   ❌ Исходный файл не найден: {old_session_with_ext}")

                print("   ℹ️  Список файлов в директории:")

                temp_files = list(Path(__file__).parent.glob("temp_session_*"))

                session_files = list(Path(__file__).parent.glob("session_*"))

                if temp_files:
                    for f in temp_files:
                        print(f"      - {f.name}")

                else:
                    print("      [нет temp файлов]")

                if session_files:
                    for f in session_files:
                        print(f"      - {f.name}")

            print(f"   📍 Создаю новый клиент с сессией: {new_session_file}")

            client = build_telegram_client(new_session_file, API_ID, API_HASH, login_proxy)

            await client.connect()

            if user_id not in user_authenticated:
                user_authenticated[user_id] = {}

            user_authenticated[user_id][account_number] = client

            print(
                f"✅ Клиент сохранен в памяти: user_authenticated[{user_id}][{account_number}]"
            )

            await message.answer(
                "✅ Отлично! Ты успешно вошел в аккаунт!",
                reply_markup=get_main_menu_keyboard(),
            )

            set_user_logged_in(user_id, True)

            delete_login_session(user_id)

            await state.clear()

            return

        await message.answer("⏳ Проверка пароля...")

        await client.sign_in(password=password_input)

        me = await client.get_me()

        print(f"✅ Получена информация об аккаунте: {me.first_name} (ID: {me.id})")

        session_limit, current_accounts, limit_reached = _get_user_session_limit_status(user_id)
        if limit_reached:
            await message.answer(
                "❌ Лимит сессий исчерпан.\n\n"
                f"Сейчас подключено: <b>{current_accounts}</b>\n"
                f"Лимит: <b>{session_limit}</b>\n\n"
                "Удалите лишний аккаунт или увеличьте лимит через VIP-настройки.",
                parse_mode="HTML",
            )
            await state.clear()
            try:
                await client.disconnect()
            except Exception:
                pass
            delete_login_session(user_id)
            return

        account_number = add_user_account(
            user_id, me.id, me.username or "", me.first_name or "User", ""
        )

        print(f"✅ Аккаунт добавлен в БД с номером: {account_number}")

        data = await state.get_data()
        login_proxy = data.get("login_proxy")
        if login_proxy:
            set_account_proxy(user_id, account_number, login_proxy)

        temp_session_file_str = data.get(
            "temp_session_file", f"temp_session_{user_id}_unknown"
        )

        try:
            await asyncio.sleep(0.5)

            await client.disconnect()

            await asyncio.sleep(0.5)

            print("✅ Клиент отключен перед переименованием сессии")

        except Exception as e:
            print(f"⚠️  Ошибка отключения клиента: {str(e)}")

        from pathlib import Path

        old_session_file = Path(temp_session_file_str)

        new_session_file = session_base_path(user_id, account_number)

        import os

        import shutil

        old_session_with_ext = Path(f"{old_session_file}.session")

        new_session_with_ext = Path(f"{new_session_file}.session")

        print("   🔍 Проверка файла сессии:")

        print(f"      Ищу: {old_session_with_ext}")

        print(f"      Существует: {old_session_with_ext.exists()}")

        if old_session_with_ext.exists():
            try:
                if new_session_with_ext.exists():
                    print("   ⚠️  Целевой файл уже существует, удаляю старый")

                    os.remove(str(new_session_with_ext))

                shutil.copy2(str(old_session_with_ext), str(new_session_with_ext))

                print(
                    f"   ✅ Файл сессии скопирован: {old_session_with_ext.name} -> {new_session_with_ext.name}"
                )

                os.remove(str(old_session_with_ext))

                print(f"   ✅ Временный файл удален: {old_session_with_ext.name}")

                if not new_session_with_ext.exists():
                    print("   ❌ ОШИБКА: Файл не был скопирован правильно!")

                else:
                    print(
                        f"   ✅ Проверка: новый файл существует - {new_session_with_ext.exists()}"
                    )

            except Exception as e:
                print(f"   ❌ Ошибка копирования файла: {str(e)}")

                import traceback

                traceback.print_exc()

        else:
            print(f"   ❌ Исходный файл не найден: {old_session_with_ext}")

            print("   ℹ️  Список файлов в директории:")

            temp_files = list(Path(__file__).parent.glob("temp_session_*"))

            session_files = list(Path(__file__).parent.glob("session_*"))

            if temp_files:
                for f in temp_files:
                    print(f"      - {f.name}")

            else:
                print("      [нет temp файлов]")

            if session_files:
                for f in session_files:
                    print(f"      - {f.name}")

        print(f"   📍 Создаю новый клиент с сессией: {new_session_file}")

        client = build_telegram_client(new_session_file, API_ID, API_HASH, login_proxy)

        await client.connect()

        if user_id not in user_authenticated:
            user_authenticated[user_id] = {}

        user_authenticated[user_id][account_number] = client

        print(
            f"✅ Клиент сохранен в памяти: user_authenticated[{user_id}][{account_number}]"
        )

        await message.answer(
            "✅ Успешно! Ты вошел в свой аккаунт Telegram!",
            reply_markup=get_main_menu_keyboard(),
        )

        set_user_logged_in(user_id, True)

        delete_login_session(user_id)

        await state.clear()

    except Exception as e:
        await message.answer(
            f"❌ Ошибка пароля: {str(e)}\n\nПопробуй еще раз или напиши 'нет' если пароля нет:"
        )


@dp.message(Command("logout"))
async def cmd_logout(message: Message):
    if not await _guard_broadcast_sensitive_action(message):
        return

    user_id = message.from_user.id

    set_user_logged_in(user_id, False)

    if user_id in user_clients:
        try:
            await user_clients[user_id].disconnect()

        except Exception as e:
            print(f"⚠️ Ошибка при отключении клиента: {str(e)}")

        del user_clients[user_id]

    if user_id in user_authenticated:
        for acc_client in list(user_authenticated[user_id].values()):
            try:
                await acc_client.disconnect()

            except Exception as e:
                print(f"⚠️ Ошибка при отключении аккаунта: {str(e)}")

        del user_authenticated[user_id]

    cleanup_user_session(user_id)

    await message.answer("❌ Ты вышел из аккаунта")


async def main():

    setup_logging()
    app_state.bot = bot
    await start_telegram_log_forwarding(bot, DEV_LOG_CHAT_ID)

    print("🚀 Запуск бота...")

    print("⏳ Загружаю сессии и конфиги...")

    await load_saved_sessions(API_ID, API_HASH, connect_on_start=False)

    await update_vip_cache()

    print(f"📊 Загружено {get_vip_cache_size()} VIP пользователей")

    load_broadcast_configs()
    restored_broadcasts = load_persisted_broadcasts()
    if restored_broadcasts:
        print(f"📦 Восстановлено рассылок после рестарта: {restored_broadcasts}")

    print("✅ Бот готов к работе!")

    try:
        await dp.start_polling(bot)
    except TelegramUnauthorizedError:
        print("❌ Бот не запущен: Telegram отклонил токен бота (Unauthorized).")
        print("Проверь TOKEN в config.local.json или в переменных окружения.")
        return 1
    finally:
        await stop_telegram_log_forwarding()

    return 0


dp.include_router(vip_router)

dp.include_router(account_router)

dp.include_router(config_router)

dp.include_router(broadcast_router)
dp.include_router(joins_router)

dp.include_router(basic_router)

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
