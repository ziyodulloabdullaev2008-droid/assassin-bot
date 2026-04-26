import asyncio
import html
import random
import re
import time

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters.command import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from telethon.errors import FloodWaitError, UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

from core.config import API_HASH, API_ID
from core.state import app_state
from database import get_user_accounts
from services.session_service import ensure_connected_client

router = Router()

LOGIN_REQUIRED_TEXT = "❌ Сначала войди через /login"

DEFAULT_DELAY_MIN = 15
DEFAULT_DELAY_MAX = 25
DEFAULT_REST_EVERY = 5
DEFAULT_REST_MIN = 60
DEFAULT_REST_MAX = 120

mass_join_tasks: dict[int, asyncio.Task] = {}
mass_join_status: dict[int, dict] = {}
mass_join_settings: dict[int, dict] = {}


class ChatsJoinState(StatesGroup):
    selecting_accounts = State()
    waiting_targets = State()
    waiting_delay = State()
    waiting_rest_every = State()
    waiting_rest_duration = State()


def _default_join_settings() -> dict:
    return {
        "delay_min": DEFAULT_DELAY_MIN,
        "delay_max": DEFAULT_DELAY_MAX,
        "rest_every": DEFAULT_REST_EVERY,
        "rest_min": DEFAULT_REST_MIN,
        "rest_max": DEFAULT_REST_MAX,
    }


def _get_join_settings(user_id: int) -> dict:
    settings = mass_join_settings.get(user_id)
    if not settings:
        settings = _default_join_settings()
        mass_join_settings[user_id] = settings
    return dict(settings)


def _save_join_settings(user_id: int, **updates) -> dict:
    settings = _get_join_settings(user_id)
    settings.update(updates)
    mass_join_settings[user_id] = settings
    return settings


def _has_logged_accounts(user_id: int) -> bool:
    return bool(app_state.user_authenticated.get(user_id))


async def _ensure_logged_message(message: Message) -> bool:
    if _has_logged_accounts(message.from_user.id):
        return True
    await message.answer(LOGIN_REQUIRED_TEXT)
    return False


async def _ensure_logged_query(query: CallbackQuery) -> bool:
    if _has_logged_accounts(query.from_user.id):
        return True
    await query.answer(LOGIN_REQUIRED_TEXT, show_alert=True)
    return False


async def _safe_edit_text(
    query: CallbackQuery, text: str, kb: InlineKeyboardMarkup
) -> None:
    try:
        await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
        raise


async def _safe_edit_message(
    bot,
    *,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> bool:
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode="HTML",
        )
        return True
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return True
        return False
    except Exception:
        return False


def _get_connected_accounts(user_id: int) -> list[tuple[int, str]]:
    connected = set(app_state.user_authenticated.get(user_id, {}).keys())
    accounts = get_user_accounts(user_id)
    rows = []
    for acc_num, _, username, first_name, is_active in accounts:
        if acc_num not in connected or not is_active:
            continue
        label = first_name or username or f"Акк {acc_num}"
        rows.append((acc_num, label))
    rows.sort(key=lambda x: x[0])
    return rows


def _format_seconds_range(min_s: int, max_s: int) -> str:
    if min_s == max_s:
        return _format_seconds(min_s)
    return f"{_format_seconds(min_s)} - {_format_seconds(max_s)}"


def _format_seconds(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds} сек"
    minutes, rest = divmod(seconds, 60)
    if rest == 0:
        return f"{minutes} мин"
    return f"{minutes} мин {rest} сек"


def _format_settings_summary(settings: dict) -> str:
    rest_every = int(settings.get("rest_every", DEFAULT_REST_EVERY) or 0)
    if rest_every > 0:
        rest_line = (
            f"каждые {rest_every} чатов, "
            f"{_format_seconds_range(int(settings['rest_min']), int(settings['rest_max']))}"
        )
    else:
        rest_line = "выключен"
    return (
        f"⏱️ Интервал: <b>{_format_seconds_range(int(settings['delay_min']), int(settings['delay_max']))}</b>\n"
        f"😴 Отдых: <b>{rest_line}</b>"
    )


def _build_chats_accounts_text(user_id: int, selected: set[int]) -> str:
    connected = _get_connected_accounts(user_id)
    if not connected:
        return LOGIN_REQUIRED_TEXT

    settings = _get_join_settings(user_id)
    selected_count = len([acc for acc, _ in connected if acc in selected])
    return (
        "🧩 <b>/chats — Массовое вступление</b>\n\n"
        f"Подключено аккаунтов: <b>{len(connected)}</b>\n"
        f"Выбрано: <b>{selected_count}</b>\n\n"
        f"{_format_settings_summary(settings)}\n\n"
        "Выбери аккаунты, при необходимости настрой интервал и отдых, затем нажми «Далее»."
    )


def _build_chats_accounts_kb(
    user_id: int, selected: set[int]
) -> InlineKeyboardMarkup:
    connected = _get_connected_accounts(user_id)
    buttons = []
    for acc_num, label in connected:
        mark = "✅" if acc_num in selected else "⬜"
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"{mark} {label}", callback_data=f"jc_acc_{acc_num}"
                )
            ]
        )

    buttons.append(
        [
            InlineKeyboardButton(text="✅ Все", callback_data="jc_all"),
            InlineKeyboardButton(text="⬜ Снять все", callback_data="jc_none"),
        ]
    )
    buttons.append(
        [
            InlineKeyboardButton(text="⏱️ Интервал", callback_data="jc_cfg_delay"),
            InlineKeyboardButton(text="😴 Отдых", callback_data="jc_cfg_rest"),
        ]
    )
    buttons.append(
        [
            InlineKeyboardButton(text="➡️ Далее", callback_data="jc_next"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="jc_cancel"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _build_rest_settings_text(user_id: int) -> str:
    settings = _get_join_settings(user_id)
    rest_every = int(settings.get("rest_every", DEFAULT_REST_EVERY) or 0)
    status = (
        "выключен"
        if rest_every <= 0
        else f"каждые {rest_every} чатов, {_format_seconds_range(int(settings['rest_min']), int(settings['rest_max']))}"
    )
    return (
        "😴 <b>Настройки отдыха /chats</b>\n\n"
        f"Сейчас: <b>{status}</b>\n\n"
        "Что хочешь поменять?"
    )


def _build_rest_settings_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔢 Каждые N чатов", callback_data="jc_cfg_rest_every"
                )
            ],
            [
                InlineKeyboardButton(
                    text="⏳ Длительность отдыха", callback_data="jc_cfg_rest_duration"
                )
            ],
            [
                InlineKeyboardButton(
                    text="🚫 Выключить отдых", callback_data="jc_cfg_rest_disable"
                )
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="jc_cfg_back")],
        ]
    )


def _build_wait_targets_text(user_id: int, selected_count: int) -> str:
    settings = _get_join_settings(user_id)
    return (
        "📥 <b>/chats</b>\n\n"
        f"Выбрано аккаунтов: <b>{selected_count}</b>\n"
        f"{_format_settings_summary(settings)}\n\n"
        "Отправь ссылки и/или юзернеймы чатов для вступления.\n"
        "Поддержка форматов:\n"
        "• <code>@channelname</code>\n"
        "• <code>https://t.me/...</code>\n"
        "• <code>t.me/...</code>\n\n"
        "Можно отправить много строк сразу."
    )


def _build_wait_targets_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="jc_wait_cancel")]
        ]
    )


def _build_delay_prompt_text(user_id: int) -> str:
    settings = _get_join_settings(user_id)
    return (
        "⏱️ <b>Интервал между вступлениями</b>\n\n"
        f"Сейчас: <b>{_format_seconds_range(int(settings['delay_min']), int(settings['delay_max']))}</b>\n\n"
        "Введи диапазон в секундах.\n"
        "Пример: <code>15-25</code>"
    )


def _build_rest_every_prompt_text(user_id: int) -> str:
    settings = _get_join_settings(user_id)
    current = int(settings.get("rest_every", DEFAULT_REST_EVERY) or 0)
    return (
        "🔢 <b>Каждые сколько чатов отдыхать</b>\n\n"
        f"Сейчас: <b>{current}</b>\n\n"
        "Введи число.\n"
        "0 — полностью выключить отдых."
    )


def _build_rest_duration_prompt_text(user_id: int) -> str:
    settings = _get_join_settings(user_id)
    return (
        "⏳ <b>Длительность отдыха</b>\n\n"
        f"Сейчас: <b>{_format_seconds_range(int(settings['rest_min']), int(settings['rest_max']))}</b>\n\n"
        "Введи диапазон в секундах.\n"
        "Пример: <code>60-120</code>"
    )


def _build_cancel_back_kb(back_callback: str = "jc_cfg_back") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)]
        ]
    )


def _parse_range_input(raw: str, *, minimum: int = 0, maximum: int = 86400) -> tuple[int, int] | None:
    text = (raw or "").strip().replace(" ", "")
    match = re.fullmatch(r"(\d+)-(\d+)", text)
    if not match:
        return None
    left = int(match.group(1))
    right = int(match.group(2))
    if left > right:
        left, right = right, left
    if left < minimum or right > maximum:
        return None
    return left, right


def _parse_positive_int(raw: str, *, minimum: int = 0, maximum: int = 100000) -> int | None:
    text = (raw or "").strip()
    if not re.fullmatch(r"\d+", text):
        return None
    value = int(text)
    if value < minimum or value > maximum:
        return None
    return value


def _parse_chat_targets(raw_text: str) -> tuple[list[str], list[str]]:
    text = (raw_text or "").strip()
    if not text:
        return [], []

    links: list[str] = []
    usernames: list[str] = []
    for token in re.split(r"[\s,;]+", text):
        item = token.strip()
        if not item:
            continue
        lower = item.lower()
        if lower.startswith("https://t.me/") or lower.startswith("http://t.me/"):
            links.append(item)
            continue
        if lower.startswith("t.me/"):
            links.append(f"https://{item}")
            continue
        if item.startswith("@"):
            name = item[1:].strip()
            if re.fullmatch(r"[A-Za-z0-9_]{4,}", name):
                usernames.append(name)
            continue
        if re.fullmatch(r"[A-Za-z0-9_]{4,}", item):
            usernames.append(item)

    links = _dedupe_keep_order(links)
    usernames = _dedupe_keep_order(usernames)
    return links, usernames


def _dedupe_keep_order(items: list[str]) -> list[str]:
    out: list[str] = []
    seen = set()
    for item in items:
        key = (item or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _extract_invite_hash(link: str) -> str | None:
    match = re.search(r"/\+([-_\w]+)$", link)
    if match:
        return match.group(1)
    match = re.search(r"/joinchat/([-_\w]+)$", link)
    if match:
        return match.group(1)
    return None


def _build_runtime_kb(running: bool) -> InlineKeyboardMarkup | None:
    if not running:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⏹️ Полностью остановить", callback_data="jc_stop"
                ),
                InlineKeyboardButton(text="🔄 Обновить", callback_data="jc_refresh"),
            ]
        ]
    )


def _render_running_text(status: dict) -> str:
    current_account = html.escape(status.get("current_account_label") or "—")
    current_target = html.escape(status.get("current_target_label") or "—")
    total_accounts = int(status.get("accounts_total", 0) or 0)
    current_account_index = int(status.get("current_account_index", 0) or 0)
    total_targets = int(status.get("targets_total", 0) or 0)
    current_target_index = int(status.get("current_target_index", 0) or 0)
    overall_joined = int(status.get("overall_joined", 0) or 0)
    overall_already = int(status.get("overall_already", 0) or 0)
    overall_failed = int(status.get("overall_failed", 0) or 0)
    started_at = float(status.get("started_at", time.time()) or time.time())
    elapsed = max(0, int(time.time() - started_at))
    delay_line = _format_settings_summary(status.get("settings") or _default_join_settings())
    phase = html.escape(status.get("phase") or "Подготовка")
    phase_extra = status.get("phase_extra")

    lines = [
        "🧩 <b>/chats — выполнение</b>",
        "",
        f"📍 Стадия: <b>{phase}</b>",
    ]
    if phase_extra:
        lines.append(html.escape(str(phase_extra)))
    lines.extend(
        [
            f"👤 Аккаунт: <b>{current_account}</b> ({current_account_index}/{total_accounts})",
            f"🎯 Цель: <b>{current_target}</b> ({current_target_index}/{total_targets})",
            f"✅ Вступил: <b>{overall_joined}</b> | ☑️ Уже был: <b>{overall_already}</b> | ❌ Ошибки: <b>{overall_failed}</b>",
            f"⏱️ Прошло: <b>{_format_seconds(elapsed)}</b>",
            "",
            delay_line,
        ]
    )
    return "\n".join(lines)


async def _push_runtime_status(bot, user_id: int) -> None:
    status = mass_join_status.get(user_id)
    if not status:
        return
    text = _render_running_text(status)
    status["rendered_text"] = text
    await _safe_edit_message(
        bot,
        chat_id=int(status["chat_id"]),
        message_id=int(status["message_id"]),
        text=text,
        reply_markup=_build_runtime_kb(bool(status.get("running"))),
    )


def _build_finished_text(status: dict, summary_lines: list[str], *, cancelled: bool) -> str:
    title = "⏹️ <b>/chats остановлен</b>" if cancelled else "✅ <b>/chats завершен</b>"
    total_targets = int(status.get("targets_total", 0) or 0)
    overall_joined = int(status.get("overall_joined", 0) or 0)
    overall_already = int(status.get("overall_already", 0) or 0)
    overall_failed = int(status.get("overall_failed", 0) or 0)
    started_at = float(status.get("started_at", time.time()) or time.time())
    elapsed = max(0, int(time.time() - started_at))
    body = [
        title,
        "",
        f"🎯 Целей: <b>{total_targets}</b>",
        f"✅ Вступил: <b>{overall_joined}</b>",
        f"☑️ Уже был: <b>{overall_already}</b>",
        f"❌ Ошибки: <b>{overall_failed}</b>",
        f"⏱️ Длительность: <b>{_format_seconds(elapsed)}</b>",
        "",
        "<b>По аккаунтам:</b>",
        "\n".join(summary_lines) if summary_lines else "• нет данных",
    ]
    return "\n".join(body)


@router.message(Command("chats"))
async def chats_join_command(message: Message, state: FSMContext):
    if not await _ensure_logged_message(message):
        await state.clear()
        return

    user_id = message.from_user.id
    connected = _get_connected_accounts(user_id)
    if not connected:
        await message.answer("❌ Нет подключенных активных аккаунтов.")
        return

    task = mass_join_tasks.get(user_id)
    if task and not task.done():
        status = mass_join_status.get(user_id)
        if status:
            await message.answer(
                status.get("rendered_text") or _render_running_text(status),
                reply_markup=_build_runtime_kb(True),
                parse_mode="HTML",
            )
        else:
            await message.answer("⏳ Уже выполняется /chats задача. Дождись завершения.")
        return

    selected = {acc_num for acc_num, _ in connected}
    sent = await message.answer(
        _build_chats_accounts_text(user_id, selected),
        reply_markup=_build_chats_accounts_kb(user_id, selected),
        parse_mode="HTML",
    )
    await state.set_state(ChatsJoinState.selecting_accounts)
    await state.update_data(
        chats_selected=sorted(selected),
        chats_menu_msg=sent.message_id,
    )


@router.callback_query(F.data.startswith("jc_"))
async def chats_join_callbacks(query: CallbackQuery, state: FSMContext):
    if not await _ensure_logged_query(query):
        await state.clear()
        return

    action = query.data
    user_id = query.from_user.id

    if action in {"jc_stop", "jc_refresh"}:
        task = mass_join_tasks.get(user_id)
        status = mass_join_status.get(user_id)
        if action == "jc_stop":
            if task and not task.done():
                if status:
                    status["phase"] = "Остановка"
                    status["phase_extra"] = "Останавливаю задачу по кнопке."
                    await _push_runtime_status(query.bot, user_id)
                task.cancel()
                await query.answer("Останавливаю /chats...")
            else:
                await query.answer("Сейчас нет активной /chats задачи.", show_alert=True)
            return
        if not status:
            await query.answer("Нет активного статуса.", show_alert=True)
            return
        await _safe_edit_text(
            query,
            status.get("rendered_text") or _render_running_text(status),
            _build_runtime_kb(bool(status.get("running"))),
        )
        await query.answer("Обновил.")
        return

    await query.answer()

    if action == "jc_wait_cancel":
        await state.clear()
        await _safe_edit_text(
            query,
            "❌ <b>/chats отменен</b>",
            InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="▶️ Запустить заново", callback_data="jc_restart")]
                ]
            ),
        )
        return

    if action == "jc_restart":
        connected = _get_connected_accounts(user_id)
        selected = {acc_num for acc_num, _ in connected}
        await state.set_state(ChatsJoinState.selecting_accounts)
        await state.update_data(
            chats_selected=sorted(selected), chats_menu_msg=query.message.message_id
        )
        await _safe_edit_text(
            query,
            _build_chats_accounts_text(user_id, selected),
            _build_chats_accounts_kb(user_id, selected),
        )
        return

    current_state = await state.get_state()
    if current_state not in {
        ChatsJoinState.selecting_accounts.state,
        ChatsJoinState.waiting_targets.state,
        ChatsJoinState.waiting_delay.state,
        ChatsJoinState.waiting_rest_every.state,
        ChatsJoinState.waiting_rest_duration.state,
    }:
        return

    data = await state.get_data()
    selected = set(data.get("chats_selected") or [])
    connected = _get_connected_accounts(user_id)
    connected_set = {acc_num for acc_num, _ in connected}
    selected = {x for x in selected if x in connected_set}

    if action == "jc_cancel":
        await state.clear()
        try:
            await query.message.delete()
        except Exception:
            pass
        return

    if action == "jc_cfg_back":
        await state.set_state(ChatsJoinState.selecting_accounts)
        await state.update_data(
            chats_selected=sorted(selected), chats_menu_msg=query.message.message_id
        )
        await _safe_edit_text(
            query,
            _build_chats_accounts_text(user_id, selected),
            _build_chats_accounts_kb(user_id, selected),
        )
        return

    if action == "jc_cfg_delay":
        await state.set_state(ChatsJoinState.waiting_delay)
        await state.update_data(
            chats_selected=sorted(selected), chats_menu_msg=query.message.message_id
        )
        await _safe_edit_text(
            query,
            _build_delay_prompt_text(user_id),
            _build_cancel_back_kb(),
        )
        return

    if action == "jc_cfg_rest":
        await state.set_state(ChatsJoinState.selecting_accounts)
        await state.update_data(
            chats_selected=sorted(selected), chats_menu_msg=query.message.message_id
        )
        await _safe_edit_text(
            query,
            _build_rest_settings_text(user_id),
            _build_rest_settings_kb(),
        )
        return

    if action == "jc_cfg_rest_every":
        await state.set_state(ChatsJoinState.waiting_rest_every)
        await state.update_data(
            chats_selected=sorted(selected), chats_menu_msg=query.message.message_id
        )
        await _safe_edit_text(
            query,
            _build_rest_every_prompt_text(user_id),
            _build_cancel_back_kb("jc_cfg_rest"),
        )
        return

    if action == "jc_cfg_rest_duration":
        await state.set_state(ChatsJoinState.waiting_rest_duration)
        await state.update_data(
            chats_selected=sorted(selected), chats_menu_msg=query.message.message_id
        )
        await _safe_edit_text(
            query,
            _build_rest_duration_prompt_text(user_id),
            _build_cancel_back_kb("jc_cfg_rest"),
        )
        return

    if action == "jc_cfg_rest_disable":
        _save_join_settings(user_id, rest_every=0)
        await _safe_edit_text(
            query,
            _build_rest_settings_text(user_id),
            _build_rest_settings_kb(),
        )
        return

    if action == "jc_all":
        selected = set(connected_set)
    elif action == "jc_none":
        selected = set()
    elif action == "jc_next":
        if not selected:
            await query.answer("Выбери хотя бы один аккаунт.", show_alert=True)
            return
        await state.set_state(ChatsJoinState.waiting_targets)
        await state.update_data(
            chats_selected=sorted(selected), chats_menu_msg=query.message.message_id
        )
        await _safe_edit_text(
            query,
            _build_wait_targets_text(user_id, len(selected)),
            _build_wait_targets_kb(),
        )
        return
    elif action.startswith("jc_acc_"):
        acc_num = int(action.split("_")[2])
        if acc_num in selected:
            selected.remove(acc_num)
        else:
            selected.add(acc_num)

    await state.set_state(ChatsJoinState.selecting_accounts)
    await state.update_data(
        chats_selected=sorted(selected), chats_menu_msg=query.message.message_id
    )
    await _safe_edit_text(
        query,
        _build_chats_accounts_text(user_id, selected),
        _build_chats_accounts_kb(user_id, selected),
    )


async def _restore_accounts_menu(message: Message, state: FSMContext, *, notice: str | None = None) -> None:
    data = await state.get_data()
    user_id = message.from_user.id
    menu_msg = data.get("chats_menu_msg")
    selected = set(data.get("chats_selected") or [])
    connected = _get_connected_accounts(user_id)
    connected_set = {acc_num for acc_num, _ in connected}
    selected = {x for x in selected if x in connected_set}
    await state.set_state(ChatsJoinState.selecting_accounts)
    await state.update_data(chats_selected=sorted(selected))
    if menu_msg:
        await _safe_edit_message(
            message.bot,
            chat_id=message.chat.id,
            message_id=int(menu_msg),
            text=_build_chats_accounts_text(user_id, selected),
            reply_markup=_build_chats_accounts_kb(user_id, selected),
        )
    if notice:
        await message.answer(notice)


@router.message(ChatsJoinState.waiting_delay)
async def chats_join_delay_input(message: Message, state: FSMContext):
    if not await _ensure_logged_message(message):
        await state.clear()
        return

    text = (message.text or "").strip().lower()
    if text in {"отмена", "назад", "/cancel"}:
        await _restore_accounts_menu(message, state)
        return

    parsed = _parse_range_input(message.text or "", minimum=1, maximum=86400)
    if not parsed:
        await message.answer("❌ Введи диапазон в секундах, например <code>15-25</code>.", parse_mode="HTML")
        return

    left, right = parsed
    _save_join_settings(message.from_user.id, delay_min=left, delay_max=right)
    await _restore_accounts_menu(
        message,
        state,
        notice=f"✅ Интервал обновлен: {_format_seconds_range(left, right)}.",
    )


@router.message(ChatsJoinState.waiting_rest_every)
async def chats_join_rest_every_input(message: Message, state: FSMContext):
    if not await _ensure_logged_message(message):
        await state.clear()
        return

    text = (message.text or "").strip().lower()
    if text in {"отмена", "назад", "/cancel"}:
        await state.set_state(ChatsJoinState.selecting_accounts)
        data = await state.get_data()
        menu_msg = data.get("chats_menu_msg")
        if menu_msg:
            await _safe_edit_message(
                message.bot,
                chat_id=message.chat.id,
                message_id=int(menu_msg),
                text=_build_rest_settings_text(message.from_user.id),
                reply_markup=_build_rest_settings_kb(),
            )
        return

    value = _parse_positive_int(message.text or "", minimum=0, maximum=1000)
    if value is None:
        await message.answer("❌ Введи число от 0 до 1000.")
        return

    _save_join_settings(message.from_user.id, rest_every=value)
    data = await state.get_data()
    menu_msg = data.get("chats_menu_msg")
    await state.set_state(ChatsJoinState.selecting_accounts)
    if menu_msg:
        await _safe_edit_message(
            message.bot,
            chat_id=message.chat.id,
            message_id=int(menu_msg),
            text=_build_rest_settings_text(message.from_user.id),
            reply_markup=_build_rest_settings_kb(),
        )
    await message.answer(
        "✅ Отдых выключен." if value == 0 else f"✅ Отдых теперь каждые {value} чатов."
    )


@router.message(ChatsJoinState.waiting_rest_duration)
async def chats_join_rest_duration_input(message: Message, state: FSMContext):
    if not await _ensure_logged_message(message):
        await state.clear()
        return

    text = (message.text or "").strip().lower()
    if text in {"отмена", "назад", "/cancel"}:
        await state.set_state(ChatsJoinState.selecting_accounts)
        data = await state.get_data()
        menu_msg = data.get("chats_menu_msg")
        if menu_msg:
            await _safe_edit_message(
                message.bot,
                chat_id=message.chat.id,
                message_id=int(menu_msg),
                text=_build_rest_settings_text(message.from_user.id),
                reply_markup=_build_rest_settings_kb(),
            )
        return

    parsed = _parse_range_input(message.text or "", minimum=1, maximum=86400)
    if not parsed:
        await message.answer("❌ Введи диапазон в секундах, например <code>60-120</code>.", parse_mode="HTML")
        return

    left, right = parsed
    _save_join_settings(message.from_user.id, rest_min=left, rest_max=right)
    data = await state.get_data()
    menu_msg = data.get("chats_menu_msg")
    await state.set_state(ChatsJoinState.selecting_accounts)
    if menu_msg:
        await _safe_edit_message(
            message.bot,
            chat_id=message.chat.id,
            message_id=int(menu_msg),
            text=_build_rest_settings_text(message.from_user.id),
            reply_markup=_build_rest_settings_kb(),
        )
    await message.answer(f"✅ Длительность отдыха обновлена: {_format_seconds_range(left, right)}.")


@router.message(ChatsJoinState.waiting_targets)
async def chats_join_targets_input(message: Message, state: FSMContext):
    if not await _ensure_logged_message(message):
        await state.clear()
        return

    user_id = message.from_user.id
    data = await state.get_data()
    selected = set(data.get("chats_selected") or [])

    if (message.text or "").strip().lower() in {"отмена", "/cancel"}:
        await state.clear()
        await message.answer("❌ Операция /chats отменена.")
        return

    links, usernames = _parse_chat_targets(message.text or "")
    if not links and not usernames:
        await message.answer(
            "❌ Не вижу валидных целей.\n"
            "Пример:\n"
            "<code>@channel_one\nhttps://t.me/channel_two</code>",
            parse_mode="HTML",
        )
        return

    if not selected:
        await message.answer("❌ Нет выбранных аккаунтов. Запусти /chats заново.")
        await state.clear()
        return

    settings = _get_join_settings(user_id)
    await state.clear()

    total_targets = len(links) + len(usernames)
    menu_msg = data.get("chats_menu_msg")
    status_text = (
        "🚀 <b>/chats запускается</b>\n\n"
        f"👤 Аккаунтов: <b>{len(selected)}</b>\n"
        f"🎯 Целей: <b>{total_targets}</b>\n"
        f"{_format_settings_summary(settings)}"
    )

    status_message_id = None
    if menu_msg:
        ok = await _safe_edit_message(
            message.bot,
            chat_id=message.chat.id,
            message_id=int(menu_msg),
            text=status_text,
            reply_markup=_build_runtime_kb(True),
        )
        if ok:
            status_message_id = int(menu_msg)

    if status_message_id is None:
        sent = await message.answer(
            status_text,
            reply_markup=_build_runtime_kb(True),
            parse_mode="HTML",
        )
        status_message_id = sent.message_id

    existing_task = mass_join_tasks.get(user_id)
    if existing_task and not existing_task.done():
        existing_task.cancel()

    mass_join_status[user_id] = {
        "chat_id": message.chat.id,
        "message_id": status_message_id,
        "started_at": time.time(),
        "accounts_total": len(selected),
        "targets_total": total_targets,
        "current_account_index": 0,
        "current_target_index": 0,
        "current_account_label": "Подготовка",
        "current_target_label": "—",
        "overall_joined": 0,
        "overall_already": 0,
        "overall_failed": 0,
        "phase": "Подготовка",
        "phase_extra": "Собираю клиентов и очередь целей.",
        "settings": settings,
        "running": True,
    }
    await _push_runtime_status(message.bot, user_id)

    mass_join_tasks[user_id] = asyncio.create_task(
        _run_mass_join(
            bot=message.bot,
            user_id=user_id,
            account_numbers=sorted(selected),
            links=links,
            usernames=usernames,
            status_message_id=status_message_id,
            status_chat_id=message.chat.id,
            settings=settings,
        )
    )


async def _join_target_once(
    client, *, link: str | None = None, username: str | None = None
) -> tuple[str, str]:
    target_label = link or (f"@{username}" if username else "-")
    try:
        if link:
            invite_hash = _extract_invite_hash(link)
            if invite_hash:
                await client(ImportChatInviteRequest(invite_hash))
                return "joined", target_label
            entity = await client.get_entity(link)
            await client(JoinChannelRequest(entity))
            return "joined", target_label
        if username:
            entity = await client.get_entity(username)
            await client(JoinChannelRequest(entity))
            return "joined", target_label
        return "skipped", target_label
    except UserAlreadyParticipantError:
        return "already", target_label
    except FloodWaitError as exc:
        wait_s = int(getattr(exc, "seconds", 60) or 60)
        await asyncio.sleep(wait_s + 2)
        return "floodwait", target_label
    except Exception:
        return "failed", target_label


async def _run_mass_join(
    *,
    bot,
    user_id: int,
    account_numbers: list[int],
    links: list[str],
    usernames: list[str],
    status_message_id: int,
    status_chat_id: int,
    settings: dict,
) -> None:
    targets: list[tuple[str, str]] = [("link", x) for x in links] + [
        ("username", x) for x in usernames
    ]
    total_targets = len(targets)
    account_titles = {
        acc_num: (name or f"Акк {acc_num}")
        for acc_num, name in _get_connected_accounts(user_id)
    }

    status = mass_join_status.get(user_id) or {}
    status["message_id"] = status_message_id
    status["chat_id"] = status_chat_id
    status["settings"] = settings
    status["running"] = True
    mass_join_status[user_id] = status

    overall_joined = 0
    overall_already = 0
    overall_failed = 0
    summary_lines = []

    try:
        for idx, acc_num in enumerate(account_numbers, start=1):
            client = await ensure_connected_client(
                user_id,
                acc_num,
                api_id=API_ID,
                api_hash=API_HASH,
            )
            acc_label_raw = account_titles.get(acc_num, f"Акк {acc_num}")
            acc_label = html.escape(acc_label_raw)

            status.update(
                {
                    "phase": "Подключение аккаунта",
                    "phase_extra": f"{acc_label_raw}",
                    "current_account_label": acc_label_raw,
                    "current_account_index": idx,
                    "current_target_label": "—",
                    "current_target_index": 0,
                }
            )
            await _push_runtime_status(bot, user_id)

            if not client:
                summary_lines.append(f"• {acc_label}: не подключен")
                overall_failed += total_targets
                status["overall_failed"] = overall_failed
                await _push_runtime_status(bot, user_id)
                continue

            joined = 0
            already = 0
            failed = 0

            for t_i, (kind, value) in enumerate(targets, start=1):
                status.update(
                    {
                        "phase": "Вступление",
                        "phase_extra": None,
                        "current_account_label": acc_label_raw,
                        "current_account_index": idx,
                        "current_target_label": value if kind == "link" else f"@{value}",
                        "current_target_index": t_i,
                    }
                )
                await _push_runtime_status(bot, user_id)

                if kind == "link":
                    result, _ = await _join_target_once(client, link=value)
                else:
                    result, _ = await _join_target_once(client, username=value)

                if result == "joined":
                    joined += 1
                    overall_joined += 1
                elif result == "already":
                    already += 1
                    overall_already += 1
                else:
                    failed += 1
                    overall_failed += 1

                status.update(
                    {
                        "overall_joined": overall_joined,
                        "overall_already": overall_already,
                        "overall_failed": overall_failed,
                    }
                )
                await _push_runtime_status(bot, user_id)

                if t_i < total_targets:
                    delay_s = random.uniform(
                        int(settings["delay_min"]), int(settings["delay_max"])
                    )
                    status.update(
                        {
                            "phase": "Интервал",
                            "phase_extra": f"Жду {int(delay_s)} сек перед следующей целью.",
                        }
                    )
                    await _push_runtime_status(bot, user_id)
                    await asyncio.sleep(delay_s)

                rest_every = int(settings.get("rest_every", 0) or 0)
                if rest_every > 0 and t_i % rest_every == 0 and t_i < total_targets:
                    rest_s = random.uniform(
                        int(settings["rest_min"]), int(settings["rest_max"])
                    )
                    status.update(
                        {
                            "phase": "Отдых",
                            "phase_extra": f"Сплю {int(rest_s)} сек после {t_i} целей.",
                        }
                    )
                    await _push_runtime_status(bot, user_id)
                    await asyncio.sleep(rest_s)

            summary_lines.append(
                f"• {acc_label}: ✅ {joined} | ☑️ {already} | ❌ {failed}"
            )

        status.update({"running": False, "phase": "Завершено", "phase_extra": None})
        final_text = _build_finished_text(status, summary_lines, cancelled=False)
        status["rendered_text"] = final_text
        await _safe_edit_message(
            bot,
            chat_id=status_chat_id,
            message_id=status_message_id,
            text=final_text,
            reply_markup=None,
        )
    except asyncio.CancelledError:
        status.update({"running": False, "phase": "Остановлено", "phase_extra": None})
        final_text = _build_finished_text(status, summary_lines, cancelled=True)
        status["rendered_text"] = final_text
        await _safe_edit_message(
            bot,
            chat_id=status_chat_id,
            message_id=status_message_id,
            text=final_text,
            reply_markup=None,
        )
        raise
    finally:
        task = mass_join_tasks.get(user_id)
        if task and task.done():
            mass_join_tasks.pop(user_id, None)
        elif task is asyncio.current_task():
            mass_join_tasks.pop(user_id, None)
