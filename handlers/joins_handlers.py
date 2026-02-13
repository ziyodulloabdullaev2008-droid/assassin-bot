import re

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters.command import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery

from core.state import app_state
from database import get_user_accounts
from services.join_service import (
    set_enabled,
    is_enabled,
    set_target_accounts,
    get_target_accounts,
    get_delay_config,
    set_delay_config,
)

router = Router()


class JoinsSettingsState(StatesGroup):
    waiting_per_target_range = State()
    waiting_between_chats_range = State()


def _parse_range(text: str) -> tuple[int, int] | None:
    raw = (text or "").strip()
    if not raw:
        return None
    m = re.search(r"(\d+)\s*[-:\s]\s*(\d+)", raw)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        return (a, b) if a <= b else (b, a)
    if raw.isdigit():
        value = int(raw)
        return value, value
    return None


async def _safe_edit_text(query: CallbackQuery, text: str, kb: InlineKeyboardMarkup) -> None:
    try:
        await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
        raise


async def _safe_refresh_main(query: CallbackQuery, user_id: int) -> None:
    await _safe_edit_text(query, _build_text(user_id), _build_menu(user_id))


async def _show_settings_menu(query: CallbackQuery, user_id: int) -> None:
    await _safe_edit_text(query, _build_settings_text(user_id), _build_settings_menu())


def _build_menu(user_id: int) -> InlineKeyboardMarkup:
    accounts = get_user_accounts(user_id)
    enabled = is_enabled(user_id)
    selected = get_target_accounts(user_id)

    buttons = []
    toggle_text = "⏸️ Выключить" if enabled else "▶️ Включить"
    buttons.append([InlineKeyboardButton(text=toggle_text, callback_data="joins_toggle")])
    buttons.append([InlineKeyboardButton(text="⚙️ Настройки", callback_data="joins_settings")])
    buttons.append([InlineKeyboardButton(text="✅ Все аккаунты", callback_data="joins_all")])

    for acc_num, _, username, first_name, _ in accounts:
        label = first_name or username or f"Акк {acc_num}"
        is_selected = (not selected) or (acc_num in selected)
        prefix = "✅" if is_selected else "❌"
        buttons.append([InlineKeyboardButton(text=f"{prefix} {label}", callback_data=f"joins_acc_{acc_num}")])

    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="joins_close")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _build_text(user_id: int) -> str:
    enabled = is_enabled(user_id)
    selected = get_target_accounts(user_id)
    cfg = get_delay_config(user_id)
    status = "✅ Включено" if enabled else "⏸️ Выключено"
    mode = "Все аккаунты" if not selected else f"Выбрано: {len(selected)}"
    queue_len = len(app_state.joins_queue.get(user_id, []))
    return (
        "⚙️ <b>/JOINS</b>\n\n"
        f"Статус: {status}\n"
        f"Режим: {mode}\n"
        f"Очередь: {queue_len}\n\n"
        f"⏱ Внутри заявки: <b>{cfg['per_target_min']}-{cfg['per_target_max']} сек</b>\n"
        f"⏳ Между чатами: <b>{cfg['between_chats_min']}-{cfg['between_chats_max']} сек</b>\n\n"
        "Ключевые слова: «подписаться», «вступить», «необходимо»."
    )


def _build_settings_text(user_id: int) -> str:
    cfg = get_delay_config(user_id)
    return (
        "⚙️ <b>Настройки /JOINS</b>\n\n"
        f"Внутри одной заявки (между ссылками/кнопками): <b>{cfg['per_target_min']}-{cfg['per_target_max']} сек</b>\n"
        f"Между заявками из разных чатов: <b>{cfg['between_chats_min']}-{cfg['between_chats_max']} сек</b>\n\n"
        "Нажми, что хочешь изменить."
    )


def _build_settings_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Внутри заявки", callback_data="joins_set_per_target")],
            [InlineKeyboardButton(text="✏️ Между чатами", callback_data="joins_set_between_chats")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="joins_settings_back")],
        ]
    )


def _build_input_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="joins_settings_cancel")]]
    )


@router.message(Command("joins"))
async def joins_menu(message: Message):
    user_id = message.from_user.id
    await message.answer(_build_text(user_id), reply_markup=_build_menu(user_id), parse_mode="HTML")


@router.callback_query(F.data == "joins_toggle")
async def joins_toggle_callback(query: CallbackQuery):
    user_id = query.from_user.id
    set_enabled(user_id, not is_enabled(user_id))
    await query.answer()
    await _safe_refresh_main(query, user_id)


@router.callback_query(F.data == "joins_all")
async def joins_all_callback(query: CallbackQuery):
    user_id = query.from_user.id
    set_target_accounts(user_id, None)
    await query.answer()
    await _safe_refresh_main(query, user_id)


@router.callback_query(F.data.startswith("joins_acc_"))
async def joins_acc_callback(query: CallbackQuery):
    user_id = query.from_user.id
    acc_num = int(query.data.split("_")[2])
    selected = get_target_accounts(user_id)
    if not selected:
        selected = set(app_state.user_authenticated.get(user_id, {}).keys())
    if acc_num in selected:
        selected.remove(acc_num)
    else:
        selected.add(acc_num)
    set_target_accounts(user_id, list(selected))
    await query.answer()
    await _safe_refresh_main(query, user_id)


@router.callback_query(F.data == "joins_settings")
async def joins_settings_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    await state.clear()
    await _show_settings_menu(query, query.from_user.id)


@router.callback_query(F.data == "joins_set_per_target")
async def joins_set_per_target_callback(query: CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    cfg = get_delay_config(user_id)
    await state.set_state(JoinsSettingsState.waiting_per_target_range)
    await state.update_data(menu_message_id=query.message.message_id, chat_id=query.message.chat.id)
    await query.answer()
    await _safe_edit_text(
        query,
        (
            "⏱ <b>Внутри заявки</b>\n\n"
            f"Сейчас: <b>{cfg['per_target_min']}-{cfg['per_target_max']} сек</b>\n"
            "Отправь новый диапазон в формате: <code>7-15</code>"
        ),
        _build_input_menu(),
    )


@router.callback_query(F.data == "joins_set_between_chats")
async def joins_set_between_chats_callback(query: CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    cfg = get_delay_config(user_id)
    await state.set_state(JoinsSettingsState.waiting_between_chats_range)
    await state.update_data(menu_message_id=query.message.message_id, chat_id=query.message.chat.id)
    await query.answer()
    await _safe_edit_text(
        query,
        (
            "⏳ <b>Между чатами</b>\n\n"
            f"Сейчас: <b>{cfg['between_chats_min']}-{cfg['between_chats_max']} сек</b>\n"
            "Отправь новый диапазон в формате: <code>20-30</code>"
        ),
        _build_input_menu(),
    )


@router.callback_query(F.data == "joins_settings_back")
async def joins_settings_back_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    await state.clear()
    await _safe_refresh_main(query, query.from_user.id)


@router.callback_query(F.data == "joins_settings_cancel")
async def joins_settings_cancel_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    await state.clear()
    await _show_settings_menu(query, query.from_user.id)


@router.message(JoinsSettingsState.waiting_per_target_range)
async def joins_per_target_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    rng = _parse_range(message.text or "")
    if not rng:
        await message.answer("❌ Формат: <code>7-15</code>", parse_mode="HTML")
        return
    min_v, max_v = rng
    if min_v < 1 or max_v > 600:
        await message.answer("❌ Допустимо: 1-600 сек")
        return

    set_delay_config(user_id, per_target_min=min_v, per_target_max=max_v)
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass

    menu_message_id = data.get("menu_message_id")
    chat_id = data.get("chat_id")
    if menu_message_id and chat_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id,
                message_id=menu_message_id,
                text=_build_settings_text(user_id),
                reply_markup=_build_settings_menu(),
                parse_mode="HTML",
            )
            return
        except Exception:
            pass
    await message.answer(_build_settings_text(user_id), reply_markup=_build_settings_menu(), parse_mode="HTML")


@router.message(JoinsSettingsState.waiting_between_chats_range)
async def joins_between_chats_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    rng = _parse_range(message.text or "")
    if not rng:
        await message.answer("❌ Формат: <code>20-30</code>", parse_mode="HTML")
        return
    min_v, max_v = rng
    if min_v < 1 or max_v > 3600:
        await message.answer("❌ Допустимо: 1-3600 сек")
        return

    set_delay_config(user_id, between_chats_min=min_v, between_chats_max=max_v)
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass

    menu_message_id = data.get("menu_message_id")
    chat_id = data.get("chat_id")
    if menu_message_id and chat_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id,
                message_id=menu_message_id,
                text=_build_settings_text(user_id),
                reply_markup=_build_settings_menu(),
                parse_mode="HTML",
            )
            return
        except Exception:
            pass
    await message.answer(_build_settings_text(user_id), reply_markup=_build_settings_menu(), parse_mode="HTML")


@router.callback_query(F.data == "joins_close")
async def joins_close_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    await state.clear()
    try:
        await query.message.delete()
    except Exception:
        pass
