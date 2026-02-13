from aiogram import Router, F
from aiogram.filters.command import Command
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery

from core.state import app_state
from database import get_user_accounts
from services.join_service import set_enabled, is_enabled, set_target_accounts, get_target_accounts

router = Router()


def _build_menu(user_id: int) -> InlineKeyboardMarkup:
    accounts = get_user_accounts(user_id)
    enabled = is_enabled(user_id)
    selected = get_target_accounts(user_id)

    buttons = []
    toggle_text = "\u23f8\ufe0f \u0412\u044b\u043a\u043b\u044e\u0447\u0438\u0442\u044c" if enabled else "\u25b6\ufe0f \u0412\u043a\u043b\u044e\u0447\u0438\u0442\u044c"
    buttons.append([InlineKeyboardButton(text=toggle_text, callback_data="joins_toggle")])

    buttons.append([InlineKeyboardButton(text="\u2705 \u0412\u0441\u0435 \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u044b", callback_data="joins_all")])

    for acc_num, telegram_id, username, first_name, is_active in accounts:
        label = first_name or username or f"\u0410\u043a\u043a {acc_num}"
        is_selected = (not selected) or (acc_num in selected)
        prefix = "\u2705" if is_selected else "\u274c"
        buttons.append([InlineKeyboardButton(text=f"{prefix} {label}", callback_data=f"joins_acc_{acc_num}")])

    buttons.append([InlineKeyboardButton(text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434", callback_data="joins_close")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _build_text(user_id: int) -> str:
    enabled = is_enabled(user_id)
    selected = get_target_accounts(user_id)
    status = "\u2705 \u0412\u043a\u043b\u044e\u0447\u0435\u043d\u043e" if enabled else "\u23f8\ufe0f \u0412\u044b\u043a\u043b\u044e\u0447\u0435\u043d\u043e"
    mode = "\u0412\u0441\u0435 \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u044b" if not selected else f"\u0412\u044b\u0431\u0440\u0430\u043d\u043e: {len(selected)}"
    queue_len = len(app_state.joins_queue.get(user_id, []))
    return (
        "\u2699\ufe0f <b>/JOINS</b>\n\n"
        f"\u0421\u0442\u0430\u0442\u0443\u0441: {status}\n"
        f"\u0420\u0435\u0436\u0438\u043c: {mode}\n"
        f"\u041e\u0447\u0435\u0440\u0435\u0434\u044c: {queue_len}\n\n"
        "\u0411\u043e\u0442 \u0431\u0443\u0434\u0435\u0442 \u0430\u0432\u0442\u043e\u0432\u0441\u0442\u0443\u043f\u0430\u0442\u044c, \u0435\u0441\u043b\u0438 \u0432 \u0442\u0435\u043a\u0441\u0442\u0435 \u0435\u0441\u0442\u044c \u0441\u043b\u043e\u0432\u0430:\n"
        "\"\u043f\u043e\u0434\u043f\u0438\u0441\u0430\u0442\u044c\u0441\u044f\", \"\u0432\u0441\u0442\u0443\u043f\u0438\u0442\u044c\", \"\u043d\u0435\u043e\u0431\u0445\u043e\u0434\u0438\u043c\u043e\"."
    )


@router.message(Command("joins"))
async def joins_menu(message: Message):
    user_id = message.from_user.id
    text = _build_text(user_id)
    kb = _build_menu(user_id)
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "joins_toggle")
async def joins_toggle_callback(query: CallbackQuery):
    user_id = query.from_user.id
    set_enabled(user_id, not is_enabled(user_id))
    await query.answer()
    await query.message.edit_text(_build_text(user_id), reply_markup=_build_menu(user_id), parse_mode="HTML")


@router.callback_query(F.data == "joins_all")
async def joins_all_callback(query: CallbackQuery):
    user_id = query.from_user.id
    set_target_accounts(user_id, None)
    await query.answer()
    await query.message.edit_text(_build_text(user_id), reply_markup=_build_menu(user_id), parse_mode="HTML")


@router.callback_query(F.data.startswith("joins_acc_"))
async def joins_acc_callback(query: CallbackQuery):
    user_id = query.from_user.id
    acc_num = int(query.data.split("_")[2])
    selected = get_target_accounts(user_id)
    if not selected:
        # switch to manual mode
        selected = set(app_state.user_authenticated.get(user_id, {}).keys())
    if acc_num in selected:
        selected.remove(acc_num)
    else:
        selected.add(acc_num)
    set_target_accounts(user_id, list(selected))
    await query.answer()
    await query.message.edit_text(_build_text(user_id), reply_markup=_build_menu(user_id), parse_mode="HTML")


@router.callback_query(F.data == "joins_close")
async def joins_close_callback(query: CallbackQuery):
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass
