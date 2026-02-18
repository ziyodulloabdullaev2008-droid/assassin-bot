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

from datetime import datetime, timezone

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
    set_status as set_broadcast_status,
    cleanup_old_broadcasts as cleanup_old_broadcasts_service,
)

from services.broadcast_config_service import get_broadcast_config

from services.broadcast_sender import schedule_broadcast_send

from services.broadcast_profiles_service import (
    ensure_active_config,
    sync_active_config_from_db,
)

from services.mention_utils import delete_message_after_delay

from ui.broadcast_ui import build_broadcast_keyboard, build_broadcast_menu_text

from ui.texts_ui import build_texts_keyboard, build_text_settings_keyboard

from ui.main_menu_ui import get_main_menu_keyboard

router = Router()

user_authenticated = app_state.user_authenticated

broadcast_update_lock = app_state.broadcast_update_lock

active_broadcasts = app_state.active_broadcasts
LOGIN_REQUIRED_TEXT = "\u274c \u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u0432\u043e\u0439\u0434\u0438 \u0447\u0435\u0440\u0435\u0437 /login"


def save_broadcast_config_with_profile(user_id: int, config: dict) -> None:

    ensure_active_config(user_id)

    save_broadcast_config(user_id, config)

    sync_active_config_from_db(user_id)


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
    waiting_for_plan_limit = State()

    waiting_for_text = State()

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
        try:
            await message_or_query.message.edit_text(
                text=info, reply_markup=kb, parse_mode="HTML"
            )

        except Exception as e:
            print(f"Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р С—РЎР‚Р С‘ edit_text: {e}")

            try:
                await message_or_query.message.answer(
                    info, reply_markup=kb, parse_mode="HTML"
                )

            except Exception as e2:
                print(f"Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р С—РЎР‚Р С‘ answer: {e2}")

    else:
        await message_or_query.answer(info, reply_markup=kb, parse_mode="HTML")


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


@router.message(Command("broadcast"))
@router.message(F.text == "СЂСџвЂњВ¤ Р В Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р В°")
async def cmd_broadcast_menu(message: Message):
    """Р вЂњР В»Р В°Р Р†Р Р…Р С•Р Вµ Р СР ВµР Р…РЎР‹ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘ - Р С‘Р Р…РЎвЂћР С•РЎР‚Р СР В°РЎвЂ Р С‘РЎРЏ Р С‘ РЎС“Р С—РЎР‚Р В°Р Р†Р В»Р ВµР Р…Р С‘Р Вµ"""

    user_id = message.from_user.id

    if user_id not in user_authenticated:
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
    """Р С›РЎвЂљР С”РЎР‚РЎвЂ№РЎвЂљРЎРЉ Р СР ВµР Р…РЎР‹ Р Р…Р В°РЎРѓРЎвЂљРЎР‚Р С•Р ВµР С” РЎвЂљР ВµР С”РЎРѓРЎвЂљР С•Р Р† (РЎР‚Р ВµР В¶Р С‘Р С Р С‘ РЎРѓР С—Р С‘РЎРѓР С•Р С”)"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    info = "СЂСџвЂњСњ Р Р€Р СџР В Р С’Р вЂ™Р вЂєР вЂўР СњР ВР вЂў Р СћР вЂўР С™Р РЋР СћР С’Р СљР В\n\n"

    info += f"Р СћР ВµР С”РЎРѓРЎвЂљР С•Р Р† Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р С•: {len(config['texts'])}\n"

    info += f"Р В Р ВµР В¶Р С‘Р С: {'Random РІСљвЂ¦' if config.get('text_mode') == 'random' else 'No Random РІСњРЉ'}\n"

    info += f"Р В¤Р С•РЎР‚Р СР В°РЎвЂљ: {config.get('parse_mode', 'HTML')}\n"

    kb = build_text_settings_keyboard(
        config.get("text_mode", "random"), config.get("parse_mode", "HTML")
    )

    await state.update_data(
        edit_message_id=query.message.message_id, chat_id=query.message.chat.id
    )

    await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "text_list")
async def text_list_callback(query: CallbackQuery, state: FSMContext):
    """Р СџР С•Р С”Р В°Р В·Р В°РЎвЂљРЎРЉ РЎРѓР С—Р С‘РЎРѓР С•Р С” РЎвЂљР ВµР С”РЎРѓРЎвЂљР С•Р Р† Р Т‘Р В»РЎРЏ РЎС“Р С—РЎР‚Р В°Р Р†Р В»Р ВµР Р…Р С‘РЎРЏ"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    if not config["texts"]:
        info = "СЂСџвЂњвЂћ Р РЋР СџР ВР РЋР С›Р С™ Р СћР вЂўР С™Р РЋР СћР С›Р вЂ™\n\n"

        info += "Р СњР ВµРЎвЂљ Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р Р…РЎвЂ№РЎвЂ¦ РЎвЂљР ВµР С”РЎРѓРЎвЂљР С•Р Р†.\n\n"

        info += "Р СњР В°Р В¶Р СР С‘ 'Р вЂќР С•Р В±Р В°Р Р†Р С‘РЎвЂљРЎРЉ Р Р…Р С•Р Р†РЎвЂ№Р в„–' РЎвЂЎРЎвЂљР С•Р В±РЎвЂ№ Р Т‘Р С•Р В±Р В°Р Р†Р С‘РЎвЂљРЎРЉ РЎвЂљР ВµР С”РЎРѓРЎвЂљ Р Т‘Р В»РЎРЏ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘."

    else:
        info = "СЂСџвЂњвЂћ Р РЋР СџР ВР РЋР С›Р С™ Р СћР вЂўР С™Р РЋР СћР С›Р вЂ™\n\n"

        info += f"Р вЂ™РЎРѓР ВµР С–Р С• РЎвЂљР ВµР С”РЎРѓРЎвЂљР С•Р Р†: {len(config['texts'])}\n"

        info += "Р вЂ™РЎвЂ№Р В±Р ВµРЎР‚Р С‘ РЎвЂљР ВµР С”РЎРѓРЎвЂљ Р Т‘Р В»РЎРЏ Р С—РЎР‚Р С•РЎРѓР СР С•РЎвЂљРЎР‚Р В° Р С‘Р В»Р С‘ РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘РЎРЏ.\n"

    kb = build_texts_keyboard(config["texts"], back_callback="bc_text")

    await state.update_data(
        edit_message_id=query.message.message_id, chat_id=query.message.chat.id
    )

    await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data.startswith("text_view_"))
async def text_view_callback(query: CallbackQuery, state: FSMContext):
    """Р СџР С•Р С”Р В°Р В·Р В°РЎвЂљРЎРЉ РЎвЂљР ВµР С”РЎРѓРЎвЂљ Р Т‘Р В»РЎРЏ Р С—РЎР‚Р С•РЎРѓР СР С•РЎвЂљРЎР‚Р В° Р С‘ РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘РЎРЏ"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    try:
        text_index = int(query.data.split("_")[2])

        if text_index >= len(config["texts"]):
            await query.answer(
                "РІСњРЉ Р СћР ВµР С”РЎРѓРЎвЂљ Р Р…Р Вµ Р Р…Р В°Р в„–Р Т‘Р ВµР Р…",
                show_alert=True,
            )

            return

        current_text = config["texts"][text_index]

        parse_mode = config.get("parse_mode", "HTML")

        info = f"СЂСџвЂњвЂ№ Р СћР вЂўР С™Р РЋР Сћ #{text_index + 1}\n\n"

        info += f"СЂСџвЂњСњ <b>Р В¤Р С•РЎР‚Р СР В°РЎвЂљ:</b> {parse_mode}\n"

        info += "РІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓРІвЂќРѓ\n"

        # Р СџР С•Р С”Р В°Р В·РЎвЂ№Р Р†Р В°Р ВµР С Р С—Р С•Р В»Р Р…РЎвЂ№Р в„– РЎвЂљР ВµР С”РЎРѓРЎвЂљ, Р Р…Р С• Р СР В°Р С”РЎРѓР С‘Р СРЎС“Р С 3500 РЎРѓР С‘Р СР Р†Р С•Р В»Р С•Р Р† Р Т‘Р В»РЎРЏ РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘РЎРЏ

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

        await state.update_data(
            edit_message_id=query.message.message_id, chat_id=query.message.chat.id
        )

        await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")

    except (ValueError, IndexError):
        await query.answer(
            "РІСњРЉ Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р С—РЎР‚Р С‘ Р Р†РЎвЂ№Р В±Р С•РЎР‚Р Вµ РЎвЂљР ВµР С”РЎРѓРЎвЂљР В°",
            show_alert=True,
        )


@router.callback_query(F.data == "text_add_new")
async def text_add_new_callback(query: CallbackQuery, state: FSMContext):
    """Р СњР В°РЎвЂЎР В°РЎвЂљРЎРЉ Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р С‘Р Вµ Р Р…Р С•Р Р†Р С•Р С–Р С• РЎвЂљР ВµР С”РЎРѓРЎвЂљР В°"""

    await query.answer()

    await state.set_state(BroadcastConfigState.waiting_for_text_add)

    await state.update_data(
        edit_message_id=query.message.message_id, chat_id=query.message.chat.id
    )

    text = "СЂСџвЂњСњ Р вЂќР С›Р вЂР С’Р вЂ™Р ВР СћР В¬ Р СњР С›Р вЂ™Р В«Р в„ў Р СћР вЂўР С™Р РЋР Сћ\n\n"

    text += "Р вЂ™Р Р†Р ВµР Т‘Р С‘ РЎвЂљР ВµР С”РЎРѓРЎвЂљ Р Т‘Р В»РЎРЏ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘.\n\n"

    text += "СЂСџвЂ™РЋ <b>Р СџР С•Р Т‘Р Т‘Р ВµРЎР‚Р В¶Р С‘Р Р†Р В°Р ВµРЎвЂљРЎРѓРЎРЏ РЎвЂћР С•РЎР‚Р СР В°РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘Р Вµ HTML:</b>\n"

    text += "<b>Р В¶Р С‘РЎР‚Р Р…РЎвЂ№Р в„–</b>, <i>Р С”РЎС“РЎР‚РЎРѓР С‘Р Р†</i>, <u>Р С—Р С•Р Т‘РЎвЂЎР ВµРЎР‚Р С”Р С‘Р Р†Р В°Р Р…Р С‘Р Вµ</u>\n"

    text += "Р СџР ВµРЎР‚Р ВµР Р…Р С•РЎРѓРЎвЂ№ РЎРѓРЎвЂљРЎР‚Р С•Р С” РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏРЎР‹РЎвЂљРЎРѓРЎРЏ.\n"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ", callback_data="text_list"
                )
            ]
        ]
    )

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data.startswith("text_edit_"))
async def text_edit_callback(query: CallbackQuery, state: FSMContext):
    """Р СњР В°РЎвЂЎР В°РЎвЂљРЎРЉ РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘Р Вµ РЎвЂљР ВµР С”РЎРѓРЎвЂљР В°"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    try:
        text_index = int(query.data.split("_")[2])

        if text_index >= len(config["texts"]):
            await query.answer(
                "РІСњРЉ Р СћР ВµР С”РЎРѓРЎвЂљ Р Р…Р Вµ Р Р…Р В°Р в„–Р Т‘Р ВµР Р…",
                show_alert=True,
            )

            return

        await state.set_state(BroadcastConfigState.waiting_for_text_edit)

        await state.update_data(
            edit_message_id=query.message.message_id,
            chat_id=query.message.chat.id,
            text_index=text_index,
        )

        text = f"РІСљРЏРїС‘РЏ Р В Р вЂўР вЂќР С’Р С™Р СћР ВР В Р С›Р вЂ™Р С’Р СћР В¬ Р СћР вЂўР С™Р РЋР Сћ #{text_index + 1}\n\n"

        text += (
            "Р вЂ™Р Р†Р ВµР Т‘Р С‘ Р Р…Р С•Р Р†РЎвЂ№Р в„– РЎвЂљР ВµР С”РЎРѓРЎвЂљ.\n\n"
        )

        text += "СЂСџвЂ™РЋ <b>Р СџР С•Р Т‘Р Т‘Р ВµРЎР‚Р В¶Р С‘Р Р†Р В°Р ВµРЎвЂљРЎРѓРЎРЏ РЎвЂћР С•РЎР‚Р СР В°РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘Р Вµ HTML:</b>\n"

        text += "<b>Р В¶Р С‘РЎР‚Р Р…РЎвЂ№Р в„–</b>, <i>Р С”РЎС“РЎР‚РЎРѓР С‘Р Р†</i>, <u>Р С—Р С•Р Т‘РЎвЂЎР ВµРЎР‚Р С”Р С‘Р Р†Р В°Р Р…Р С‘Р Вµ</u>\n"

        text += "Р СџР ВµРЎР‚Р ВµР Р…Р С•РЎРѓРЎвЂ№ РЎРѓРЎвЂљРЎР‚Р С•Р С” РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏРЎР‹РЎвЂљРЎРѓРЎРЏ.\n"

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ",
                        callback_data=f"text_view_{text_index}",
                    )
                ]
            ]
        )

        await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

    except (ValueError, IndexError):
        await query.answer(
            "РІСњРЉ Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р С—РЎР‚Р С‘ Р Р†РЎвЂ№Р В±Р С•РЎР‚Р Вµ РЎвЂљР ВµР С”РЎРѓРЎвЂљР В°",
            show_alert=True,
        )


@router.callback_query(F.data.startswith("text_delete_"))
async def text_delete_callback(query: CallbackQuery, state: FSMContext):
    """Р Р€Р Т‘Р В°Р В»Р С‘РЎвЂљРЎРЉ РЎвЂљР ВµР С”РЎРѓРЎвЂљ"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    try:
        text_index = int(query.data.split("_")[2])

        if text_index >= len(config["texts"]):
            await query.answer(
                "РІСњРЉ Р СћР ВµР С”РЎРѓРЎвЂљ Р Р…Р Вµ Р Р…Р В°Р в„–Р Т‘Р ВµР Р…",
                show_alert=True,
            )

            return

        # Р Р€Р Т‘Р В°Р В»РЎРЏР ВµР С РЎвЂљР ВµР С”РЎРѓРЎвЂљ

        config["texts"].pop(text_index)

        save_broadcast_config_with_profile(user_id, config)

        # Р вЂўРЎРѓР В»Р С‘ РЎРЊРЎвЂљР С• Р В±РЎвЂ№Р В» Р С—Р С•РЎРѓР В»Р ВµР Т‘Р Р…Р С‘Р в„– РЎвЂљР ВµР С”РЎРѓРЎвЂљ, РЎРѓР В±РЎР‚Р В°РЎРѓРЎвЂ№Р Р†Р В°Р ВµР С Р С‘Р Р…Р Т‘Р ВµР С”РЎРѓ

        if text_index >= len(config["texts"]) and text_index > 0:
            config["text_index"] = len(config["texts"]) - 1

            save_broadcast_config_with_profile(user_id, config)

        await query.answer(
            "РІСљвЂ¦ Р СћР ВµР С”РЎРѓРЎвЂљ РЎС“Р Т‘Р В°Р В»Р ВµР Р…", show_alert=False
        )

        # Р СџР С•Р С”Р В°Р В·РЎвЂ№Р Р†Р В°Р ВµР С Р С•Р В±Р Р…Р С•Р Р†Р В»Р ВµР Р…Р Р…РЎвЂ№Р в„– РЎРѓР С—Р С‘РЎРѓР С•Р С”

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

        await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")

    except (ValueError, IndexError):
        await query.answer(
            "РІСњРЉ Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р С—РЎР‚Р С‘ РЎС“Р Т‘Р В°Р В»Р ВµР Р…Р С‘Р С‘ РЎвЂљР ВµР С”РЎРѓРЎвЂљР В°",
            show_alert=True,
        )


@router.callback_query(F.data == "text_mode_toggle")
async def text_mode_toggle_callback(query: CallbackQuery, state: FSMContext):
    """Р СџР ВµРЎР‚Р ВµР С”Р В»РЎР‹РЎвЂЎР С‘РЎвЂљРЎРЉ РЎР‚Р ВµР В¶Р С‘Р С РЎвЂљР ВµР С”РЎРѓРЎвЂљР С•Р Р† (random <-> sequence)"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    if not config["texts"]:
        await query.answer(
            "РІСњРЉ Р вЂќР С•Р В±Р В°Р Р†РЎРЉ РЎРѓР Р…Р В°РЎвЂЎР В°Р В»Р В° РЎвЂљР ВµР С”РЎРѓРЎвЂљРЎвЂ№",
            show_alert=True,
        )

        return

    # Р СџР ВµРЎР‚Р ВµР С”Р В»РЎР‹РЎвЂЎР В°Р ВµР С РЎР‚Р ВµР В¶Р С‘Р С

    config["text_mode"] = (
        "sequence" if config.get("text_mode") == "random" else "random"
    )

    config["text_index"] = (
        0  # Р РЋР В±РЎР‚Р В°РЎРѓРЎвЂ№Р Р†Р В°Р ВµР С Р С‘Р Р…Р Т‘Р ВµР С”РЎРѓ Р С—РЎР‚Р С‘ Р С—Р ВµРЎР‚Р ВµР С”Р В»РЎР‹РЎвЂЎР ВµР Р…Р С‘Р С‘
    )

    save_broadcast_config_with_profile(user_id, config)

    # Р СџР С•Р С”Р В°Р В·РЎвЂ№Р Р†Р В°Р ВµР С Р С•Р В±Р Р…Р С•Р Р†Р В»Р ВµР Р…Р Р…Р С•Р Вµ Р СР ВµР Р…РЎР‹

    info = "СЂСџвЂњСњ Р Р€Р СџР В Р С’Р вЂ™Р вЂєР вЂўР СњР ВР вЂў Р СћР вЂўР С™Р РЋР СћР С’Р СљР В\n\n"

    info += f"Р СћР ВµР С”РЎРѓРЎвЂљР С•Р Р† Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р С•: {len(config['texts'])}\n"

    info += f"Р В Р ВµР В¶Р С‘Р С: {'Random РІСљвЂ¦' if config.get('text_mode') == 'random' else 'No Random РІСњРЉ'}\n"

    info += f"Р В¤Р С•РЎР‚Р СР В°РЎвЂљ: {config.get('parse_mode', 'HTML')}\n"

    kb = build_text_settings_keyboard(
        config.get("text_mode", "random"), config.get("parse_mode", "HTML")
    )

    await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "text_format_toggle")
async def text_format_toggle_callback(query: CallbackQuery, state: FSMContext):
    """Р СџР ВµРЎР‚Р ВµР С”Р В»РЎР‹РЎвЂЎР С‘РЎвЂљРЎРЉ РЎвЂћР С•РЎР‚Р СР В°РЎвЂљ РЎвЂљР ВµР С”РЎРѓРЎвЂљР С•Р Р† (HTML <-> Markdown)"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    # Р СџР ВµРЎР‚Р ВµР С”Р В»РЎР‹РЎвЂЎР В°Р ВµР С РЎвЂћР С•РЎР‚Р СР В°РЎвЂљ

    config["parse_mode"] = "Markdown" if config.get("parse_mode") == "HTML" else "HTML"

    save_broadcast_config_with_profile(user_id, config)

    # Р СџР С•Р С”Р В°Р В·РЎвЂ№Р Р†Р В°Р ВµР С Р С•Р В±Р Р…Р С•Р Р†Р В»Р ВµР Р…Р Р…Р С•Р Вµ Р СР ВµР Р…РЎР‹

    info = "СЂСџвЂњСњ Р Р€Р СџР В Р С’Р вЂ™Р вЂєР вЂўР СњР ВР вЂў Р СћР вЂўР С™Р РЋР СћР С’Р СљР В\n\n"

    info += f"Р СћР ВµР С”РЎРѓРЎвЂљР С•Р Р† Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р С•: {len(config['texts'])}\n"

    info += f"Р В Р ВµР В¶Р С‘Р С: {'Random РІСљвЂ¦' if config.get('text_mode') == 'random' else 'No Random РІСњРЉ'}\n"

    info += f"Р В¤Р С•РЎР‚Р СР В°РЎвЂљ: {config.get('parse_mode', 'HTML')}\n"

    kb = build_text_settings_keyboard(
        config.get("text_mode", "random"), config.get("parse_mode", "HTML")
    )

    await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "bc_quantity")
async def bc_quantity_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    await state.set_state(BroadcastConfigState.waiting_for_count)

    await state.update_data(
        edit_message_id=query.message.message_id, chat_id=query.message.chat.id
    )

    config = get_broadcast_config(query.from_user.id)

    text = f"СЂСџвЂњР‰ Р С™Р С›Р вЂєР ВР В§Р вЂўР РЋР СћР вЂ™Р С› Р РЋР С›Р С›Р вЂР В©Р вЂўР СњР ВР в„ў\n\nР СћР ВµР С”РЎС“РЎвЂ°Р ВµР Вµ: {config.get('count', 0)}\n\nР вЂ™Р Р†Р ВµР Т‘Р С‘ Р Р…Р С•Р Р†Р С•Р Вµ (1-1000) Р С‘Р В»Р С‘ Р Р…Р В°Р В¶Р СР С‘ Р С•РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ:"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="РІСњРЉ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ",
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

    current_interval = config.get("interval", "10-30")

    text = f"РІРЏВ±РїС‘РЏ <b>Р ВР СњР СћР вЂўР В Р вЂ™Р С’Р вЂє Р СљР вЂўР вЂ“Р вЂќР Р€ Р РЋР С›Р С›Р вЂР В©Р вЂўР СњР ВР Р‡Р СљР В</b>\n\nР СћР ВµР С”РЎС“РЎвЂ°Р С‘Р в„–: {current_interval} Р СР С‘Р Р…\n\nР вЂ™Р Р†Р ВµР Т‘Р С‘ Р Р…Р С•Р Р†РЎвЂ№Р в„– (РЎвЂћР С•РЎР‚Р СР В°РЎвЂљ: Р СР С‘Р Р…-Р СР В°Р С”РЎРѓ, Р Р…Р В°Р С—РЎР‚Р С‘Р СР ВµРЎР‚: 10-30) Р С‘Р В»Р С‘ Р С•Р Т‘Р Р…Р С• РЎвЂЎР С‘РЎРѓР В»Р С• (15):"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="РІСњРЉ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ",
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

    current_pause = config.get("chat_pause", "1-3")

    text = (
        f"РІРЏС– <b>Р СћР вЂўР СљР Сџ</b>\n\n"
        "Р СћР ВµР СР С— = Р В·Р В°Р Т‘Р ВµРЎР‚Р В¶Р С”Р В° Р СР ВµР В¶Р Т‘РЎС“ Р С•РЎвЂљР С—РЎР‚Р В°Р Р†Р С”Р В°Р СР С‘ Р С—Р С• РЎР‚Р В°Р В·Р Р…РЎвЂ№Р С РЎвЂЎР В°РЎвЂљР В°Р С Р Р†Р С• Р Р†РЎР‚Р ВµР СРЎРЏ Р В°Р С”РЎвЂљР С‘Р Р†Р Р…Р С•Р в„– РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘.\n\n"
        f"Р СћР ВµР С”РЎС“РЎвЂ°Р С‘Р в„–: <b>{current_pause}</b> РЎРѓР ВµР С”\n\n"
        "Р вЂ™Р Р†Р ВµР Т‘Р С‘ Р Р…Р С•Р Р†РЎвЂ№Р в„–:\n"
        "РІР‚Сћ Р Т‘Р С‘Р В°Р С—Р В°Р В·Р С•Р Р…: <code>1-3</code>\n"
        "РІР‚Сћ Р С•Р Т‘Р Р…Р С• Р В·Р Р…Р В°РЎвЂЎР ВµР Р…Р С‘Р Вµ: <code>2</code>"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="РІСњРЉ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ",
                    callback_data="bc_cancel_tempo",
                )
            ]
        ]
    )

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "bc_plan_limit")
async def bc_plan_limit_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    await state.set_state(BroadcastConfigState.waiting_for_plan_limit)
    await state.update_data(
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
        previous_menu="broadcast",
    )

    config = get_broadcast_config(query.from_user.id)
    limit_count = config.get("plan_limit_count", 0)
    limit_rest = config.get("plan_limit_rest", 0)

    text = (
        "РІРЏС– <b>Р вЂєР ВР СљР ВР Сћ</b>\n\n"
        "Р вЂєР С‘Р СР С‘РЎвЂљ = РЎРѓР С”Р С•Р В»РЎРЉР С”Р С• РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р в„– Р С—Р В»Р В°Р Р…Р С‘РЎР‚Р С•Р Р†Р В°РЎвЂљРЎРЉ Р С•Р Т‘Р Р…Р С•Р Р†РЎР‚Р ВµР СР ВµР Р…Р Р…Р С• Р С‘ Р С”Р В°Р С”Р С•Р в„– Р С•РЎвЂљР Т‘РЎвЂ№РЎвЂ¦ Р Т‘Р ВµР В»Р В°РЎвЂљРЎРЉ Р С—Р С•РЎРѓР В»Р Вµ Р С—Р В°Р С”Р ВµРЎвЂљР В°.\n\n"
        f"Р СћР ВµР С”РЎС“РЎвЂ°Р С‘Р в„–: <b>{limit_count}</b> / Р С•РЎвЂљР Т‘РЎвЂ№РЎвЂ¦ <b>{limit_rest}</b> Р СР С‘Р Р…\n\n"
        "Р вЂ™Р Р†Р ВµР Т‘Р С‘ Р Т‘Р Р†Р В° РЎвЂЎР С‘РЎРѓР В»Р В° РЎвЂЎР ВµРЎР‚Р ВµР В· Р С—РЎР‚Р С•Р В±Р ВµР В»:\n"
        "<code>Р В»Р С‘Р СР С‘РЎвЂљ Р С•РЎвЂљР Т‘РЎвЂ№РЎвЂ¦_Р Р†_Р СР С‘Р Р…РЎС“РЎвЂљР В°РЎвЂ¦</code>\n"
        "Р СџРЎР‚Р С‘Р СР ВµРЎР‚: <code>10 3</code>\n"
        "Р С›РЎвЂљР С”Р В»РЎР‹РЎвЂЎР С‘РЎвЂљРЎРЉ Р В»Р С‘Р СР С‘РЎвЂљ: <code>0 0</code>"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="\u274c \u041e\u0442\u043c\u0435\u043d\u0438\u0442\u044c",
                    callback_data="bc_cancel",
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

        try:
            await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

        except Exception:
            await query.message.answer(text, reply_markup=kb, parse_mode="HTML")

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

    buttons = []

    for gid, items in sorted(groups.items()):
        status = (
            "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
            if any(b["status"] == "running" for _, b in items)
            else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
        )

        info += f"\u0413\u0440\u0443\u043f\u043f\u0430 #{gid} {status} | \u0410\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432: {len(items)}\n"

        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"\u0413\u0440\u0443\u043f\u043f\u0430 #{gid}",
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

        info += f"\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 #{bid} {status} | {account_name}\n"

        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 #{bid}",
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

    try:
        await query.message.edit_text(
            info,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML",
        )

    except Exception:
        await query.message.answer(
            info,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML",
        )


async def _render_group_detail(query: CallbackQuery, user_id: int, gid: int) -> None:

    items = [
        (bid, b)
        for bid, b in active_broadcasts.items()
        if b.get("group_id") == gid
        and b.get("user_id") == user_id
        and b.get("status") in ("running", "paused")
    ]

    if not items:
        await query.answer(
            "\u0413\u0440\u0443\u043f\u043f\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )

        return

    total_accounts = len(items)

    total_chats = sum(b.get("total_chats", 0) for _, b in items)

    total_count = sum((b.get("total_chats", 0) * b.get("count", 0)) for _, b in items)

    sent = sum(b.get("sent_chats", 0) for _, b in items)

    status = (
        "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
        if any(b["status"] == "running" for _, b in items)
        else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
    )

    info = f"\U0001f4e6 <b>\u0413\u0440\u0443\u043f\u043f\u0430 #{gid}</b>\n\n"

    info += f"\u0421\u0442\u0430\u0442\u0443\u0441: {status}\n"

    info += (
        f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432: {total_accounts}\n"
    )

    info += f"\u0427\u0430\u0442\u043e\u0432: {total_chats}\n"

    info += f"\u041f\u043b\u0430\u043d: {total_count}\n"

    info += f"\u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e: {sent}\n\n"

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
                text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                callback_data="bc_active",
            )
        ],
    ]

    try:
        await query.message.edit_text(
            info,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML",
        )

    except Exception:
        await query.message.answer(
            info,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML",
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

    status = (
        "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
        if b["status"] == "running"
        else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
        if b["status"] == "paused"
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

    info += f"\u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e: {b.get('sent_chats', 0)}\n"

    info += f"\u041a\u043e\u043b-\u0432\u043e: {b.get('count', 0)}\n"

    info += f"\u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b: {b.get('interval_minutes', '?')} \u043c\u0438\u043d\n"

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
        ],
        [
            InlineKeyboardButton(
                text="\u26d4 \u041e\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u044c",
                callback_data=f"cancel_bc_{bid}",
            )
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
        [
            InlineKeyboardButton(
                text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                callback_data="bc_active",
            )
        ],
    ]

    try:
        await query.message.edit_text(
            info,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML",
        )

    except Exception:
        await query.message.answer(
            info,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML",
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

    await view_bc_callback(query)


@router.callback_query(F.data.startswith("cancel_bc_"))
async def cancel_bc_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    bid = int(query.data.split("_")[2])

    if bid in active_broadcasts and active_broadcasts[bid]["user_id"] == user_id:
        await set_broadcast_status(bid, "cancelled")

    await bc_active_callback(query)


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


@router.message(F.text == "СЂСџвЂњР‰ Р С™Р С•Р В»Р С‘РЎвЂЎР ВµРЎРѓРЎвЂљР Р†Р С•")
async def broadcast_count_button(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С”Р Р…Р С•Р С—Р С”Р С‘ Р Р†РЎвЂ№Р В±Р С•РЎР‚Р В° Р С”Р С•Р В»Р С‘РЎвЂЎР ВµРЎРѓРЎвЂљР Р†Р В° РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р в„–"""

    user_id = message.from_user.id

    config = get_broadcast_config(user_id)

    await state.update_data(previous_menu="broadcast")

    await state.set_state(BroadcastConfigState.waiting_for_count)

    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="РІвЂ В©РїС‘РЏ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ")]
        ],
        resize_keyboard=True,
    )

    await message.answer(
        f"СЂСџвЂњР‰ Р С™Р С›Р вЂєР ВР В§Р вЂўР РЋР СћР вЂ™Р С› Р РЋР С›Р С›Р вЂР В©Р вЂўР СњР ВР в„ў\n\nСЂСџвЂњРЉ Р СћР ВµР С”РЎС“РЎвЂ°Р ВµР Вµ: {config.get('count', 0)} РЎв‚¬РЎвЂљ\n\nР С›РЎвЂљР С—РЎР‚Р В°Р Р†РЎРЉ Р Р…Р С•Р Р†Р С•Р Вµ Р С”Р С•Р В»Р С‘РЎвЂЎР ВµРЎРѓРЎвЂљР Р†Р С•:\n(РЎвЂЎР С‘РЎРѓР В»Р С• Р С•РЎвЂљ 1 Р Т‘Р С• 1000)",
        reply_markup=keyboard,
    )


@router.message(BroadcastConfigState.waiting_for_count)
async def process_broadcast_count(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С—Р С•Р В»РЎС“РЎвЂЎР ВµР Р…Р С‘РЎРЏ Р С”Р С•Р В»Р С‘РЎвЂЎР ВµРЎРѓРЎвЂљР Р†Р В° РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р в„–"""

    user_id = message.from_user.id

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С РЎвЂЎРЎвЂљР С• РЎРЊРЎвЂљР С• Р Р…Р Вµ Р С”Р Р…Р С•Р С—Р С”Р В° Р С•РЎвЂљР СР ВµР Р…РЎвЂ№

    if message.text == "РІвЂ В©РїС‘РЏ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ":
        await return_to_previous_menu(message, state)

        return

    try:
        count = int(message.text)

        if count < 1 or count > 1000:
            await message.answer(
                "РІСњРЉ Р С™Р С•Р В»Р С‘РЎвЂЎР ВµРЎРѓРЎвЂљР Р†Р С• Р Т‘Р С•Р В»Р В¶Р Р…Р С• Р В±РЎвЂ№РЎвЂљРЎРЉ Р С•РЎвЂљ 1 Р Т‘Р С• 1000"
            )

            return

        config = get_broadcast_config(user_id)

        config["count"] = count

        save_broadcast_config_with_profile(user_id, config)

        data = await state.get_data()

        edit_message_id = data.get("edit_message_id")

        chat_id = data.get("chat_id")

        await state.clear()

        # Р Р€Р Т‘Р В°Р В»РЎРЏР ВµР С РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ Р С—Р С•Р В»РЎРЉР В·Р С•Р Р†Р В°РЎвЂљР ВµР В»РЎРЏ

        try:
            await message.delete()

        except Exception:
            pass

        # Р В Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚РЎС“Р ВµР С РЎвЂљР С• Р В¶Р Вµ РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ РЎРѓ Р С‘Р Р…РЎвЂћР С•РЎР‚Р СР В°РЎвЂ Р С‘Р ВµР в„– Р С• РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р Вµ Р С‘Р В»Р С‘ Р С•РЎвЂљР С—РЎР‚Р В°Р Р†Р В»РЎРЏР ВµР С Р Р…Р С•Р Р†Р С•Р Вµ

        chats = get_broadcast_chats(user_id)

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
                    "Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р С—РЎР‚Р С‘ Р С•Р В±Р Р…Р С•Р Р†Р В»Р ВµР Р…Р С‘Р С‘ Р СР ВµР Р…РЎР‹"
                )

        else:
            await cmd_broadcast_menu(message)

    except ValueError:
        await message.answer("РІСњРЉ Р вЂ™Р Р†Р ВµР Т‘Р С‘ РЎвЂЎР С‘РЎРѓР В»Р С•!")


@router.message(F.text == "РІРЏВ±РїС‘РЏ Р ВР Р…РЎвЂљР ВµРЎР‚Р Р†Р В°Р В»")
async def broadcast_interval_button(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С”Р Р…Р С•Р С—Р С”Р С‘ Р Р†РЎвЂ№Р В±Р С•РЎР‚Р В° Р С‘Р Р…РЎвЂљР ВµРЎР‚Р Р†Р В°Р В»Р В°"""

    user_id = message.from_user.id

    config = get_broadcast_config(user_id)

    await state.update_data(previous_menu="broadcast")

    await state.set_state(BroadcastConfigState.waiting_for_interval)

    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="РІвЂ В©РїС‘РЏ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ")]
        ],
        resize_keyboard=True,
    )

    await message.answer(
        f"РІРЏВ±РїС‘РЏ Р ВР СњР СћР вЂўР В Р вЂ™Р С’Р вЂє Р СљР вЂўР вЂ“Р вЂќР Р€ Р РЋР С›Р С›Р вЂР В©Р вЂўР СњР ВР Р‡Р СљР В\n\nСЂСџвЂњРЉ Р СћР ВµР С”РЎС“РЎвЂ°Р С‘Р в„–: {config.get('interval', 0)} Р СР С‘Р Р…\n\nР С›РЎвЂљР С—РЎР‚Р В°Р Р†РЎРЉ Р Р…Р С•Р Р†РЎвЂ№Р в„– Р С‘Р Р…РЎвЂљР ВµРЎР‚Р Р†Р В°Р В» Р Р† Р СР С‘Р Р…РЎС“РЎвЂљР В°РЎвЂ¦:\n(РЎвЂЎР С‘РЎРѓР В»Р С• Р С•РЎвЂљ 1 Р Т‘Р С• 60 Р СР С‘Р Р…)",
        reply_markup=keyboard,
    )


@router.message(BroadcastConfigState.waiting_for_interval)
async def process_broadcast_interval(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С—Р С•Р В»РЎС“РЎвЂЎР ВµР Р…Р С‘РЎРЏ Р С‘Р Р…РЎвЂљР ВµРЎР‚Р Р†Р В°Р В»Р В°"""

    user_id = message.from_user.id

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С РЎвЂЎРЎвЂљР С• РЎРЊРЎвЂљР С• Р Р…Р Вµ Р С”Р Р…Р С•Р С—Р С”Р В° Р С•РЎвЂљР СР ВµР Р…РЎвЂ№

    if message.text == "РІвЂ В©РїС‘РЏ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ":
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
                    "РІСњРЉ Р СњР ВµР Р†Р ВµРЎР‚Р Р…РЎвЂ№Р в„– РЎвЂћР С•РЎР‚Р СР В°РЎвЂљ. Р ВРЎРѓР С—Р С•Р В»РЎРЉР В·РЎС“Р в„–: 10-30 Р С‘Р В»Р С‘ 15"
                )

                return

            try:
                min_interval = int(parts[0].strip())

                max_interval = int(parts[1].strip())

                if min_interval < 1 or max_interval < 1 or min_interval > max_interval:
                    await message.answer(
                        "РІСњРЉ Р вЂ”Р Р…Р В°РЎвЂЎР ВµР Р…Р С‘РЎРЏ Р Т‘Р С•Р В»Р В¶Р Р…РЎвЂ№ Р В±РЎвЂ№РЎвЂљРЎРЉ Р С—Р С•Р В»Р С•Р В¶Р С‘РЎвЂљР ВµР В»РЎРЉР Р…РЎвЂ№Р СР С‘, Р СР С‘Р Р… РІвЂ°В¤ Р СР В°Р С”РЎРѓ"
                    )

                    return

                if min_interval > 480 or max_interval > 480:
                    await message.answer(
                        "РІСњРЉ Р ВР Р…РЎвЂљР ВµРЎР‚Р Р†Р В°Р В» Р Р…Р Вµ Р Т‘Р С•Р В»Р В¶Р ВµР Р… Р В±РЎвЂ№РЎвЂљРЎРЉ Р В±Р С•Р В»РЎРЉРЎв‚¬Р Вµ 480 Р СР С‘Р Р…РЎС“РЎвЂљ (8 РЎвЂЎР В°РЎРѓР С•Р Р†)"
                    )

                    return

                interval_value = text  # Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµР С Р С”Р В°Р С” РЎРѓРЎвЂљРЎР‚Р С•Р С”РЎС“ "Р СР С‘Р Р…-Р СР В°Р С”РЎРѓ"

            except ValueError:
                await message.answer(
                    "РІСњРЉ Р вЂ™Р Р†Р ВµР Т‘Р С‘ РЎвЂЎР С‘РЎРѓР В»Р В° Р Р† РЎвЂћР С•РЎР‚Р СР В°РЎвЂљР Вµ: 10-30"
                )

                return

        else:
            # Р С›Р Т‘Р Р…Р С• РЎвЂЎР С‘РЎРѓР В»Р С•

            try:
                interval_int = int(text)

                if interval_int < 1 or interval_int > 480:
                    await message.answer(
                        "РІСњРЉ Р ВР Р…РЎвЂљР ВµРЎР‚Р Р†Р В°Р В» Р Т‘Р С•Р В»Р В¶Р ВµР Р… Р В±РЎвЂ№РЎвЂљРЎРЉ Р С•РЎвЂљ 1 Р Т‘Р С• 480 Р СР С‘Р Р…РЎС“РЎвЂљ"
                    )

                    return

                interval_value = text

            except ValueError:
                await message.answer(
                    "РІСњРЉ Р вЂ™Р Р†Р ВµР Т‘Р С‘ РЎвЂЎР С‘РЎРѓР В»Р С• Р С‘Р В»Р С‘ Р Т‘Р С‘Р В°Р С—Р В°Р В·Р С•Р Р… (Р СР С‘Р Р…-Р СР В°Р С”РЎРѓ)"
                )

                return

        # Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµР С Р С”Р С•Р Р…РЎвЂћР С‘Р С–

        config = get_broadcast_config(user_id)

        config["interval"] = interval_value

        save_broadcast_config_with_profile(user_id, config)

        data = await state.get_data()

        edit_message_id = data.get("edit_message_id")

        chat_id = data.get("chat_id")

        await state.clear()

        # Р Р€Р Т‘Р В°Р В»РЎРЏР ВµР С РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ Р С—Р С•Р В»РЎРЉР В·Р С•Р Р†Р В°РЎвЂљР ВµР В»РЎРЏ

        try:
            await message.delete()

        except Exception:
            pass

        # Р В Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚РЎС“Р ВµР С РЎвЂљР С• Р В¶Р Вµ РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ РЎРѓ Р С‘Р Р…РЎвЂћР С•РЎР‚Р СР В°РЎвЂ Р С‘Р ВµР в„– Р С• РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р Вµ

        chats = get_broadcast_chats(user_id)

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
                    "Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р С—РЎР‚Р С‘ Р С•Р В±Р Р…Р С•Р Р†Р В»Р ВµР Р…Р С‘Р С‘ Р СР ВµР Р…РЎР‹"
                )

        else:
            await cmd_broadcast_menu(message)

    except ValueError:
        await message.answer("РІСњРЉ Р вЂ™Р Р†Р ВµР Т‘Р С‘ РЎвЂЎР С‘РЎРѓР В»Р С•!")


@router.message(BroadcastConfigState.waiting_for_plan_limit)
async def process_broadcast_plan_limit(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р В»Р С‘Р СР С‘РЎвЂљР В° Р С—Р В»Р В°Р Р…Р С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘РЎРЏ"""
    user_id = message.from_user.id

    if message.text == "РІвЂ В©РїС‘РЏ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ":
        await return_to_previous_menu(message, state)
        return

    raw = message.text.strip().replace(",", " ")
    parts = [p for p in raw.split() if p]

    if len(parts) < 2:
        await message.answer(
            "РІСњРЉ Р В¤Р С•РЎР‚Р СР В°РЎвЂљ: Р В»Р С‘Р СР С‘РЎвЂљ Р С•РЎвЂљР Т‘РЎвЂ№РЎвЂ¦. Р СџРЎР‚Р С‘Р СР ВµРЎР‚: 10 3"
        )
        return

    try:
        limit_count = int(parts[0])
        limit_rest = int(parts[1])
    except ValueError:
        await message.answer(
            "РІСњРЉ Р вЂ™Р Р†Р ВµР Т‘Р С‘ РЎвЂЎР С‘РЎРѓР В»Р В°. Р СџРЎР‚Р С‘Р СР ВµРЎР‚: 10 3"
        )
        return

    if limit_count < 0 or limit_rest < 0:
        await message.answer(
            "РІСњРЉ Р вЂ”Р Р…Р В°РЎвЂЎР ВµР Р…Р С‘РЎРЏ Р Т‘Р С•Р В»Р В¶Р Р…РЎвЂ№ Р В±РЎвЂ№РЎвЂљРЎРЉ Р Р…Р ВµР С•РЎвЂљРЎР‚Р С‘РЎвЂ Р В°РЎвЂљР ВµР В»РЎРЉР Р…РЎвЂ№Р Вµ"
        )
        return

    config = get_broadcast_config(user_id)
    config["plan_limit_count"] = limit_count
    config["plan_limit_rest"] = limit_rest
    save_broadcast_config_with_profile(user_id, config)

    data = await state.get_data()
    edit_message_id = data.get("edit_message_id")
    chat_id = data.get("chat_id")
    await state.clear()

    try:
        await message.delete()
    except Exception:
        pass

    if edit_message_id and chat_id:
        try:
            chats = get_broadcast_chats(user_id)
            info = build_broadcast_menu_text(config, chats, active_broadcasts, user_id)
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
        except Exception:
            await cmd_broadcast_menu(message)
    else:
        await cmd_broadcast_menu(message)


@router.message(BroadcastConfigState.waiting_for_chat_pause)
async def process_broadcast_chat_pause(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С—Р С•Р В»РЎС“РЎвЂЎР ВµР Р…Р С‘РЎРЏ Р В·Р В°Р Т‘Р ВµРЎР‚Р В¶Р С”Р С‘ Р СР ВµР В¶Р Т‘РЎС“ РЎвЂЎР В°РЎвЂљР В°Р СР С‘"""

    user_id = message.from_user.id

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С РЎвЂЎРЎвЂљР С• РЎРЊРЎвЂљР С• Р Р…Р Вµ Р С”Р Р…Р С•Р С—Р С”Р В° Р С•РЎвЂљР СР ВµР Р…РЎвЂ№

    if message.text == "РІвЂ В©РїС‘РЏ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ":
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
                    "РІСњРЉ Р СњР ВµР Р†Р ВµРЎР‚Р Р…РЎвЂ№Р в„– РЎвЂћР С•РЎР‚Р СР В°РЎвЂљ. Р ВРЎРѓР С—Р С•Р В»РЎРЉР В·РЎС“Р в„–: 1-3 Р С‘Р В»Р С‘ 2"
                )

                return

            try:
                min_pause = int(parts[0].strip())

                max_pause = int(parts[1].strip())

                if min_pause < 1 or max_pause < 1 or min_pause > max_pause:
                    await message.answer(
                        "РІСњРЉ Р вЂ”Р Р…Р В°РЎвЂЎР ВµР Р…Р С‘РЎРЏ Р Т‘Р С•Р В»Р В¶Р Р…РЎвЂ№ Р В±РЎвЂ№РЎвЂљРЎРЉ Р С—Р С•Р В»Р С•Р В¶Р С‘РЎвЂљР ВµР В»РЎРЉР Р…РЎвЂ№Р СР С‘, Р СР С‘Р Р… РІвЂ°В¤ Р СР В°Р С”РЎРѓ"
                    )

                    return

                if min_pause > 30 or max_pause > 30:
                    await message.answer(
                        "РІСњРЉ Р вЂ”Р В°Р Т‘Р ВµРЎР‚Р В¶Р С”Р В° Р Р…Р Вµ Р Т‘Р С•Р В»Р В¶Р Р…Р В° Р В±РЎвЂ№РЎвЂљРЎРЉ Р В±Р С•Р В»РЎРЉРЎв‚¬Р Вµ 30 РЎРѓР ВµР С”РЎС“Р Р…Р Т‘"
                    )

                    return

                pause_value = text  # Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµР С Р С”Р В°Р С” РЎРѓРЎвЂљРЎР‚Р С•Р С”РЎС“ "Р СР С‘Р Р…-Р СР В°Р С”РЎРѓ"

            except ValueError:
                await message.answer(
                    "РІСњРЉ Р вЂ™Р Р†Р ВµР Т‘Р С‘ РЎвЂЎР С‘РЎРѓР В»Р В° Р Р† РЎвЂћР С•РЎР‚Р СР В°РЎвЂљР Вµ: 1-3"
                )

                return

        else:
            # Р С›Р Т‘Р Р…Р С• РЎвЂЎР С‘РЎРѓР В»Р С•

            try:
                pause_int = int(text)

                if pause_int < 1 or pause_int > 30:
                    await message.answer(
                        "РІСњРЉ Р вЂ”Р В°Р Т‘Р ВµРЎР‚Р В¶Р С”Р В° Р Т‘Р С•Р В»Р В¶Р Р…Р В° Р В±РЎвЂ№РЎвЂљРЎРЉ Р С•РЎвЂљ 1 Р Т‘Р С• 30 РЎРѓР ВµР С”РЎС“Р Р…Р Т‘"
                    )

                    return

                pause_value = text

            except ValueError:
                await message.answer(
                    "РІСњРЉ Р вЂ™Р Р†Р ВµР Т‘Р С‘ РЎвЂЎР С‘РЎРѓР В»Р С• Р С‘Р В»Р С‘ Р Т‘Р С‘Р В°Р С—Р В°Р В·Р С•Р Р… (Р СР С‘Р Р…-Р СР В°Р С”РЎРѓ)"
                )

                return

        # Р РЋР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµР С Р С”Р С•Р Р…РЎвЂћР С‘Р С–

        config = get_broadcast_config(user_id)

        config["chat_pause"] = pause_value

        save_broadcast_config_with_profile(user_id, config)

        data = await state.get_data()

        edit_message_id = data.get("edit_message_id")

        chat_id = data.get("chat_id")

        await state.clear()

        # Р Р€Р Т‘Р В°Р В»РЎРЏР ВµР С РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ Р С—Р С•Р В»РЎРЉР В·Р С•Р Р†Р В°РЎвЂљР ВµР В»РЎРЏ

        try:
            await message.delete()

        except Exception:
            pass

        # Р В Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚РЎС“Р ВµР С Р СР ВµР Р…РЎР‹ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘

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
                    "Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р С—РЎР‚Р С‘ Р С•Р В±Р Р…Р С•Р Р†Р В»Р ВµР Р…Р С‘Р С‘ Р СР ВµР Р…РЎР‹"
                )

        else:
            await cmd_broadcast_menu(message)

    except Exception as e:
        print(
            f"Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р С•Р В±РЎР‚Р В°Р В±Р С•РЎвЂљР С”Р С‘ Р В·Р В°Р Т‘Р ВµРЎР‚Р В¶Р С”Р С‘ Р СР ВµР В¶Р Т‘РЎС“ РЎвЂЎР В°РЎвЂљР В°Р СР С‘: {e}"
        )

        await message.answer(
            "РІСњРЉ Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р С—РЎР‚Р С‘ РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…Р ВµР Р…Р С‘Р С‘ Р В·Р В°Р Т‘Р ВµРЎР‚Р В¶Р С”Р С‘"
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


async def execute_broadcast(
    message_or_query,
    user_id: int,
    account_number: int,
    config: dict,
    chats: list,
    group_id: int = None,
) -> None:

    chat_ids = [cid for cid, _ in chats]

    broadcast_id = next_broadcast_id()

    account_name = None

    for acc_num, telegram_id, username, first_name, is_active in get_user_accounts(
        user_id
    ):
        if acc_num == account_number:
            account_name = (
                first_name
                or username
                or f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442 {acc_num}"
            )

            break

    payload = {
        "user_id": user_id,
        "account": account_number,
        "account_name": account_name
        or f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442 {account_number}",
        "total_chats": len(chat_ids),
        "sent_chats": 0,
        "planned_count": len(chat_ids) * int(config.get("count", 1)),
        "count": int(config.get("count", 1)),
        "interval_minutes": config.get("interval", 1),
        "start_time": datetime.now(timezone.utc),
        "status": "running",
    }

    if group_id is not None:
        payload["group_id"] = group_id

    create_broadcast(broadcast_id, payload)

    asyncio.create_task(
        schedule_broadcast_send(
            user_id=user_id,
            account_number=account_number,
            chat_ids=chat_ids,
            texts=config.get("texts"),
            interval_minutes=int(config.get("interval", 1))
            if str(config.get("interval", 1)).isdigit()
            else 1,
            count=int(config.get("count", 1)),
            broadcast_id=broadcast_id,
            parse_mode=config.get("parse_mode", "HTML"),
            text_mode=config.get("text_mode", "random"),
        )
    )

    await _send_broadcast_notice(
        message_or_query,
        f"\u2705 \u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 #{broadcast_id} \u0437\u0430\u043f\u0443\u0449\u0435\u043d\u0430",
    )


@router.callback_query(F.data == "bc_launch")
async def bc_launch_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id

    if user_id not in user_authenticated or not user_authenticated[user_id]:
        await _send_broadcast_notice(
            query,
            "\u274c \u0422\u044b \u043d\u0435 \u0437\u0430\u043b\u043e\u0433\u0438\u0440\u043e\u0432\u0430\u043d!",
        )
        return

    config = get_broadcast_config(user_id)
    chats = get_broadcast_chats(user_id)

    if not config.get("texts"):
        await _send_broadcast_notice(
            query,
            "\u274c \u0422\u0435\u043a\u0441\u0442 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438 \u043d\u0435 \u0443\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d!\n\n\u041d\u0430\u0436\u043c\u0438 '\U0001f4dd \u0412\u044b\u0431\u0440\u0430\u0442\u044c \u0442\u0435\u043a\u0441\u0442' \u0447\u0442\u043e\u0431\u044b \u0443\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u044c \u0442\u0435\u043a\u0441\u0442",
        )
        return

    if not chats:
        await _send_broadcast_notice(
            query,
            "\u274c \u041d\u0435\u0442 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438!\n\n\u0414\u043e\u0431\u0430\u0432\u044c \u0447\u0430\u0442\u044b \u0447\u0435\u0440\u0435\u0437 '\U0001f4ac \u0427\u0430\u0442\u044b \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438'",
        )
        return

    accounts = get_user_accounts(user_id)
    if len(accounts) == 1:
        account_number = accounts[0][0]
        await execute_broadcast(query, user_id, account_number, config, chats)
        return

    buttons = []
    for acc_num, telegram_id, username, first_name, is_active in accounts:
        is_connected = (
            user_id in user_authenticated and acc_num in user_authenticated[user_id]
        )
        if is_connected:
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=f"\U0001f7e2 {first_name}",
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

    if not buttons:
        await _send_broadcast_notice(
            query,
            "\u274c \u041d\u0435\u0442 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043d\u044b\u0445 \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432!",
        )
        return

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

    if not config.get("texts"):
        await _send_broadcast_notice(
            query,
            "\u274c \u0422\u0435\u043a\u0441\u0442 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438 \u043d\u0435 \u0443\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d",
        )

        return

    if not chats:
        await _send_broadcast_notice(
            query,
            "\u274c \u041d\u0435\u0442 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438",
        )

        return

    if query.data == "start_bc_all":
        accounts = get_user_accounts(user_id)

        connected_accounts = [
            acc_num
            for acc_num, _, _, _, _ in accounts
            if user_id in user_authenticated and acc_num in user_authenticated[user_id]
        ]

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

    if user_id not in user_authenticated or not user_authenticated[user_id]:
        await message.answer(LOGIN_REQUIRED_TEXT)

        return

    # Р СџР С•Р В»РЎС“РЎвЂЎР В°Р ВµР С Р С”Р С•Р Р…РЎвЂћР С‘Р С– РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘

    config = get_broadcast_config(user_id)

    chats = get_broadcast_chats(user_id)

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С РЎвЂЎРЎвЂљР С• Р ВµРЎРѓРЎвЂљРЎРЉ РЎвЂљР ВµР С”РЎРѓРЎвЂљ

    if not config.get("texts"):
        await message.answer(
            "РІСњРЉ Р СћР ВµР С”РЎРѓРЎвЂљ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘ Р Р…Р Вµ РЎС“РЎРѓРЎвЂљР В°Р Р…Р С•Р Р†Р В»Р ВµР Р…!\n\nР СњР В°Р В¶Р СР С‘ 'СЂСџвЂњСњ Р вЂ™РЎвЂ№Р В±РЎР‚Р В°РЎвЂљРЎРЉ РЎвЂљР ВµР С”РЎРѓРЎвЂљ' РЎвЂЎРЎвЂљР С•Р В±РЎвЂ№ РЎС“РЎРѓРЎвЂљР В°Р Р…Р С•Р Р†Р С‘РЎвЂљРЎРЉ РЎвЂљР ВµР С”РЎРѓРЎвЂљ"
        )

        return

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С РЎвЂЎРЎвЂљР С• Р ВµРЎРѓРЎвЂљРЎРЉ РЎвЂЎР В°РЎвЂљРЎвЂ№

    if not chats:
        await message.answer(
            "РІСњРЉ Р СњР ВµРЎвЂљ РЎвЂЎР В°РЎвЂљР С•Р Р† Р Т‘Р В»РЎРЏ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘!\n\nР вЂќР С•Р В±Р В°Р Р†РЎРЉ РЎвЂЎР В°РЎвЂљРЎвЂ№ РЎвЂЎР ВµРЎР‚Р ВµР В· 'СЂСџвЂ™В¬ Р В§Р В°РЎвЂљРЎвЂ№ Р Т‘Р В»РЎРЏ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘'"
        )

        return

    # Р вЂўРЎРѓР В»Р С‘ РЎвЂљР С•Р В»РЎРЉР С”Р С• Р С•Р Т‘Р С‘Р Р… Р В°Р С”Р С”Р В°РЎС“Р Р…РЎвЂљ - Р С‘РЎРѓР С—Р С•Р В»РЎРЉР В·РЎС“Р ВµР С Р ВµР С–Р С•

    accounts = get_user_accounts(user_id)

    if len(accounts) == 1:
        account_number = accounts[0][0]

    else:
        # Р вЂўРЎРѓР В»Р С‘ Р Р…Р ВµРЎРѓР С”Р С•Р В»РЎРЉР С”Р С• - Р С—Р С•Р С”Р В°Р В·РЎвЂ№Р Р†Р В°Р ВµР С Р Р†РЎвЂ№Р В±Р С•РЎР‚

        buttons = []

        for acc_num, telegram_id, username, first_name, is_active in accounts:
            is_connected = (
                user_id in user_authenticated and acc_num in user_authenticated[user_id]
            )

            if is_connected:
                buttons.append(
                    [
                        InlineKeyboardButton(
                            text=f"СЂСџСџСћ {first_name}",
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

        if not buttons:
            await message.answer(LOGIN_REQUIRED_TEXT)

            return

        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

        await message.answer(
            "Р вЂ™РЎвЂ№Р В±Р ВµРЎР‚Р С‘ Р В°Р С”Р С”Р В°РЎС“Р Р…РЎвЂљ Р Т‘Р В»РЎРЏ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘:",
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

    text = "СЂСџвЂ™В¬ <b>Р вЂќР С›Р вЂР С’Р вЂ™Р вЂєР вЂўР СњР ВР вЂў Р В§Р С’Р СћР С’</b>\n\nР С›РЎвЂљР С—РЎР‚Р В°Р Р†РЎРЉ ID РЎвЂЎР В°РЎвЂљР В° Р С‘Р В»Р С‘ РЎРѓРЎРѓРЎвЂ№Р В»Р С”РЎС“ Р Р…Р В° Р С”Р В°Р Р…Р В°Р В»:\nР СџРЎР‚Р С‘Р СР ВµРЎР‚РЎвЂ№:\n  РІР‚Сћ ID: -1001234567890\n  РІР‚Сћ Р РЋРЎРѓРЎвЂ№Р В»Р С”Р В°: @mychannel\n\nРІС™В РїС‘РЏ Р В§Р В°РЎвЂљ Р Т‘Р С•Р В»Р В¶Р ВµР Р… Р В±РЎвЂ№РЎвЂљРЎРЉ Р С•РЎвЂљР С”РЎР‚РЎвЂ№РЎвЂљРЎвЂ№Р С Р С‘Р В»Р С‘ Р Т‘Р С•РЎРѓРЎвЂљРЎС“Р С—Р Р…РЎвЂ№Р С РЎвЂљР Р†Р С•Р ВµР СРЎС“ Р В°Р С”Р С”Р В°РЎС“Р Р…РЎвЂљРЎС“"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="РІСњРЉ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ",
                    callback_data="bc_cancel",
                )
            ]
        ]
    )

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.message(BroadcastConfigState.waiting_for_chat_id)
async def process_add_broadcast_chat_with_profile(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р С‘РЎРЏ РЎвЂЎР В°РЎвЂљР В° Р Р† РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”РЎС“"""

    user_id = message.from_user.id

    chat_input = message.text.strip()

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С РЎвЂЎРЎвЂљР С• РЎРЊРЎвЂљР С• Р Р…Р Вµ Р С”Р Р…Р С•Р С—Р С”Р В° Р С•РЎвЂљР СР ВµР Р…РЎвЂ№

    if chat_input == "РІвЂ В©РїС‘РЏ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ":
        await return_to_previous_menu(message, state)

        return

    # Р Р€Р Т‘Р В°Р В»РЎРЏР ВµР С РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ Р С—Р С•Р В»РЎРЉР В·Р С•Р Р†Р В°РЎвЂљР ВµР В»РЎРЏ Р Т‘Р В»РЎРЏ РЎвЂЎР С‘РЎРѓРЎвЂљР С•РЎвЂљРЎвЂ№ РЎвЂЎР В°РЎвЂљР В°

    try:
        await message.delete()

    except Exception:
        pass

    # Р СџР С•Р С”Р В°Р В·РЎвЂ№Р Р†Р В°Р ВµР С Р В·Р В°Р С–РЎР‚РЎС“Р В·Р С”РЎС“

    loading_msg = await message.answer(
        "РІРЏС– Р вЂ”Р В°Р С–РЎР‚РЎС“Р В¶Р В°РЎР‹ Р С‘Р Р…РЎвЂћР С•РЎР‚Р СР В°РЎвЂ Р С‘РЎР‹ Р С• РЎвЂЎР В°РЎвЂљР Вµ..."
    )

    try:
        # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С Р В°Р Р†РЎвЂљР С•РЎР‚Р С‘Р В·Р В°РЎвЂ Р С‘РЎР‹

        if user_id not in user_authenticated or not user_authenticated[user_id]:
            await message.answer(LOGIN_REQUIRED_TEXT)

            await state.clear()

            return

        # Р вЂР ВµРЎР‚РЎвЂР С Р СџР вЂўР В Р вЂ™Р В«Р в„ў Р С—Р С•Р Т‘Р С”Р В»РЎР‹РЎвЂЎР ВµР Р…Р Р…РЎвЂ№Р в„– Р В°Р С”Р С”Р В°РЎС“Р Р…РЎвЂљ Р Т‘Р В»РЎРЏ Р С—Р С•Р В»РЎС“РЎвЂЎР ВµР Р…Р С‘РЎРЏ Р С‘Р Р…РЎвЂћР С•РЎР‚Р СР В°РЎвЂ Р С‘Р С‘ Р С• РЎвЂЎР В°РЎвЂљР Вµ

        account_number = next(iter(user_authenticated[user_id].keys()))

        client = user_authenticated[user_id][account_number]

        chat_id = None

        chat_name = None
        chat_link = None

        # Р СџРЎвЂ№РЎвЂљР В°Р ВµР СРЎРѓРЎРЏ Р С—Р С•Р В»РЎС“РЎвЂЎР С‘РЎвЂљРЎРЉ Р С‘Р Р…РЎвЂћР С•РЎР‚Р СР В°РЎвЂ Р С‘РЎР‹ Р С• РЎвЂЎР В°РЎвЂљР Вµ

        chat = None

        # Р СџРЎвЂ№РЎвЂљР В°Р ВµР СРЎРѓРЎРЏ Р С—Р В°РЎР‚РЎРѓР С‘РЎвЂљРЎРЉ Р С”Р В°Р С” РЎвЂЎР С‘РЎРѓР В»Р С• (ID РЎвЂЎР В°РЎвЂљР В°)

        try:
            # Р вЂўРЎРѓР В»Р С‘ РЎРЊРЎвЂљР С• РЎвЂЎР С‘РЎРѓР В»Р С• - Р С—РЎвЂ№РЎвЂљР В°Р ВµР СРЎРѓРЎРЏ Р С‘РЎРѓР С—Р С•Р В»РЎРЉР В·Р С•Р Р†Р В°РЎвЂљРЎРЉ Р С”Р В°Р С” chat_id Р Р…Р В°Р С—РЎР‚РЎРЏР СРЎС“РЎР‹

            if chat_input.lstrip("-").isdigit():
                chat_id = int(chat_input)

                # Р СџРЎвЂ№РЎвЂљР В°Р ВµР СРЎРѓРЎРЏ Р С—Р С•Р В»РЎС“РЎвЂЎР С‘РЎвЂљРЎРЉ Р С‘Р Р…РЎвЂћР С•РЎР‚Р СР В°РЎвЂ Р С‘РЎР‹ Р С• РЎвЂЎР В°РЎвЂљР Вµ РЎвЂЎР ВµРЎР‚Р ВµР В· ID

                try:
                    # Р вЂќР В»РЎРЏ РЎРѓРЎС“Р С—Р ВµРЎР‚Р С–РЎР‚РЎС“Р С—Р С— ID Р Р†РЎвЂ№Р С–Р В»РЎРЏР Т‘Р С‘РЎвЂљ Р С”Р В°Р С” -1001234567890

                    # Telethon РЎвЂљРЎР‚Р ВµР В±РЎС“Р ВµРЎвЂљ Р С—РЎР‚Р ВµР С•Р В±РЎР‚Р В°Р В·Р С•Р Р†Р В°Р Р…Р С‘Р Вµ: -1001234567890 -> 1234567890 (РЎС“Р В±Р С‘РЎР‚Р В°Р ВµР С -100)

                    if chat_id < 0 and str(chat_id).startswith("-100"):
                        # Р В­РЎвЂљР С• РЎРѓРЎС“Р С—Р ВµРЎР‚Р С–РЎР‚РЎС“Р С—Р С—Р В°, Р С—РЎР‚Р ВµР С•Р В±РЎР‚Р В°Р В·РЎС“Р ВµР С ID

                        actual_id = chat_id

                    else:
                        actual_id = chat_id

                    chat = await client.get_entity(actual_id)

                    if chat:
                        title = getattr(chat, "title", None) or getattr(
                            chat, "first_name", None
                        )

                        if not title and hasattr(chat, "id"):
                            title = f"user{chat.id}"

                        chat_name = str(title) if title else f"Р В§Р В°РЎвЂљ {chat_id}"
                        chat_link = _detect_chat_link(chat_input, chat)

                    else:
                        chat_name = f"Р В§Р В°РЎвЂљ {chat_id}"

                except Exception:
                    # Р вЂўРЎРѓР В»Р С‘ Р Р…Р Вµ Р С—Р С•Р В»РЎС“РЎвЂЎР С‘Р В»Р С•РЎРѓРЎРЉ Р Р…Р В°Р С—РЎР‚РЎРЏР СРЎС“РЎР‹, Р С—РЎвЂ№РЎвЂљР В°Р ВµР СРЎРѓРЎРЏ Р С”Р В°Р С” Р С•Р В±РЎвЂ№РЎвЂЎР Р…РЎвЂ№Р в„– entity

                    try:
                        chat = await client.get_entity(chat_input)

                        if chat:
                            chat_id = chat.id

                            title = getattr(chat, "title", None) or getattr(
                                chat, "first_name", None
                            )

                            if not title and hasattr(chat, "id"):
                                title = f"user{chat.id}"

                            chat_name = (
                                str(title) if title else f"Р В§Р В°РЎвЂљ {chat_id}"
                            )
                            chat_link = _detect_chat_link(chat_input, chat)

                        else:
                            chat_id = int(chat_input)

                            chat_name = f"Р В§Р В°РЎвЂљ {chat_id}"

                    except Exception:
                        # Р вЂўРЎРѓР В»Р С‘ Р Р†РЎРѓРЎвЂ РЎР‚Р В°Р Р†Р Р…Р С• Р Р…Р Вµ Р С—Р С•Р В»РЎС“РЎвЂЎР С‘Р В»Р С•РЎРѓРЎРЉ, Р С—РЎР‚Р С•РЎРѓРЎвЂљР С• Р С‘РЎРѓР С—Р С•Р В»РЎРЉР В·РЎС“Р ВµР С ID Р С”Р В°Р С” Р ВµРЎРѓРЎвЂљРЎРЉ

                        chat_id = int(chat_input)

                        chat_name = f"Р В§Р В°РЎвЂљ {chat_id}"

                        chat = None

            else:
                # Р В­РЎвЂљР С• РЎР‹Р В·Р ВµРЎР‚Р Р…Р ВµР в„–Р С Р С‘Р В»Р С‘ Р Т‘РЎР‚РЎС“Р С–Р С•Р в„– Р С‘Р Т‘Р ВµР Р…РЎвЂљР С‘РЎвЂћР С‘Р С”Р В°РЎвЂљР С•РЎР‚

                try:
                    chat = await client.get_entity(chat_input)

                    if chat:
                        chat_id = chat.id

                        title = getattr(chat, "title", None) or getattr(
                            chat, "first_name", None
                        )

                        if not title and hasattr(chat, "id"):
                            title = f"user{chat.id}"

                        chat_name = str(title) if title else f"Р В§Р В°РЎвЂљ {chat_id}"
                        chat_link = _detect_chat_link(chat_input, chat)

                    else:
                        await message.answer(
                            "РІСњРЉ Р В§Р В°РЎвЂљ Р Р…Р Вµ Р Р…Р В°Р в„–Р Т‘Р ВµР Р…"
                        )

                        return

                except Exception as e:
                    print(
                        f"РІС™В РїС‘РЏ  Р СњР Вµ РЎРѓР СР С•Р С– Р С—Р С•Р В»РЎС“РЎвЂЎР С‘РЎвЂљРЎРЉ РЎвЂЎР В°РЎвЂљ Р С—Р С• {chat_input}: {str(e)}"
                    )

                    await message.answer(
                        "РІСњРЉ Р В§Р В°РЎвЂљ Р Р…Р Вµ Р Р…Р В°Р в„–Р Т‘Р ВµР Р…. Р Р€Р В±Р ВµР Т‘Р С‘РЎРѓРЎРЉ РЎвЂЎРЎвЂљР С• РЎвЂљРЎвЂ№ Р Р† РЎРЊРЎвЂљР С•Р С РЎвЂЎР В°РЎвЂљР Вµ.\n\nСЂСџвЂ™РЋ Р СљР С•Р В¶Р ВµРЎв‚¬РЎРЉ Р С—РЎР‚Р С•РЎРѓРЎвЂљР С• Р Р†Р Р†Р ВµРЎРѓРЎвЂљР С‘ ID РЎвЂЎР В°РЎвЂљР В° РЎвЂЎР С‘РЎРѓР В»Р С•, Р Р…Р В°Р С—РЎР‚Р С‘Р СР ВµРЎР‚: `-1003880811528`"
                    )

                    return

        except Exception as e:
            print(
                f"РІСњРЉ Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р С—Р В°РЎР‚РЎРѓР С‘Р Р…Р С–Р В°: {str(e)}"
            )

            await message.answer(
                "РІСњРЉ Р СњР ВµР Р†Р ВµРЎР‚Р Р…РЎвЂ№Р в„– РЎвЂћР С•РЎР‚Р СР В°РЎвЂљ. Р вЂ™Р Р†Р ВµР Т‘Р С‘ ID РЎвЂЎР В°РЎвЂљР В° (Р Р…Р В°Р С—РЎР‚Р С‘Р СР ВµРЎР‚ `-1003880811528`) Р С‘Р В»Р С‘ РЎР‹Р В·Р ВµРЎР‚Р Р…Р ВµР в„–Р С (Р Р…Р В°Р С—РЎР‚Р С‘Р СР ВµРЎР‚ `@mychannel`)"
            )

            return

        if chat_id is None:
            await message.answer(
                "РІСњРЉ Р СњР Вµ РЎС“Р Т‘Р В°Р В»Р С•РЎРѓРЎРЉ Р С•Р С—РЎР‚Р ВµР Т‘Р ВµР В»Р С‘РЎвЂљРЎРЉ ID РЎвЂЎР В°РЎвЂљР В°"
            )

            return

        if not chat_link:
            chat_link = _detect_chat_link(chat_input, None)

        # Р вЂќР С•Р В±Р В°Р Р†Р В»РЎРЏР ВµР С РЎвЂЎР В°РЎвЂљ Р Р† Р вЂР вЂќ

        added = add_broadcast_chat_with_profile(
            user_id,
            chat_id,
            chat_name or f"Р В§Р В°РЎвЂљ {chat_id}",
            chat_link=chat_link,
        )

        # Р С›РЎвЂљР С—РЎР‚Р В°Р Р†Р В»РЎРЏР ВµР С РЎС“Р Р†Р ВµР Т‘Р С•Р СР В»Р ВµР Р…Р С‘Р Вµ

        if added:
            notify_msg = await message.answer(
                f"РІСљвЂ¦ Р В§Р В°РЎвЂљ '{chat_name or chat_id}' РЎС“РЎРѓР С—Р ВµРЎв‚¬Р Р…Р С• Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…!"
            )

        else:
            notify_msg = await message.answer(
                f"РІС™В РїС‘РЏ Р В§Р В°РЎвЂљ '{chat_name or chat_id}' РЎС“Р В¶Р Вµ Р Р† РЎРѓР С—Р С‘РЎРѓР С”Р Вµ!"
            )

        # Р Р€Р Т‘Р В°Р В»РЎРЏР ВµР С РЎС“Р Р†Р ВµР Т‘Р С•Р СР В»Р ВµР Р…Р С‘Р Вµ Р С—Р С•РЎРѓР В»Р Вµ 5 РЎРѓР ВµР С”РЎС“Р Р…Р Т‘

        import asyncio

        asyncio.create_task(delete_message_after_delay(notify_msg, 5))

        # Р Р€Р Т‘Р В°Р В»РЎРЏР ВµР С РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµР Р…Р С‘Р Вµ Р В·Р В°Р С–РЎР‚РЎС“Р В·Р С”Р С‘

        try:
            await loading_msg.delete()

        except Exception:
            pass

        # Р С›РЎвЂљР С”РЎР‚РЎвЂ№Р Р†Р В°Р ВµР С Р СР ВµР Р…РЎР‹ РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘

        state_data = await state.get_data()
        await state.clear()
        await show_broadcast_chats_menu(
            message, user_id, menu_message_id=state_data.get("menu_message_id")
        )

    except Exception as e:
        print(f"Р С›РЎв‚¬Р С‘Р В±Р С”Р В° Р Р† process_add_broadcast_chat: {str(e)}")

        await message.answer(f"РІСњРЉ Р С›РЎв‚¬Р С‘Р В±Р С”Р В°: {str(e)}")


@router.callback_query(F.data.startswith("select_chat_"))
async def select_chat_callback(query: CallbackQuery, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р Р†РЎвЂ№Р В±Р С•РЎР‚Р В° РЎвЂЎР В°РЎвЂљР В° Р С‘Р В· Р С—Р С•РЎвЂ¦Р С•Р В¶Р С‘РЎвЂ¦"""

    user_id = query.from_user.id

    try:
        chat_id = int(query.data.split("_")[2])

        if user_id not in user_authenticated or not user_authenticated[user_id]:
            await query.answer(LOGIN_REQUIRED_TEXT, show_alert=True)

            return

        account_number = next(iter(user_authenticated[user_id].keys()))

        client = user_authenticated[user_id][account_number]

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
            "РІСњРЉ Р В§Р В°РЎвЂљ Р Р…Р Вµ Р Р…Р В°Р в„–Р Т‘Р ВµР Р…", show_alert=True
        )

    except Exception as e:
        await query.answer(
            f"РІСњРЉ Р С›РЎв‚¬Р С‘Р В±Р С”Р В°: {str(e)}", show_alert=True
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
            keyboard=[
                [KeyboardButton(text="РІвЂ В©РїС‘РЏ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ")]
            ],
            resize_keyboard=True,
        )

        await query.message.delete()

        await query.message.answer(
            f"РІСљРЏРїС‘РЏ Р вЂ™Р Р†Р ВµР Т‘Р С‘ Р С‘Р СРЎРЏ/Р С•Р С—Р С‘РЎРѓР В°Р Р…Р С‘Р Вµ Р Т‘Р В»РЎРЏ РЎвЂЎР В°РЎвЂљР В° РЎРѓ ID {chat_id}:",
            reply_markup=keyboard,
        )

    except Exception as e:
        await query.answer(
            f"РІСњРЉ Р С›РЎв‚¬Р С‘Р В±Р С”Р В°: {str(e)}", show_alert=True
        )


@router.message(BroadcastConfigState.waiting_for_chat_name)
async def process_broadcast_chat_name(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р Р†Р Р†Р С•Р Т‘Р В° Р С‘Р СР ВµР Р…Р С‘ РЎвЂЎР В°РЎвЂљР В° Р С—РЎР‚Р С‘ Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…Р С‘Р С‘"""

    user_id = message.from_user.id

    # Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚РЎРЏР ВµР С Р С•РЎвЂљР СР ВµР Р…РЎС“

    if message.text == "РІвЂ В©РїС‘РЏ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ":
        await return_to_previous_menu(message, state)

        return

    try:
        data = await state.get_data()

        chat_id = data.get("chat_id")

        chat_name = message.text.strip()

        if not chat_id:
            await message.answer(
                "РІСњРЉ Р С›РЎв‚¬Р С‘Р В±Р С”Р В°! Chat ID Р Р…Р Вµ РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎвЂР Р…. Р СџР С•Р С—РЎР‚Р С•Р В±РЎС“Р в„– РЎРѓР Р…Р С•Р Р†Р В°"
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
                f"РІСљвЂ¦ Р В§Р В°РЎвЂљ '{chat_name}' РЎС“РЎРѓР С—Р ВµРЎв‚¬Р Р…Р С• Р Т‘Р С•Р В±Р В°Р Р†Р В»Р ВµР Р…!"
            )

        else:
            notify_msg = await message.answer(
                "РІС™В РїС‘РЏ Р В§Р В°РЎвЂљ РЎРѓ РЎРЊРЎвЂљР С‘Р С ID РЎС“Р В¶Р Вµ Р Р† РЎРѓР С—Р С‘РЎРѓР С”Р Вµ"
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

        await message.answer(f"РІСњРЉ Р С›РЎв‚¬Р С‘Р В±Р С”Р В°: {str(e)}")

        await state.clear()


@router.callback_query(F.data == "bc_chats_delete")
async def bc_chats_delete_callback(query: CallbackQuery, state: FSMContext):
    """Show broadcast chat removal UI with multi-delete and clear-all."""

    await query.answer()

    user_id = query.from_user.id

    chats = get_broadcast_chats(user_id)

    if not chats:
        text = "СЂСџвЂњВ­ Р СњР ВµРЎвЂљ РЎвЂЎР В°РЎвЂљР С•Р Р† Р Т‘Р В»РЎРЏ РЎС“Р Т‘Р В°Р В»Р ВµР Р…Р С‘РЎРЏ!"

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="РІВ¬вЂ¦РїС‘РЏ Р СњР В°Р В·Р В°Р Т‘",
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

    text = "СЂСџвЂ”вЂРїС‘РЏ <b>Р Р€Р вЂќР С’Р вЂєР вЂўР СњР ВР вЂў Р В§Р С’Р СћР С›Р вЂ™</b>\n\n"

    for idx, (chat_id, chat_name) in enumerate(chats, 1):
        text += f"{idx}РїС‘РЏРІС“Р€ {chat_name}\n"

    text += (
        f"\nР вЂ™Р Р†Р ВµР Т‘Р С‘ Р Р…Р С•Р СР ВµРЎР‚Р В° РЎвЂЎР В°РЎвЂљР С•Р Р† Р Т‘Р В»РЎРЏ РЎС“Р Т‘Р В°Р В»Р ВµР Р…Р С‘РЎРЏ (Р С•РЎвЂљ 1 Р Т‘Р С• {len(chats)}).\n"
        "Р СљР С•Р В¶Р Р…Р С• Р Р…Р ВµРЎРѓР С”Р С•Р В»РЎРЉР С”Р С• РЎвЂЎР ВµРЎР‚Р ВµР В· Р С—РЎР‚Р С•Р В±Р ВµР В»/Р В·Р В°Р С—РЎРЏРЎвЂљРЎС“РЎР‹, Р Р…Р В°Р С—РЎР‚Р С‘Р СР ВµРЎР‚: 1 4"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="СЂСџВ§в„– Р С›РЎвЂЎР С‘РЎРѓРЎвЂљР С‘РЎвЂљРЎРЉ Р Р†РЎРѓР Вµ",
                    callback_data="bc_chats_delete_all",
                )
            ],
            [
                InlineKeyboardButton(
                    text="РІСњРЉ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ",
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


@router.message(F.text == "СЂСџвЂ”вЂРїС‘РЏ Р Р€Р Т‘Р В°Р В»Р С‘РЎвЂљРЎРЉ")
async def delete_broadcast_chat_button(message: Message, state: FSMContext):
    """Р С›Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р С”Р Р…Р С•Р С—Р С”Р С‘ РЎС“Р Т‘Р В°Р В»Р ВµР Р…Р С‘РЎРЏ РЎвЂЎР В°РЎвЂљР В° Р С‘Р В· РЎР‚Р В°РЎРѓРЎРѓРЎвЂ№Р В»Р С”Р С‘ - Р РЋР СћР С’Р В Р В«Р в„ў Р С›Р вЂР В Р С’Р вЂР С›Р СћР В§Р ВР С™ (Р Р€Р вЂР В Р С’Р СћР В¬)"""

    # Р В­РЎвЂљР С•РЎвЂљ Р С•Р В±РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С” Р В±Р С•Р В»РЎРЉРЎв‚¬Р Вµ Р Р…Р Вµ Р С‘РЎРѓР С—Р С•Р В»РЎРЉР В·РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ

    pass


@router.message(BroadcastConfigState.waiting_for_chat_delete)
async def process_delete_broadcast_chat(message: Message, state: FSMContext):
    """Delete one or many broadcast chats by numeric indexes."""

    user_id = message.from_user.id

    if message.text in {
        "СЂСџвЂќв„ў Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ",
        "РІСњРЉ Р С›РЎвЂљР СР ВµР Р…Р С‘РЎвЂљРЎРЉ",
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
                f"РІСњРЉ Р вЂ™Р Р†Р ВµР Т‘Р С‘ Р Р…Р С•Р СР ВµРЎР‚Р В° Р С•РЎвЂљ 1 Р Т‘Р С• {len(chats)}"
            )
            return

        indexes = []
        for token in tokens:
            value = int(token) - 1
            if value < 0 or value >= len(chats):
                await message.answer(
                    f"РІСњРЉ Р вЂ™Р Р†Р ВµР Т‘Р С‘ РЎвЂЎР С‘РЎРѓР В»Р В° Р С•РЎвЂљ 1 Р Т‘Р С• {len(chats)}"
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
            "РІСњРЉ Р вЂ™Р Р†Р ВµР Т‘Р С‘ РЎвЂЎР С‘РЎРѓР В»Р В° РЎвЂЎР ВµРЎР‚Р ВµР В· Р С—РЎР‚Р С•Р В±Р ВµР В» Р С‘Р В»Р С‘ Р В·Р В°Р С—РЎРЏРЎвЂљРЎС“РЎР‹"
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
