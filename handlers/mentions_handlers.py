import asyncio
from aiogram import Router, F
from aiogram.filters.command import Command
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from core.state import app_state
from database import get_tracked_chats, add_tracked_chat, remove_tracked_chat, get_broadcast_chats
from services.broadcast_profiles_service import get_active_config_id, get_config_detail
from services.mention_service import stop_mention_monitoring, start_mention_monitoring as start_mention_monitoring_service
from services.mention_utils import normalize_chat_id, delete_message_after_delay
from database import get_user_accounts

router = Router()


class TrackedChatsState(StatesGroup):
    waiting_for_chat_id = State()
    waiting_for_number_to_delete = State()


async def _start_monitoring(bot, user_id: int):
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


def _build_tracked_menu(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    chats = get_tracked_chats(user_id)
    is_monitoring = user_id in app_state.mention_monitors and any(app_state.mention_monitors[user_id].values())
    monitoring_status = "🟢 Включено" if is_monitoring else "🔴 Выключено"

    info = "🔔 <b>ОТСЛЕЖИВАНИЕ УПОМИНАНИЙ</b>\n"
    info += f"Статус: {monitoring_status}\n\n"

    if chats:
        for idx, (chat_id, chat_name) in enumerate(chats, 1):
            info += f"{idx}️⃣ {chat_name}\n   ID: {chat_id}\n\n"
    else:
        info += "📭 Нет отслеживаемых чатов\n\n"

    info += "Выберите действие:"

    toggle_text = "⏸️ Выключить" if is_monitoring else "▶️ Включить"
    buttons = []
    if chats:
        buttons.append([
            InlineKeyboardButton(text="➕ Добавить", callback_data="tc_add_chat"),
            InlineKeyboardButton(text="🗑️ Удалить", callback_data="tc_delete_chat"),
        ])
    else:
        buttons.append([InlineKeyboardButton(text="➕ Добавить", callback_data="tc_add_chat")])

    buttons.append([InlineKeyboardButton(text="⬇️ Загрузить с рассылки", callback_data="tc_import_from_broadcast")])
    buttons.append([InlineKeyboardButton(text=toggle_text, callback_data="tc_toggle_monitoring")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="delete_tc_menu")])
    return info, InlineKeyboardMarkup(inline_keyboard=buttons)


async def show_tracked_menu(message_or_query, user_id: int, menu_message_id: int | None = None) -> None:
    info, kb = _build_tracked_menu(user_id)

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

    target = message_or_query.message if hasattr(message_or_query, "message") else message_or_query
    try:
        await target.edit_text(info, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await target.answer(info, reply_markup=kb, parse_mode="HTML")


@router.message(F.text.contains("Упоминания"))
async def cmd_tracked_chats_menu(message: Message):
    user_id = message.from_user.id

    try:
        await message.delete()
    except Exception:
        pass

    await show_tracked_menu(message, user_id)


@router.message(Command("tracked"))
async def cmd_tracked(message: Message):
    """Быстрый доступ к меню упоминаний"""
    await cmd_tracked_chats_menu(message)


@router.callback_query(F.data == "delete_tc_menu")
async def delete_tc_menu_callback(query: CallbackQuery):
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass


@router.callback_query(F.data == "tc_toggle_monitoring")
async def tc_toggle_monitoring_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id

    try:
        is_monitoring = user_id in app_state.mention_monitors and any(app_state.mention_monitors[user_id].values())

        if is_monitoring:
            stop_mention_monitoring(user_id)
            notification = "⏸️ Мониторинг выключен"
        else:
            await _start_monitoring(query.bot, user_id)
            notification = "▶️ Мониторинг включен"

        await query.answer(notification, show_alert=True)

        await show_tracked_menu(query, user_id, menu_message_id=query.message.message_id)
    except Exception:
        await query.answer("❌ Ошибка", show_alert=True)


@router.callback_query(F.data == "tc_import_from_broadcast")
async def tc_import_from_broadcast_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id

    active_id = get_active_config_id(user_id)
    cfg = get_config_detail(user_id, active_id)
    if active_id == 0:
        chats = get_broadcast_chats(user_id)
    else:
        chats = (cfg or {}).get("chats") or []

    if not chats:
        await query.answer("❗️В активном конфиге рассылки нет чатов", show_alert=True)
        return

    added_count = 0
    for chat_id, chat_name in chats:
        if add_tracked_chat(user_id, chat_id, chat_name):
            added_count += 1

    await query.answer(f"✅ Импортировано чатов: {added_count}", show_alert=True)

    await show_tracked_menu(query, user_id, menu_message_id=query.message.message_id)


@router.callback_query(F.data == "tc_add_chat")
async def tc_add_chat_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    await state.set_state(TrackedChatsState.waiting_for_chat_id)

    await query.message.edit_text(
        "📝 <b>Добавление чата</b>\n\n"
        "Отправь ID чата (числом) или @username.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="tc_cancel")]
        ]),
    )


@router.message(TrackedChatsState.waiting_for_chat_id)
async def process_add_tracked_chat(message: Message, state: FSMContext):
    user_id = message.from_user.id
    raw_input = (message.text or "").strip()
    lines = [line.strip() for line in raw_input.replace(",", "\n").splitlines() if line.strip()]

    if not lines:
        await message.answer("❌ Некорректный ID")
        return

    client = None
    user_clients = app_state.user_authenticated.get(user_id, {})
    if user_clients:
        client = next(iter(user_clients.values()))

    added_count = 0
    already_count = 0
    invalid_count = 0

    for chat_input in lines:
        try:
            chat_id: int
            chat_name: str

            if chat_input.lstrip("-").isdigit():
                chat_id = int(chat_input)
                chat_name = f"Чат {chat_id}"
            else:
                if not client:
                    invalid_count += 1
                    continue
                entity = await client.get_entity(chat_input)
                chat_id = int(getattr(entity, "id", 0))
                if not chat_id:
                    invalid_count += 1
                    continue
                title = getattr(entity, "title", None) or getattr(entity, "first_name", None)
                username = getattr(entity, "username", None)
                chat_name = str(title or (f"@{username}" if username else chat_input))

            added = add_tracked_chat(user_id, chat_id, chat_name)
            if added:
                added_count += 1
            else:
                already_count += 1
        except Exception:
            invalid_count += 1

    await state.clear()

    summary = f"✅ Добавлено: {added_count}"
    if already_count:
        summary += f"\n№ Уже в списке: {already_count}"
    if invalid_count:
        summary += f"\n❌ Ошибок: {invalid_count}"

    notify_msg = await message.answer(summary)
    asyncio.create_task(delete_message_after_delay(notify_msg, 3))

@router.callback_query(F.data == "tc_delete_chat")
async def tc_delete_chat_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id
    chats = get_tracked_chats(user_id)

    if not chats:
        await query.answer("Нет отслеживаемых чатов", show_alert=True)
        return

    text = "🗑️ <b>Удаление чата из отслеживания</b>\n\n"
    for idx, (chat_id, chat_name) in enumerate(chats, 1):
        text += f"{idx}️⃣ {chat_name} (ID: {chat_id})\n\n"
    text += (
        "Введи номер(а) чата для удаления.\n"
        "Можно несколько через пробел/запятую, например: 1 4"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧹 Очистить все", callback_data="tc_delete_all")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="tc_cancel")]
    ])

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await state.update_data(menu_message_id=query.message.message_id)
    await state.set_state(TrackedChatsState.waiting_for_number_to_delete)


@router.message(TrackedChatsState.waiting_for_number_to_delete)
async def process_delete_tracked_chat(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    menu_message_id = data.get("menu_message_id")

    try:
        chats = get_tracked_chats(user_id)
        raw = (message.text or "").replace(",", " ")
        tokens = [token for token in raw.split() if token]
        if not tokens:
            await message.answer("❌ Введи номер(а) для удаления")
            return

        indexes = []
        for token in tokens:
            num = int(token)
            if num < 1 or num > len(chats):
                await message.answer(f"❌ Номера должны быть от 1 до {len(chats)}")
                return
            indexes.append(num - 1)

        removed = 0
        for idx in sorted(set(indexes), reverse=True):
            chat_id, _ = chats[idx]
            remove_tracked_chat(user_id, chat_id)
            removed += 1

        await state.clear()
        try:
            await message.delete()
        except Exception:
            pass
        await message.answer(f"✅ Удалено чатов: {removed}")
        await show_tracked_menu(message, user_id, menu_message_id=menu_message_id)
    except ValueError:
        await message.answer("❌ Введи числа через пробел/запятую")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")
        await state.clear()


@router.callback_query(F.data == "tc_delete_all")
async def tc_delete_all_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id
    chats = get_tracked_chats(user_id)
    for chat_id, _ in chats:
        remove_tracked_chat(user_id, chat_id)
    await state.clear()
    await show_tracked_menu(query, user_id, menu_message_id=query.message.message_id)


@router.callback_query(F.data == "tc_cancel")
async def tc_cancel_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    await state.clear()
    user_id = query.from_user.id
    await show_tracked_menu(query, user_id, menu_message_id=query.message.message_id)

