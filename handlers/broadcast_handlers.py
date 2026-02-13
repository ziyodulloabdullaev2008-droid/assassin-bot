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

from services.broadcast_profiles_service import ensure_active_config, sync_active_config_from_db, get_active_config_id, get_config_detail

from services.mention_utils import delete_message_after_delay

from ui.broadcast_ui import build_broadcast_keyboard, build_broadcast_menu_text

from ui.texts_ui import build_texts_keyboard, build_text_settings_keyboard

from ui.main_menu_ui import get_main_menu_keyboard

router = Router()

user_authenticated = app_state.user_authenticated

broadcast_update_lock = app_state.broadcast_update_lock

active_broadcasts = app_state.active_broadcasts

def save_broadcast_config_with_profile(user_id: int, config: dict) -> None:

    ensure_active_config(user_id)

    save_broadcast_config(user_id, config)

    sync_active_config_from_db(user_id)

def add_broadcast_chat_with_profile(user_id: int, chat_id: int, chat_name: str) -> None:

    ensure_active_config(user_id)

    add_broadcast_chat(user_id, chat_id, chat_name)

    sync_active_config_from_db(user_id)

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

    waiting_for_chat_id = State()  # Для добавления чата

    waiting_for_chat_name = State()  # Для ввода имени чата если ID недоступен

    waiting_for_chat_delete = State()  # Для удаления чата

    viewing_active_broadcast = State()  # Для просмотра активной рассылки

    waiting_for_text_add = State()  # Для добавления нового текста

    waiting_for_text_edit = State()  # Для редактирования текста

class FakeMessage:

    """Вспомогательный класс для редактирования сообщений через callback"""

    def __init__(self, user_id, query=None):

        self.from_user = type('obj', (object,), {'id': user_id})()

        self.query = query
    async def answer(self, text, **kwargs):

        """\u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u0443\u0435\u0442 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u0438\u043b\u0438 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044f\u0435\u0442 \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435."""

        if not self.query:

            return

        try:

            reply_markup = kwargs.get('reply_markup')

            if reply_markup and isinstance(reply_markup, InlineKeyboardMarkup):

                await self.query.message.edit_text(

                    text,

                    reply_markup=reply_markup,

                    parse_mode=kwargs.get('parse_mode', 'HTML')

                )

            else:

                await self.query.message.answer(

                    text,

                    reply_markup=reply_markup,

                    parse_mode=kwargs.get('parse_mode', 'HTML')

                )

        except Exception as e:

            # Если сообщение не изменилось, просто отправляем уведомление

            if "not modified" in str(e).lower():

                await self.query.answer("✅", show_alert=False)

            else:

                print(f"⚠️  Ошибка при редактировании сообщения: {str(e)}")

async def show_broadcast_menu(message_or_query, user_id: int, is_edit: bool = False):

    """Показывает меню рассылки (отправляет или редактирует сообщение)"""

    config = get_broadcast_config(user_id)

    chats = get_broadcast_chats(user_id)

    info = build_broadcast_menu_text(config, chats, active_broadcasts, user_id)

    kb = build_broadcast_keyboard(include_active=False, user_id=user_id, active_broadcasts=active_broadcasts, back_callback="delete_bc_menu")

    if is_edit:

        try:

            await message_or_query.message.edit_text(text=info, reply_markup=kb, parse_mode="HTML")

        except Exception as e:

            print(f"Ошибка при edit_text: {e}")

            try:

                await message_or_query.message.answer(info, reply_markup=kb, parse_mode="HTML")

            except Exception as e2:

                print(f"Ошибка при answer: {e2}")

    else:

        await message_or_query.answer(info, reply_markup=kb, parse_mode="HTML")



def _build_broadcast_chats_view(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    chats = get_broadcast_chats(user_id)
    info = "\U0001F4AC <b>\u0427\u0410\u0422\u042B \u0414\u041B\u042F \u0420\u0410\u0421\u0421\u042B\u041B\u041A\u0418</b>\n\n"

    if chats:
        for idx, (chat_id, chat_name) in enumerate(chats, 1):
            info += f"{idx}\ufe0f\u20e3 {chat_name}\n   ID: {chat_id}\n\n"
    else:
        info += "\U0001F4ED \u041D\u0435\u0442 \u0447\u0430\u0442\u043E\u0432 \u0434\u043B\u044F \u0440\u0430\u0441\u0441\u044B\u043B\u043A\u0438\n\n"

    info += "\u041D\u0430\u0436\u043C\u0438 \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u0435:"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\u2795 \u0414\u043E\u0431\u0430\u0432\u0438\u0442\u044C", callback_data="bc_chats_add"),
         InlineKeyboardButton(text="\U0001F5D1\ufe0f \u0423\u0434\u0430\u043B\u0438\u0442\u044C", callback_data="bc_chats_delete")],
        [InlineKeyboardButton(text="\u2B05\ufe0f \u041D\u0430\u0437\u0430\u0434", callback_data="bc_back")]
    ])
    return info, kb


async def show_broadcast_chats_menu(message_or_query, user_id: int, menu_message_id: int | None = None) -> None:
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

    target = message_or_query.message if hasattr(message_or_query, "message") else message_or_query
    try:
        await target.edit_text(info, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await target.answer(info, reply_markup=kb, parse_mode="HTML")



async def broadcast_chats_menu(message: Message):
    """Backward-compatible wrapper for old calls."""
    await show_broadcast_chats_menu(message, message.from_user.id)

@router.message(Command("broadcast"))

@router.message(F.text == "📤 Рассылка")

async def cmd_broadcast_menu(message: Message):

    """Главное меню рассылки - информация и управление"""

    user_id = message.from_user.id

    if user_id not in user_authenticated:

        await message.answer("Ты не залогирован! Используй /login")

        return

    await show_broadcast_menu(message, user_id, is_edit=False)

@router.callback_query(F.data == "close_bc_menu")

async def close_bc_menu_callback(query: CallbackQuery):

    """Return to broadcast chats menu."""

    await query.answer()
    user_id = query.from_user.id
    try:
        await show_broadcast_chats_menu(query, user_id, menu_message_id=query.message.message_id)
    except Exception:
        pass

@router.callback_query(F.data == "bc_text")

async def bc_text_callback(query: CallbackQuery, state: FSMContext):

    """Открыть меню настроек текстов (режим и список)"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    info = "📝 УПРАВЛЕНИЕ ТЕКСТАМИ\n\n"

    info += f"Текстов добавлено: {len(config['texts'])}\n"

    info += f"Режим: {'Random ✅' if config.get('text_mode') == 'random' else 'No Random ❌'}\n"

    info += f"Формат: {config.get('parse_mode', 'HTML')}\n"

    kb = build_text_settings_keyboard(config.get('text_mode', 'random'), config.get('parse_mode', 'HTML'))

    await state.update_data(edit_message_id=query.message.message_id, chat_id=query.message.chat.id)

    await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "text_list")

async def text_list_callback(query: CallbackQuery, state: FSMContext):

    """Показать список текстов для управления"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    if not config['texts']:

        info = "📄 СПИСОК ТЕКСТОВ\n\n"

        info += "Нет добавленных текстов.\n\n"

        info += "Нажми 'Добавить новый' чтобы добавить текст для рассылки."

    else:

        info = "📄 СПИСОК ТЕКСТОВ\n\n"

        info += f"Всего текстов: {len(config['texts'])}\n"

        info += "Выбери текст для просмотра или редактирования.\n"

    kb = build_texts_keyboard(config['texts'], back_callback="bc_text")

    await state.update_data(edit_message_id=query.message.message_id, chat_id=query.message.chat.id)

    await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data.startswith("text_view_"))

async def text_view_callback(query: CallbackQuery, state: FSMContext):

    """Показать текст для просмотра и редактирования"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    try:

        text_index = int(query.data.split("_")[2])

        if text_index >= len(config['texts']):

            await query.answer("❌ Текст не найден", show_alert=True)

            return

        current_text = config['texts'][text_index]

        parse_mode = config.get('parse_mode', 'HTML')

        info = f"📋 ТЕКСТ #{text_index + 1}\n\n"

        info += f"📝 <b>Формат:</b> {parse_mode}\n"

        info += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

        # Показываем полный текст, но максимум 3500 символов для сообщения

        max_text_length = 3500

        if len(current_text) > max_text_length:

            display_text = current_text[:max_text_length]

            info += f"<code>{display_text}</code>\n"

            info += f"<i>... (текст обрезан, всего {len(current_text)} символов)</i>\n"

        else:

            info += f"<code>{current_text}</code>\n"

        info += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

        kb = InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="Изменить", callback_data=f"text_edit_{text_index}"),

             InlineKeyboardButton(text="Удалить", callback_data=f"text_delete_{text_index}")],

            [InlineKeyboardButton(text="Назад", callback_data="text_list")]

        ])

        await state.update_data(edit_message_id=query.message.message_id, chat_id=query.message.chat.id)

        await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")

    except (ValueError, IndexError):

        await query.answer("❌ Ошибка при выборе текста", show_alert=True)

@router.callback_query(F.data == "text_add_new")

async def text_add_new_callback(query: CallbackQuery, state: FSMContext):

    """Начать добавление нового текста"""

    await query.answer()

    await state.set_state(BroadcastConfigState.waiting_for_text_add)

    await state.update_data(edit_message_id=query.message.message_id, chat_id=query.message.chat.id)

    text = "📝 ДОБАВИТЬ НОВЫЙ ТЕКСТ\n\n"

    text += "Введи текст для рассылки.\n\n"

    text += "💡 <b>Поддерживается форматирование HTML:</b>\n"

    text += "<b>жирный</b>, <i>курсив</i>, <u>подчеркивание</u>\n"

    text += "Переносы строк сохраняются.\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="Отменить", callback_data="text_list")]

    ])

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data.startswith("text_edit_"))

async def text_edit_callback(query: CallbackQuery, state: FSMContext):

    """Начать редактирование текста"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    try:

        text_index = int(query.data.split("_")[2])

        if text_index >= len(config['texts']):

            await query.answer("❌ Текст не найден", show_alert=True)

            return

        await state.set_state(BroadcastConfigState.waiting_for_text_edit)

        await state.update_data(

            edit_message_id=query.message.message_id,

            chat_id=query.message.chat.id,

            text_index=text_index

        )

        text = f"✏️ РЕДАКТИРОВАТЬ ТЕКСТ #{text_index + 1}\n\n"

        text += "Введи новый текст.\n\n"

        text += "💡 <b>Поддерживается форматирование HTML:</b>\n"

        text += "<b>жирный</b>, <i>курсив</i>, <u>подчеркивание</u>\n"

        text += "Переносы строк сохраняются.\n"

        kb = InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="Отменить", callback_data=f"text_view_{text_index}")]

        ])

        await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

    except (ValueError, IndexError):

        await query.answer("❌ Ошибка при выборе текста", show_alert=True)

@router.callback_query(F.data.startswith("text_delete_"))

async def text_delete_callback(query: CallbackQuery, state: FSMContext):

    """Удалить текст"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    try:

        text_index = int(query.data.split("_")[2])

        if text_index >= len(config['texts']):

            await query.answer("❌ Текст не найден", show_alert=True)

            return

        # Удаляем текст

        config['texts'].pop(text_index)

        save_broadcast_config_with_profile(user_id, config)

        # Если это был последний текст, сбрасываем индекс

        if text_index >= len(config['texts']) and text_index > 0:

            config['text_index'] = len(config['texts']) - 1

            save_broadcast_config_with_profile(user_id, config)

        await query.answer("✅ Текст удален", show_alert=False)

        # Показываем обновленный список

        if not config['texts']:

            info = "📄 СПИСОК ТЕКСТОВ\n\n"

            info += "Нет добавленных текстов.\n\n"

            info += "Нажми 'Добавить новый' чтобы добавить текст для рассылки."

        else:

            info = "📄 СПИСОК ТЕКСТОВ\n\n"

            info += f"Всего текстов: {len(config['texts'])}\n"

            info += "Выбери текст для просмотра или редактирования.\n"

        kb = build_texts_keyboard(config['texts'], back_callback="bc_text")

        await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")

    except (ValueError, IndexError):

        await query.answer("❌ Ошибка при удалении текста", show_alert=True)

@router.callback_query(F.data == "text_mode_toggle")

async def text_mode_toggle_callback(query: CallbackQuery, state: FSMContext):

    """Переключить режим текстов (random <-> sequence)"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    if not config['texts']:

        await query.answer("❌ Добавь сначала тексты", show_alert=True)

        return

    # Переключаем режим

    config['text_mode'] = 'sequence' if config.get('text_mode') == 'random' else 'random'

    config['text_index'] = 0  # Сбрасываем индекс при переключении

    save_broadcast_config_with_profile(user_id, config)

    # Показываем обновленное меню

    info = "📝 УПРАВЛЕНИЕ ТЕКСТАМИ\n\n"

    info += f"Текстов добавлено: {len(config['texts'])}\n"

    info += f"Режим: {'Random ✅' if config.get('text_mode') == 'random' else 'No Random ❌'}\n"

    info += f"Формат: {config.get('parse_mode', 'HTML')}\n"

    kb = build_text_settings_keyboard(config.get('text_mode', 'random'), config.get('parse_mode', 'HTML'))

    await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "text_format_toggle")

async def text_format_toggle_callback(query: CallbackQuery, state: FSMContext):

    """Переключить формат текстов (HTML <-> Markdown)"""

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    # Переключаем формат

    config['parse_mode'] = 'Markdown' if config.get('parse_mode') == 'HTML' else 'HTML'

    save_broadcast_config_with_profile(user_id, config)

    # Показываем обновленное меню

    info = "📝 УПРАВЛЕНИЕ ТЕКСТАМИ\n\n"

    info += f"Текстов добавлено: {len(config['texts'])}\n"

    info += f"Режим: {'Random ✅' if config.get('text_mode') == 'random' else 'No Random ❌'}\n"

    info += f"Формат: {config.get('parse_mode', 'HTML')}\n"

    kb = build_text_settings_keyboard(config.get('text_mode', 'random'), config.get('parse_mode', 'HTML'))

    await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "bc_quantity")

async def bc_quantity_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    await state.set_state(BroadcastConfigState.waiting_for_count)

    await state.update_data(edit_message_id=query.message.message_id, chat_id=query.message.chat.id)

    config = get_broadcast_config(query.from_user.id)

    text = f"📊 КОЛИЧЕСТВО СООБЩЕНИЙ\n\nТекущее: {config.get('count', 0)}\n\nВведи новое (1-1000) или нажми отменить:"

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="❌ Отменить", callback_data="bc_cancel")]

    ])

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "bc_interval")

async def bc_interval_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    await state.set_state(BroadcastConfigState.waiting_for_interval)

    await state.update_data(edit_message_id=query.message.message_id, chat_id=query.message.chat.id)

    config = get_broadcast_config(query.from_user.id)

    current_interval = config.get('interval', '10-30')

    text = f"⏱️ <b>ИНТЕРВАЛ МЕЖДУ СООБЩЕНИЯМИ</b>\n\nТекущий: {current_interval} мин\n\nВведи новый (формат: мин-макс, например: 10-30) или одно число (15):"

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="❌ Отменить", callback_data="bc_cancel")]

    ])

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "bc_batch_pause")

async def bc_batch_pause_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    await state.set_state(BroadcastConfigState.waiting_for_chat_pause)

    await state.update_data(edit_message_id=query.message.message_id, chat_id=query.message.chat.id)

    config = get_broadcast_config(query.from_user.id)

    current_pause = config.get('chat_pause', '1-3')

    text = (
        f"⏳ <b>ТЕМП</b>\n\n"
        "Темп = задержка между отправками по разным чатам во время активной рассылки.\n\n"
        f"Текущий: <b>{current_pause}</b> сек\n\n"
        "Введи новый:\n"
        "• диапазон: <code>1-3</code>\n"
        "• одно значение: <code>2</code>"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="❌ Отменить", callback_data="bc_cancel_tempo")]

    ])

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "bc_plan_limit")
async def bc_plan_limit_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    await state.set_state(BroadcastConfigState.waiting_for_plan_limit)
    await state.update_data(edit_message_id=query.message.message_id, chat_id=query.message.chat.id, previous_menu='broadcast')

    config = get_broadcast_config(query.from_user.id)
    limit_count = config.get('plan_limit_count', 0)
    limit_rest = config.get('plan_limit_rest', 0)

    text = (
        "⏳ <b>ЛИМИТ</b>\n\n"
        "Лимит = сколько сообщений планировать одновременно и какой отдых делать после пакета.\n\n"
        f"Текущий: <b>{limit_count}</b> / отдых <b>{limit_rest}</b> мин\n\n"
        "Введи два числа через пробел:\n"
        "<code>лимит отдых_в_минутах</code>\n"
        "Пример: <code>10 3</code>\n"
        "Отключить лимит: <code>0 0</code>"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\u274c \u041e\u0442\u043c\u0435\u043d\u0438\u0442\u044c", callback_data="bc_cancel")]
    ])
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
    await state.update_data(previous_menu='broadcast', menu_message_id=query.message.message_id)
    await show_broadcast_chats_menu(query, user_id, menu_message_id=query.message.message_id)

@router.callback_query(F.data == "bc_active")

async def bc_active_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    user_broadcasts = {

        bid: b for bid, b in active_broadcasts.items()

        if b["user_id"] == user_id and b["status"] in ("running", "paused")

    }

    if not user_broadcasts:

        text = "\U0001F4ED \u041D\u0435\u0442 \u0430\u043A\u0442\u0438\u0432\u043D\u044B\u0445 \u0440\u0430\u0441\u0441\u044B\u043B\u043E\u043A"

        kb = InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="\u2B05\uFE0F \u041D\u0430\u0437\u0430\u0434", callback_data="bc_back")]

        ])

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

    info = "\U0001F4E4 <b>\u0410\u041A\u0422\u0418\u0412\u041D\u042B\u0415 \u0420\u0410\u0421\u0421\u042B\u041B\u041A\u0418</b>\n\n"

    buttons = []

    for gid, items in sorted(groups.items()):

        status = "\u25B6\uFE0F \u0410\u043A\u0442\u0438\u0432\u043D\u0430" if any(b["status"] == "running" for _, b in items) else "\u23F8\uFE0F \u041F\u0430\u0443\u0437\u0430"

        info += f"\u0413\u0440\u0443\u043F\u043F\u0430 #{gid} {status} | \u0410\u043A\u043A\u0430\u0443\u043D\u0442\u043E\u0432: {len(items)}\n"

        buttons.append([InlineKeyboardButton(text=f"\u0413\u0440\u0443\u043F\u043F\u0430 #{gid}", callback_data=f"view_group_{gid}")])

    for bid, b in sorted(singles):

        status = "\u25B6\uFE0F \u0410\u043A\u0442\u0438\u0432\u043D\u0430" if b["status"] == "running" else "\u23F8\uFE0F \u041F\u0430\u0443\u0437\u0430"

        account_name = b.get("account_name", f"\u0410\u043A\u043A\u0430\u0443\u043D\u0442 {b.get('account', '?')}")

        info += f"\u0420\u0430\u0441\u0441\u044B\u043B\u043A\u0430 #{bid} {status} | {account_name}\n"

        buttons.append([InlineKeyboardButton(text=f"\u0420\u0430\u0441\u0441\u044B\u043B\u043A\u0430 #{bid}", callback_data=f"view_bc_{bid}")])

    buttons.append([InlineKeyboardButton(text="\u2B05\uFE0F \u041D\u0430\u0437\u0430\u0434", callback_data="bc_back")])

    try:

        await query.message.edit_text(info, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

    except Exception:

        await query.message.answer(info, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

async def _render_group_detail(query: CallbackQuery, user_id: int, gid: int) -> None:

    items = [

        (bid, b) for bid, b in active_broadcasts.items()

        if b.get("group_id") == gid and b.get("user_id") == user_id and b.get("status") in ("running", "paused")

    ]

    if not items:

        await query.answer("\u0413\u0440\u0443\u043F\u043F\u0430 \u043D\u0435 \u043D\u0430\u0439\u0434\u0435\u043D\u0430", show_alert=True)

        return

    total_accounts = len(items)

    total_chats = sum(b.get("total_chats", 0) for _, b in items)

    total_count = sum((b.get("total_chats", 0) * b.get("count", 0)) for _, b in items)

    sent = sum(b.get("sent_chats", 0) for _, b in items)

    status = "\u25B6\uFE0F \u0410\u043A\u0442\u0438\u0432\u043D\u0430" if any(b["status"] == "running" for _, b in items) else "\u23F8\uFE0F \u041F\u0430\u0443\u0437\u0430"

    info = f"\U0001F4E6 <b>\u0413\u0440\u0443\u043F\u043F\u0430 #{gid}</b>\n\n"

    info += f"\u0421\u0442\u0430\u0442\u0443\u0441: {status}\n"

    info += f"\u0410\u043A\u043A\u0430\u0443\u043D\u0442\u043E\u0432: {total_accounts}\n"

    info += f"\u0427\u0430\u0442\u043E\u0432: {total_chats}\n"

    info += f"\u041F\u043B\u0430\u043D: {total_count}\n"

    info += f"\u041E\u0442\u043F\u0440\u0430\u0432\u043B\u0435\u043D\u043E: {sent}\n\n"

    buttons = [

        [InlineKeyboardButton(text="\u23F8\uFE0F \u041F\u0430\u0443\u0437\u0430", callback_data=f"bc_group_pause_{gid}"),

         InlineKeyboardButton(text="\u25B6\uFE0F \u041F\u0440\u043E\u0434\u043E\u043B\u0436\u0438\u0442\u044C", callback_data=f"bc_group_resume_{gid}")],

        [InlineKeyboardButton(text="\u26D4 \u041E\u0441\u0442\u0430\u043D\u043E\u0432\u0438\u0442\u044C", callback_data=f"bc_group_cancel_{gid}")],

        [InlineKeyboardButton(text="\u2B05\uFE0F \u041D\u0430\u0437\u0430\u0434", callback_data="bc_active")],

    ]

    try:

        await query.message.edit_text(info, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

    except Exception:

        await query.message.answer(info, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

@router.callback_query(F.data.startswith("view_group_"))

async def view_group_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    try:

        gid = int(query.data.split("_")[2])

    except Exception:

        await query.answer("\u041E\u0448\u0438\u0431\u043A\u0430", show_alert=True)

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

        await query.answer("\u041E\u0448\u0438\u0431\u043A\u0430", show_alert=True)

        return

    if bid not in active_broadcasts or active_broadcasts[bid]["user_id"] != user_id:

        await query.answer("\u0420\u0430\u0441\u0441\u044B\u043B\u043A\u0430 \u043D\u0435 \u043D\u0430\u0439\u0434\u0435\u043D\u0430", show_alert=True)

        return

    b = active_broadcasts[bid]

    status = "\u25B6\uFE0F \u0410\u043A\u0442\u0438\u0432\u043D\u0430" if b["status"] == "running" else "\u23F8\uFE0F \u041F\u0430\u0443\u0437\u0430" if b["status"] == "paused" else "\u2705 \u0417\u0430\u0432\u0435\u0440\u0448\u0435\u043D\u0430"

    account_name = b.get("account_name", f"\u0410\u043A\u043A\u0430\u0443\u043D\u0442 {b.get('account', '?')}")

    info = f"\U0001F4E4 <b>\u0420\u0430\u0441\u0441\u044B\u043B\u043A\u0430 #{bid}</b>\n\n"

    info += f"\u0421\u0442\u0430\u0442\u0443\u0441: {status}\n"

    info += f"\u0410\u043A\u043A\u0430\u0443\u043D\u0442: {account_name}\n"

    info += f"\u0427\u0430\u0442\u043E\u0432: {b.get('total_chats', 0)}\n"

    info += f"\u041E\u0442\u043F\u0440\u0430\u0432\u043B\u0435\u043D\u043E: {b.get('sent_chats', 0)}\n"

    info += f"\u041A\u043E\u043B-\u0432\u043E: {b.get('count', 0)}\n"

    info += f"\u0418\u043D\u0442\u0435\u0440\u0432\u0430\u043B: {b.get('interval_minutes', '?')} \u043C\u0438\u043D\n"

    buttons = [

        [InlineKeyboardButton(text="\u23F8\uFE0F \u041F\u0430\u0443\u0437\u0430", callback_data=f"pause_bc_{bid}"),

         InlineKeyboardButton(text="\u25B6\uFE0F \u041F\u0440\u043E\u0434\u043E\u043B\u0436\u0438\u0442\u044C", callback_data=f"resume_bc_{bid}")],

        [InlineKeyboardButton(text="\u26D4 \u041E\u0441\u0442\u0430\u043D\u043E\u0432\u0438\u0442\u044C", callback_data=f"cancel_bc_{bid}")],

        [InlineKeyboardButton(text="\u270F\uFE0F \u041A\u043E\u043B-\u0432\u043E", callback_data=f"bc_edit_count_{bid}"),

         InlineKeyboardButton(text="\u23F1\uFE0F \u0418\u043D\u0442\u0435\u0440\u0432\u0430\u043B", callback_data=f"bc_edit_interval_{bid}")],

        [InlineKeyboardButton(text="\u2B05\uFE0F \u041D\u0430\u0437\u0430\u0434", callback_data="bc_active")],

    ]

    try:

        await query.message.edit_text(info, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

    except Exception:

        await query.message.answer(info, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

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

    """Изменить кол-во активной рассылки"""

    await query.answer()

    user_id = query.from_user.id

    try:

        bid = int(query.data.split("_")[3])

    except:

        await query.answer("Ошибка", show_alert=True)

        return

    if bid not in active_broadcasts or active_broadcasts[bid]['user_id'] != user_id:

        await query.answer("Рассылка не найдена", show_alert=True)

        return

    await state.set_state(BroadcastConfigState.waiting_for_count)

    await state.update_data(edit_broadcast_id=bid, edit_message_id=query.message.message_id, chat_id=query.message.chat.id)

    info = "Введи новое кол-во сообщений (1-1000, или нажми Отменить):"

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="Отменить", callback_data=f"view_bc_{bid}")]

    ])

    await query.message.edit_text(info, reply_markup=kb)

@router.callback_query(F.data.startswith("bc_edit_interval_"))

async def bc_edit_interval_callback(query: CallbackQuery, state: FSMContext):

    """Изменить интервал активной рассылки"""

    await query.answer()

    user_id = query.from_user.id

    try:

        bid = int(query.data.split("_")[3])

    except:

        await query.answer("Ошибка", show_alert=True)

        return

    if bid not in active_broadcasts or active_broadcasts[bid]['user_id'] != user_id:

        await query.answer("Рассылка не найдена", show_alert=True)

        return

    await state.set_state(BroadcastConfigState.waiting_for_interval)

    await state.update_data(edit_broadcast_id=bid, edit_message_id=query.message.message_id, chat_id=query.message.chat.id)

    info = "Введи новый интервал в минутах (1-60, или нажми Отменить):"

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="Отменить", callback_data=f"view_bc_{bid}")]

    ])

    await query.message.edit_text(info, reply_markup=kb)

@router.message(BroadcastConfigState.waiting_for_text_add)

async def process_text_add(message: Message, state: FSMContext):

    """Обработчик добавления нового текста в список"""

    user_id = message.from_user.id

    # Проверяем что это не отмена

    if message.text and message.text.startswith("↩️"):

        await state.clear()

        # Показываем список текстов

        config = get_broadcast_config(user_id)

        if not config['texts']:

            info = "📄 СПИСОК ТЕКСТОВ\n\n"

            info += "Нет добавленных текстов.\n\n"

            info += "Нажми 'Добавить новый' чтобы добавить текст для рассылки."

        else:

            info = "📄 СПИСОК ТЕКСТОВ\n\n"

            info += f"Всего текстов: {len(config['texts'])}\n"

            info += "Выбери текст для просмотра или редактирования.\n"

        kb = build_texts_keyboard(config['texts'], back_callback="bc_text")

        data = await state.get_data()

        chat_id = data.get('chat_id')

        edit_message_id = data.get('edit_message_id')

        if edit_message_id and chat_id:

            try:

                await message.bot.edit_message_text(

                    chat_id=chat_id,

                    message_id=edit_message_id,

                    text=info,

                    reply_markup=kb,

                    parse_mode="HTML"

                )

            except:

                await message.answer(info, reply_markup=kb, parse_mode="HTML")

        return

    # Добавляем новый текст

    config = get_broadcast_config(user_id)

    config['texts'].append(message.text)

    save_broadcast_config_with_profile(user_id, config)

    await state.clear()

    await message.delete()

    # Показываем обновленный список

    if not config['texts']:

        info = "📄 СПИСОК ТЕКСТОВ\n\n"

        info += "Нет добавленных текстов.\n\n"

        info += "Нажми 'Добавить новый' чтобы добавить текст для рассылки."

    else:

        info = "📄 СПИСОК ТЕКСТОВ\n\n"

        info += f"Всего текстов: {len(config['texts'])}\n"

        info += "Выбери текст для просмотра или редактирования.\n"

    kb = build_texts_keyboard(config['texts'], back_callback="bc_text")

    data = await state.get_data()

    chat_id = data.get('chat_id')

    edit_message_id = data.get('edit_message_id')

    if edit_message_id and chat_id:

        try:

            await message.bot.edit_message_text(

                chat_id=chat_id,

                message_id=edit_message_id,

                text=info,

                reply_markup=kb,

                parse_mode="HTML"

            )

        except:

            await message.answer(info, reply_markup=kb, parse_mode="HTML")

    else:

        await message.answer(info, reply_markup=kb, parse_mode="HTML")

@router.message(BroadcastConfigState.waiting_for_text_edit)

async def process_text_edit(message: Message, state: FSMContext):

    """Обработчик редактирования текста"""

    user_id = message.from_user.id

    # Проверяем что это не отмена

    if message.text and message.text.startswith("↩️"):

        data = await state.get_data()

        text_index = data.get('text_index', 0)

        await state.clear()

        # Показываем измененный текст

        config = get_broadcast_config(user_id)

        if text_index >= len(config['texts']):

            text_index = len(config['texts']) - 1

        current_text = config['texts'][text_index]

        parse_mode = config.get('parse_mode', 'HTML')

        info = f"📋 ТЕКСТ #{text_index + 1}\n\n"

        info += f"📝 <b>Формат:</b> {parse_mode}\n"

        info += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

        max_text_length = 3500

        if len(current_text) > max_text_length:

            display_text = current_text[:max_text_length]

            info += f"<code>{display_text}</code>\n"

            info += f"<i>... (текст обрезан, всего {len(current_text)} символов)</i>\n"

        else:

            info += f"<code>{current_text}</code>\n"

        info += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

        kb = InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="Изменить", callback_data=f"text_edit_{text_index}"),

             InlineKeyboardButton(text="Удалить", callback_data=f"text_delete_{text_index}")],

            [InlineKeyboardButton(text="Назад", callback_data="text_list")]

        ])

        data = await state.get_data()

        chat_id = data.get('chat_id')

        edit_message_id = data.get('edit_message_id')

        if edit_message_id and chat_id:

            try:

                await message.bot.edit_message_text(

                    chat_id=chat_id,

                    message_id=edit_message_id,

                    text=info,

                    reply_markup=kb,

                    parse_mode="HTML"

                )

            except:

                await message.answer(info, reply_markup=kb, parse_mode="HTML")

        return

    # Редактируем текст

    data = await state.get_data()

    text_index = data.get('text_index', 0)

    config = get_broadcast_config(user_id)

    if text_index < len(config['texts']):

        config['texts'][text_index] = message.text

        save_broadcast_config_with_profile(user_id, config)

    await state.clear()

    await message.delete()

    # Показываем обновленный текст

    if text_index >= len(config['texts']):

        text_index = len(config['texts']) - 1

    current_text = config['texts'][text_index]

    parse_mode = config.get('parse_mode', 'HTML')

    info = f"📋 ТЕКСТ #{text_index + 1}\n\n"

    info += f"📝 <b>Формат:</b> {parse_mode}\n"

    info += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

    max_text_length = 3500

    if len(current_text) > max_text_length:

        display_text = current_text[:max_text_length]

        info += f"<code>{display_text}</code>\n"

        info += f"<i>... (текст обрезан, всего {len(current_text)} символов)</i>\n"

    else:

        info += f"<code>{current_text}</code>\n"

    info += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="Изменить", callback_data=f"text_edit_{text_index}"),

         InlineKeyboardButton(text="Удалить", callback_data=f"text_delete_{text_index}")],

        [InlineKeyboardButton(text="Назад", callback_data="text_list")]

    ])

    chat_id = data.get('chat_id')

    edit_message_id = data.get('edit_message_id')

    if edit_message_id and chat_id:

        try:

            await message.bot.edit_message_text(

                chat_id=chat_id,

                message_id=edit_message_id,

                text=info,

                reply_markup=kb,

                parse_mode="HTML"

            )

        except:

            await message.answer(info, reply_markup=kb, parse_mode="HTML")

    else:

        await message.answer(info, reply_markup=kb, parse_mode="HTML")

@router.message(F.text == "📊 Количество")

async def broadcast_count_button(message: Message, state: FSMContext):

    """Обработчик кнопки выбора количества сообщений"""

    user_id = message.from_user.id

    config = get_broadcast_config(user_id)

    await state.update_data(previous_menu='broadcast')

    await state.set_state(BroadcastConfigState.waiting_for_count)

    keyboard = ReplyKeyboardMarkup(

        keyboard=[[KeyboardButton(text="↩️ Отменить")]],

        resize_keyboard=True

    )

    await message.answer(f"📊 КОЛИЧЕСТВО СООБЩЕНИЙ\n\n📌 Текущее: {config.get('count', 0)} шт\n\nОтправь новое количество:\n(число от 1 до 1000)", reply_markup=keyboard)

@router.message(BroadcastConfigState.waiting_for_count)

async def process_broadcast_count(message: Message, state: FSMContext):

    """Обработчик получения количества сообщений"""

    user_id = message.from_user.id

    # Проверяем что это не кнопка отмены

    if message.text == "↩️ Отменить":

        await return_to_previous_menu(message, state)

        return

    try:

        count = int(message.text)

        if count < 1 or count > 1000:

            await message.answer("❌ Количество должно быть от 1 до 1000")

            return

        config = get_broadcast_config(user_id)

        config['count'] = count

        save_broadcast_config_with_profile(user_id, config)

        data = await state.get_data()

        edit_message_id = data.get('edit_message_id')

        chat_id = data.get('chat_id')

        await state.clear()

        # Удаляем сообщение пользователя

        try:

            await message.delete()

        except:

            pass

        # Редактируем то же сообщение с информацией о рассылке или отправляем новое

        chats = get_broadcast_chats(user_id)

        if edit_message_id and chat_id:

            try:

                info = build_broadcast_menu_text(config, chats, active_broadcasts, user_id)

                kb = build_broadcast_keyboard(include_active=False, user_id=user_id, active_broadcasts=active_broadcasts, back_callback="delete_bc_menu")

                await message.bot.edit_message_text(

                    chat_id=chat_id,

                    message_id=edit_message_id,

                    text=info,

                    reply_markup=kb,

                    parse_mode="HTML"

                )

            except Exception as e:

                print(f"Ошибка редактирования: {e}")

                import traceback

                traceback.print_exc()

                await message.answer("Ошибка при обновлении меню")

        else:

            await cmd_broadcast_menu(message)

    except ValueError:

        await message.answer("❌ Введи число!")

@router.message(F.text == "⏱️ Интервал")

async def broadcast_interval_button(message: Message, state: FSMContext):

    """Обработчик кнопки выбора интервала"""

    user_id = message.from_user.id

    config = get_broadcast_config(user_id)

    await state.update_data(previous_menu='broadcast')

    await state.set_state(BroadcastConfigState.waiting_for_interval)

    keyboard = ReplyKeyboardMarkup(

        keyboard=[[KeyboardButton(text="↩️ Отменить")]],

        resize_keyboard=True

    )

    await message.answer(f"⏱️ ИНТЕРВАЛ МЕЖДУ СООБЩЕНИЯМИ\n\n📌 Текущий: {config.get('interval', 0)} мин\n\nОтправь новый интервал в минутах:\n(число от 1 до 60 мин)", reply_markup=keyboard)

@router.message(BroadcastConfigState.waiting_for_interval)

async def process_broadcast_interval(message: Message, state: FSMContext):

    """Обработчик получения интервала"""

    user_id = message.from_user.id

    # Проверяем что это не кнопка отмены

    if message.text == "↩️ Отменить":

        await return_to_previous_menu(message, state)

        return

    try:

        text = message.text.strip()

        # Парсим формат: может быть число или диапазон мин-макс

        if '-' in text:

            # Формат: мин-макс

            parts = text.split('-')

            if len(parts) != 2:

                await message.answer("❌ Неверный формат. Используй: 10-30 или 15")

                return

            try:

                min_interval = int(parts[0].strip())

                max_interval = int(parts[1].strip())

                if min_interval < 1 or max_interval < 1 or min_interval > max_interval:

                    await message.answer("❌ Значения должны быть положительными, мин ≤ макс")

                    return

                if min_interval > 480 or max_interval > 480:

                    await message.answer("❌ Интервал не должен быть больше 480 минут (8 часов)")

                    return

                interval_value = text  # Сохраняем как строку "мин-макс"

            except ValueError:

                await message.answer("❌ Введи числа в формате: 10-30")

                return

        else:

            # Одно число

            try:

                interval_int = int(text)

                if interval_int < 1 or interval_int > 480:

                    await message.answer("❌ Интервал должен быть от 1 до 480 минут")

                    return

                interval_value = text

            except ValueError:

                await message.answer("❌ Введи число или диапазон (мин-макс)")

                return

        # Сохраняем конфиг

        config = get_broadcast_config(user_id)

        config['interval'] = interval_value

        save_broadcast_config_with_profile(user_id, config)

        data = await state.get_data()

        edit_message_id = data.get('edit_message_id')

        chat_id = data.get('chat_id')

        await state.clear()

        # Удаляем сообщение пользователя

        try:

            await message.delete()

        except:

            pass

        # Редактируем то же сообщение с информацией о рассылке

        chats = get_broadcast_chats(user_id)

        if edit_message_id and chat_id:

            try:

                info = build_broadcast_menu_text(config, chats, active_broadcasts, user_id)

                kb = build_broadcast_keyboard(include_active=False, user_id=user_id, active_broadcasts=active_broadcasts, back_callback="delete_bc_menu")

                await message.bot.edit_message_text(

                    chat_id=chat_id,

                    message_id=edit_message_id,

                    text=info,

                    reply_markup=kb,

                    parse_mode="HTML"

                )

            except Exception as e:

                print(f"Ошибка редактирования: {e}")

                import traceback

                traceback.print_exc()

                await message.answer("Ошибка при обновлении меню")

        else:

            await cmd_broadcast_menu(message)

    except ValueError:

        await message.answer("❌ Введи число!")

@router.message(BroadcastConfigState.waiting_for_plan_limit)
async def process_broadcast_plan_limit(message: Message, state: FSMContext):
    """Обработчик лимита планирования"""
    user_id = message.from_user.id

    if message.text == "↩️ Отменить":
        await return_to_previous_menu(message, state)
        return

    raw = message.text.strip().replace(",", " ")
    parts = [p for p in raw.split() if p]

    if len(parts) < 2:
        await message.answer("❌ Формат: лимит отдых. Пример: 10 3")
        return

    try:
        limit_count = int(parts[0])
        limit_rest = int(parts[1])
    except ValueError:
        await message.answer("❌ Введи числа. Пример: 10 3")
        return

    if limit_count < 0 or limit_rest < 0:
        await message.answer("❌ Значения должны быть неотрицательные")
        return

    config = get_broadcast_config(user_id)
    config["plan_limit_count"] = limit_count
    config["plan_limit_rest"] = limit_rest
    save_broadcast_config_with_profile(user_id, config)

    data = await state.get_data()
    edit_message_id = data.get('edit_message_id')
    chat_id = data.get('chat_id')
    await state.clear()

    try:
        await message.delete()
    except Exception:
        pass

    if edit_message_id and chat_id:
        try:
            chats = get_broadcast_chats(user_id)
            info = build_broadcast_menu_text(config, chats, active_broadcasts, user_id)
            kb = build_broadcast_keyboard(include_active=False, user_id=user_id, active_broadcasts=active_broadcasts, back_callback="delete_bc_menu")
            await message.bot.edit_message_text(
                chat_id=chat_id,
                message_id=edit_message_id,
                text=info,
                reply_markup=kb,
                parse_mode="HTML"
            )
        except Exception:
            await cmd_broadcast_menu(message)
    else:
        await cmd_broadcast_menu(message)


@router.message(BroadcastConfigState.waiting_for_chat_pause)
async def process_broadcast_chat_pause(message: Message, state: FSMContext):

    """Обработчик получения задержки между чатами"""

    user_id = message.from_user.id

    # Проверяем что это не кнопка отмены

    if message.text == "↩️ Отменить":

        await return_to_previous_menu(message, state)

        return

    try:

        text = message.text.strip()

        # Проверяем формат: может быть число или диапазон мин-макс

        if '-' in text:

            # Формат: мин-макс

            parts = text.split('-')

            if len(parts) != 2:

                await message.answer("❌ Неверный формат. Используй: 1-3 или 2")

                return

            try:

                min_pause = int(parts[0].strip())

                max_pause = int(parts[1].strip())

                if min_pause < 1 or max_pause < 1 or min_pause > max_pause:

                    await message.answer("❌ Значения должны быть положительными, мин ≤ макс")

                    return

                if min_pause > 30 or max_pause > 30:

                    await message.answer("❌ Задержка не должна быть больше 30 секунд")

                    return

                pause_value = text  # Сохраняем как строку "мин-макс"

            except ValueError:

                await message.answer("❌ Введи числа в формате: 1-3")

                return

        else:

            # Одно число

            try:

                pause_int = int(text)

                if pause_int < 1 or pause_int > 30:

                    await message.answer("❌ Задержка должна быть от 1 до 30 секунд")

                    return

                pause_value = text

            except ValueError:

                await message.answer("❌ Введи число или диапазон (мин-макс)")

                return

        # Сохраняем конфиг

        config = get_broadcast_config(user_id)

        config['chat_pause'] = pause_value

        save_broadcast_config_with_profile(user_id, config)

        data = await state.get_data()

        edit_message_id = data.get('edit_message_id')

        chat_id = data.get('chat_id')

        await state.clear()

        # Удаляем сообщение пользователя

        try:

            await message.delete()

        except:

            pass

        # Редактируем меню рассылки

        if edit_message_id and chat_id:

            try:

                chats = get_broadcast_chats(user_id)

                info = build_broadcast_menu_text(config, chats, active_broadcasts, user_id)

                kb = build_broadcast_keyboard(include_active=False, user_id=user_id, active_broadcasts=active_broadcasts, back_callback="delete_bc_menu")

                await message.bot.edit_message_text(

                    chat_id=chat_id,

                    message_id=edit_message_id,

                    text=info,

                    reply_markup=kb,

                    parse_mode="HTML"

                )

            except Exception as e:

                print(f"Ошибка редактирования: {e}")

                await message.answer("Ошибка при обновлении меню")

        else:

            await cmd_broadcast_menu(message)

    except Exception as e:

        print(f"Ошибка обработки задержки между чатами: {e}")

        await message.answer("❌ Ошибка при сохранении задержки")

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

async def execute_broadcast(message_or_query, user_id: int, account_number: int, config: dict, chats: list, group_id: int = None) -> None:

    chat_ids = [cid for cid, _ in chats]

    broadcast_id = next_broadcast_id()

    account_name = None

    for acc_num, telegram_id, username, first_name, is_active in get_user_accounts(user_id):

        if acc_num == account_number:

            account_name = first_name or username or f"\u0410\u043A\u043A\u0430\u0443\u043D\u0442 {acc_num}"

            break

    payload = {

        "user_id": user_id,

        "account": account_number,

        "account_name": account_name or f"\u0410\u043A\u043A\u0430\u0443\u043D\u0442 {account_number}",

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

            interval_minutes=int(config.get("interval", 1)) if str(config.get("interval", 1)).isdigit() else 1,

            count=int(config.get("count", 1)),

            broadcast_id=broadcast_id,

            parse_mode=config.get("parse_mode", "HTML"),

            text_mode=config.get("text_mode", "random"),

        )

    )

    await _send_broadcast_notice(message_or_query, f"\u2705 \u0420\u0430\u0441\u0441\u044B\u043B\u043A\u0430 #{broadcast_id} \u0437\u0430\u043F\u0443\u0449\u0435\u043D\u0430")

@router.callback_query(F.data == "bc_launch")
async def bc_launch_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id

    if user_id not in user_authenticated or not user_authenticated[user_id]:
        await _send_broadcast_notice(query, "\u274C \u0422\u044b \u043d\u0435 \u0437\u0430\u043b\u043e\u0433\u0438\u0440\u043e\u0432\u0430\u043d!")
        return

    config = get_broadcast_config(user_id)
    chats = get_broadcast_chats(user_id)

    if not config.get("texts"):
        await _send_broadcast_notice(query, "\u274C \u0422\u0435\u043a\u0441\u0442 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438 \u043d\u0435 \u0443\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d!\n\n\u041d\u0430\u0436\u043c\u0438 '\U0001f4dd \u0412\u044b\u0431\u0440\u0430\u0442\u044c \u0442\u0435\u043a\u0441\u0442' \u0447\u0442\u043e\u0431\u044b \u0443\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u044c \u0442\u0435\u043a\u0441\u0442")
        return

    if not chats:
        await _send_broadcast_notice(query, "\u274C \u041d\u0435\u0442 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438!\n\n\u0414\u043e\u0431\u0430\u0432\u044c \u0447\u0430\u0442\u044b \u0447\u0435\u0440\u0435\u0437 '\U0001f4ac \u0427\u0430\u0442\u044b \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438'")
        return

    accounts = get_user_accounts(user_id)
    if len(accounts) == 1:
        account_number = accounts[0][0]
        await execute_broadcast(query, user_id, account_number, config, chats)
        return

    buttons = []
    for acc_num, telegram_id, username, first_name, is_active in accounts:
        is_connected = user_id in user_authenticated and acc_num in user_authenticated[user_id]
        if is_connected:
            buttons.append([InlineKeyboardButton(
                text=f"\U0001f7e2 {first_name}",
                callback_data=f"start_bc_{acc_num}"
            )])

    if len(buttons) > 1:
        buttons.insert(0, [InlineKeyboardButton(text="\U0001f7e2 \u0412\u0441\u0435 \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u044b", callback_data="start_bc_all")])

    if not buttons:
        await _send_broadcast_notice(query, "\u274C \u041d\u0435\u0442 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043d\u044b\u0445 \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432!")
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await query.message.answer("\u0412\u044b\u0431\u0435\u0440\u0438 \u0430\u043a\u043a\u0430\u0443\u043d\u0442 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438:", reply_markup=keyboard)


@router.callback_query(F.data.startswith("start_bc_"))

async def start_bc_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    config = get_broadcast_config(user_id)

    chats = get_broadcast_chats(user_id)

    if not config.get("texts"):

        await _send_broadcast_notice(query, "\u274C \u0422\u0435\u043A\u0441\u0442 \u0440\u0430\u0441\u0441\u044B\u043B\u043A\u0438 \u043D\u0435 \u0443\u0441\u0442\u0430\u043D\u043E\u0432\u043B\u0435\u043D")

        return

    if not chats:

        await _send_broadcast_notice(query, "\u274C \u041D\u0435\u0442 \u0447\u0430\u0442\u043E\u0432 \u0434\u043B\u044F \u0440\u0430\u0441\u0441\u044B\u043B\u043A\u0438")

        return

    if query.data == "start_bc_all":

        accounts = get_user_accounts(user_id)

        connected_accounts = [acc_num for acc_num, _, _, _, _ in accounts if user_id in user_authenticated and acc_num in user_authenticated[user_id]]

        if not connected_accounts:

            await _send_broadcast_notice(query, "\u274C \u041D\u0435\u0442 \u043F\u043E\u0434\u043A\u043B\u044E\u0447\u0435\u043D\u043D\u044B\u0445 \u0430\u043A\u043A\u0430\u0443\u043D\u0442\u043E\u0432")

            return

        group_id = next_broadcast_id()

        for acc_num in connected_accounts:

            await execute_broadcast(query, user_id, acc_num, config, chats, group_id=group_id)

        await _send_broadcast_notice(query, f"\u2705 \u0417\u0430\u043F\u0443\u0449\u0435\u043D\u043E \u0430\u043A\u043A\u0430\u0443\u043D\u0442\u043E\u0432: {len(connected_accounts)}")

        return

    try:

        account_number = int(query.data.split("_")[2])

    except Exception:

        await _send_broadcast_notice(query, "\u041E\u0448\u0438\u0431\u043A\u0430")

        return

    await execute_broadcast(query, user_id, account_number, config, chats)

@router.message(F.text.in_([
    "\U0001f680 \u0417\u0430\u043f\u0443\u0441\u0442\u0438\u0442\u044c",
    "\U0001f680 \u0417\u0430\u043f\u0443\u0441\u0442\u0438\u0442\u044c \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0443",
]))

async def start_broadcast_button(message: Message):

    """Обработчик кнопки запуска рассылки"""

    user_id = message.from_user.id

    # Проверяем залогирован ли

    if user_id not in user_authenticated or not user_authenticated[user_id]:

        await message.answer("❌ Ты не залогирован!")

        return

    # Получаем конфиг рассылки

    config = get_broadcast_config(user_id)

    chats = get_broadcast_chats(user_id)

    # Проверяем что есть текст

    if not config.get('texts'):

        await message.answer("❌ Текст рассылки не установлен!\n\nНажми '📝 Выбрать текст' чтобы установить текст")

        return

    # Проверяем что есть чаты

    if not chats:

        await message.answer("❌ Нет чатов для рассылки!\n\nДобавь чаты через '💬 Чаты для рассылки'")

        return

    # Если только один аккаунт - используем его

    accounts = get_user_accounts(user_id)

    if len(accounts) == 1:

        account_number = accounts[0][0]

    else:

        # Если несколько - показываем выбор

        buttons = []

        for acc_num, telegram_id, username, first_name, is_active in accounts:

            is_connected = user_id in user_authenticated and acc_num in user_authenticated[user_id]

            if is_connected:

                buttons.append([InlineKeyboardButton(

                    text=f"🟢 {first_name}",

                    callback_data=f"start_bc_{acc_num}"

                )])

        if len(buttons) > 1:

            buttons.insert(0, [InlineKeyboardButton(text="\U0001F7E2 \u0412\u0441\u0435 \u0430\u043A\u043A\u0430\u0443\u043D\u0442\u044B", callback_data="start_bc_all")])

        if not buttons:

            await message.answer("❌ Нет подключенных аккаунтов!")

            return

        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

        await message.answer("Выбери аккаунт для рассылки:", reply_markup=keyboard)

        return

    # Запускаем рассылку

    await execute_broadcast(message, user_id, account_number, config, chats)

# Обработчик кнопки "Активные" для просмотра активных рассылок

@router.message(F.text == "\U0001F4E4 \u0410\u043A\u0442\u0438\u0432\u043D\u044B\u0435")

async def active_broadcasts_button(message: Message):

    user_id = message.from_user.id

    user_broadcasts = {

        bid: b for bid, b in active_broadcasts.items()

        if b["user_id"] == user_id and b["status"] in ("running", "paused")

    }

    if not user_broadcasts:

        await message.answer("\u274C \u041D\u0435\u0442 \u0430\u043A\u0442\u0438\u0432\u043D\u044B\u0445 \u0440\u0430\u0441\u0441\u044B\u043B\u043E\u043A")

        return

    groups = {}

    singles = []

    for bid, b in user_broadcasts.items():

        gid = b.get("group_id")

        if gid is None:

            singles.append((bid, b))

        else:

            groups.setdefault(gid, []).append((bid, b))

    info = "\U0001F4E4 <b>\u0410\u041A\u0422\u0418\u0412\u041D\u042B\u0415 \u0420\u0410\u0421\u0421\u042B\u041B\u041A\u0418</b>\n\n"

    for gid, items in sorted(groups.items()):

        status = "\u25B6\uFE0F \u0410\u043A\u0442\u0438\u0432\u043D\u0430" if any(b["status"] == "running" for _, b in items) else "\u23F8\uFE0F \u041F\u0430\u0443\u0437\u0430"

        info += f"\u0413\u0440\u0443\u043F\u043F\u0430 #{gid} {status} | \u0410\u043A\u043A\u0430\u0443\u043D\u0442\u043E\u0432: {len(items)}\n"

    for bid, b in sorted(singles):

        status = "\u25B6\uFE0F \u0410\u043A\u0442\u0438\u0432\u043D\u0430" if b["status"] == "running" else "\u23F8\uFE0F \u041F\u0430\u0443\u0437\u0430"

        account_name = b.get("account_name", f"\u0410\u043A\u043A\u0430\u0443\u043D\u0442 {b.get('account', '?')}")

        info += f"\u0420\u0430\u0441\u0441\u044B\u043B\u043A\u0430 #{bid} {status} | {account_name}\n"

    await message.answer(info, parse_mode="HTML")

    inline_buttons = []

    for gid, items in sorted(groups.items()):

        inline_buttons.append([

            InlineKeyboardButton(text=f"\u0413\u0440\u0443\u043F\u043F\u0430 #{gid}", callback_data=f"view_group_{gid}")

        ])

    for bid, b in sorted(singles):

        inline_buttons.append([

            InlineKeyboardButton(text=f"\u0420\u0430\u0441\u0441\u044B\u043B\u043A\u0430 #{bid}", callback_data=f"view_bc_{bid}")

        ])

    inline_buttons.append([

        InlineKeyboardButton(text="\u2B05\uFE0F \u041D\u0430\u0437\u0430\u0434 \u0432 \u043C\u0435\u043D\u044E", callback_data="back_to_broadcast_menu")

    ])

    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=inline_buttons)

    await message.answer("\u0412\u044B\u0431\u0435\u0440\u0438 \u0440\u0430\u0441\u0441\u044B\u043B\u043A\u0443 \u0434\u043B\u044F \u0443\u043F\u0440\u0430\u0432\u043B\u0435\u043D\u0438\u044F:", reply_markup=inline_keyboard)

@router.callback_query(F.data == "bc_chats_add")

async def bc_chats_add_callback(query: CallbackQuery, state: FSMContext):

    """Обработчик кнопки добавления чата из меню"""

    await query.answer()

    await state.update_data(previous_menu='broadcast_chats', menu_message_id=query.message.message_id)

    await state.set_state(BroadcastConfigState.waiting_for_chat_id)

    text = "💬 <b>ДОБАВЛЕНИЕ ЧАТА</b>\n\nОтправь ID чата или ссылку на канал:\nПримеры:\n  • ID: -1001234567890\n  • Ссылка: @mychannel\n\n⚠️ Чат должен быть открытым или доступным твоему аккаунту"

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="❌ Отменить", callback_data="bc_cancel")]

    ])

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.message(BroadcastConfigState.waiting_for_chat_id)

async def process_add_broadcast_chat_with_profile(message: Message, state: FSMContext):

    """Обработчик добавления чата в рассылку"""

    user_id = message.from_user.id

    chat_input = message.text.strip()

    # Проверяем что это не кнопка отмены

    if chat_input == "↩️ Отменить":

        await return_to_previous_menu(message, state)

        return

    # Удаляем сообщение пользователя для чистоты чата

    try:

        await message.delete()

    except Exception:

        pass

    # Показываем загрузку

    loading_msg = await message.answer("⏳ Загружаю информацию о чате...")

    try:

        # Проверяем авторизацию

        if user_id not in user_authenticated or not user_authenticated[user_id]:

            await message.answer("❌ Ты не залогирован!")

            await state.clear()

            return

        # Берём ПЕРВЫЙ подключенный аккаунт для получения информации о чате

        account_number = next(iter(user_authenticated[user_id].keys()))

        client = user_authenticated[user_id][account_number]

        chat_id = None

        chat_name = None

        # Пытаемся получить информацию о чате

        chat = None

        # Пытаемся парсить как число (ID чата)

        try:

            # Если это число - пытаемся использовать как chat_id напрямую

            if chat_input.lstrip('-').isdigit():

                chat_id = int(chat_input)

                # Пытаемся получить информацию о чате через ID

                try:

                    # Для супергрупп ID выглядит как -1001234567890

                    # Telethon требует преобразование: -1001234567890 -> 1234567890 (убираем -100)

                    if chat_id < 0 and str(chat_id).startswith('-100'):

                        # Это супергруппа, преобразуем ID

                        actual_id = chat_id

                    else:

                        actual_id = chat_id

                    chat = await client.get_entity(actual_id)

                    if chat:

                        title = getattr(chat, 'title', None) or getattr(chat, 'first_name', None)

                        if not title and hasattr(chat, 'id'):

                            title = f"user{chat.id}"

                        chat_name = str(title) if title else f"Чат {chat_id}"

                    else:

                        chat_name = f"Чат {chat_id}"

                except Exception as e:

                    # Если не получилось напрямую, пытаемся как обычный entity

                    try:

                        chat = await client.get_entity(chat_input)

                        if chat:

                            chat_id = chat.id

                            title = getattr(chat, 'title', None) or getattr(chat, 'first_name', None)

                            if not title and hasattr(chat, 'id'):

                                title = f"user{chat.id}"

                            chat_name = str(title) if title else f"Чат {chat_id}"

                        else:

                            chat_id = int(chat_input)

                            chat_name = f"Чат {chat_id}"

                    except:

                        # Если всё равно не получилось, просто используем ID как есть

                        chat_id = int(chat_input)

                        chat_name = f"Чат {chat_id}"

                        chat = None

            else:

                # Это юзернейм или другой идентификатор

                try:

                    chat = await client.get_entity(chat_input)

                    if chat:

                        chat_id = chat.id

                        title = getattr(chat, 'title', None) or getattr(chat, 'first_name', None)

                        if not title and hasattr(chat, 'id'):

                            title = f"user{chat.id}"

                        chat_name = str(title) if title else f"Чат {chat_id}"

                    else:

                        await message.answer(f"❌ Чат не найден")

                        return

                except Exception as e:

                    print(f"⚠️  Не смог получить чат по {chat_input}: {str(e)}")

                    await message.answer(f"❌ Чат не найден. Убедись что ты в этом чате.\n\n💡 Можешь просто ввести ID чата число, например: `-1003880811528`")

                    return

        except Exception as e:

            print(f"❌ Ошибка парсинга: {str(e)}")

            await message.answer(f"❌ Неверный формат. Введи ID чата (например `-1003880811528`) или юзернейм (например `@mychannel`)")

            return

        if chat_id is None:

            await message.answer("❌ Не удалось определить ID чата")

            return

        # Добавляем чат в БД

        added = add_broadcast_chat_with_profile(user_id, chat_id, chat_name or f"Чат {chat_id}")

        # Отправляем уведомление

        if added:

            notify_msg = await message.answer(f"✅ Чат '{chat_name or chat_id}' успешно добавлен!")

        else:

            notify_msg = await message.answer(f"⚠️ Чат '{chat_name or chat_id}' уже в списке!")

        # Удаляем уведомление после 5 секунд

        import asyncio

        asyncio.create_task(delete_message_after_delay(notify_msg, 5))

        # Удаляем сообщение загрузки

        try:

            await loading_msg.delete()

        except Exception:

            pass

        # Открываем меню рассылки

        state_data = await state.get_data()
        await state.clear()
        await show_broadcast_chats_menu(message, user_id, menu_message_id=state_data.get('menu_message_id'))

    except Exception as e:

        print(f"Ошибка в process_add_broadcast_chat: {str(e)}")

        await message.answer(f"❌ Ошибка: {str(e)}")

@router.callback_query(F.data.startswith("select_chat_"))

async def select_chat_callback(query: CallbackQuery, state: FSMContext):

    """Обработчик выбора чата из похожих"""

    user_id = query.from_user.id

    try:

        chat_id = int(query.data.split("_")[2])

        if user_id not in user_authenticated or not user_authenticated[user_id]:

            await query.answer("❌ Ты не залогирован!", show_alert=True)

            return

        account_number = next(iter(user_authenticated[user_id].keys()))

        client = user_authenticated[user_id][account_number]

        # Получаем информацию о выбранном чате

        dialogs = await client.get_dialogs(limit=None)

        for dialog in dialogs:

            if dialog.entity.id == chat_id:

                entity = dialog.entity

                chat_name = entity.title if hasattr(entity, 'title') else (entity.first_name or str(chat_id))

                # Добавляем чат

                added = add_broadcast_chat_with_profile(user_id, chat_id, chat_name)

                state_data = await state.get_data()

                await state.clear()

                await show_broadcast_chats_menu(query, user_id, menu_message_id=state_data.get('menu_message_id') or query.message.message_id)

                return

        await query.answer("❌ Чат не найден", show_alert=True)

    except Exception as e:

        await query.answer(f"❌ Ошибка: {str(e)}", show_alert=True)

@router.callback_query(F.data.startswith("manual_chat_"))

async def manual_chat_callback(query: CallbackQuery, state: FSMContext):

    """Обработчик ввода имени чата вручную"""

    try:

        chat_id = int(query.data.split("_")[2])

        await state.update_data(chat_id=chat_id, previous_menu='broadcast_chats')

        await state.set_state(BroadcastConfigState.waiting_for_chat_name)

        await query.answer()

        keyboard = ReplyKeyboardMarkup(

            keyboard=[[KeyboardButton(text="↩️ Отменить")]],

            resize_keyboard=True

        )

        await query.message.delete()

        await query.message.answer(

            f"✏️ Введи имя/описание для чата с ID {chat_id}:",

            reply_markup=keyboard

        )

    except Exception as e:

        await query.answer(f"❌ Ошибка: {str(e)}", show_alert=True)

@router.message(BroadcastConfigState.waiting_for_chat_name)

async def process_broadcast_chat_name(message: Message, state: FSMContext):

    """Обработчик ввода имени чата при добавлении"""

    user_id = message.from_user.id

    # Проверяем отмену

    if message.text == "↩️ Отменить":

        await return_to_previous_menu(message, state)

        return

    try:

        data = await state.get_data()

        chat_id = data.get('chat_id')

        chat_name = message.text.strip()

        if not chat_id:

            await message.answer("❌ Ошибка! Chat ID не сохранён. Попробуй снова")

            await state.clear()

            await show_broadcast_chats_menu(message, message.from_user.id, menu_message_id=data.get('menu_message_id'))

            return

        # Добавляем чат с введённым именем

        added = add_broadcast_chat_with_profile(user_id, chat_id, chat_name)

        # Отправляем уведомление и сразу удаляем (быстрое всплывающее уведомление)

        if added:

            notify_msg = await message.answer(f"✅ Чат '{chat_name}' успешно добавлен!")

        else:

            notify_msg = await message.answer(f"⚠️ Чат с этим ID уже в списке")

        # Удаляем уведомление почти сразу (500мс) для эффекта всплывающего сообщения

        import asyncio

        asyncio.create_task(delete_message_after_delay(notify_msg, 0.5))

        await state.clear()

        await show_broadcast_chats_menu(message, message.from_user.id, menu_message_id=data.get('menu_message_id'))

    except Exception as e:

        print(f"Ошибка в process_broadcast_chat_name: {str(e)}")

        await message.answer(f"❌ Ошибка: {str(e)}")

        await state.clear()

@router.callback_query(F.data == "bc_chats_delete")

async def bc_chats_delete_callback(query: CallbackQuery, state: FSMContext):

    """Show broadcast chat removal UI with multi-delete and clear-all."""

    await query.answer()

    user_id = query.from_user.id

    chats = get_broadcast_chats(user_id)

    if not chats:

        text = "📭 Нет чатов для удаления!"

        kb = InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="⬅️ Назад", callback_data="close_bc_menu")]

        ])

        await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

        return

    await state.update_data(previous_menu='broadcast_chats', menu_message_id=query.message.message_id)

    await state.set_state(BroadcastConfigState.waiting_for_chat_delete)

    text = "🗑️ <b>УДАЛЕНИЕ ЧАТОВ</b>\n\n"

    for idx, (chat_id, chat_name) in enumerate(chats, 1):

        text += f"{idx}️⃣ {chat_name}\n"

    text += (
        f"\nВведи номера чатов для удаления (от 1 до {len(chats)}).\n"
        "Можно несколько через пробел/запятую, например: 1 4"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="🧹 Очистить все", callback_data="bc_chats_delete_all")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="bc_cancel")]

    ])

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "bc_chats_delete_all")
async def bc_chats_delete_all_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id

    chats = get_broadcast_chats(user_id)
    for chat_id, _ in chats:
        remove_broadcast_chat_with_profile(user_id, chat_id)

    await state.clear()
    await show_broadcast_chats_menu(query, user_id, menu_message_id=query.message.message_id)

@router.message(F.text == "🗑️ Удалить")

async def delete_broadcast_chat_button(message: Message, state: FSMContext):

    """Обработчик кнопки удаления чата из рассылки - СТАРЫЙ ОБРАБОТЧИК (УБРАТЬ)"""

    # Этот обработчик больше не используется

    pass

@router.message(BroadcastConfigState.waiting_for_chat_delete)

async def process_delete_broadcast_chat(message: Message, state: FSMContext):

    """Delete one or many broadcast chats by numeric indexes."""

    user_id = message.from_user.id

    if message.text in {"🔙 Отменить", "❌ Отменить"}:
        await return_to_previous_menu(message, state)
        return

    data = await state.get_data()
    menu_message_id = data.get('menu_message_id')

    try:
        chats = get_broadcast_chats(user_id)

        if not chats:
            await state.clear()
            await show_broadcast_chats_menu(message, user_id, menu_message_id=menu_message_id)
            return

        raw = (message.text or "").replace(",", " ")
        tokens = [token for token in raw.split() if token]
        if not tokens:
            await message.answer(f"❌ Введи номера от 1 до {len(chats)}")
            return

        indexes = []
        for token in tokens:
            value = int(token) - 1
            if value < 0 or value >= len(chats):
                await message.answer(f"❌ Введи числа от 1 до {len(chats)}")
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

        await show_broadcast_chats_menu(message, user_id, menu_message_id=menu_message_id)

    except ValueError:
        await message.answer("❌ Введи числа через пробел или запятую")

async def return_to_previous_menu(message: Message, state: FSMContext):

    """\u0412\u043e\u0437\u0432\u0440\u0430\u0449\u0430\u0435\u0442 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f \u0432 \u043f\u0440\u0435\u0434\u044b\u0434\u0443\u0449\u0435\u0435 \u043c\u0435\u043d\u044e \u0431\u0435\u0437 \u043b\u0438\u0448\u043d\u0435\u0433\u043e \u0442\u0435\u043a\u0441\u0442\u0430."""

    data = await state.get_data()
    previous_menu = data.get('previous_menu', 'broadcast')
    await state.clear()

    if previous_menu == 'broadcast':
        await cmd_broadcast_menu(message)
        return

    if previous_menu == 'broadcast_chats':
        await show_broadcast_chats_menu(message, message.from_user.id, menu_message_id=data.get('menu_message_id'))
        return

    await message.answer("\u0413\u043b\u0430\u0432\u043d\u043e\u0435 \u043c\u0435\u043d\u044e", reply_markup=get_main_menu_keyboard())

# Обработчик команды /se - управление всеми сессиями
