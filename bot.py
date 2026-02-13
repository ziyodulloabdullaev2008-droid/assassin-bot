# -*- coding: utf-8 -*-

import asyncio

import sys

from datetime import datetime

from pathlib import Path

from aiogram import Bot, Dispatcher, F

from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton

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

from services.session_service import recover_sessions_from_files, load_saved_sessions
from services.user_paths import session_base_path, temp_session_base_path, user_sessions_dir

from services.mention_service import start_mention_monitoring as start_mention_monitoring_service, stop_mention_monitoring

from services.mention_utils import normalize_chat_id

from services.broadcast_config_service import load_broadcast_configs
from services.join_service import load_all_settings as load_all_joins_settings

from ui.main_menu_ui import get_main_menu_keyboard

from handlers.basic_handlers import router as basic_router

from handlers.vip_handlers import router as vip_router, get_vip_cache_size

from handlers.account_handlers import router as account_router

from handlers.mentions_handlers import router as mentions_router

from handlers.config_handlers import router as config_router

from handlers.broadcast_handlers import router as broadcast_router
from handlers.joins_handlers import router as joins_router

from services.vip_service import is_vip_user_cached, update_vip_cache

from database import (

    init_db, add_or_update_user, set_user_logged_in,

    start_login_session, get_login_session, update_login_step,

    save_phone_number, delete_login_session, set_phone_code_hash, get_phone_code_hash,

    add_user_account, get_user_accounts, get_active_account, set_active_account,

    add_user_account_with_number,

    add_tracked_chat, remove_tracked_chat, get_tracked_chats, is_chat_tracked,
    get_broadcast_chats,

)

# Ensure console can print Unicode logs (emoji/cyrillic) on Windows terminals.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass



# РЎРѕР·РґР°С‘Рј Р±РѕС‚Р° Рё РґРёСЃРїРµС‚С‡РµСЂ

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

mention_monitors = app_state.mention_monitors





async def retry_with_backoff(async_func, max_retries=3, base_delay=0.5):

    """Выполнить async функцию с retry при database locked ошибке"""

    import asyncio

    

    for attempt in range(max_retries):

        try:

            return await async_func()

        except Exception as e:

            error_str = str(e).lower()

                                                                                                     

            if ('database is locked' in error_str or 

                'database locked' in error_str or

                'connection' in error_str and 'reset' in error_str):

                if attempt < max_retries - 1:

                                                       

                    delay = base_delay * (2 ** attempt)

                    print(f"⚠️  Database locked, retry {attempt + 1}/{max_retries} через {delay}s...")

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

    

                                                                                                  

    completed_ids = [bid for bid, b in active_broadcasts.items() 

                     if b['user_id'] == user_id and b['status'] in ('completed', 'error', 'cancelled')]

    for bid in completed_ids:

        del active_broadcasts[bid]

    

                                                                                                            

    now = datetime.now().timestamp()

    old_denial_users = [uid for uid, ts in vip_denial_messages.items() if now - ts > 3600]

    for uid in old_denial_users:

        del vip_denial_messages[uid]



                                                                          

    stop_mention_monitoring(user_id, account_number)





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

    waiting_phone = State()

    waiting_code = State()

    waiting_password = State()

    adding_chat = State()





class DeleteChatState(StatesGroup):

    waiting_for_number = State()





                                                                                                                                                          

vip_denial_messages = {}                        





                                                               



from aiogram.types import Update

from aiogram.dispatcher.middlewares.base import BaseMiddleware


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

    

                                                                                       

    PUBLIC_COMMANDS = {'/start', '/help', '/restart', '/login', '/logout'}

    

                                                                             

    VIP_ONLY_COMMANDS = {'/sa', '/se', '/broadcast', '/track', '/mention', '/settings', '/config'}

    

                                                                                                                                                        

    PUBLIC_CALLBACKS = {

        'bc_launch', 'bc_pause', 'bc_resume', 'bc_cancel_broadcast',                    

        'start_broadcast_with_account',                                                       

        'monitor_start', 'monitor_stop', 'monitor_toggle',                                             

        'view_account_', 'get_chats_list', 'export_private_chats', 'export_groups',

        'export_channels', 'export_all_chats', 'back_to_account_menu', 'refresh_menu',

        'close_accounts_menu',

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

                                                                

                    for cmd in self.VIP_ONLY_COMMANDS:

                        if text_lower.startswith(cmd):

                                                                                                                                                           

                            now = datetime.now().timestamp()

                            last_denial = vip_denial_messages.get(user_id, 0)

                            

                            if now - last_denial > 5:                                                                                         

                                try:

                                    await message.answer(

                                        "❌ Доступ ограничен.\n\n"

                                        "Для получения доступа обратитесь к @assassin_admin"

                                    )

                                except Exception as e:

                                    print(f"⚠️ Ошибка при отправке сообщения об отказе в доступе: {str(e)}")

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

                            "❌ Доступ ограничен. Для получения доступа обратитесь к @assassin_admin",

                            show_alert=True

                        )

                    except Exception as e:

                        print(f"⚠️ Ошибка при ответе на callback: {str(e)}")

                    return                                 

        

                                                                                                                                        

        return await handler(event, data)





                                                              

dp.update.outer_middleware(PrivateOnlyMiddleware())
dp.update.outer_middleware(VIPCheckMiddleware())





                                                                         



@dp.message(Command("se"))

async def cmd_sessions(message: Message):

    bot_user_id = message.from_user.id

    

    info, inline_keyboard = await get_sessions_text_and_keyboard(bot_user_id)

    

    if not info:

        await message.answer("❌ Нет добавленных аккаунтов")

        return

    

    await message.answer(info, reply_markup=inline_keyboard, parse_mode="HTML", disable_web_page_preview=True)





async def get_sessions_text_and_keyboard(user_id):
    """Build account menu text and keyboard."""
    accounts = get_user_accounts(user_id)

    stale_accounts = []
    for account_number, *_ in accounts:
        session_candidates = [
            Path(f"{session_base_path(user_id, account_number)}.session"),
            Path(__file__).resolve().parent / f"session_{user_id}_{account_number}.session",
        ]
        if not any(p.exists() for p in session_candidates):
            stale_accounts.append(account_number)

    if stale_accounts:
        for account_number in stale_accounts:
            if user_id in user_authenticated and account_number in user_authenticated[user_id]:
                try:
                    await asyncio.wait_for(
                        user_authenticated[user_id][account_number].disconnect(),
                        timeout=5.0,
                    )
                except Exception:
                    pass
                del user_authenticated[user_id][account_number]

            if user_id in mention_monitors and account_number in mention_monitors[user_id]:
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

    info = f"👤 <b>МОИ АККАУНТЫ</b> • {len(accounts)}\n"
    info += "━━━━━━━━━━━━━━━━━━━━━━\n\n"

    keyboard_buttons = []
    for account_number, telegram_id, username, first_name, is_active in accounts:
        client_in_memory = (
            user_id in user_authenticated
            and account_number in user_authenticated[user_id]
        )

        status_icon = "🟢" if is_active else "🔴"
        mode_text = "в работе" if is_active else "выключен"
        conn_text = "онлайн" if client_in_memory else "оффлайн"

        first_name_safe = (
            first_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            if first_name
            else "Неизвестно"
        )
        username_safe = (
            username.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            if username
            else ""
        )

        info += f"{status_icon} <b>Аккаунт #{account_number}</b>\n"
        info += f"👤 <b>Имя:</b> {first_name_safe}\n"
        info += f"🆔 <b>ID:</b> <code>{telegram_id}</code>\n"
        if username_safe:
            info += f"🔗 <b>Username:</b> @{username_safe}\n"
        info += f"⚙️ <b>Статус:</b> {mode_text}\n"
        info += f"📡 <b>Подключение:</b> {conn_text}\n"
        info += "──────────────────────\n\n"

        toggle_text = "⏸️ Отключить" if is_active else "▶️ Включить"
        keyboard_buttons.append(
            [
                InlineKeyboardButton(
                    text=toggle_text,
                    callback_data=f"toggle_account_{account_number}",
                ),
                InlineKeyboardButton(
                    text="🗑️ Удалить",
                    callback_data=f"delete_account_{account_number}",
                ),
            ]
        )

    keyboard_buttons.append(
        [InlineKeyboardButton(text="➕ Добавить аккаунт", callback_data="add_new_account")]
    )
    keyboard_buttons.append(
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="close_sessions_menu")]
    )

    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    return info, inline_keyboard


@dp.callback_query(F.data.startswith("toggle_account_"))

async def toggle_account(query: CallbackQuery):

    bot_user_id = query.from_user.id

    account_number = int(query.data.split("_")[2])

    

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

            cursor.execute("UPDATE user_accounts SET is_active = 0 WHERE bot_user_id = ? AND account_number = ?", 

                          (bot_user_id, account_number))

            conn.commit()

            conn.close()

            

                                                                                                                                  

            if bot_user_id in user_authenticated and account_number in user_authenticated[bot_user_id]:

                try:

                    await asyncio.wait_for(user_authenticated[bot_user_id][account_number].disconnect(), timeout=5.0)

                except Exception:

                    pass

                del user_authenticated[bot_user_id][account_number]

            

            await query.answer("🔴 Аккаунт отключен от рассылки", show_alert=False)

        

        else:

                                                                                                            

            try:

                session_file = session_base_path(bot_user_id, account_number)

                

                if not Path(f"{session_file}.session").exists():

                    await query.answer("❌ Файл сессии не найден. Используй /login для переподключения", show_alert=True)

                    return

                

                client = TelegramClient(str(session_file), API_ID, API_HASH)

                

                                          

                import asyncio

                for attempt in range(3):

                    try:

                        await client.connect()

                        await asyncio.sleep(1)

                        if client.is_connected():

                            break

                    except Exception as e:

                        if attempt == 2:

                            raise

                        await asyncio.sleep(1)

                

                                                           

                if await client.is_user_authorized():

                                                             

                    conn = sqlite3.connect(DB_PATH, timeout=30.0)                           

                    cursor = conn.cursor()

                    cursor.execute("UPDATE user_accounts SET is_active = 1 WHERE bot_user_id = ? AND account_number = ?", 

                                  (bot_user_id, account_number))

                    conn.commit()

                    conn.close()

                    

                                                        

                    if bot_user_id not in user_authenticated:

                        user_authenticated[bot_user_id] = {}

                    user_authenticated[bot_user_id][account_number] = client

                    

                                                                                                                     

                    await start_mention_monitoring(bot_user_id)


                    

                    await query.answer("🟢 Аккаунт включен", show_alert=False)

                else:

                    await client.disconnect()

                    await query.answer("❌ Аккаунт не авторизован. Используй /login", show_alert=True)

                    return

            

            except Exception as e:

                await query.answer(f"❌ Ошибка подключения: {str(e)}", show_alert=True)

                return

        

                                     

        info, inline_keyboard = await get_sessions_text_and_keyboard(bot_user_id)

        if info and inline_keyboard:

            try:

                await query.message.edit_text(info, reply_markup=inline_keyboard, parse_mode="HTML")

            except Exception as e:

                                                                                               

                pass

        else:

            await query.message.edit_text("❌ Нет добавленных аккаунтов")

    except Exception as e:

        print(f"❌ Ошибка в toggle_account: {str(e)}")

        await query.answer(f"❌ Ошибка: {str(e)}", show_alert=True)





                                                        

@dp.callback_query(F.data == "close_sessions_menu")

async def close_sessions_menu_callback(query: CallbackQuery):

    """Удалить меню сессий"""

    await query.answer()

    try:

        await query.message.delete()

    except Exception as e:

        print(f"⚠️ Ошибка при удалении меню сессий: {str(e)}")





                                                   

@dp.callback_query(F.data.startswith("delete_account_"))

async def delete_account(query: CallbackQuery):

    bot_user_id = query.from_user.id

    account_number = int(query.data.split("_")[2])

    

    try:

                                                                                                                

        if bot_user_id in mention_monitors and account_number in mention_monitors[bot_user_id]:

            task = mention_monitors[bot_user_id][account_number]

            if task and not task.done():

                task.cancel()

                try:

                    await asyncio.wait_for(task, timeout=2)

                except (asyncio.CancelledError, asyncio.TimeoutError):

                    pass

            del mention_monitors[bot_user_id][account_number]

        

                                                                       

        if bot_user_id in user_authenticated and account_number in user_authenticated[bot_user_id]:

            try:

                client = user_authenticated[bot_user_id][account_number]

                await client.disconnect()

            except:

                pass

            del user_authenticated[bot_user_id][account_number]

        

                                                                                             

        await asyncio.sleep(2)

        

                                   

        import gc

        gc.collect()

        await asyncio.sleep(1)

        

                              
        from pathlib import Path
        import os

        session_extensions = ['.session', '.session-journal', '.session-wal', '.session-shm']
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
                                                                                       
                            temp_name = Path(str(session_file) + ext + '.delete')
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

        cursor.execute("DELETE FROM user_accounts WHERE bot_user_id = ? AND account_number = ?", 

                      (bot_user_id, account_number))

        conn.commit()

        

        await query.answer("❌ Аккаунт удален", show_alert=False)

        

                                                                                

        cursor.execute("SELECT account_number FROM user_accounts WHERE bot_user_id = ? ORDER BY account_number", (bot_user_id,))

        remaining = cursor.fetchall()

        

                                                                             

        for new_idx, (old_idx,) in enumerate(remaining, 1):

            if old_idx != new_idx:

                cursor.execute("UPDATE user_accounts SET account_number = ? WHERE bot_user_id = ? AND account_number = ?",

                              (new_idx, bot_user_id, old_idx))

                                                                      
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
                        print(f"⚠️  Не удалось переименовать {old_path} -> {new_path}: {rename_error}")

                                                                                 

                if bot_user_id in user_authenticated and old_idx in user_authenticated[bot_user_id]:

                    user_authenticated[bot_user_id][new_idx] = user_authenticated[bot_user_id][old_idx]

                    del user_authenticated[bot_user_id][old_idx]

        

        conn.commit()

        conn.close()

        

                                                                                                                            

        info, inline_keyboard = await get_sessions_text_and_keyboard(bot_user_id)

        if info and inline_keyboard:

            await query.message.edit_text(info, reply_markup=inline_keyboard, parse_mode="HTML")

        else:

            await query.message.edit_text("❌ Нет добавленных аккаунтов")

    except Exception as e:

        print(f"❌ Ошибка в delete_account: {str(e)}")

        await query.answer(f"❌ Ошибка: {str(e)}", show_alert=True)















                                            

@dp.message(Command("login"))

async def cmd_login(message: Message, state: FSMContext):

    user = message.from_user

    

    print(f"📱 ЛОГИН: Пользователь {user.id} ({user.first_name}) нажал /login")

    add_or_update_user(user.id, user.username or "unknown", user.first_name)

    

    await state.set_state(LoginStates.waiting_phone)

    print(f"   ✅ Состояние установлено на waiting_phone")

    

    keyboard = ReplyKeyboardMarkup(

        keyboard=[

            [KeyboardButton(text="↩️ Отменить действие")]

        ],

        resize_keyboard=True

    )

    

    await message.answer(

        "🔐 <b>ВХОД В АККАУНТ</b>\n\n"

        "Для работы бота необходимо получить доступ к твоему Telegram аккаунту.\n\n"

        "📱 <b>Введи свой номер телефона в формате:</b>\n"

        "+7XXXXXXXXXX или +1XXXXXXXXX",

        parse_mode="HTML",

        reply_markup=keyboard

    )





                                                               

@dp.message(LoginStates.waiting_phone, ~F.text.startswith("/"), ~(F.text == "↩️ Отменить действие"))

async def process_phone(message: Message, state: FSMContext):

    phone = message.text.strip()

    user_id = message.from_user.id

    print(f"📱 НОМЕР: Получен номер {phone} от {user_id}")

    

                                     

    if not phone.startswith("+") or not phone[1:].isdigit() or len(phone) < 10:

        print(f"   ❌ Неверный формат номера")

        await message.answer("❌ Неверный формат! Используй +7XXXXXXXXXX")

        return

    

    save_phone_number(user_id, phone)

    

                                                                                                                                     

                                                                                                

    import time

    login_id = int(time.time() * 1000) % 1000000                                                     

    temp_session_file = temp_session_base_path(user_id, login_id)

    client = TelegramClient(str(temp_session_file), API_ID, API_HASH)

    

    try:

        await message.answer("⏳ Подключение к Telegram...")

        print(f"   ⏳ Подключаюсь к Telegram...")

        

                                                                             

        import asyncio

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

        

                                       

        print(f"   ⏳ Запрашиваю код...")

        try:

            sent_code = await client.send_code_request(phone)

            phone_code_hash = sent_code.phone_code_hash

            print(f"   ✅ Код отправлен на номер {phone}")

        except Exception as e:

            print(f"   ❌ Ошибка при запросе кода: {str(e)}")

            await client.disconnect()

            await message.answer(f"❌ Ошибка при запросе кода: {str(e)}\n\nПопробуй /login снова")

            return

        

                                                   

        user_hashes[user_id] = phone_code_hash

        user_clients[user_id] = client

        start_login_session(user_id, phone)

        

        await state.set_state(LoginStates.waiting_code)

        await state.update_data(login_id=login_id, temp_session_file=str(temp_session_file))                                                                     

        user_code_input[user_id] = ""                                                  

        print(f"   ✅ Состояние установлено на waiting_code")

        

                                                              

        keyboard = InlineKeyboardMarkup(inline_keyboard=[

            [InlineKeyboardButton(text="1", callback_data="digit_1"),

             InlineKeyboardButton(text="2", callback_data="digit_2"),

             InlineKeyboardButton(text="3", callback_data="digit_3")],

            [InlineKeyboardButton(text="4", callback_data="digit_4"),

             InlineKeyboardButton(text="5", callback_data="digit_5"),

             InlineKeyboardButton(text="6", callback_data="digit_6")],

            [InlineKeyboardButton(text="7", callback_data="digit_7"),

             InlineKeyboardButton(text="8", callback_data="digit_8"),

             InlineKeyboardButton(text="9", callback_data="digit_9")],

            [InlineKeyboardButton(text="0", callback_data="digit_0")],

            [InlineKeyboardButton(text="❌ Очистить", callback_data="clear_code"),

             InlineKeyboardButton(text="✅ Отправить", callback_data="submit_code")]

        ])

        

        await message.answer("✅ Код отправлен на твой номер!\n\n📝 Нажимай кнопки для ввода 5-значного кода:", reply_markup=keyboard)

        

    except Exception as e:

        print(f"   ❌ ОШИБКА: {type(e).__name__}: {str(e)}")

        import traceback

        traceback.print_exc()

        

                                                          

        try:

            if user_id in user_clients:

                await user_clients[user_id].disconnect()

                del user_clients[user_id]

        except:

            pass

        

        await message.answer(f"❌ Ошибка подключения: {str(e)}\n\nПопробуй /login снова")

        await state.clear()









                                                     

@dp.callback_query(F.data.startswith("digit_"))

async def process_digit(query: CallbackQuery, state: FSMContext):

    user_id = query.from_user.id

    digit = query.data.split("_")[1]

    print(f"🔢 КОД: Введена цифра {digit} от {user_id}")

    

    if user_id not in user_code_input:

        print(f"   ❌ Пользователь не в user_code_input")

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

    

                                                                    

    keyboard = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="1", callback_data="digit_1"),

         InlineKeyboardButton(text="2", callback_data="digit_2"),

         InlineKeyboardButton(text="3", callback_data="digit_3")],

        [InlineKeyboardButton(text="4", callback_data="digit_4"),

         InlineKeyboardButton(text="5", callback_data="digit_5"),

         InlineKeyboardButton(text="6", callback_data="digit_6")],

        [InlineKeyboardButton(text="7", callback_data="digit_7"),

         InlineKeyboardButton(text="8", callback_data="digit_8"),

         InlineKeyboardButton(text="9", callback_data="digit_9")],

        [InlineKeyboardButton(text="0", callback_data="digit_0")],

        [InlineKeyboardButton(text="❌ Очистить", callback_data="clear_code"),

         InlineKeyboardButton(text="✅ Отправить", callback_data="submit_code")]

    ])

    

    await query.message.edit_text(f"📝 Введено: {display}\n\nНажимай кнопки для ввода 5-значного кода:", reply_markup=keyboard)





                                              

@dp.callback_query(F.data == "clear_code")

async def clear_code(query: CallbackQuery, state: FSMContext):

    user_id = query.from_user.id

    user_code_input[user_id] = ""

    await query.answer()

    

    keyboard = InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="1", callback_data="digit_1"),

         InlineKeyboardButton(text="2", callback_data="digit_2"),

         InlineKeyboardButton(text="3", callback_data="digit_3")],

        [InlineKeyboardButton(text="4", callback_data="digit_4"),

         InlineKeyboardButton(text="5", callback_data="digit_5"),

         InlineKeyboardButton(text="6", callback_data="digit_6")],

        [InlineKeyboardButton(text="7", callback_data="digit_7"),

         InlineKeyboardButton(text="8", callback_data="digit_8"),

         InlineKeyboardButton(text="9", callback_data="digit_9")],

        [InlineKeyboardButton(text="0", callback_data="digit_0")],

        [InlineKeyboardButton(text="❌ Очистить", callback_data="clear_code"),

         InlineKeyboardButton(text="✅ Отправить", callback_data="submit_code")]

    ])

    

    await query.message.edit_text("📝 Введено: \n\nНажимай кнопки для ввода 5-значного кода:", reply_markup=keyboard)





                                                

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





async def process_code_login(message: Message, code: str, user_id: int, state: FSMContext):

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

        await client.sign_in(phone=phone_number, code=code, phone_code_hash=phone_code_hash)

        print(f"   ✅ Код принят, требуется пароль или готово")

        

                                                                                         

        await state.set_state(LoginStates.waiting_password)

        update_login_step(user_id, "logged_in")

        user_code_input[user_id] = ""                                            

        await message.answer("✅ Код верный!\n\n🔐 Нужен ли пароль двухэтапной аутентификации? (напиши пароль или 'нет')")

        

    except SessionPasswordNeededError:

        print(f"   ⚠️  Требуется пароль двухэтапной аутентификации")

        await state.set_state(LoginStates.waiting_password)

        user_code_input[user_id] = ""

        await message.answer("🔐 Требуется пароль двухэтапной аутентификации.\n\n📝 Введи пароль:")

        

    except PhoneCodeInvalidError:

        print(f"   ❌ Неверный код")

        user_code_input[user_id] = ""

        await message.answer("❌ Неверный код! Попробуй снова (или напиши /login для нового кода):")

        

    except Exception as e:

        print(f"   ❌ Ошибка логина: {str(e)}")

        if user_id in user_clients:

            try:

                await user_clients[user_id].disconnect()

            except:

                pass

            del user_clients[user_id]

        if user_id in user_hashes:

            del user_hashes[user_id]

        if user_id in user_code_input:

            del user_code_input[user_id]

        delete_login_session(user_id)

        await message.answer(f"❌ Ошибка: {str(e)}\n\nПопробуй /login снова")

        await state.clear()





                                              

@dp.message(LoginStates.waiting_password, ~F.text.startswith("/"), ~(F.text == "↩️ Отменить действие"))

async def process_password(message: Message, state: FSMContext):

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

            print(f"✅ Получена информация об аккаунте (без пароля): {me.first_name} (ID: {me.id})")

            

                                                                                                    

            account_number = add_user_account(user_id, me.id, me.username or "", me.first_name or "User", "")

            print(f"✅ Аккаунт добавлен в БД с номером: {account_number}")

            

                                                                                                                        

            data = await state.get_data()

            temp_session_file_str = data.get('temp_session_file', f"temp_session_{user_id}_unknown")

            

                                                                                                                           

            try:

                await asyncio.sleep(0.5)                                                           

                await client.disconnect()

                await asyncio.sleep(0.5)                                                            

                print(f"✅ Клиент отключен перед переименованием сессии")

            except Exception as e:

                print(f"⚠️  Ошибка отключения клиента: {str(e)}")

            

                                                                                                     

            from pathlib import Path

            old_session_file = Path(temp_session_file_str)

            new_session_file = session_base_path(user_id, account_number)

            

                                                                                                 

            import os

            import shutil

            old_session_with_ext = Path(f"{old_session_file}.session")

            new_session_with_ext = Path(f"{new_session_file}.session")

            

            print(f"   🔍 Проверка файла сессии:")

            print(f"      Существует: {old_session_with_ext.exists()}")

            

            if old_session_with_ext.exists():

                try:

                                                                                                                                         

                    if new_session_with_ext.exists():

                        print(f"   ⚠️  Целевой файл уже существует, удаляю старый")

                        os.remove(str(new_session_with_ext))

                    

                                                                                                                                       

                    shutil.copy2(str(old_session_with_ext), str(new_session_with_ext))

                    print(f"   ✅ Файл сессии скопирован: {old_session_with_ext.name} -> {new_session_with_ext.name}")

                    

                                                                             

                    os.remove(str(old_session_with_ext))

                    print(f"   ✅ Временный файл удален: {old_session_with_ext.name}")

                    

                                                                                            

                    if not new_session_with_ext.exists():

                        print(f"   ❌ ОШИБКА: Файл не был скопирован правильно!")

                    else:

                        print(f"   ✅ Проверка: новый файл существует - {new_session_with_ext.exists()}")

                except Exception as e:

                    print(f"   ❌ Ошибка копирования файла: {str(e)}")

                    import traceback

                    traceback.print_exc()

            else:

                print(f"   ❌ Исходный файл не найден: {old_session_with_ext}")

                print(f"   ℹ️  Список файлов в директории:")

                temp_files = list(Path(__file__).parent.glob("temp_session_*"))

                session_files = list(Path(__file__).parent.glob("session_*"))

                if temp_files:

                    for f in temp_files:

                        print(f"      - {f.name}")

                else:

                    print(f"      [нет temp файлов]")

                if session_files:

                    for f in session_files:

                        print(f"      - {f.name}")

            

                                                                                         

            print(f"   📍 Создаю новый клиент с сессией: {new_session_file}")

            client = TelegramClient(str(new_session_file), API_ID, API_HASH)

            await client.connect()

            

                                                                                                                              

            if user_id not in user_authenticated:

                user_authenticated[user_id] = {}

            user_authenticated[user_id][account_number] = client

            print(f"✅ Клиент сохранен в памяти: user_authenticated[{user_id}][{account_number}]")

            

                                                                          

            await start_mention_monitoring(user_id)

            

            await message.answer("✅ Отлично! Ты успешно вошел в аккаунт!", reply_markup=get_main_menu_keyboard())

            set_user_logged_in(user_id, True)

            delete_login_session(user_id)

            await state.clear()

            return

        

        await message.answer("⏳ Проверка пароля...")

        await client.sign_in(password=password_input)

        

                                                                     

        me = await client.get_me()

        print(f"✅ Получена информация об аккаунте: {me.first_name} (ID: {me.id})")

        

                                                                                                

        account_number = add_user_account(user_id, me.id, me.username or "", me.first_name or "User", "")

        print(f"✅ Аккаунт добавлен в БД с номером: {account_number}")

        

                                                       

        data = await state.get_data()

        temp_session_file_str = data.get('temp_session_file', f"temp_session_{user_id}_unknown")

        

                                                                                                                       

        try:

            await asyncio.sleep(0.5)                                                           

            await client.disconnect()

            await asyncio.sleep(0.5)                                                            

            print(f"✅ Клиент отключен перед переименованием сессии")

        except Exception as e:

            print(f"⚠️  Ошибка отключения клиента: {str(e)}")

        

                                                                                                 

        from pathlib import Path

        old_session_file = Path(temp_session_file_str)

        new_session_file = session_base_path(user_id, account_number)

        

                                                                                             

        import os

        import shutil

        old_session_with_ext = Path(f"{old_session_file}.session")

        new_session_with_ext = Path(f"{new_session_file}.session")

        

        print(f"   🔍 Проверка файла сессии:")

        print(f"      Ищу: {old_session_with_ext}")

        print(f"      Существует: {old_session_with_ext.exists()}")

        

        if old_session_with_ext.exists():

            try:

                                                                                                                                     

                if new_session_with_ext.exists():

                    print(f"   ⚠️  Целевой файл уже существует, удаляю старый")

                    os.remove(str(new_session_with_ext))

                

                                                                                                                                   

                shutil.copy2(str(old_session_with_ext), str(new_session_with_ext))

                print(f"   ✅ Файл сессии скопирован: {old_session_with_ext.name} -> {new_session_with_ext.name}")

                

                                                                         

                os.remove(str(old_session_with_ext))

                print(f"   ✅ Временный файл удален: {old_session_with_ext.name}")

                

                                                                                        

                if not new_session_with_ext.exists():

                    print(f"   ❌ ОШИБКА: Файл не был скопирован правильно!")

                else:

                    print(f"   ✅ Проверка: новый файл существует - {new_session_with_ext.exists()}")

            except Exception as e:

                print(f"   ❌ Ошибка копирования файла: {str(e)}")

                import traceback

                traceback.print_exc()

        else:

            print(f"   ❌ Исходный файл не найден: {old_session_with_ext}")

            print(f"   ℹ️  Список файлов в директории:")

            temp_files = list(Path(__file__).parent.glob("temp_session_*"))

            session_files = list(Path(__file__).parent.glob("session_*"))

            if temp_files:

                for f in temp_files:

                    print(f"      - {f.name}")

            else:

                print(f"      [нет temp файлов]")

            if session_files:

                for f in session_files:

                    print(f"      - {f.name}")

        

                                                                                     

        print(f"   📍 Создаю новый клиент с сессией: {new_session_file}")

        client = TelegramClient(str(new_session_file), API_ID, API_HASH)

        await client.connect()

        

                                                                                                                          

        if user_id not in user_authenticated:

            user_authenticated[user_id] = {}

        user_authenticated[user_id][account_number] = client

        print(f"✅ Клиент сохранен в памяти: user_authenticated[{user_id}][{account_number}]")

        

                                                                      

        await start_mention_monitoring(user_id)

        

        await message.answer("✅ Успешно! Ты вошел в свой аккаунт Telegram!", reply_markup=get_main_menu_keyboard())

        set_user_logged_in(user_id, True)

        delete_login_session(user_id)

        await state.clear()

        

    except Exception as e:

        await message.answer(f"❌ Ошибка пароля: {str(e)}\n\nПопробуй еще раз или напиши 'нет' если пароля нет:")





                                             

@dp.message(Command("logout"))

async def cmd_logout(message: Message):

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
    await start_telegram_log_forwarding(bot, DEV_LOG_CHAT_ID)

    print("🚀 Запуск бота...")

    print("⏳ Загружаю сессии и конфиги...")



    async def _start_monitors_for_loaded():

        for user_id in list(user_authenticated.keys()):

            await start_mention_monitoring(user_id)



                                                                 

    await recover_sessions_from_files(API_ID, API_HASH)



                                             

    await load_saved_sessions(API_ID, API_HASH, on_loaded=_start_monitors_for_loaded)

    

                                                

    await update_vip_cache()

    print(f"📊 Загружено {get_vip_cache_size()} VIP пользователей")

    

                                       

    load_broadcast_configs()
    load_all_joins_settings()

    

    print("✅ Бот готов к работе!")

    try:
        await dp.start_polling(bot)
    finally:
        await stop_telegram_log_forwarding()





                                                                                                                                             

dp.include_router(vip_router)

dp.include_router(account_router)

dp.include_router(mentions_router)

dp.include_router(config_router)

dp.include_router(broadcast_router)
dp.include_router(joins_router)

dp.include_router(basic_router)





if __name__ == "__main__":

    asyncio.run(main())





