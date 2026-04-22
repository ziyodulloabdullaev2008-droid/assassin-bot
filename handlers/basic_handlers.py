from aiogram import Router, F
from aiogram.filters.command import Command
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from core.state import app_state
from database import add_or_update_user, add_user_account_with_number
from core.config import API_HASH, API_ID
from services.admin_notify_service import notify_new_bot_user
from services.session_service import ensure_connected_client
from ui.main_menu_ui import (
    ACCOUNT_BUTTON_TEXT,
    BROADCAST_BUTTON_TEXT,
    get_main_menu_keyboard,
)
from ui.broadcast_texts import COUNT_BUTTON_TEXT, INTERVAL_BUTTON_TEXT

router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message):
    user = message.from_user
    created = add_or_update_user(user.id, user.username or "unknown", user.first_name)
    if created:
        await notify_new_bot_user(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            source="/start",
        )

    welcome_text = (
        f"Добро пожаловать {user.first_name} в наш бот!\n\n"
        "Помощь по боту по команде /help"
    )

    await message.answer(welcome_text)
    await message.answer("Главное меню:", reply_markup=get_main_menu_keyboard())


@router.message(Command("restart"))
async def cmd_restart(message: Message, state: FSMContext):
    await state.clear()

    user_id = message.from_user.id
    synced = 0
    total = 0

    user_clients = app_state.user_authenticated.get(user_id, {}) or {}
    for account_number in list(user_clients.keys()):
        total += 1
        try:
            client = await ensure_connected_client(
                user_id,
                account_number,
                api_id=API_ID,
                api_hash=API_HASH,
            )
            if not client:
                continue

            me = await client.get_me()
            if me:
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

    if total > 0:
        text = f"Главное меню обновлено\nСинхронизировано аккаунтов: {synced}/{total}"
    else:
        text = "Главное меню обновлено"

    await message.answer(text, reply_markup=get_main_menu_keyboard())


@router.message(Command("help"))
async def cmd_help(message: Message):
    help_text = (
        "📘 <b>СПРАВКА ПО БОТУ</b>\n\n"
        "1) <b>Порядок работы</b>\n"
        "• /start — открыть меню\n"
        "• /login — войти в Telegram-аккаунт\n"
        "• /menu или /se — проверить аккаунты\n"
        "• /broadcast — настроить и запустить рассылку\n"
        "\n"
        "2) <b>Команды</b>\n"
        "• /start — главное меню\n"
        "• /help — эта справка\n"
        "• /restart — обновить меню/сбросить текущий шаг\n"
        "• /login — вход в Telegram\n"
        "• /logout — выход из аккаунтов\n"
        "• /menu — список аккаунтов\n"
        "• /se — управление сессиями\n"
        "• /broadcast — меню рассылки\n"
        "• /chats — массовое вступление по списку ссылок\n"
        "• /config — профили конфигов рассылки\n\n"
        "Команды разработчика в справке не показываются."
    )

    await message.answer(
        help_text, reply_markup=get_main_menu_keyboard(), parse_mode="HTML"
    )


@router.message(F.from_user.id == 777000)
async def ignore_telegram_service_messages(message: Message):
    return


@router.message(F.from_user.username == "telegram")
async def ignore_telegram_bot_messages(message: Message):
    return


@router.message(F.text == BROADCAST_BUTTON_TEXT)
@router.message(F.text.in_({"Рассылка", "📤 Рассылка"}))
async def open_broadcast_from_main_menu(message: Message):
    from handlers.broadcast_shared import show_broadcast_menu

    try:
        await message.delete()
    except Exception:
        pass

    await show_broadcast_menu(message, message.from_user.id, is_edit=False)


@router.message(
    ~F.text.startswith("/"),
    ~F.text.contains("Мой аккаунт"),
    ~F.text.contains("Рассылка"),
    ~(F.text == ACCOUNT_BUTTON_TEXT),
    ~(F.text == BROADCAST_BUTTON_TEXT),
    ~(F.text == COUNT_BUTTON_TEXT),
    ~(F.text == INTERVAL_BUTTON_TEXT),
    ~(F.text == "📤 Активные"),
    ~(F.text == "🗑️ Удалить"),
)
async def echo_handler(message: Message):
    user = message.from_user
    created = add_or_update_user(user.id, user.username or "unknown", user.first_name)
    if created:
        await notify_new_bot_user(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            source="echo_handler",
        )
    await message.answer(
        "Неизвестная команда. Используй /help",
        reply_markup=get_main_menu_keyboard(),
    )
