from aiogram import Router, F
from aiogram.filters.command import Command
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.fsm.context import FSMContext

from database import add_or_update_user
from ui.main_menu_ui import get_main_menu_keyboard

router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message):
    user = message.from_user
    add_or_update_user(user.id, user.username or "unknown", user.first_name)

    welcome_text = (
        f"Добро пожаловать {user.first_name} в наш бот!\n\n"
        "Помощь по боту по команде /help"
    )

    channel_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Наш канал", url="https://t.me/assassin2026")]]
    )

    await message.answer(welcome_text, reply_markup=channel_kb)
    await message.answer("Главное меню:", reply_markup=get_main_menu_keyboard())


@router.message(Command("restart"))
async def cmd_restart(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню обновлено", reply_markup=get_main_menu_keyboard())


@router.message(Command("help"))
async def cmd_help(message: Message):
    help_text = (
        "📘 <b>СПРАВКА ПО БОТУ</b>\n\n"
        "1) <b>Порядок работы</b>\n"
        "• /start — открыть меню\n"
        "• /login — войти в Telegram-аккаунт\n"
        "• /menu или /se — проверить аккаунты\n"
        "• /broadcast — настроить и запустить рассылку\n"
        "• /tracked — настроить упоминания\n"
        "• /joins — включить авто-вступление\n\n"
        "2) <b>Команды</b>\n"
        "• /start — главное меню\n"
        "• /help — эта справка\n"
        "• /restart — обновить меню/сбросить текущий шаг\n"
        "• /login — вход в Telegram\n"
        "• /logout — выход из аккаунтов\n"
        "• /menu — список аккаунтов\n"
        "• /se — управление сессиями\n"
        "• /broadcast — меню рассылки\n"
        "• /tracked — меню упоминаний\n"
        "• /chats — массовое вступление по списку ссылок\n"
        "• /joins — авто-вступление по ссылкам/кнопкам\n"
        "• /config — профили конфигов рассылки\n\n"
        "Команды разработчика в справке не показываются."
    )

    await message.answer(help_text, reply_markup=get_main_menu_keyboard(), parse_mode="HTML")


@router.message(F.from_user.id == 777000)
async def ignore_telegram_service_messages(message: Message):
    return


@router.message(F.from_user.username == "telegram")
async def ignore_telegram_bot_messages(message: Message):
    return


@router.message()
async def echo_handler(message: Message):
    known_texts = {
        "Мой аккаунт",
        "Рассылка",
        "Упоминания",
        "Отменить",
        "Количество",
        "Интервал",
        "Запустить",
        "Активные",
        "Удалить",
        "Назад",
    }

    if message.text in known_texts:
        return

    user = message.from_user
    add_or_update_user(user.id, user.username or "unknown", user.first_name)
    await message.answer(
        "Неизвестная команда. Используй /help",
        reply_markup=get_main_menu_keyboard(),
    )
