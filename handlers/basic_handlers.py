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
        f'Добро пожаловать {user.first_name} в наш бот!\n\n'
        "Помощь по боту по команде /help"
    )

    channel_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Наш канал", url="https://t.me/assassin2026")]
    ])

    await message.answer(welcome_text, reply_markup=channel_kb)
    await message.answer("Главное меню:", reply_markup=get_main_menu_keyboard())


@router.message(Command("restart"))
async def cmd_restart(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню обновлено", reply_markup=get_main_menu_keyboard())


@router.message(Command("help"))
async def cmd_help(message: Message):
    help_text = (
        "СПРАВКА ПО КОМАНДАМ\n\n"
        "ОСНОВНЫЕ КОМАНДЫ\n"
        "/start - Главное меню\n"
        "/restart - Сброс меню (обновить кнопки)\n"
        "/help - Эта справка\n\n"
        "ФУНКЦИИ (КНОПКИ)\n"
        "Мой аккаунт - Управление аккаунтами\n"
        "Рассылка - Отправка сообщений в чаты\n"
        "Упоминания - Отслеживание упоминаний\n\n"
        "Для доступа требуется VIP статус"
    )

    await message.answer(help_text, reply_markup=get_main_menu_keyboard())


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
