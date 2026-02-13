from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def get_main_menu_keyboard() -> ReplyKeyboardMarkup:
    """Главное меню с основными кнопками."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Мой аккаунт")],
            [KeyboardButton(text="📤 Рассылка")],
            [KeyboardButton(text="🔔 Упоминания")],
        ],
        resize_keyboard=True,
    )
