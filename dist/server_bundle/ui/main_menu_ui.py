from aiogram.types import KeyboardButton, ReplyKeyboardMarkup


ACCOUNT_BUTTON_TEXT = "\U0001f4f1 \u041c\u043e\u0439 \u0430\u043a\u043a\u0430\u0443\u043d\u0442"
BROADCAST_BUTTON_TEXT = "\U0001f4e4 \u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430"


def get_main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=ACCOUNT_BUTTON_TEXT)],
            [KeyboardButton(text=BROADCAST_BUTTON_TEXT)],
        ],
        resize_keyboard=True,
    )
