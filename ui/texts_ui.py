from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def build_texts_keyboard(texts: list, back_callback: str = "bc_text") -> InlineKeyboardMarkup:
    """Построить клавиатуру для списка текстов."""
    buttons = []

    text_buttons = []
    for i in range(len(texts)):
        text_buttons.append(InlineKeyboardButton(text=f"Text {i+1}", callback_data=f"text_view_{i}"))

    for i in range(0, len(text_buttons), 3):
        buttons.append(text_buttons[i:i + 3])

    buttons.append([InlineKeyboardButton(text="Добавить новый", callback_data="text_add_new")])
    buttons.append([InlineKeyboardButton(text="Назад", callback_data=back_callback)])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_text_settings_keyboard(text_mode: str = "random", parse_mode: str = "HTML") -> InlineKeyboardMarkup:
    """Построить клавиатуру для меню настроек текстов."""
    buttons = [
        [InlineKeyboardButton(text="Список текстов", callback_data="text_list")],
        [InlineKeyboardButton(
            text=f"Режим: {'Random ✅' if text_mode == 'random' else 'No Random ❌'}",
            callback_data="text_mode_toggle",
        )],
        [InlineKeyboardButton(text=f"Формат: {parse_mode}", callback_data="text_format_toggle")],
        [InlineKeyboardButton(text="Назад", callback_data="bc_back")],
    ]

    return InlineKeyboardMarkup(inline_keyboard=buttons)
