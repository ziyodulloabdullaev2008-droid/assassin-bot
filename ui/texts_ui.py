from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def build_texts_keyboard(
    items: list,
    back_callback: str = "bc_text",
    *,
    item_prefix: str = "Text",
    allow_add: bool = True,
    extra_buttons: list[list[InlineKeyboardButton]] | None = None,
) -> InlineKeyboardMarkup:
    """Build keyboard for manual texts or source posts."""
    buttons = []

    item_buttons = []
    for i, item in enumerate(items):
        status_suffix = ""
        if isinstance(item, dict) and "enabled" in item:
            status_suffix = " ✅" if item.get("enabled", True) else " ⏸️"
        item_buttons.append(
            InlineKeyboardButton(
                text=f"{item_prefix} {i + 1}{status_suffix}",
                callback_data=f"text_view_{i}",
            )
        )

    for i in range(0, len(item_buttons), 3):
        buttons.append(item_buttons[i : i + 3])

    if extra_buttons:
        buttons.extend(extra_buttons)

    if allow_add:
        buttons.append(
            [InlineKeyboardButton(text="Добавить новый", callback_data="text_add_new")]
        )
    buttons.append([InlineKeyboardButton(text="Назад", callback_data=back_callback)])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_text_settings_keyboard(
    source_type: str = "manual",
    text_mode: str = "random",
    parse_mode: str = "HTML",
    show_forward_source: bool = False,
) -> InlineKeyboardMarkup:
    """Build content settings keyboard."""
    buttons = [
        [
            InlineKeyboardButton(
                text=f"Источник: {'Канал' if source_type == 'channel' else 'Вручную'}",
                callback_data="text_source_toggle",
            )
        ]
    ]

    if source_type == "channel":
        buttons.extend(
            [
                [
                    InlineKeyboardButton(
                        text="Канал-источник",
                        callback_data="text_channel_source",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text=(
                            "🙈 Скрыть источник"
                            if show_forward_source
                            else "👁️ Показать источник"
                        ),
                        callback_data="text_forward_source_toggle",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="Список постов",
                        callback_data="text_list",
                    ),
                    InlineKeyboardButton(
                        text="Обновить посты",
                        callback_data="text_channel_refresh",
                    ),
                ],
            ]
        )
    else:
        buttons.extend(
            [
                [
                    InlineKeyboardButton(
                        text="Список текстов",
                        callback_data="text_list",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text=f"Формат: {parse_mode}",
                        callback_data="text_format_toggle",
                    )
                ],
            ]
        )

    buttons.append(
        [
            InlineKeyboardButton(
                text=f"Режим: {'Random ✅' if text_mode == 'random' else 'No Random ❌'}",
                callback_data="text_mode_toggle",
            )
        ]
    )
    buttons.append([InlineKeyboardButton(text="Назад", callback_data="bc_back")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)
