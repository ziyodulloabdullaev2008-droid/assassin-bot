from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def build_broadcast_keyboard(
    include_active: bool = False,
    user_id: int = None,
    active_broadcasts: dict = None,
    back_callback: str = "bc_back",
) -> InlineKeyboardMarkup:
    """Построить стандартную клавиатуру для меню рассылки."""
    buttons = [
        [InlineKeyboardButton(text="Текст", callback_data="bc_text"),
         InlineKeyboardButton(text="Кол-во", callback_data="bc_quantity"),
         InlineKeyboardButton(text="Интервал", callback_data="bc_interval")],
        [InlineKeyboardButton(text="Темп", callback_data="bc_batch_pause"),
         InlineKeyboardButton(text="Чаты", callback_data="bc_chats"),
         InlineKeyboardButton(text="Активные", callback_data="bc_active")],
        [InlineKeyboardButton(text="Лимит", callback_data="bc_plan_limit")],
        [InlineKeyboardButton(text="Запустить", callback_data="bc_launch"),
         InlineKeyboardButton(text="Назад", callback_data=back_callback)],
    ]

    if include_active and user_id and active_broadcasts:
        user_broadcasts = {
            bid: b for bid, b in active_broadcasts.items()
            if b["user_id"] == user_id and b["status"] in ("running", "paused")
        }
        if user_broadcasts:
            groups = {}
            singles = []
            for bid, b in user_broadcasts.items():
                gid = b.get("group_id")
                if gid is None:
                    singles.append((bid, b))
                else:
                    groups.setdefault(gid, []).append((bid, b))

            for gid, items in sorted(groups.items()):
                statuses = {b["status"] for _, b in items}
                if "running" in statuses:
                    status_icon = "▶️"
                elif statuses == {"paused"}:
                    status_icon = "⏸️"
                else:
                    status_icon = "✅"
                buttons.append([
                    InlineKeyboardButton(
                        text=f"{status_icon} Группа #{gid} ({len(items)})",
                        callback_data=f"view_group_{gid}",
                    )
                ])

            for bid, broadcast in sorted(singles):
                status = "▶️" if broadcast["status"] == "running" else "⏸️" if broadcast["status"] == "paused" else "✅"
                buttons.append([
                    InlineKeyboardButton(text=f"{status} Рассылка #{bid}", callback_data=f"view_bc_{bid}")
                ])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_broadcast_menu_text(
    config: dict,
    chats: list,
    active_broadcasts: dict,
    user_id: int,
    show_title: bool = True,
    show_active_count: bool = True,
) -> str:
    """Строит текст меню рассылки."""
    info = "📤 <b>РАССЫЛКА</b>\n\n" if show_title else ""
    mode_text = "random" if config.get("text_mode") == "random" else "no random"
    info += f"💬 <b>Текстов:</b> {len(config.get('texts', []))} ({mode_text})\n"
    info += f"🔢 <b>Кол-во:</b> {config.get('count', 0)}\n"
    info += f"⏱️ <b>Интервал:</b> {config.get('interval', 0)} мин\n"
    info += f"⚡ <b>Темп:</b> {config.get('chat_pause', '1-3')} сек\n"
    limit_count = config.get("plan_limit_count", 0)
    limit_rest = config.get("plan_limit_rest", 0)
    if limit_count and limit_rest:
        info += f"⏳ <b>Лимит:</b> {limit_count} сообщ. / отдых {limit_rest} мин\n"
    else:
        info += "⏳ <b>Лимит:</b> без лимита\n"
    info += f"💭 <b>Чатов:</b> {len(chats)}\n"

    if show_active_count:
        user_broadcasts = {
            bid: b for bid, b in active_broadcasts.items()
            if b["user_id"] == user_id and b["status"] in ("running", "paused")
        }
        info += f"\n\nАктивных рассылок: {len(user_broadcasts)}"

    return info
