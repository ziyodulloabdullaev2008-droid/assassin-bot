from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from services.broadcast_profiles_service import get_active_config_id, get_config_detail
from services.broadcast_runtime_service import interval_unit_display
from services.channel_post_service import (
    build_text_source_label,
    count_source_items,
    source_channel_title,
)


def _active_config_name(user_id: int) -> str:
    config_id = get_active_config_id(user_id)
    detail = get_config_detail(user_id, config_id)
    if detail and detail.get("name"):
        return str(detail["name"])
    if config_id == 0:
        return "По умолчанию"
    return f"Конфиг {config_id}"


def build_broadcast_keyboard(
    include_active: bool = False,
    user_id: int = None,
    active_broadcasts: dict = None,
    back_callback: str = "bc_back",
) -> InlineKeyboardMarkup:
    """Build the standard keyboard for the broadcast menu."""
    buttons = [
        [
            InlineKeyboardButton(text="Текст", callback_data="bc_text"),
            InlineKeyboardButton(text="Кол-во", callback_data="bc_quantity"),
            InlineKeyboardButton(text="Интервал", callback_data="bc_interval"),
        ],
        [
            InlineKeyboardButton(text="Темп", callback_data="bc_batch_pause"),
            InlineKeyboardButton(text="Чаты", callback_data="bc_chats"),
            InlineKeyboardButton(text="Активные", callback_data="bc_active"),
        ],
        [
            InlineKeyboardButton(text="Запустить", callback_data="bc_launch"),
            InlineKeyboardButton(text="Назад", callback_data=back_callback),
        ],
    ]

    if include_active and user_id and active_broadcasts:
        user_broadcasts = {
            bid: b
            for bid, b in active_broadcasts.items()
            if b["user_id"] == user_id and b["status"] in ("running", "paused")
        }
        if user_broadcasts:
            groups = {}
            singles = []
            display_numbers = {
                bid: index
                for index, bid in enumerate(sorted(user_broadcasts.keys()), start=1)
            }
            for bid, broadcast in user_broadcasts.items():
                gid = broadcast.get("group_id")
                if gid is None:
                    singles.append((bid, broadcast))
                else:
                    groups.setdefault(gid, []).append((bid, broadcast))

            for gid, items in sorted(groups.items()):
                statuses = {broadcast["status"] for _, broadcast in items}
                if "running" in statuses:
                    status_icon = "▶️"
                elif statuses == {"paused"}:
                    status_icon = "⏸️"
                else:
                    status_icon = "✅"
                buttons.append(
                    [
                        InlineKeyboardButton(
                            text=f"{status_icon} Группа #{gid} ({len(items)})",
                            callback_data=f"view_group_{gid}",
                        )
                    ]
                )

            for bid, broadcast in sorted(singles):
                status = (
                    "▶️"
                    if broadcast["status"] == "running"
                    else "⏸️"
                    if broadcast["status"] == "paused"
                    else "✅"
                )
                display_number = display_numbers.get(bid, bid)
                buttons.append(
                    [
                        InlineKeyboardButton(
                            text=f"{status} Рассылка #{display_number}",
                            callback_data=f"view_bc_{bid}",
                        )
                    ]
                )

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_broadcast_menu_text(
    config: dict,
    chats: list,
    active_broadcasts: dict,
    user_id: int,
    show_title: bool = True,
    show_active_count: bool = True,
) -> str:
    """Build the main broadcast menu text."""
    info = "📤 <b>РАССЫЛКА</b>\n\n" if show_title else ""
    info += f"⚙️ <b>Конфиг:</b> {_active_config_name(user_id)}\n"
    mode_text = "random" if config.get("text_mode") == "random" else "no random"
    info += f"💬 <b>Источник текста:</b> {build_text_source_label(config)}\n"
    info += f"💬 <b>Вариантов:</b> {count_source_items(config)} ({mode_text})\n"
    if config.get("text_source_type") == "channel":
        info += f"📡 <b>Канал:</b> {source_channel_title(config)}\n"
    info += f"🔢 <b>Кол-во:</b> {config.get('count', 0)}\n"
    info += (
        f"⏱️ <b>Интервал:</b> {config.get('interval', 0)} "
        f"{interval_unit_display(config.get('interval_unit'))}\n"
    )
    info += f"⚡️ <b>Темп:</b> {config.get('chat_pause', '1-3')} сек\n"
    info += f"💭 <b>Чатов:</b> {len(chats)}\n"

    if show_active_count:
        user_broadcasts = {
            bid: broadcast
            for bid, broadcast in active_broadcasts.items()
            if broadcast["user_id"] == user_id and broadcast["status"] in ("running", "paused")
        }
        info += f"\n\nАктивных рассылок: {len(user_broadcasts)}"

    return info
