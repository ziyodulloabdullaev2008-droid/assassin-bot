import asyncio
from typing import Optional, Callable

from telethon import events

from core.logging import get_logger
from core.state import app_state
from services.join_service import enqueue_join, extract_join_links, message_has_keywords

logger = get_logger("mention_service")


async def monitor_mentions(
    bot,
    user_id: int,
    account_number: int,
    *,
    get_tracked_chats: Callable,
    get_broadcast_chats: Optional[Callable] = None,
    get_user_accounts: Callable,
    normalize_chat_id: Callable,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
) -> None:
    """Мониторит упоминания пользователя в чатах."""
    if user_id not in app_state.user_authenticated:
        return
    if account_number not in app_state.user_authenticated[user_id]:
        return

    client = app_state.user_authenticated[user_id][account_number]

    try:
        await asyncio.sleep(2)
        me = None
        for attempt in range(5):
            try:
                me = await client.get_me()
                break
            except Exception as exc:
                if "database is locked" in str(exc):
                    await asyncio.sleep(2)
                else:
                    raise

        if not me:
            return

        my_id = me.id
        my_username = me.username or f"user{me.id}"

        @client.on(events.NewMessage)
        async def handler(event):
            try:
                if event.sender_id == my_id:
                    return

                tracked_chats = get_tracked_chats(user_id)
                if not tracked_chats and get_broadcast_chats:
                    tracked_chats = get_broadcast_chats(user_id)
                chat_ids = [chat_id for chat_id, _ in tracked_chats]

                current_chat_id = normalize_chat_id(event.chat_id)
                normalized_chat_ids = [normalize_chat_id(cid) for cid in chat_ids]

                if current_chat_id not in normalized_chat_ids:
                    return

                message = event.message
                sender = await event.get_sender()

                is_mentioned = False
                if message.reply_to_msg_id:
                    try:
                        replied_to = await client.get_messages(event.chat_id, ids=message.reply_to_msg_id)
                        if replied_to and replied_to.sender_id == my_id:
                            is_mentioned = True
                    except Exception:
                        pass

                if not is_mentioned and message.text:
                    text_lower = message.text.lower()
                    username_str = str(my_username) if my_username else f"user{my_id}"
                    username_check = f"@{username_str.lower()}" in text_lower
                    id_check = f"user{my_id}" in text_lower
                    if username_check or id_check:
                        is_mentioned = True

                if is_mentioned:
                    try:
                        chat = await event.get_chat()
                        title = getattr(chat, "title", None) if hasattr(chat, "title") else None
                        chat_name = str(title) if title else "Личный чат"
                        username = getattr(chat, "username", None)

                        if username:
                            msg_url = f"https://t.me/{username}/{message.id}"
                        else:
                            msg_url = f"https://t.me/c/{abs(int(event.chat_id))}/{message.id}"

                        msg_time = message.date.strftime("%H:%M:%S") if message.date else "??:??:??"
                        sender_name = f"{sender.first_name} {sender.last_name or ''}".strip()

                        account_info = get_user_accounts(user_id)
                        account_name = "Неизвестно"
                        for acc_num, telegram_id, username_acc, first_name, is_active in account_info:
                            if acc_num == account_number:
                                account_name = first_name or username_acc or f"Акк {account_number}"
                                break

                        notification = "🔔 <b>УПОМИНАНИЕ В ЧАТЕ</b>\n\n"
                        notification += f"👤 <b>Упомянут аккаунт:</b> {account_name}\n"
                        notification += f"💬 <b>Чат:</b> {chat_name} | <code>{event.chat_id}</code>\n"
                        notification += f"👤 <b>От:</b> {sender_name}\n"
                        notification += f"⏰ <b>Время:</b> {msg_time}\n"
                        notification += f"📝 <b>Текст:</b> {message.text[:200] if message.text else '(без текста)'}\n"

                        buttons = [[
                            InlineKeyboardButton(text="📬 Сообщение", url=msg_url),
                            InlineKeyboardButton(text="👤 Профиль", url=f"tg://user?id={sender.id}"),
                        ]]
                        kb = InlineKeyboardMarkup(inline_keyboard=buttons)

                        await bot.send_message(user_id, notification, reply_markup=kb, parse_mode="HTML")

                        if message.text and message_has_keywords(message.text):
                            links, usernames = extract_join_links(message.text)
                            try:
                                if message.buttons:
                                    for row in message.buttons:
                                        for btn in row:
                                            url = getattr(btn, "url", None)
                                            if url:
                                                links.append(url)
                            except Exception:
                                pass
                            await enqueue_join(
                                user_id,
                                chat_id=event.chat_id,
                                links=links,
                                usernames=usernames,
                            )
                    except Exception:
                        pass
            except Exception:
                pass

        await client.run_until_disconnected()
    except Exception as exc:
        logger.warning("Ошибка мониторинга упоминаний: %s", exc)


async def start_mention_monitoring(
    bot,
    user_id: int,
    *,
    get_tracked_chats: Callable,
    get_broadcast_chats: Optional[Callable] = None,
    get_user_accounts: Callable,
    normalize_chat_id: Callable,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
) -> None:
    """Запускает мониторинг упоминаний для всех аккаунтов пользователя."""
    if user_id not in app_state.user_authenticated:
        return

    if user_id not in app_state.mention_monitors:
        app_state.mention_monitors[user_id] = {}

    for account_number in list(app_state.user_authenticated[user_id].keys()):
        task = app_state.mention_monitors[user_id].get(account_number)
        if task and not task.done():
            continue
        task = asyncio.create_task(
            monitor_mentions(
                bot,
                user_id,
                account_number,
                get_tracked_chats=get_tracked_chats,
                get_broadcast_chats=get_broadcast_chats,
                get_user_accounts=get_user_accounts,
                normalize_chat_id=normalize_chat_id,
                InlineKeyboardButton=InlineKeyboardButton,
                InlineKeyboardMarkup=InlineKeyboardMarkup,
            )
        )
        app_state.mention_monitors[user_id][account_number] = task


def stop_mention_monitoring(user_id: int, account_number: Optional[int] = None) -> None:
    """Останавливает мониторинг упоминаний для пользователя или одного аккаунта."""
    if user_id not in app_state.mention_monitors:
        return

    if account_number is None:
        for task in list(app_state.mention_monitors[user_id].values()):
            if task and not task.done():
                task.cancel()
        app_state.mention_monitors[user_id].clear()
    else:
        task = app_state.mention_monitors[user_id].get(account_number)
        if task and not task.done():
            task.cancel()
        app_state.mention_monitors[user_id].pop(account_number, None)
