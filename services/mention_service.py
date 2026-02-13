import asyncio
from typing import Optional, Callable

from telethon import events
from telethon.tl.types import MessageEntityMentionName

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
    logger.info("Запуск мониторинга упоминаний: user=%s account=%s", user_id, account_number)
    last_chat_filters = []
    my_id = 0
    my_username = ""
    handler_registered = False

    async def handler(event):
        nonlocal last_chat_filters, my_id, my_username
        try:
            active_client = app_state.user_authenticated.get(user_id, {}).get(account_number)
            if active_client is not client:
                return
            if event.sender_id == my_id:
                return

            tracked_chats = []
            try:
                tracked_chats = get_tracked_chats(user_id) or []
                if get_broadcast_chats:
                    tracked_chats = list(tracked_chats) + list(get_broadcast_chats(user_id) or [])
                if tracked_chats:
                    last_chat_filters = tracked_chats
            except Exception as exc:
                if last_chat_filters:
                    tracked_chats = list(last_chat_filters)
                    logger.warning(
                        "Не удалось обновить список чатов (использую кеш) user=%s acc=%s: %s",
                        user_id,
                        account_number,
                        exc,
                    )
                else:
                    logger.warning(
                        "Не удалось получить список чатов user=%s acc=%s: %s",
                        user_id,
                        account_number,
                        exc,
                    )
                    return

            current_chat_id = normalize_chat_id(event.chat_id)
            normalized_chat_ids = set()
            tracked_usernames = set()
            for chat_id, _ in tracked_chats:
                try:
                    normalized_chat_ids.add(normalize_chat_id(chat_id))
                except Exception:
                    chat_id_str = str(chat_id or "").strip().lower()
                    if chat_id_str.startswith("@"):
                        tracked_usernames.add(chat_id_str[1:])
                    elif chat_id_str:
                        tracked_usernames.add(chat_id_str)

            if current_chat_id not in normalized_chat_ids:
                if tracked_usernames:
                    try:
                        chat = await event.get_chat()
                        username = str(getattr(chat, "username", "") or "").lower().strip()
                        if not username or username not in tracked_usernames:
                            return
                    except Exception:
                        return
                else:
                    return

            message = event.message
            sender = await event.get_sender()

            is_mentioned = bool(getattr(message, "mentioned", False))
            if message.reply_to_msg_id:
                try:
                    replied_to = await client.get_messages(event.chat_id, ids=message.reply_to_msg_id)
                    if replied_to and replied_to.sender_id == my_id:
                        is_mentioned = True
                except Exception:
                    pass

            if not is_mentioned:
                for ent in (getattr(message, "entities", None) or []):
                    if isinstance(ent, MessageEntityMentionName):
                        if getattr(ent, "user_id", None) == my_id:
                            is_mentioned = True
                            break

            text_body = (message.text or getattr(message, "raw_text", "") or "").strip()
            if not is_mentioned and text_body:
                text_lower = text_body.lower()
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
                    notification += f"📝 <b>Текст:</b> {text_body[:200] if text_body else '(без текста)'}\n"

                    buttons = [[
                        InlineKeyboardButton(text="📬 Сообщение", url=msg_url),
                        InlineKeyboardButton(text="👤 Профиль", url=f"tg://user?id={sender.id}"),
                    ]]
                    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

                    await bot.send_message(user_id, notification, reply_markup=kb, parse_mode="HTML")

                    if text_body and message_has_keywords(text_body):
                        links, usernames = extract_join_links(text_body)
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
                except Exception as exc:
                    logger.warning("Ошибка обработки упоминания user=%s acc=%s: %s", user_id, account_number, exc)
        except Exception as exc:
            logger.warning("mention handler error user=%s acc=%s: %s", user_id, account_number, exc)

    try:
        await asyncio.sleep(2)
        while True:
            if user_id not in app_state.user_authenticated:
                return
            active_client = app_state.user_authenticated[user_id].get(account_number)
            if active_client is not client:
                return

            try:
                if not client.is_connected():
                    await client.connect()

                me = None
                for attempt in range(5):
                    try:
                        me = await client.get_me()
                        break
                    except Exception as exc:
                        exc_str = str(exc).lower()
                        if (
                            "database is locked" in exc_str
                            or "disconnected" in exc_str
                            or "cannot send requests while disconnected" in exc_str
                        ):
                            await asyncio.sleep(1 + attempt * 0.5)
                            if not client.is_connected():
                                try:
                                    await client.connect()
                                except Exception:
                                    pass
                            continue
                        raise

                if not me:
                    await asyncio.sleep(2)
                    continue

                my_id = me.id
                my_username = me.username or f"user{me.id}"

                if not handler_registered:
                    client.add_event_handler(handler, events.NewMessage())
                    handler_registered = True

                await client.run_until_disconnected()
                logger.warning("Монитор отключен user=%s acc=%s, пробую переподключить", user_id, account_number)
                await asyncio.sleep(1.5)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                exc_str = str(exc).lower()
                if (
                    "database is locked" in exc_str
                    or "disconnected" in exc_str
                    or "cannot send requests while disconnected" in exc_str
                ):
                    await asyncio.sleep(1.5)
                    continue
                logger.warning("Ошибка мониторинга упоминаний: %s", exc)
                return
    finally:
        if handler_registered:
            try:
                client.remove_event_handler(handler)
            except Exception:
                pass


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


def has_running_monitors(user_id: int) -> bool:
    monitors = app_state.mention_monitors.get(user_id, {})
    for task in monitors.values():
        if task and not task.done():
            return True
    return False
