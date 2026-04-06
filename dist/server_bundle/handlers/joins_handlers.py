import asyncio
import html
import random
import re

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters.command import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from telethon.errors import FloodWaitError, UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

from core.config import API_HASH, API_ID
from core.state import app_state
from database import get_user_accounts
from services.session_service import ensure_connected_client

router = Router()
mass_join_tasks: dict[int, asyncio.Task] = {}
LOGIN_REQUIRED_TEXT = "❌ Сначала войди через /login"

CHATS_JOIN_DELAY_MIN = 15
CHATS_JOIN_DELAY_MAX = 25
CHATS_JOIN_REST_EVERY = 5
CHATS_JOIN_REST_MIN = 60
CHATS_JOIN_REST_MAX = 120


class ChatsJoinState(StatesGroup):
    selecting_accounts = State()
    waiting_targets = State()


def _has_logged_accounts(user_id: int) -> bool:
    return bool(app_state.user_authenticated.get(user_id))


async def _ensure_logged_message(message: Message) -> bool:
    if _has_logged_accounts(message.from_user.id):
        return True
    await message.answer(LOGIN_REQUIRED_TEXT)
    return False


async def _ensure_logged_query(query: CallbackQuery) -> bool:
    if _has_logged_accounts(query.from_user.id):
        return True
    await query.answer(LOGIN_REQUIRED_TEXT, show_alert=True)
    return False


async def _safe_edit_text(
    query: CallbackQuery, text: str, kb: InlineKeyboardMarkup
) -> None:
    try:
        await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
        raise


def _get_connected_accounts(user_id: int) -> list[tuple[int, str]]:
    connected = set(app_state.user_authenticated.get(user_id, {}).keys())
    accounts = get_user_accounts(user_id)
    rows = []
    for acc_num, _, username, first_name, _ in accounts:
        if acc_num not in connected:
            continue
        label = first_name or username or f"Акк {acc_num}"
        rows.append((acc_num, label))
    rows.sort(key=lambda x: x[0])
    return rows


def _build_chats_accounts_text(user_id: int, selected: set[int]) -> str:
    connected = _get_connected_accounts(user_id)
    if not connected:
        return LOGIN_REQUIRED_TEXT

    selected_count = len([acc for acc, _ in connected if acc in selected])
    total_targets = len(connected)
    return (
        "🧩 <b>/chats — Массовое вступление</b>\n\n"
        f"Подключено аккаунтов: <b>{total_targets}</b>\n"
        f"Выбрано: <b>{selected_count}</b>\n\n"
        "Выбери один или несколько аккаунтов, затем нажми «Далее»."
    )


def _build_chats_accounts_kb(user_id: int, selected: set[int]) -> InlineKeyboardMarkup:
    connected = _get_connected_accounts(user_id)
    buttons = []
    for acc_num, label in connected:
        mark = "✅" if acc_num in selected else "❌"
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"{mark} {label}", callback_data=f"jc_acc_{acc_num}"
                )
            ]
        )

    buttons.append(
        [
            InlineKeyboardButton(text="✅ Все", callback_data="jc_all"),
            InlineKeyboardButton(text="⬜ Снять все", callback_data="jc_none"),
        ]
    )
    buttons.append(
        [
            InlineKeyboardButton(text="➡️ Далее", callback_data="jc_next"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="jc_cancel"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _parse_chat_targets(raw_text: str) -> tuple[list[str], list[str]]:
    text = (raw_text or "").strip()
    if not text:
        return [], []

    links: list[str] = []
    usernames: list[str] = []
    for token in re.split(r"[\s,;]+", text):
        item = token.strip()
        if not item:
            continue
        lower = item.lower()
        if lower.startswith("https://t.me/") or lower.startswith("http://t.me/"):
            links.append(item)
            continue
        if lower.startswith("t.me/"):
            links.append(f"https://{item}")
            continue
        if item.startswith("@"):
            name = item[1:].strip()
            if re.fullmatch(r"[A-Za-z0-9_]{4,}", name):
                usernames.append(name)
            continue
        if re.fullmatch(r"[A-Za-z0-9_]{4,}", item):
            usernames.append(item)

    links = _dedupe_keep_order(links)
    usernames = _dedupe_keep_order(usernames)
    return links, usernames


def _dedupe_keep_order(items: list[str]) -> list[str]:
    out: list[str] = []
    seen = set()
    for item in items:
        key = (item or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _extract_invite_hash(link: str) -> str | None:
    m = re.search(r"/\+([-_\w]+)$", link)
    if m:
        return m.group(1)
    m = re.search(r"/joinchat/([-_\w]+)$", link)
    if m:
        return m.group(1)
    return None


@router.message(Command("chats"))
async def chats_join_command(message: Message, state: FSMContext):
    if not await _ensure_logged_message(message):
        await state.clear()
        return

    user_id = message.from_user.id
    connected = _get_connected_accounts(user_id)
    if not connected:
        await message.answer(LOGIN_REQUIRED_TEXT)
        return

    task = mass_join_tasks.get(user_id)
    if task and not task.done():
        await message.answer("⏳ Уже выполняется /chats задача. Дождись завершения.")
        return

    selected = {acc_num for acc_num, _ in connected}
    await state.set_state(ChatsJoinState.selecting_accounts)
    await state.update_data(chats_selected=sorted(selected))
    await message.answer(
        _build_chats_accounts_text(user_id, selected),
        reply_markup=_build_chats_accounts_kb(user_id, selected),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("jc_"))
async def chats_join_callbacks(query: CallbackQuery, state: FSMContext):
    if not await _ensure_logged_query(query):
        await state.clear()
        return

    await query.answer()
    user_id = query.from_user.id
    action = query.data

    if action == "jc_wait_cancel":
        await state.clear()
        try:
            await query.message.delete()
        except Exception:
            pass
        return

    current_state = await state.get_state()
    if current_state != ChatsJoinState.selecting_accounts.state:
        return

    data = await state.get_data()
    selected = set(data.get("chats_selected") or [])
    connected = _get_connected_accounts(user_id)
    connected_set = {acc_num for acc_num, _ in connected}
    selected = {x for x in selected if x in connected_set}

    if action == "jc_cancel":
        await state.clear()
        try:
            await query.message.delete()
        except Exception:
            pass
        return

    if action == "jc_all":
        selected = set(connected_set)
    elif action == "jc_none":
        selected = set()
    elif action == "jc_next":
        if not selected:
            await query.answer("Выбери хотя бы один аккаунт", show_alert=True)
            return
        await state.set_state(ChatsJoinState.waiting_targets)
        await state.update_data(
            chats_selected=sorted(selected), chats_menu_msg=query.message.message_id
        )
        try:
            await query.message.edit_text(
                "📥 <b>/chats</b>\n\n"
                "Отправь ссылки/юзернеймы чатов для вступления.\n"
                "Поддержка форматов:\n"
                "• <code>@channelname</code>\n"
                "• <code>https://t.me/...</code>\n\n"
                "Можно много строк сразу.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="❌ Отмена", callback_data="jc_wait_cancel"
                            )
                        ]
                    ]
                ),
            )
        except Exception:
            pass
        return
    elif action.startswith("jc_acc_"):
        acc_num = int(action.split("_")[2])
        if acc_num in selected:
            selected.remove(acc_num)
        else:
            selected.add(acc_num)

    await state.update_data(chats_selected=sorted(selected))
    await _safe_edit_text(
        query,
        _build_chats_accounts_text(user_id, selected),
        _build_chats_accounts_kb(user_id, selected),
    )


@router.message(ChatsJoinState.waiting_targets)
async def chats_join_targets_input(message: Message, state: FSMContext):
    if not await _ensure_logged_message(message):
        await state.clear()
        return

    user_id = message.from_user.id
    data = await state.get_data()
    selected = set(data.get("chats_selected") or [])

    if (message.text or "").strip().lower() in {"отмена", "/cancel"}:
        await state.clear()
        await message.answer("❌ Операция /chats отменена.")
        return

    links, usernames = _parse_chat_targets(message.text or "")
    if not links and not usernames:
        await message.answer(
            "❌ Не вижу валидных целей.\n"
            "Пример:\n"
            "<code>@channel_one\nhttps://t.me/channel_two</code>",
            parse_mode="HTML",
        )
        return

    if not selected:
        await message.answer("❌ Нет выбранных аккаунтов. Запусти /chats заново.")
        await state.clear()
        return

    await state.clear()
    status_msg = await message.answer(
        "🚀 Запускаю массовое вступление...\n"
        f"Аккаунтов: {len(selected)} | Целей: {len(links) + len(usernames)}\n"
        f"Интервал: {CHATS_JOIN_DELAY_MIN}-{CHATS_JOIN_DELAY_MAX} сек, отдых {CHATS_JOIN_REST_MIN // 60}-{CHATS_JOIN_REST_MAX // 60} мин"
    )

    existing_task = mass_join_tasks.get(user_id)
    if existing_task and not existing_task.done():
        existing_task.cancel()
    mass_join_tasks[user_id] = asyncio.create_task(
        _run_mass_join(
            bot=message.bot,
            user_id=user_id,
            account_numbers=sorted(selected),
            links=links,
            usernames=usernames,
            status_message_id=status_msg.message_id,
            status_chat_id=message.chat.id,
        )
    )


async def _safe_edit_progress(bot, chat_id: int, message_id: int, text: str) -> None:
    try:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=message_id, text=text, parse_mode="HTML"
        )
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
    except Exception:
        return


async def _join_target_once(
    client, *, link: str | None = None, username: str | None = None
) -> tuple[str, str]:
    try:
        if link:
            invite_hash = _extract_invite_hash(link)
            if invite_hash:
                await client(ImportChatInviteRequest(invite_hash))
                return "joined", link
            entity = await client.get_entity(link)
            await client(JoinChannelRequest(entity))
            return "joined", link
        if username:
            entity = await client.get_entity(username)
            await client(JoinChannelRequest(entity))
            return "joined", f"@{username}"
        return "skipped", "-"
    except UserAlreadyParticipantError:
        return "already", link or f"@{username}"
    except FloodWaitError as exc:
        wait_s = int(getattr(exc, "seconds", 60) or 60)
        await asyncio.sleep(wait_s + 2)
        return "floodwait", link or f"@{username}"
    except Exception:
        return "failed", link or f"@{username}"


async def _run_mass_join(
    *,
    bot,
    user_id: int,
    account_numbers: list[int],
    links: list[str],
    usernames: list[str],
    status_message_id: int,
    status_chat_id: int,
) -> None:
    targets: list[tuple[str, str]] = [("link", x) for x in links] + [
        ("username", x) for x in usernames
    ]
    total_targets = len(targets)
    account_titles = {
        acc_num: (name or f"Акк {acc_num}")
        for acc_num, name in _get_connected_accounts(user_id)
    }

    overall_joined = 0
    overall_already = 0
    overall_failed = 0
    summary_lines = []

    for idx, acc_num in enumerate(account_numbers, start=1):
        client = await ensure_connected_client(
            user_id,
            acc_num,
            api_id=API_ID,
            api_hash=API_HASH,
        )
        acc_label = html.escape(account_titles.get(acc_num, f"Акк {acc_num}"))
        if not client:
            summary_lines.append(f"• {acc_label}: не подключен")
            continue

        joined = 0
        already = 0
        failed = 0

        for t_i, (kind, value) in enumerate(targets, start=1):
            if kind == "link":
                status, _ = await _join_target_once(client, link=value)
            else:
                status, _ = await _join_target_once(client, username=value)

            if status == "joined":
                joined += 1
                overall_joined += 1
            elif status == "already":
                already += 1
                overall_already += 1
            else:
                failed += 1
                overall_failed += 1

            progress_text = (
                "🧩 <b>/chats — выполнение</b>\n\n"
                f"Аккаунт: <b>{acc_label}</b> ({idx}/{len(account_numbers)})\n"
                f"Цель: {t_i}/{total_targets}\n"
                f"Успех: {joined} | Уже был: {already} | Ошибки: {failed}\n\n"
                f"Общий итог: ✅ {overall_joined} | ☑️ {overall_already} | ❌ {overall_failed}"
            )
            await _safe_edit_progress(
                bot, status_chat_id, status_message_id, progress_text
            )

            if t_i < total_targets:
                await asyncio.sleep(
                    random.uniform(CHATS_JOIN_DELAY_MIN, CHATS_JOIN_DELAY_MAX)
                )

            if t_i % CHATS_JOIN_REST_EVERY == 0 and t_i < total_targets:
                rest_s = random.uniform(CHATS_JOIN_REST_MIN, CHATS_JOIN_REST_MAX)
                rest_text = (
                    "😴 <b>Антифлуд-отдых</b>\n\n"
                    f"Аккаунт: <b>{acc_label}</b>\n"
                    f"Отдых: {int(rest_s)} сек\n"
                    f"Прогресс: {t_i}/{total_targets}"
                )
                await _safe_edit_progress(
                    bot, status_chat_id, status_message_id, rest_text
                )
                await asyncio.sleep(rest_s)

        summary_lines.append(f"• {acc_label}: ✅ {joined} | ☑️ {already} | ❌ {failed}")

    final_text = (
        "✅ <b>/chats завершен</b>\n\n"
        f"Целей: {total_targets}\n"
        f"Итог: ✅ {overall_joined} | ☑️ {overall_already} | ❌ {overall_failed}\n\n"
        "<b>По аккаунтам:</b>\n"
        + ("\n".join(summary_lines) if summary_lines else "• нет данных")
    )
    await _safe_edit_progress(bot, status_chat_id, status_message_id, final_text)
