import asyncio
import html
import random
import re

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters.command import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from telethon.errors import FloodWaitError, UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

from core.state import app_state
from database import get_user_accounts
from services.join_service import (
    set_enabled,
    is_enabled,
    set_target_accounts,
    get_target_accounts,
    get_delay_config,
    set_delay_config,
)

router = Router()
mass_join_tasks: dict[int, asyncio.Task] = {}

CHATS_JOIN_DELAY_MIN = 15
CHATS_JOIN_DELAY_MAX = 25
CHATS_JOIN_REST_EVERY = 5
CHATS_JOIN_REST_MIN = 60
CHATS_JOIN_REST_MAX = 120


class JoinsSettingsState(StatesGroup):
    waiting_per_target_range = State()
    waiting_between_chats_range = State()


class ChatsJoinState(StatesGroup):
    selecting_accounts = State()
    waiting_targets = State()


def _parse_range(text: str) -> tuple[int, int] | None:
    raw = (text or "").strip()
    if not raw:
        return None
    m = re.search(r"(\d+)\s*[-:\s]\s*(\d+)", raw)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        return (a, b) if a <= b else (b, a)
    if raw.isdigit():
        value = int(raw)
        return value, value
    return None


async def _safe_edit_text(query: CallbackQuery, text: str, kb: InlineKeyboardMarkup) -> None:
    try:
        await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
        raise


async def _safe_refresh_main(query: CallbackQuery, user_id: int) -> None:
    await _safe_edit_text(query, _build_text(user_id), _build_menu(user_id))


async def _show_settings_menu(query: CallbackQuery, user_id: int) -> None:
    await _safe_edit_text(query, _build_settings_text(user_id), _build_settings_menu())


def _build_menu(user_id: int) -> InlineKeyboardMarkup:
    accounts = get_user_accounts(user_id)
    enabled = is_enabled(user_id)
    selected = get_target_accounts(user_id)

    buttons = []
    toggle_text = "⏸️ Выключить" if enabled else "▶️ Включить"
    buttons.append([InlineKeyboardButton(text=toggle_text, callback_data="joins_toggle")])
    buttons.append([InlineKeyboardButton(text="⚙️ Настройки", callback_data="joins_settings")])
    buttons.append([InlineKeyboardButton(text="✅ Все аккаунты", callback_data="joins_all")])

    for acc_num, _, username, first_name, _ in accounts:
        label = first_name or username or f"Акк {acc_num}"
        is_selected = (not selected) or (acc_num in selected)
        prefix = "✅" if is_selected else "❌"
        buttons.append([InlineKeyboardButton(text=f"{prefix} {label}", callback_data=f"joins_acc_{acc_num}")])

    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="joins_close")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _build_text(user_id: int) -> str:
    enabled = is_enabled(user_id)
    selected = get_target_accounts(user_id)
    cfg = get_delay_config(user_id)
    status = "✅ Включено" if enabled else "⏸️ Выключено"
    mode = "Все аккаунты" if not selected else f"Выбрано: {len(selected)}"
    queue_len = len(app_state.joins_queue.get(user_id, []))
    return (
        "⚙️ <b>/JOINS</b>\n\n"
        f"Статус: {status}\n"
        f"Режим: {mode}\n"
        f"Очередь: {queue_len}\n\n"
        f"⏱ Внутри заявки: <b>{cfg['per_target_min']}-{cfg['per_target_max']} сек</b>\n"
        f"⏳ Между чатами: <b>{cfg['between_chats_min']}-{cfg['between_chats_max']} сек</b>\n\n"
        "Ключевые слова: «подписаться», «вступить», «необходимо»."
    )


def _build_settings_text(user_id: int) -> str:
    cfg = get_delay_config(user_id)
    return (
        "⚙️ <b>Настройки /JOINS</b>\n\n"
        f"Внутри одной заявки (между ссылками/кнопками): <b>{cfg['per_target_min']}-{cfg['per_target_max']} сек</b>\n"
        f"Между заявками из разных чатов: <b>{cfg['between_chats_min']}-{cfg['between_chats_max']} сек</b>\n\n"
        "Нажми, что хочешь изменить."
    )


def _build_settings_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Внутри заявки", callback_data="joins_set_per_target")],
            [InlineKeyboardButton(text="✏️ Между чатами", callback_data="joins_set_between_chats")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="joins_settings_back")],
        ]
    )


def _build_input_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="joins_settings_cancel")]]
    )


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
        return "❌ Нет подключенных аккаунтов. Сначала войди через /login"

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
        buttons.append([InlineKeyboardButton(text=f"{mark} {label}", callback_data=f"jc_acc_{acc_num}")])

    buttons.append([
        InlineKeyboardButton(text="✅ Все", callback_data="jc_all"),
        InlineKeyboardButton(text="⬜ Снять все", callback_data="jc_none"),
    ])
    buttons.append([
        InlineKeyboardButton(text="➡️ Далее", callback_data="jc_next"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="jc_cancel"),
    ])
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


@router.message(Command("joins"))
async def joins_menu(message: Message):
    user_id = message.from_user.id
    await message.answer(_build_text(user_id), reply_markup=_build_menu(user_id), parse_mode="HTML")


@router.callback_query(F.data == "joins_toggle")
async def joins_toggle_callback(query: CallbackQuery):
    user_id = query.from_user.id
    set_enabled(user_id, not is_enabled(user_id))
    await query.answer()
    await _safe_refresh_main(query, user_id)


@router.callback_query(F.data == "joins_all")
async def joins_all_callback(query: CallbackQuery):
    user_id = query.from_user.id
    set_target_accounts(user_id, None)
    await query.answer()
    await _safe_refresh_main(query, user_id)


@router.callback_query(F.data.startswith("joins_acc_"))
async def joins_acc_callback(query: CallbackQuery):
    user_id = query.from_user.id
    acc_num = int(query.data.split("_")[2])
    selected = get_target_accounts(user_id)
    if not selected:
        selected = set(app_state.user_authenticated.get(user_id, {}).keys())
    if acc_num in selected:
        selected.remove(acc_num)
    else:
        selected.add(acc_num)
    set_target_accounts(user_id, list(selected))
    await query.answer()
    await _safe_refresh_main(query, user_id)


@router.callback_query(F.data == "joins_settings")
async def joins_settings_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    await state.clear()
    await _show_settings_menu(query, query.from_user.id)


@router.callback_query(F.data == "joins_set_per_target")
async def joins_set_per_target_callback(query: CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    cfg = get_delay_config(user_id)
    await state.set_state(JoinsSettingsState.waiting_per_target_range)
    await state.update_data(menu_message_id=query.message.message_id, chat_id=query.message.chat.id)
    await query.answer()
    await _safe_edit_text(
        query,
        (
            "⏱ <b>Внутри заявки</b>\n\n"
            f"Сейчас: <b>{cfg['per_target_min']}-{cfg['per_target_max']} сек</b>\n"
            "Отправь новый диапазон в формате: <code>7-15</code>"
        ),
        _build_input_menu(),
    )


@router.callback_query(F.data == "joins_set_between_chats")
async def joins_set_between_chats_callback(query: CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    cfg = get_delay_config(user_id)
    await state.set_state(JoinsSettingsState.waiting_between_chats_range)
    await state.update_data(menu_message_id=query.message.message_id, chat_id=query.message.chat.id)
    await query.answer()
    await _safe_edit_text(
        query,
        (
            "⏳ <b>Между чатами</b>\n\n"
            f"Сейчас: <b>{cfg['between_chats_min']}-{cfg['between_chats_max']} сек</b>\n"
            "Отправь новый диапазон в формате: <code>20-30</code>"
        ),
        _build_input_menu(),
    )


@router.callback_query(F.data == "joins_settings_back")
async def joins_settings_back_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    await state.clear()
    await _safe_refresh_main(query, query.from_user.id)


@router.callback_query(F.data == "joins_settings_cancel")
async def joins_settings_cancel_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    await state.clear()
    await _show_settings_menu(query, query.from_user.id)


@router.message(JoinsSettingsState.waiting_per_target_range)
async def joins_per_target_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    rng = _parse_range(message.text or "")
    if not rng:
        await message.answer("❌ Формат: <code>7-15</code>", parse_mode="HTML")
        return
    min_v, max_v = rng
    if min_v < 1 or max_v > 600:
        await message.answer("❌ Допустимо: 1-600 сек")
        return

    set_delay_config(user_id, per_target_min=min_v, per_target_max=max_v)
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass

    menu_message_id = data.get("menu_message_id")
    chat_id = data.get("chat_id")
    if menu_message_id and chat_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id,
                message_id=menu_message_id,
                text=_build_settings_text(user_id),
                reply_markup=_build_settings_menu(),
                parse_mode="HTML",
            )
            return
        except Exception:
            pass
    await message.answer(_build_settings_text(user_id), reply_markup=_build_settings_menu(), parse_mode="HTML")


@router.message(JoinsSettingsState.waiting_between_chats_range)
async def joins_between_chats_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    rng = _parse_range(message.text or "")
    if not rng:
        await message.answer("❌ Формат: <code>20-30</code>", parse_mode="HTML")
        return
    min_v, max_v = rng
    if min_v < 1 or max_v > 3600:
        await message.answer("❌ Допустимо: 1-3600 сек")
        return

    set_delay_config(user_id, between_chats_min=min_v, between_chats_max=max_v)
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass

    menu_message_id = data.get("menu_message_id")
    chat_id = data.get("chat_id")
    if menu_message_id and chat_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id,
                message_id=menu_message_id,
                text=_build_settings_text(user_id),
                reply_markup=_build_settings_menu(),
                parse_mode="HTML",
            )
            return
        except Exception:
            pass
    await message.answer(_build_settings_text(user_id), reply_markup=_build_settings_menu(), parse_mode="HTML")


@router.message(Command("chats"))
async def chats_join_command(message: Message, state: FSMContext):
    user_id = message.from_user.id

    connected = _get_connected_accounts(user_id)
    if not connected:
        await message.answer("❌ Нет подключенных аккаунтов. Сначала войди через /login")
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
        await state.update_data(chats_selected=sorted(selected), chats_menu_msg=query.message.message_id)
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
                    inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="jc_wait_cancel")]]
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
        f"Интервал: {CHATS_JOIN_DELAY_MIN}-{CHATS_JOIN_DELAY_MAX} сек, отдых {CHATS_JOIN_REST_MIN//60}-{CHATS_JOIN_REST_MAX//60} мин"
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
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode="HTML")
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
    except Exception:
        return


async def _join_target_once(client, *, link: str | None = None, username: str | None = None) -> tuple[str, str]:
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
    targets: list[tuple[str, str]] = [("link", x) for x in links] + [("username", x) for x in usernames]
    total_targets = len(targets)
    account_titles = {acc_num: (name or f"Акк {acc_num}") for acc_num, name in _get_connected_accounts(user_id)}

    overall_joined = 0
    overall_already = 0
    overall_failed = 0
    summary_lines = []

    for idx, acc_num in enumerate(account_numbers, start=1):
        client = app_state.user_authenticated.get(user_id, {}).get(acc_num)
        acc_label = html.escape(account_titles.get(acc_num, f"Акк {acc_num}"))
        if not client:
            summary_lines.append(f"• {acc_label}: не подключен")
            continue

        joined = 0
        already = 0
        failed = 0
        done = 0

        if not client.is_connected():
            try:
                await client.connect()
            except Exception:
                summary_lines.append(f"• {acc_label}: ошибка подключения")
                continue

        for t_i, (kind, value) in enumerate(targets, start=1):
            if kind == "link":
                status, _ = await _join_target_once(client, link=value)
            else:
                status, _ = await _join_target_once(client, username=value)

            done += 1
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
            await _safe_edit_progress(bot, status_chat_id, status_message_id, progress_text)

            if t_i < total_targets:
                await asyncio.sleep(random.uniform(CHATS_JOIN_DELAY_MIN, CHATS_JOIN_DELAY_MAX))

            if t_i % CHATS_JOIN_REST_EVERY == 0 and t_i < total_targets:
                rest_s = random.uniform(CHATS_JOIN_REST_MIN, CHATS_JOIN_REST_MAX)
                rest_text = (
                    "😴 <b>Антифлуд-отдых</b>\n\n"
                    f"Аккаунт: <b>{acc_label}</b>\n"
                    f"Отдых: {int(rest_s)} сек\n"
                    f"Прогресс: {t_i}/{total_targets}"
                )
                await _safe_edit_progress(bot, status_chat_id, status_message_id, rest_text)
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


@router.callback_query(F.data == "joins_close")
async def joins_close_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    await state.clear()
    try:
        await query.message.delete()
    except Exception:
        pass
