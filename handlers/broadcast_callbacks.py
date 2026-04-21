from handlers.broadcast_shared import *  # noqa: F401,F403
from handlers.broadcast_text_flow import return_to_previous_menu
from services.broadcast_preflight_service import build_broadcast_preflight_text
from services.operation_guard_service import get_active_operation
from ui.main_menu_ui import BROADCAST_BUTTON_TEXT

@router.message(Command("broadcast"))
@router.message(F.text == BROADCAST_BUTTON_TEXT)
@router.message(F.text.in_({"Рассылка", "📤 Рассылка"}))
@router.message(F.text.contains("\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430"))
async def cmd_broadcast_menu(message: Message):
    """Handle cmd broadcast menu."""

    user_id = message.from_user.id

    if not _iter_connected_account_numbers(user_id):
        await message.answer(LOGIN_REQUIRED_TEXT)

        return

    await show_broadcast_menu(message, user_id, is_edit=False)

@router.callback_query(F.data == "close_bc_menu")
async def close_bc_menu_callback(query: CallbackQuery):
    """Return to broadcast chats menu."""

    await query.answer()
    user_id = query.from_user.id
    try:
        await show_broadcast_chats_menu(
            query, user_id, menu_message_id=query.message.message_id
        )
    except Exception:
        pass

@router.callback_query(F.data.in_(["delete_bc_menu", "delete_bs_menu"]))
async def delete_bc_menu_callback(query: CallbackQuery):
    """Close broadcast menu message (legacy callbacks supported)."""
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass

@router.callback_query(F.data == "bc_back")
async def bc_back_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id
    await show_broadcast_menu(query, user_id, is_edit=True)

@router.callback_query(F.data == "bc_cancel")
async def bc_cancel_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id
    fake = FakeMessage(user_id, query)
    await return_to_previous_menu(fake, state)

@router.callback_query(F.data == "bc_active")
async def bc_active_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    user_broadcasts = {
        bid: b
        for bid, b in active_broadcasts.items()
        if b["user_id"] == user_id and b["status"] in ("running", "paused")
    }

    if not user_broadcasts:
        text = "\U0001f4ed \u041d\u0435\u0442 \u0430\u043a\u0442\u0438\u0432\u043d\u044b\u0445 \u0440\u0430\u0441\u0441\u044b\u043b\u043e\u043a"

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                        callback_data="bc_back",
                    )
                ]
            ]
        )

        await _edit_or_notice(query, text, kb)
        return

    groups = {}

    singles = []
    display_numbers = _broadcast_display_numbers(user_id)
    display_numbers = _broadcast_display_numbers(user_id)

    for bid, b in user_broadcasts.items():
        gid = b.get("group_id")

        if gid is None:
            singles.append((bid, b))

        else:
            groups.setdefault(gid, []).append((bid, b))

    total_running = sum(1 for _, b in user_broadcasts.items() if b["status"] == "running")
    total_paused = sum(1 for _, b in user_broadcasts.items() if b["status"] == "paused")
    now_ts = datetime.now(timezone.utc).timestamp()
    info = (
        "\U0001f4e4 <b>\u0410\u041a\u0422\u0418\u0412\u041d\u042b\u0415 \u0420\u0410\u0421\u0421\u042b\u041b\u041a\u0418</b>\n\n"
        f"\u0412\u0441\u0435\u0433\u043e: {len(user_broadcasts)} | "
        f"\u25b6\ufe0f {total_running} | \u23f8\ufe0f {total_paused}\n\n"
    )

    buttons = []

    for gid, items in sorted(groups.items()):
        status = (
            "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
            if any(b["status"] == "running" for _, b in items)
            else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
        )

        sent = sum(int(b.get("sent_chats", 0) or 0) for _, b in items)
        plan = sum(int(b.get("planned_count", 0) or 0) for _, b in items)
        group_finish_ts = _estimate_group_finish_timestamp(items, now_ts=now_ts)
        group_next_send_ts = _estimate_group_next_send_timestamp(items, now_ts=now_ts)
        eta_text = _format_eta_duration(
            None if group_finish_ts is None else group_finish_ts - now_ts
        )
        info += (
            f"{status} <b>\u0413\u0440\u0443\u043f\u043f\u0430 #{gid}</b>\n"
            f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432: {len(items)} | "
            f"\u041f\u0440\u043e\u0433\u0440\u0435\u0441\u0441: {sent}/{plan}\n"
            f"\u23ed\ufe0f \u0421\u043b\u0435\u0434\u0443\u044e\u0449\u0430\u044f: "
            f"{_format_eta_duration(None if group_next_send_ts is None else group_next_send_ts - now_ts)}\n"
            f"\u23f3 \u0414\u043e \u043a\u043e\u043d\u0446\u0430: {eta_text}\n\n"
        )

        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"{status.split()[0]} \u0413\u0440\u0443\u043f\u043f\u0430 #{gid}",
                    callback_data=f"view_group_{gid}",
                )
            ]
        )

    for bid, b in sorted(singles):
        status = (
            "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
            if b["status"] == "running"
            else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
        )
        display_number = display_numbers.get(bid, bid)

        account_name = b.get(
            "account_name",
            f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442 {b.get('account', '?')}",
        )
        config_name = str(b.get("config_name") or "По умолчанию")
        finish_ts = _estimate_broadcast_finish_timestamp(b, now_ts=now_ts)
        next_send_ts = _estimate_next_send_timestamp(b, now_ts=now_ts)
        eta_text = _format_eta_duration(None if finish_ts is None else finish_ts - now_ts)

        info += (
            f"{status} <b>\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 #{display_number}</b>\n"
            f"\U0001f464 {account_name} | \U0001f4ec {b.get('sent_chats', 0)}/{b.get('planned_count', 0)}\n"
            f"\u2699\ufe0f \u041a\u043e\u043d\u0444\u0438\u0433: {html.escape(config_name)}\n"
            f"\u23ed\ufe0f \u0421\u043b\u0435\u0434\u0443\u044e\u0449\u0430\u044f: "
            f"{_format_eta_duration(None if next_send_ts is None else next_send_ts - now_ts)}\n"
            f"\u23f3 \u0414\u043e \u043a\u043e\u043d\u0446\u0430: {eta_text}\n\n"
        )

        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"{status.split()[0]} \u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 #{display_number}",
                    callback_data=f"view_bc_{bid}",
                )
            ]
        )

    buttons.append(
        [
            InlineKeyboardButton(
                text="\U0001f504 \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c",
                callback_data="bc_active",
            )
        ]
    )

    buttons.append(
        [
            InlineKeyboardButton(
                text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                callback_data="bc_back",
            )
        ]
    )

    await _edit_or_notice(
        query,
        info.strip(),
        InlineKeyboardMarkup(inline_keyboard=buttons),
    )

@router.callback_query(F.data.startswith("view_group_"))
async def view_group_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    try:
        gid = int(query.data.split("_")[2])

    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)

        return

    await _render_group_detail(query, user_id, gid)

@router.callback_query(F.data.startswith("bc_group_errors_"))
async def bc_group_errors_callback(query: CallbackQuery):
    await query.answer()
    try:
        gid = int(query.data.split("_")[3])
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    await _render_group_error_log(query, gid)

@router.callback_query(F.data.startswith("bc_group_edit_count_"))
async def bc_group_edit_count_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id
    try:
        gid = int(query.data.split("_")[4])
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    if not _group_runtime_items(user_id, gid):
        await query.answer(
            "\u0413\u0440\u0443\u043f\u043f\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )
        return

    await state.set_state(BroadcastConfigState.waiting_for_count)
    await state.update_data(
        edit_group_id=gid,
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
        previous_menu="group_detail",
    )
    await query.message.edit_text(
        "\U0001f522 <b>\u041a\u041e\u041b-\u0412\u041e \u0414\u041b\u042f \u0413\u0420\u0423\u041f\u041f\u042b</b>\n\n"
        "\u041d\u043e\u0432\u043e\u0435 \u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u043f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u0441\u044f \u043a \u043a\u0430\u0436\u0434\u043e\u0439 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0435 \u0432 \u0433\u0440\u0443\u043f\u043f\u0435.\n\n"
        "\u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u043e \u043e\u0442 1 \u0434\u043e 1000:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=CANCEL_TEXT,
                        callback_data=f"view_group_{gid}",
                    )
                ]
            ]
        ),
        parse_mode="HTML",
    )

@router.callback_query(F.data.startswith("bc_group_edit_interval_"))
async def bc_group_edit_interval_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id
    try:
        gid = int(query.data.split("_")[4])
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    if not _group_runtime_items(user_id, gid):
        await query.answer(
            "\u0413\u0440\u0443\u043f\u043f\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )
        return

    group_items = _group_runtime_items(user_id, gid)
    first_broadcast = group_items[0][1]
    interval_unit = _current_interval_unit(first_broadcast)
    current_value = first_broadcast.get(
        "interval_value",
        first_broadcast.get("interval_minutes", 0),
    )

    await state.set_state(BroadcastConfigState.waiting_for_interval)
    await state.update_data(
        edit_group_id=gid,
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
        previous_menu="group_detail",
        interval_unit=interval_unit,
    )
    await query.message.edit_text(
        _build_interval_input_text(current_value, interval_unit),
        reply_markup=_build_interval_input_keyboard(interval_unit, f"view_group_{gid}"),
        parse_mode="HTML",
    )

@router.callback_query(F.data.startswith("bc_group_edit_pause_"))
async def bc_group_edit_pause_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id
    try:
        gid = int(query.data.split("_")[4])
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    if not _group_runtime_items(user_id, gid):
        await query.answer(
            "\u0413\u0440\u0443\u043f\u043f\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )
        return

    await state.set_state(BroadcastConfigState.waiting_for_chat_pause)
    await state.update_data(
        edit_group_id=gid,
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
        previous_menu="group_detail",
    )
    await query.message.edit_text(
        "\u26a1 <b>\u0422\u0415\u041c\u041f \u0414\u041b\u042f \u0413\u0420\u0423\u041f\u041f\u042b</b>\n\n"
        "\u041d\u043e\u0432\u044b\u0439 \u0442\u0435\u043c\u043f \u043f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u0441\u044f \u043a\u043e \u0432\u0441\u0435\u043c \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0430\u043c \u0432 \u0433\u0440\u0443\u043f\u043f\u0435.\n\n"
        "\u0412\u0432\u0435\u0434\u0438 \u043e\u0434\u043d\u043e \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d:\n"
        "\u2022 <code>2</code>\n"
        "\u2022 <code>1-3</code>\n"
        f"\u041c\u0430\u043a\u0441\u0438\u043c\u0443\u043c: <code>{CHAT_PAUSE_MAX_SECONDS}</code> \u0441\u0435\u043a",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=CANCEL_TEXT,
                        callback_data=f"view_group_{gid}",
                    )
                ]
            ]
        ),
        parse_mode="HTML",
    )

@router.callback_query(F.data.startswith("bc_group_pause_"))
async def bc_group_pause_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    gid = int(query.data.split("_")[3])

    for bid, b in list(active_broadcasts.items()):
        if b.get("group_id") == gid and b.get("user_id") == user_id:
            await set_broadcast_status(bid, "paused")

    await _render_group_detail(query, user_id, gid)

@router.callback_query(F.data.startswith("bc_group_resume_"))
async def bc_group_resume_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    gid = int(query.data.split("_")[3])

    for bid, b in list(active_broadcasts.items()):
        if b.get("group_id") == gid and b.get("user_id") == user_id:
            await set_broadcast_status(bid, "running")
            _start_or_resume_broadcast_task(bid)

    await _render_group_detail(query, user_id, gid)

@router.callback_query(F.data.startswith("bc_group_cancel_"))
async def bc_group_cancel_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    gid = int(query.data.split("_")[3])

    for bid, b in list(active_broadcasts.items()):
        if b.get("group_id") == gid and b.get("user_id") == user_id:
            await set_broadcast_status(bid, "cancelled")

    await bc_active_callback(query)

@router.callback_query(F.data.startswith("view_bc_"))
async def view_bc_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    try:
        bid = int(query.data.split("_")[2])

    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)

        return

    if bid not in active_broadcasts or active_broadcasts[bid]["user_id"] != user_id:
        await query.answer(
            "\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430",
            show_alert=True,
        )

        return

    b = active_broadcasts[bid]
    chat_items = _broadcast_chat_runtime_items(b)
    display_number = _broadcast_display_numbers(user_id).get(bid, bid)
    active_chats, paused_chats, disabled_chats = _active_chat_counts(b)

    status = (
        "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
        if b["status"] == "running"
        else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
        if b["status"] == "paused"
        else "\u26d4 \u041e\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d\u0430"
        if b["status"] == "cancelled"
        else "\u274c \u041e\u0448\u0438\u0431\u043a\u0430"
        if b["status"] == "error"
        else "\u2705 \u0417\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0430"
    )

    account_name = b.get(
        "account_name",
        f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442 {b.get('account', '?')}",
    )

    config_name = str(b.get("config_name") or "По умолчанию")
    info = (
        f"\U0001f4e4 <b>\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 #{display_number}</b>\n\n"
    )

    info += f"{status}\n"

    info += f"\U0001f464 \u0410\u043a\u043a\u0430\u0443\u043d\u0442: {account_name}\n"
    info += f"\u2699\ufe0f \u041a\u043e\u043d\u0444\u0438\u0433: {html.escape(config_name)}\n"

    info += f"\U0001f4ad \u0427\u0430\u0442\u043e\u0432: {b.get('total_chats', 0)}\n"
    info += (
        f"\U0001f7e2 \u0410\u043a\u0442\u0438\u0432\u043d\u044b: {active_chats} | "
        f"\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430: {paused_chats} | "
        f"\u26aa \u041e\u0442\u043a\u043b: {disabled_chats}\n"
    )

    info += (
        f"\U0001f4ec \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e: "
        f"{b.get('sent_chats', 0)}/{b.get('planned_count', 0)}\n"
    )
    info += f"\u26a0\ufe0f \u041e\u0448\u0438\u0431\u043e\u043a: {b.get('failed_count', 0)}\n"

    info += f"\U0001f522 \u041a\u043e\u043b-\u0432\u043e: {b.get('count', 0)}\n"

    info += (
        f"\u23f1\ufe0f \u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b: "
        f"{b.get('interval_value', b.get('interval_minutes', '?'))} "
        f"{_interval_unit_display(b.get('interval_unit'))}\n"
    )
    now_ts = datetime.now(timezone.utc).timestamp()
    next_send_ts = _estimate_next_send_timestamp(b, now_ts=now_ts)
    finish_ts = _estimate_broadcast_finish_timestamp(b, now_ts=now_ts)
    if next_send_ts is not None:
        info += (
            f"\u23ed\ufe0f \u0421\u043b\u0435\u0434. \u0448\u0430\u0433 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438: "
            f"{_format_eta_duration(next_send_ts - now_ts)}\n"
        )
    if finish_ts is not None:
        info += (
            f"\u23f3 \u0414\u043e \u043a\u043e\u043d\u0446\u0430: "
            f"{_format_eta_duration(finish_ts - now_ts)}\n"
        )

    error_items = [item for item in chat_items if item.get("last_error")]

    buttons = [
        [
            InlineKeyboardButton(
                text="\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430",
                callback_data=f"pause_bc_{bid}",
            ),
            InlineKeyboardButton(
                text="\u25b6\ufe0f \u041f\u0440\u043e\u0434\u043e\u043b\u0436\u0438\u0442\u044c",
                callback_data=f"resume_bc_{bid}",
            ),
            InlineKeyboardButton(
                text="\u26d4 \u0421\u0442\u043e\u043f",
                callback_data=f"cancel_bc_{bid}",
            ),
        ],
        [
            InlineKeyboardButton(
                text="\u270f\ufe0f \u041a\u043e\u043b-\u0432\u043e",
                callback_data=f"bc_edit_count_{bid}",
            ),
            InlineKeyboardButton(
                text="\u23f1\ufe0f \u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b",
                callback_data=f"bc_edit_interval_{bid}",
            ),
        ],
    ]

    action_row = [
        InlineKeyboardButton(
            text="\U0001f4dd \u0427\u0430\u0442\u044b",
            callback_data=f"bc_chat_list_{bid}",
        )
    ]
    if error_items:
        action_row.append(
            InlineKeyboardButton(
                text=f"\u26a0\ufe0f \u041e\u0448\u0438\u0431\u043a\u0438 ({len(error_items)})",
                callback_data=f"bc_errors_{bid}",
            )
        )
    buttons.append(action_row)

    buttons.append(
        [
            InlineKeyboardButton(
                text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                callback_data="bc_active",
            ),
            InlineKeyboardButton(
                text="\U0001f504 \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c",
                callback_data=f"view_bc_{bid}",
            ),
        ]
    )

    await _edit_or_notice(
        query,
        info,
        InlineKeyboardMarkup(inline_keyboard=buttons),
    )

@router.callback_query(F.data.startswith("pause_bc_"))
async def pause_bc_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    bid = int(query.data.split("_")[2])

    if bid in active_broadcasts and active_broadcasts[bid]["user_id"] == user_id:
        await set_broadcast_status(bid, "paused")

    await view_bc_callback(query)

@router.callback_query(F.data.startswith("resume_bc_"))
async def resume_bc_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    bid = int(query.data.split("_")[2])

    if bid in active_broadcasts and active_broadcasts[bid]["user_id"] == user_id:
        await set_broadcast_status(bid, "running")
        _start_or_resume_broadcast_task(bid)

    await view_bc_callback(query)

@router.callback_query(F.data.startswith("cancel_bc_"))
async def cancel_bc_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    bid = int(query.data.split("_")[2])

    if bid in active_broadcasts and active_broadcasts[bid]["user_id"] == user_id:
        await set_broadcast_status(bid, "cancelled")

    await bc_active_callback(query)

@router.callback_query(F.data.startswith("bc_errors_"))
async def bc_errors_callback(query: CallbackQuery):
    await query.answer()
    try:
        bid = int(query.data.split("_")[2])
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    await _render_broadcast_error_log(query, bid)

@router.callback_query(F.data == "back_to_broadcast_menu")
async def back_to_broadcast_menu_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id

    await show_broadcast_menu(query, user_id, is_edit=True)

@router.callback_query(F.data.startswith("bc_edit_count_"))
async def bc_edit_count_callback(query: CallbackQuery, state: FSMContext):
    """Handle bc edit count callback."""

    await query.answer()

    user_id = query.from_user.id

    try:
        bid = int(query.data.split("_")[3])

    except Exception:
        await query.answer("Ошибка", show_alert=True)

        return

    if bid not in active_broadcasts or active_broadcasts[bid]["user_id"] != user_id:
        await query.answer(
            "Рассылка не найдена",
            show_alert=True,
        )

        return

    await state.set_state(BroadcastConfigState.waiting_for_count)

    await state.update_data(
        edit_broadcast_id=bid,
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
    )

    info = "Введи новое общее количество сообщений (1-1000) или нажми Отменить:"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Отменить",
                    callback_data=f"view_bc_{bid}",
                )
            ]
        ]
    )

    await query.message.edit_text(info, reply_markup=kb)

@router.callback_query(F.data.startswith("bc_edit_interval_"))
async def bc_edit_interval_callback(query: CallbackQuery, state: FSMContext):
    """Handle bc edit interval callback."""

    await query.answer()

    user_id = query.from_user.id

    try:
        bid = int(query.data.split("_")[3])
    except Exception:
        await query.answer("??????", show_alert=True)
        return

    if bid not in active_broadcasts or active_broadcasts[bid]["user_id"] != user_id:
        await query.answer("???????? ?? ???????", show_alert=True)
        return

    interval_unit = _current_interval_unit(active_broadcasts.get(bid))
    current_value = active_broadcasts[bid].get(
        "interval_value",
        active_broadcasts[bid].get("interval_minutes", 0),
    )

    await state.set_state(BroadcastConfigState.waiting_for_interval)
    await state.update_data(
        edit_broadcast_id=bid,
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
        interval_unit=interval_unit,
    )

    await query.message.edit_text(
        _build_interval_input_text(current_value, interval_unit),
        reply_markup=_build_interval_input_keyboard(interval_unit, f"view_bc_{bid}"),
        parse_mode="HTML",
    )

@router.callback_query(F.data == "bc_launch")
async def bc_launch_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id

    accounts = get_user_accounts(user_id)
    available_accounts = [
        (acc_num, username, first_name)
        for acc_num, _, username, first_name, is_active in accounts
        if is_active
    ] or [
        (acc_num, username, first_name)
        for acc_num, _, username, first_name, _ in accounts
    ]

    if not available_accounts:
        await _send_broadcast_notice(
            query,
            "\u274c \u0422\u044b \u043d\u0435 \u0437\u0430\u043b\u043e\u0433\u0438\u0440\u043e\u0432\u0430\u043d!",
        )
        return

    config = get_broadcast_config(user_id)
    chats = get_broadcast_chats(user_id)

    if not _broadcast_content_ready(config):
        await _send_broadcast_notice(query, _build_missing_content_notice(config))
        return

    if not chats:
        await _send_broadcast_notice(
            query,
            "\u274c \u041d\u0435\u0442 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438!\n\n\u0414\u043e\u0431\u0430\u0432\u044c \u0447\u0430\u0442\u044b \u0447\u0435\u0440\u0435\u0437 '\U0001f4ac \u0427\u0430\u0442\u044b \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438'",
        )
        return

    buttons = []
    for acc_num, username, first_name in available_accounts:
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"\U0001f464 {_account_label(acc_num, username, first_name)}",
                    callback_data=f"start_bc_{acc_num}",
                )
            ]
        )

    if len(buttons) > 1:
        buttons.insert(
            0,
            [
                InlineKeyboardButton(
                    text="\U0001f7e2 \u0412\u0441\u0435 \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u044b",
                    callback_data="start_bc_all",
                )
            ],
        )

    buttons.append(
        [
            InlineKeyboardButton(
                text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                callback_data="bc_back",
            )
        ]
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await query.message.answer(
        build_broadcast_preflight_text(
            user_id,
            config=config,
            chats=chats,
            available_accounts=available_accounts,
            active_broadcasts=active_broadcasts,
        ),
        reply_markup=keyboard,
        parse_mode="HTML",
    )

@router.callback_query(F.data.startswith("start_bc_"))
async def start_bc_callback(query: CallbackQuery):

    await query.answer()

    user_id = query.from_user.id
    active_operation = get_active_operation(user_id)
    if active_operation:
        await _send_broadcast_notice(
            query,
            f"\u23f3 \u0421\u0435\u0439\u0447\u0430\u0441 \u0438\u0434\u0451\u0442 \u0434\u0440\u0443\u0433\u0430\u044f \u043e\u043f\u0435\u0440\u0430\u0446\u0438\u044f: {active_operation}.",
        )
        return

    config = get_broadcast_config(user_id)

    chats = get_broadcast_chats(user_id)

    if not _broadcast_content_ready(config):
        await _send_broadcast_notice(query, _build_missing_content_notice(config))
        return

    if not chats:
        await _send_broadcast_notice(
            query,
            "\u274c \u041d\u0435\u0442 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438",
        )

        return

    try:
        await query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    if query.data == "start_bc_all":
        connected_accounts = []
        for acc_num in _iter_connected_account_numbers(user_id):
            client = await _ensure_account_ready(user_id, acc_num)
            if client:
                connected_accounts.append(acc_num)

        if not connected_accounts:
            await _send_broadcast_notice(
                query,
                "\u274c \u041d\u0435\u0442 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043d\u044b\u0445 \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432",
            )

            return

        group_id = next_broadcast_id()

        for acc_num in connected_accounts:
            await execute_broadcast(
                query, user_id, acc_num, config, chats, group_id=group_id
            )

        try:
            await query.message.delete()
        except Exception:
            pass

        await _send_broadcast_notice(
            query,
            f"\u2705 \u0417\u0430\u043f\u0443\u0449\u0435\u043d\u043e \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432: {len(connected_accounts)}",
        )

        return

    try:
        account_number = int(query.data.split("_")[2])

    except Exception:
        await _send_broadcast_notice(query, "\u041e\u0448\u0438\u0431\u043a\u0430")

        return

    await execute_broadcast(query, user_id, account_number, config, chats)
    try:
        await query.message.delete()
    except Exception:
        pass

@router.message(
    F.text.in_(
        [
            "\U0001f680 \u0417\u0430\u043f\u0443\u0441\u0442\u0438\u0442\u044c",
            "\U0001f680 \u0417\u0430\u043f\u0443\u0441\u0442\u0438\u0442\u044c \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0443",
        ]
    )
)
async def start_broadcast_button(message: Message):
    """Handle start broadcast button."""

    user_id = message.from_user.id

  # cleaned comment

    accounts = get_user_accounts(user_id)
    available_accounts = [
        (acc_num, username, first_name)
        for acc_num, _, username, first_name, is_active in accounts
        if is_active
    ] or [
        (acc_num, username, first_name)
        for acc_num, _, username, first_name, _ in accounts
    ]

    if not available_accounts:
        await message.answer(LOGIN_REQUIRED_TEXT)

        return

  # cleaned comment

    config = get_broadcast_config(user_id)

    chats = get_broadcast_chats(user_id)

  # cleaned comment

    if not _broadcast_content_ready(config):
        await message.answer(_build_missing_content_notice(config))
        return

    if not chats:
        await message.answer(
            "\u274c \u041d\u0435\u0442 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438!\n\n"
            "\u0414\u043e\u0431\u0430\u0432\u044c \u0447\u0430\u0442\u044b \u0447\u0435\u0440\u0435\u0437 '\U0001f4ac \u0427\u0430\u0442\u044b'."
        )

        return

  # cleaned comment

    if len(available_accounts) == 1:
        account_number = available_accounts[0][0]

    else:
  # cleaned comment

        buttons = []

        for acc_num, username, first_name in available_accounts:
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=f"\U0001f464 {_account_label(acc_num, username, first_name)}",
                        callback_data=f"start_bc_{acc_num}",
                    )
                ]
            )

        if len(buttons) > 1:
            buttons.insert(
                0,
                [
                    InlineKeyboardButton(
                        text="\U0001f7e2 \u0412\u0441\u0435 \u0430\u043a\u043a\u0430\u0443\u043d\u0442\u044b",
                        callback_data="start_bc_all",
                    )
                ],
            )

        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

        await message.answer(
            "\u0412\u044b\u0431\u0435\u0440\u0438 \u0430\u043a\u043a\u0430\u0443\u043d\u0442 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438:",
            reply_markup=keyboard,
        )

        return

  # cleaned comment

    await execute_broadcast(message, user_id, account_number, config, chats)

@router.message(F.text == "\U0001f4e4 \u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0435")
async def active_broadcasts_button(message: Message):

    user_id = message.from_user.id

    user_broadcasts = {
        bid: b
        for bid, b in active_broadcasts.items()
        if b["user_id"] == user_id and b["status"] in ("running", "paused")
    }

    if not user_broadcasts:
        await message.answer(
            "\u274c \u041d\u0435\u0442 \u0430\u043a\u0442\u0438\u0432\u043d\u044b\u0445 \u0440\u0430\u0441\u0441\u044b\u043b\u043e\u043a"
        )

        return

    groups = {}

    singles = []

    for bid, b in user_broadcasts.items():
        gid = b.get("group_id")

        if gid is None:
            singles.append((bid, b))

        else:
            groups.setdefault(gid, []).append((bid, b))

    info = "\U0001f4e4 <b>\u0410\u041a\u0422\u0418\u0412\u041d\u042b\u0415 \u0420\u0410\u0421\u0421\u042b\u041b\u041a\u0418</b>\n\n"
    display_numbers = _broadcast_display_numbers(user_id)

    for gid, items in sorted(groups.items()):
        status = (
            "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
            if any(b["status"] == "running" for _, b in items)
            else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
        )

        info += f"\u0413\u0440\u0443\u043f\u043f\u0430 #{gid} {status} | \u0410\u043a\u043a\u0430\u0443\u043d\u0442\u043e\u0432: {len(items)}\n"

    for bid, b in sorted(singles):
        status = (
            "\u25b6\ufe0f \u0410\u043a\u0442\u0438\u0432\u043d\u0430"
            if b["status"] == "running"
            else "\u23f8\ufe0f \u041f\u0430\u0443\u0437\u0430"
        )
        display_number = display_numbers.get(bid, bid)

        account_name = b.get(
            "account_name",
            f"\u0410\u043a\u043a\u0430\u0443\u043d\u0442 {b.get('account', '?')}",
        )

        info += f"\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 #{display_number} {status} | {account_name}\n"

    await message.answer(info, parse_mode="HTML")

    inline_buttons = []

    for gid, items in sorted(groups.items()):
        inline_buttons.append(
            [
                InlineKeyboardButton(
                    text=f"\u0413\u0440\u0443\u043f\u043f\u0430 #{gid}",
                    callback_data=f"view_group_{gid}",
                )
            ]
        )

    for bid, b in sorted(singles):
        display_number = display_numbers.get(bid, bid)
        inline_buttons.append(
            [
                InlineKeyboardButton(
                    text=f"\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 #{display_number}",
                    callback_data=f"view_bc_{bid}",
                )
            ]
        )

    inline_buttons.append(
        [
            InlineKeyboardButton(
                text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434 \u0432 \u043c\u0435\u043d\u044e",
                callback_data="back_to_broadcast_menu",
            )
        ]
    )

    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=inline_buttons)

    await message.answer(
        "\u0412\u044b\u0431\u0435\u0440\u0438 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0443 \u0434\u043b\u044f \u0443\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u044f:",
        reply_markup=inline_keyboard,
    )
