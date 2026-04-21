from handlers.broadcast_shared import *  # noqa: F401,F403
from handlers.broadcast_text_flow import return_to_previous_menu

@router.callback_query(F.data == "bc_chats_back")
async def bc_chats_back_callback(query: CallbackQuery):
    await query.answer()
    await show_broadcast_chats_menu(query, query.from_user.id)

@router.callback_query(F.data == "bc_chats")
async def bc_chats_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    user_id = query.from_user.id
    await state.update_data(
        previous_menu="broadcast", menu_message_id=query.message.message_id
    )
    await show_broadcast_chats_menu(
        query, user_id, menu_message_id=query.message.message_id
    )

@router.callback_query(F.data.startswith("bc_chat_list_"))
async def bc_chat_list_callback(query: CallbackQuery):
    await query.answer()
    try:
        bid = int(query.data.split("_")[3])
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    await _render_broadcast_chat_list(query, bid)

@router.callback_query(F.data.startswith("bc_chat_view_"))
async def bc_chat_view_callback(query: CallbackQuery):
    await query.answer()
    try:
        _, _, _, bid_text, order_text = query.data.split("_")
        bid = int(bid_text)
        order = int(order_text)
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    await _render_broadcast_chat_detail(query, bid, order)

@router.callback_query(F.data.startswith("bc_chat_pause_"))
async def bc_chat_pause_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id
    try:
        _, _, _, bid_text, order_text = query.data.split("_")
        bid = int(bid_text)
        order = int(order_text)
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    await _set_broadcast_chat_status(user_id, bid, order, "paused")
    await _render_broadcast_chat_detail(query, bid, order)


@router.callback_query(F.data.startswith("bc_err_pause_"))
async def bc_err_pause_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id
    try:
        _, _, _, bid_text, chat_id_text = query.data.split("_", 4)
        bid = int(bid_text)
        chat_id = int(chat_id_text)
    except Exception:
        await query.answer("Ошибка", show_alert=True)
        return

    changed = await _set_broadcast_chat_status_by_chat_id(
        user_id,
        bid,
        chat_id,
        "paused",
    )
    if not changed:
        await query.answer("Не удалось поставить чат на паузу", show_alert=True)
        return

    try:
        await query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await query.answer("Чат поставлен на паузу", show_alert=True)

@router.callback_query(F.data.startswith("bc_chat_resume_"))
async def bc_chat_resume_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id
    try:
        _, _, _, bid_text, order_text = query.data.split("_")
        bid = int(bid_text)
        order = int(order_text)
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    changed = await _set_broadcast_chat_status(user_id, bid, order, "active")
    if changed:
        broadcast = active_broadcasts.get(bid)
        if broadcast and broadcast.get("status") == "running":
            _start_or_resume_broadcast_task(bid)
        elif broadcast and broadcast.get("status") == "completed":
            chat_items = _broadcast_chat_runtime_items(broadcast)
            if any(
                str(item.get("status") or "active") == "active"
                and _broadcast_chat_has_quota(item)
                for item in chat_items
            ):
                await set_broadcast_status(bid, "running")
                _start_or_resume_broadcast_task(bid)
    await _render_broadcast_chat_detail(query, bid, order)

@router.callback_query(F.data.startswith("bc_chat_disable_"))
async def bc_chat_disable_callback(query: CallbackQuery):
    await query.answer()
    user_id = query.from_user.id
    try:
        _, _, _, bid_text, order_text = query.data.split("_")
        bid = int(bid_text)
        order = int(order_text)
    except Exception:
        await query.answer("\u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    await _set_broadcast_chat_status(user_id, bid, order, "disabled")
    await _render_broadcast_chat_detail(query, bid, order)

@router.callback_query(F.data == "bc_chats_add")
async def bc_chats_add_callback(query: CallbackQuery, state: FSMContext):
    """Handle bc chats add callback."""

    await query.answer()

    await state.update_data(
        previous_menu="broadcast_chats", menu_message_id=query.message.message_id
    )

    await state.set_state(BroadcastConfigState.waiting_for_chat_id)

    text = (
        "\U0001f4ec <b>\u0414\u041e\u0411\u0410\u0412\u041b\u0415\u041d\u0418\u0415 \u0427\u0410\u0422\u0410</b>\n\n"
        "\u041e\u0442\u043f\u0440\u0430\u0432\u044c ID \u0447\u0430\u0442\u0430 \u0438\u043b\u0438 \u0441\u0441\u044b\u043b\u043a\u0443/\u044e\u0437\u0435\u0440\u043d\u0435\u0439\u043c \u043a\u0430\u043d\u0430\u043b\u0430:\n"
        "\u041c\u043e\u0436\u043d\u043e \u0441\u0440\u0430\u0437\u0443 \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u043e: \u043a\u0430\u0436\u0434\u044b\u0439 ID/\u0441\u0441\u044b\u043b\u043a\u0443 \u0441 \u043d\u043e\u0432\u043e\u0439 \u0441\u0442\u0440\u043e\u043a\u0438.\n\n"
        "\u041f\u0440\u0438\u043c\u0435\u0440\u044b:\n"
        "  \u2022 <code>-1001234567890</code>\n"
        "  \u2022 <code>@mychannel</code>\n\n"
        "\u0427\u0430\u0442 \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u043e\u0442\u043a\u0440\u044b\u0442 \u0438\u043b\u0438 \u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d \u0442\u0432\u043e\u0435\u043c\u0443 Telegram-\u0430\u043a\u043a\u0430\u0443\u043d\u0442\u0443."
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=CANCEL_TEXT,
                    callback_data="bc_cancel",
                )
            ]
        ]
    )

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "bc_chats_import")
async def bc_chats_import_callback(query: CallbackQuery):
    await query.answer()

    user_id = query.from_user.id
    accounts = _iter_connected_account_numbers(user_id)
    if not accounts:
        await query.answer(LOGIN_REQUIRED_TEXT, show_alert=True)
        return

    text, kb = _build_folder_account_picker(user_id)
    await _edit_or_notice(query, text, kb, fallback_to_answer=True)

@router.callback_query(F.data.startswith("bc_folder_acc_"))
async def bc_folder_account_callback(query: CallbackQuery):
    await query.answer()

    user_id = query.from_user.id
    try:
        account_number = int(query.data.rsplit("_", 1)[1])
    except Exception:
        await query.answer("Ошибка аккаунта", show_alert=True)
        return

    try:
        await query.message.edit_text(
            f"⏳ <b>Загружаю папки аккаунта {account_number}</b>",
            parse_mode="HTML",
        )
    except Exception:
        pass

    try:
        _, folders = await _load_account_folders(user_id, account_number)
    except Exception as exc:
        await query.message.answer(
            f"❌ Не удалось загрузить папки: {html.escape(str(exc))}",
            parse_mode="HTML",
        )
        return

    text, kb = _build_folder_list_view(account_number, folders)
    await _edit_or_notice(query, text, kb, fallback_to_answer=True)

@router.callback_query(F.data.startswith("bc_folder_pick_"))
async def bc_folder_pick_callback(query: CallbackQuery):
    await query.answer()

    user_id = query.from_user.id
    try:
        account_number, folder_id = _parse_folder_callback(query.data, "pick")
    except Exception:
        await query.answer("Ошибка папки", show_alert=True)
        return

    try:
        await query.message.edit_text(
            f"⏳ <b>Загружаю чаты из папки {folder_id}</b>",
            parse_mode="HTML",
        )
    except Exception:
        pass

    try:
        _, folder, folder_chats = await _load_folder_chats(
            user_id,
            account_number,
            folder_id,
        )
    except Exception as exc:
        await query.message.answer(
            f"❌ Не удалось загрузить чаты папки: {html.escape(str(exc))}",
            parse_mode="HTML",
        )
        return

    text, kb = _build_folder_preview_view(account_number, folder, folder_chats)
    await _edit_or_notice(query, text, kb, fallback_to_answer=True)

@router.callback_query(F.data.startswith("bc_folder_add_"))
async def bc_folder_add_callback(query: CallbackQuery):
    await query.answer()

    try:
        account_number, folder_id = _parse_folder_callback(query.data, "add")
        await _apply_folder_import(
            query,
            query.from_user.id,
            account_number,
            folder_id,
            replace_existing=False,
        )
    except Exception as exc:
        await query.message.answer(
            f"❌ Ошибка импорта из папки: {html.escape(str(exc))}",
            parse_mode="HTML",
        )

@router.callback_query(F.data.startswith("bc_folder_replace_"))
async def bc_folder_replace_callback(query: CallbackQuery):
    await query.answer()

    try:
        account_number, folder_id = _parse_folder_callback(query.data, "replace")
        await _apply_folder_import(
            query,
            query.from_user.id,
            account_number,
            folder_id,
            replace_existing=True,
        )
    except Exception as exc:
        await query.message.answer(
            f"❌ Ошибка замены чатов из папки: {html.escape(str(exc))}",
            parse_mode="HTML",
        )

@router.message(BroadcastConfigState.waiting_for_chat_id)
async def process_add_broadcast_chat_with_profile(message: Message, state: FSMContext):
    """Add one or many chats to the broadcast list."""

    user_id = message.from_user.id
    raw_input = message.text or ""
    chat_inputs = [line.strip() for line in raw_input.splitlines() if line.strip()]

    if not chat_inputs:
        await message.answer(
            "\u274c \u041e\u0442\u043f\u0440\u0430\u0432\u044c ID \u0447\u0430\u0442\u0430, \u0441\u0441\u044b\u043b\u043a\u0443 \u0438\u043b\u0438 \u044e\u0437\u0435\u0440\u043d\u0435\u0439\u043c. \u041c\u043e\u0436\u043d\u043e \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u043e \u0441\u0442\u0440\u043e\u043a \u0441\u0440\u0430\u0437\u0443.",
            parse_mode="HTML",
        )
        return

    if len(chat_inputs) == 1 and chat_inputs[0] == CANCEL_TEXT:
        await return_to_previous_menu(message, state)
        return

    try:
        await message.delete()
    except Exception:
        pass

    loading_text = (
        "\u23f3 \u0417\u0430\u0433\u0440\u0443\u0436\u0430\u044e \u0438\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044e \u043e \u0447\u0430\u0442\u0430\u0445..."
        if len(chat_inputs) > 1
        else "\u23f3 \u0417\u0430\u0433\u0440\u0443\u0436\u0430\u044e \u0438\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044e \u043e \u0447\u0430\u0442\u0435..."
    )
    loading_msg = await message.answer(loading_text)

    async def _delete_loading():
        try:
            await loading_msg.delete()
        except Exception:
            pass

    async def _resolve_chat_input(chat_input: str) -> dict:
        chat_reference = parse_numeric_reference(chat_input)
        if chat_reference is None:
            chat_reference = chat_input

        chat, resolved_account = await _resolve_chat_for_user(user_id, chat_reference)
        _ = resolved_account  # kept for future diagnostics

        try:
            chat_id = int(get_peer_id(chat))
        except Exception:
            chat_id = int(getattr(chat, "id"))

        title = getattr(chat, "title", None) or getattr(chat, "first_name", None)
        if not title and hasattr(chat, "id"):
            title = f"user{chat.id}"

        chat_name = str(title) if title else f"\u0427\u0430\u0442 {chat_id}"
        chat_link = _detect_chat_link(chat_input, chat) or _detect_chat_link(chat_input, None)
        return {
            "input": chat_input,
            "chat_id": chat_id,
            "chat_name": chat_name,
            "chat_link": chat_link,
        }

    def _item_line(item: dict) -> str:
        name = html.escape(str(item.get("chat_name") or item.get("chat_id") or item.get("input")))
        chat_id = item.get("chat_id")
        if chat_id is None:
            return f"\u2022 {name}"
        return f"\u2022 {name} <code>{chat_id}</code>"

    def _build_add_summary(added: list[dict], duplicates: list[dict], failed: list[dict]) -> str:
        lines = [
            "\U0001f4ec <b>\u0418\u0442\u043e\u0433 \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u0438\u044f \u0447\u0430\u0442\u043e\u0432</b>",
            "",
            f"\u2705 \u0414\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u043e: <b>{len(added)}</b>",
            f"\u26a0\ufe0f \u0423\u0436\u0435 \u0431\u044b\u043b\u0438: <b>{len(duplicates)}</b>",
            f"\u274c \u041e\u0448\u0438\u0431\u043e\u043a: <b>{len(failed)}</b>",
        ]

        def append_items(title: str, items: list[dict]) -> None:
            if not items:
                return
            lines.extend(["", title])
            for item in items[:8]:
                lines.append(_item_line(item))
            if len(items) > 8:
                lines.append(f"... \u0435\u0449\u0435 {len(items) - 8}")

        append_items("\u2705 <b>\u0414\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u044b:</b>", added)
        append_items("\u26a0\ufe0f <b>\u0423\u0436\u0435 \u0432 \u0441\u043f\u0438\u0441\u043a\u0435:</b>", duplicates)

        if failed:
            lines.extend(["", "\u274c <b>\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0434\u043e\u0431\u0430\u0432\u0438\u0442\u044c:</b>"])
            for item in failed[:8]:
                source = html.escape(str(item.get("input", "")))
                error = html.escape(str(item.get("error", ""))[:180])
                lines.append(f"\u2022 <code>{source}</code> - {error}")
            if len(failed) > 8:
                lines.append(f"... \u0435\u0449\u0435 {len(failed) - 8}")

        return "\n".join(lines)

    try:
        if not _iter_connected_account_numbers(user_id):
            await message.answer(LOGIN_REQUIRED_TEXT)
            await state.clear()
            await _delete_loading()
            return

        added_chats: list[dict] = []
        duplicate_chats: list[dict] = []
        failed_chats: list[dict] = []

        for chat_input in chat_inputs:
            if chat_input == CANCEL_TEXT:
                continue

            try:
                chat_data = await _resolve_chat_input(chat_input)
                chat_id = chat_data.get("chat_id")
                chat_name = chat_data.get("chat_name") or f"\u0427\u0430\u0442 {chat_id}"
                if chat_id is None:
                    raise RuntimeError("\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u043f\u0440\u0435\u0434\u0435\u043b\u0438\u0442\u044c ID \u0447\u0430\u0442\u0430")

                added = add_broadcast_chat_with_profile(
                    user_id,
                    chat_id,
                    chat_name,
                    chat_link=chat_data.get("chat_link"),
                )

                if added:
                    added_chats.append(chat_data)
                else:
                    duplicate_chats.append(chat_data)

            except Exception as e:
                print(f"Broadcast chat add failed for {chat_input}: {str(e)}")
                failed_chats.append({"input": chat_input, "error": str(e)})

        await _delete_loading()

        if not added_chats and not duplicate_chats:
            if len(failed_chats) == 1:
                error_text = str(failed_chats[0].get("error", "")).lower()
                if "timed out" in error_text or "timeout" in error_text:
                    await message.answer(
                        "\u274c Telegram \u0441\u043b\u0438\u0448\u043a\u043e\u043c \u0434\u043e\u043b\u0433\u043e \u043e\u0442\u0432\u0435\u0447\u0430\u0435\u0442 \u043f\u0440\u0438 \u043f\u043e\u0438\u0441\u043a\u0435 \u0447\u0430\u0442\u0430. "
                        "\u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439 \u0435\u0449\u0435 \u0440\u0430\u0437 \u0438\u043b\u0438 \u0434\u0440\u0443\u0433\u0443\u044e "
                        "\u0441\u0441\u044b\u043b\u043a\u0443/\u0430\u0439\u0434\u0438.",
                        parse_mode="HTML",
                    )
                    return
                await message.answer(
                    "\u274c \u0427\u0430\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d. \u0412\u0432\u0435\u0434\u0438 ID \u0447\u0430\u0442\u0430 "
                    "(<code>-1003880811528</code>), \u0441\u0441\u044b\u043b\u043a\u0443 \u0438\u043b\u0438 \u044e\u0437\u0435\u0440\u043d\u0435\u0439\u043c "
                    "(<code>@mychannel</code>). \u0410\u043a\u043a\u0430\u0443\u043d\u0442, \u0447\u0435\u0440\u0435\u0437 \u043a\u043e\u0442\u043e\u0440\u044b\u0439 "
                    "\u0438\u0434\u0435\u0442 \u043f\u043e\u0438\u0441\u043a, \u0434\u043e\u043b\u0436\u0435\u043d \u0432\u0438\u0434\u0435\u0442\u044c \u044d\u0442\u043e\u0442 \u0447\u0430\u0442.",
                    parse_mode="HTML",
                )
            else:
                await message.answer(
                    _build_add_summary(added_chats, duplicate_chats, failed_chats),
                    parse_mode="HTML",
                )
            return

        notify_msg = await message.answer(
            _build_add_summary(added_chats, duplicate_chats, failed_chats),
            parse_mode="HTML",
        )

        if not failed_chats:
            asyncio.create_task(delete_message_after_delay(notify_msg, 7))

        state_data = await state.get_data()
        await state.clear()
        await show_broadcast_chats_menu(
            message, user_id, menu_message_id=state_data.get("menu_message_id")
        )

    except Exception as e:
        await _delete_loading()
        print(f"Error in process_add_broadcast_chat: {str(e)}")
        await message.answer(f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {html.escape(str(e))}", parse_mode="HTML")

@router.callback_query(F.data.startswith("select_chat_"))
async def select_chat_callback(query: CallbackQuery, state: FSMContext):
    """Handle chat selection from the intermediate list."""

    user_id = query.from_user.id

    try:
        chat_id = int(query.data.split("_")[2])

        available_accounts = _iter_connected_account_numbers(user_id)
        if not available_accounts:
            await query.answer(LOGIN_REQUIRED_TEXT, show_alert=True)

            return

        account_number = available_accounts[0]
        client = await _ensure_account_ready(user_id, account_number)
        if not client:
            await query.answer(
                "❌ Не удалось подключить аккаунт. Проверь сессию и прокси.",
                show_alert=True,
            )
            return

  # cleaned comment

        dialogs = await client.get_dialogs(limit=None)

        for dialog in dialogs:
            if dialog.entity.id == chat_id:
                entity = dialog.entity

                chat_name = (
                    entity.title
                    if hasattr(entity, "title")
                    else (entity.first_name or str(chat_id))
                )
                chat_link = _detect_chat_link(None, entity)

  # cleaned comment

                add_broadcast_chat_with_profile(
                    user_id, chat_id, chat_name, chat_link=chat_link
                )

                state_data = await state.get_data()

                await state.clear()

                await show_broadcast_chats_menu(
                    query,
                    user_id,
                    menu_message_id=state_data.get("menu_message_id")
                    or query.message.message_id,
                )

                return

        await query.answer(
            "\u274c \u0427\u0430\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d", show_alert=True
        )

    except Exception as e:
        await query.answer(
            f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {str(e)}", show_alert=True
        )

@router.callback_query(F.data.startswith("manual_chat_"))
async def manual_chat_callback(query: CallbackQuery, state: FSMContext):
    """Handle manual chat callback."""

    try:
        chat_id = int(query.data.split("_")[2])

        await state.update_data(chat_id=chat_id, previous_menu="broadcast_chats")

        await state.set_state(BroadcastConfigState.waiting_for_chat_name)

        await query.answer()

        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=CANCEL_TEXT)]],
            resize_keyboard=True,
        )

        await query.message.delete()

        await query.message.answer(
            f"\u270f\ufe0f \u0412\u0432\u0435\u0434\u0438 \u0438\u043c\u044f \u0438\u043b\u0438 \u043e\u043f\u0438\u0441\u0430\u043d\u0438\u0435 \u0434\u043b\u044f \u0447\u0430\u0442\u0430 \u0441 ID {chat_id}:",
            reply_markup=keyboard,
        )

    except Exception as e:
        await query.answer(f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {str(e)}", show_alert=True)

@router.message(BroadcastConfigState.waiting_for_chat_name)
async def process_broadcast_chat_name(message: Message, state: FSMContext):
    """Handle process broadcast chat name."""

    user_id = message.from_user.id

  # cleaned comment

    if message.text == CANCEL_TEXT:
        await return_to_previous_menu(message, state)

        return

    try:
        data = await state.get_data()

        chat_id = data.get("chat_id")

        chat_name = message.text.strip()

        if not chat_id:
            await message.answer(
                "\u274c \u041e\u0448\u0438\u0431\u043a\u0430: Chat ID \u043d\u0435 \u0441\u043e\u0445\u0440\u0430\u043d\u0435\u043d. \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439 \u0441\u043d\u043e\u0432\u0430"
            )

            await state.clear()

            await show_broadcast_chats_menu(
                message,
                message.from_user.id,
                menu_message_id=data.get("menu_message_id"),
            )

            return

  # cleaned comment

        added = add_broadcast_chat_with_profile(user_id, chat_id, chat_name)

  # cleaned comment

        if added:
            notify_msg = await message.answer(
                f"\u2705 \u0427\u0430\u0442 '{chat_name}' \u0443\u0441\u043f\u0435\u0448\u043d\u043e \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d!"
            )

        else:
            notify_msg = await message.answer(
                "\u26a0\ufe0f \u0427\u0430\u0442 \u0441 \u044d\u0442\u0438\u043c ID \u0443\u0436\u0435 \u0435\u0441\u0442\u044c \u0432 \u0441\u043f\u0438\u0441\u043a\u0435"
            )

  # cleaned comment

        import asyncio

        asyncio.create_task(delete_message_after_delay(notify_msg, 0.5))

        await state.clear()

        await show_broadcast_chats_menu(
            message, message.from_user.id, menu_message_id=data.get("menu_message_id")
        )

    except Exception as e:
        print(f"Error in process_broadcast_chat_name: {str(e)}")

        await message.answer(f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {str(e)}")

        await state.clear()

@router.callback_query(F.data == "bc_chats_delete")
async def bc_chats_delete_callback(query: CallbackQuery, state: FSMContext):
    """Show broadcast chat removal UI with multi-delete and clear-all."""

    await query.answer()

    user_id = query.from_user.id

    chats = get_broadcast_chats(user_id)

    if not chats:
        text = "\U0001f6ab \u041d\u0435\u0442 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0443\u0434\u0430\u043b\u0435\u043d\u0438\u044f!"

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                        callback_data="close_bc_menu",
                    )
                ]
            ]
        )

        await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

        return

    await state.update_data(
        previous_menu="broadcast_chats", menu_message_id=query.message.message_id
    )

    await state.set_state(BroadcastConfigState.waiting_for_chat_delete)

    text = "\U0001f5d1\ufe0f <b>\u0423\u0414\u0410\u041b\u0415\u041d\u0418\u0415 \u0427\u0410\u0422\u041e\u0412</b>\n\n"

    for idx, (chat_id, chat_name) in enumerate(chats, 1):
        text += f"{idx}. {chat_name}\n"

    text += (
        f"\n\u0412\u0432\u0435\u0434\u0438 \u043d\u043e\u043c\u0435\u0440\u0430 \u0447\u0430\u0442\u043e\u0432 \u0434\u043b\u044f \u0443\u0434\u0430\u043b\u0435\u043d\u0438\u044f (\u043e\u0442 1 \u0434\u043e {len(chats)}).\n"
        "\u041c\u043e\u0436\u043d\u043e \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u043e \u0447\u0438\u0441\u0435\u043b \u0447\u0435\u0440\u0435\u0437 \u043f\u0440\u043e\u0431\u0435\u043b \u0438\u043b\u0438 \u0437\u0430\u043f\u044f\u0442\u0443\u044e, \u043d\u0430\u043f\u0440\u0438\u043c\u0435\u0440: 1 4"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="\U0001f9f9 \u041e\u0447\u0438\u0441\u0442\u0438\u0442\u044c \u0432\u0441\u0435",
                    callback_data="bc_chats_delete_all",
                )
            ],
            [
                InlineKeyboardButton(
                    text=CANCEL_TEXT,
                    callback_data="bc_cancel",
                )
            ],
        ]
    )

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "bc_chats_delete_all")
async def bc_chats_delete_all_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id

    chats = get_broadcast_chats(user_id)
    for chat_id, _ in chats:
        remove_broadcast_chat_with_profile(user_id, chat_id)

    await state.clear()
    await show_broadcast_chats_menu(
        query, user_id, menu_message_id=query.message.message_id
    )

@router.message(F.text == "\U0001f5d1\ufe0f \u0423\u0434\u0430\u043b\u0438\u0442\u044c")
async def delete_broadcast_chat_button(message: Message, state: FSMContext):
    """Handle delete broadcast chat button."""

  # cleaned comment

    pass

@router.message(BroadcastConfigState.waiting_for_chat_delete)
async def process_delete_broadcast_chat(message: Message, state: FSMContext):
    """Delete one or many broadcast chats by numeric indexes."""

    user_id = message.from_user.id

    if message.text in {
        CANCEL_TEXT,
    }:
        await return_to_previous_menu(message, state)
        return

    data = await state.get_data()
    menu_message_id = data.get("menu_message_id")

    try:
        chats = get_broadcast_chats(user_id)

        if not chats:
            await state.clear()
            await show_broadcast_chats_menu(
                message, user_id, menu_message_id=menu_message_id
            )
            return

        raw = (message.text or "").replace(",", " ")
        tokens = [token for token in raw.split() if token]
        if not tokens:
            await message.answer(
                f"\u274c \u0412\u0432\u0435\u0434\u0438 \u043d\u043e\u043c\u0435\u0440\u0430 \u043e\u0442 1 \u0434\u043e {len(chats)}"
            )
            return

        indexes = []
        for token in tokens:
            value = int(token) - 1
            if value < 0 or value >= len(chats):
                await message.answer(
                    f"\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u0430 \u043e\u0442 1 \u0434\u043e {len(chats)}"
                )
                return
            indexes.append(value)

        for idx in sorted(set(indexes), reverse=True):
            chat_id, _ = chats[idx]
            remove_broadcast_chat_with_profile(user_id, chat_id)

        await state.clear()

        try:
            await message.delete()
        except Exception:
            pass

        await show_broadcast_chats_menu(
            message, user_id, menu_message_id=menu_message_id
        )

    except ValueError:
        await message.answer(
            "\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u0430 \u0447\u0435\u0440\u0435\u0437 \u043f\u0440\u043e\u0431\u0435\u043b \u0438\u043b\u0438 \u0437\u0430\u043f\u044f\u0442\u0443\u044e"
        )
