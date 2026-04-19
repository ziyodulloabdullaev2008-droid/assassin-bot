from handlers.broadcast_shared import *  # noqa: F401,F403

@router.callback_query(F.data == "bc_text")
async def bc_text_callback(query: CallbackQuery, state: FSMContext):
    """Open content source settings for broadcast."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    kb = build_text_settings_keyboard(
        config.get("text_source_type", "manual"),
        config.get("text_mode", "random"),
        config.get("parse_mode", "HTML"),
    )

    await state.update_data(
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
    )

    await query.message.edit_text(
        _build_text_settings_info(config),
        reply_markup=kb,
        parse_mode="HTML",
    )

@router.callback_query(F.data == "text_source_toggle")
async def text_source_toggle_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    config["text_source_type"] = "channel" if not _is_channel_source(config) else "manual"
    config["text_index"] = 0
    save_broadcast_config_with_profile(user_id, config)

    kb = build_text_settings_keyboard(
        config.get("text_source_type", "manual"),
        config.get("text_mode", "random"),
        config.get("parse_mode", "HTML"),
    )
    await query.message.edit_text(
        _build_text_settings_info(config),
        reply_markup=kb,
        parse_mode="HTML",
    )

@router.callback_query(F.data == "text_channel_source")
async def text_channel_source_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    config = get_broadcast_config(query.from_user.id)

    await state.set_state(BroadcastConfigState.waiting_for_source_channel)
    await state.update_data(
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
    )

    current_ref = html.escape(config.get("source_channel_ref") or "\u043d\u0435 \u0432\u044b\u0431\u0440\u0430\u043d")
    text = (
        "\U0001f4e1 <b>\u041a\u0430\u043d\u0430\u043b-\u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a</b>\n\n"
        f"\u0422\u0435\u043a\u0443\u0449\u0438\u0439 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a: <code>{current_ref}</code>\n\n"
        "\u041e\u0442\u043f\u0440\u0430\u0432\u044c \u0441\u0441\u044b\u043b\u043a\u0443 \u043d\u0430 \u043a\u0430\u043d\u0430\u043b, @username \u0438\u043b\u0438 ID \u043a\u0430\u043d\u0430\u043b\u0430.\n"
        "\u0411\u043e\u0442 \u0437\u0430\u0433\u0440\u0443\u0437\u0438\u0442 \u043f\u043e\u0441\u0442\u044b \u0438 \u0431\u0443\u0434\u0435\u0442 \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u044c \u0438\u0445 \u043a\u0430\u043a \u0432\u0430\u0440\u0438\u0430\u043d\u0442\u044b \u0442\u0435\u043a\u0441\u0442\u0430."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="\u041d\u0430\u0437\u0430\u0434", callback_data="bc_text")]]
    )
    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "text_channel_refresh")
async def text_channel_refresh_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    source_ref = config.get("source_channel_ref")
    if not source_ref:
        await query.answer("\u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u0443\u043a\u0430\u0436\u0438 \u043a\u0430\u043d\u0430\u043b-\u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a", show_alert=True)
        return

    try:
        source_data = await _load_channel_source_for_user(user_id, source_ref)
    except Exception as exc:
        await query.answer(str(exc), show_alert=True)
        return

    config.update(source_data)
    save_broadcast_config_with_profile(user_id, config)

    kb = build_texts_keyboard(
        config.get("source_posts") or [],
        back_callback="bc_text",
        item_prefix="Post",
        allow_add=False,
        extra_buttons=[
            [
                InlineKeyboardButton(
                    text="\u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043f\u043e\u0441\u0442\u044b",
                    callback_data="text_channel_refresh",
                )
            ]
        ],
    )
    await query.message.edit_text(
        _build_text_list_info(config),
        reply_markup=kb,
        parse_mode="HTML",
    )

@router.callback_query(F.data == "text_list")
async def text_list_callback(query: CallbackQuery, state: FSMContext):
    """Show either manual texts or loaded channel posts."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    is_channel = _is_channel_source(config)
    items = config.get("source_posts") if is_channel else config.get("texts", [])
    extra_buttons = (
        [
            [
                InlineKeyboardButton(
                    text="\u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043f\u043e\u0441\u0442\u044b",
                    callback_data="text_channel_refresh",
                )
            ]
        ]
        if is_channel
        else None
    )
    kb = build_texts_keyboard(
        items or [],
        back_callback="bc_text",
        item_prefix="Пост" if is_channel else "Text",
        allow_add=not is_channel,
        extra_buttons=extra_buttons,
    )

    await state.update_data(
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
    )

    await query.message.edit_text(
        _build_text_list_info(config),
        reply_markup=kb,
        parse_mode="HTML",
    )

@router.callback_query(F.data.startswith("text_view_"))
async def text_view_callback(query: CallbackQuery, state: FSMContext):
    """Open a single manual text or a channel post preview."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    is_channel = _is_channel_source(config)

    try:
        text_index = int(query.data.split("_")[2])
        items = config.get("source_posts") if is_channel else config.get("texts", [])
        if text_index >= len(items):
            await query.answer("\u042d\u043b\u0435\u043c\u0435\u043d\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d", show_alert=True)
            return

        await state.update_data(
            edit_message_id=query.message.message_id,
            chat_id=query.message.chat.id,
        )

        if is_channel:
            post_item = items[text_index]
            info = (
                f"\U0001f4e8 <b>\u041f\u043e\u0441\u0442 #{text_index + 1}</b>\n\n"
                f"\u041a\u0430\u043d\u0430\u043b: {html.escape(source_channel_title(config))}\n"
                f"Message ID: <code>{int(post_item['message_id'])}</code>\n"
                f"\u041f\u0440\u0435\u0432\u044c\u044e: <code>{html.escape(post_preview_text(post_item.get('preview', '')))}</code>\n"
            )
            post_link = format_source_channel_link(config, int(post_item["message_id"]))
            buttons = []
            if post_link:
                buttons.append([
                    InlineKeyboardButton(
                        text="\u041e\u0442\u043a\u0440\u044b\u0442\u044c \u043f\u043e\u0441\u0442",
                        url=post_link,
                    )
                ])
            nav_row = []
            if text_index > 0:
                nav_row.append(
                    InlineKeyboardButton(
                        text="\u2b05\ufe0f \u041f\u0440\u0435\u0434",
                        callback_data=f"text_view_{text_index - 1}",
                    )
                )
            if text_index + 1 < len(items):
                nav_row.append(
                    InlineKeyboardButton(
                        text="\u0414\u0430\u043b\u0435\u0435 \u27a1\ufe0f",
                        callback_data=f"text_view_{text_index + 1}",
                    )
                )
            if nav_row:
                buttons.append(nav_row)
            buttons.append(
                [
                    InlineKeyboardButton(
                        text="\U0001f9ea \u0422\u0435\u0441\u0442 \u0441\u0435\u0431\u0435",
                        callback_data=f"text_channel_test_{text_index}",
                    )
                ]
            )
            buttons.append([InlineKeyboardButton(text="\u041d\u0430\u0437\u0430\u0434", callback_data="text_list")])
            kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        else:
            current_text = str(items[text_index])
            parse_mode = config.get("parse_mode", "HTML")
            preview_text = current_text
            suffix = ""
            if len(preview_text) > 3500:
                preview_text = preview_text[:3500]
                suffix = f"\n<i>... \u043e\u0431\u0440\u0435\u0437\u0430\u043d\u043e, \u0432\u0441\u0435\u0433\u043e {len(current_text)} \u0441\u0438\u043c\u0432\u043e\u043b\u043e\u0432</i>"

            info = (
                f"\U0001f4dd <b>\u0422\u0435\u043a\u0441\u0442 #{text_index + 1}</b>\n\n"
                f"\u0424\u043e\u0440\u043c\u0430\u0442: <b>{html.escape(parse_mode)}</b>\n"
                f"<code>{html.escape(preview_text)}</code>{suffix}"
            )
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="\u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u0442\u044c",
                            callback_data=f"text_edit_{text_index}",
                        ),
                        InlineKeyboardButton(
                            text="\u0423\u0434\u0430\u043b\u0438\u0442\u044c",
                            callback_data=f"text_delete_{text_index}",
                        ),
                    ],
                    [InlineKeyboardButton(text="\u041d\u0430\u0437\u0430\u0434", callback_data="text_list")],
                ]
            )

        await query.message.edit_text(info, reply_markup=kb, parse_mode="HTML")
    except (ValueError, IndexError):
        await query.answer("\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0442\u043a\u0440\u044b\u0442\u044c \u044d\u043b\u0435\u043c\u0435\u043d\u0442", show_alert=True)

@router.callback_query(F.data.startswith("text_channel_test_"))
async def text_channel_test_callback(query: CallbackQuery):
    await query.answer("Отправляю тест...")

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    if not _is_channel_source(config):
        await query.answer("Этот тест работает только для канального режима", show_alert=True)
        return

    try:
        text_index = int(query.data.split("_")[3])
        client, source_message, account_number = await _load_channel_preview_message(
            user_id,
            config,
            text_index,
        )
        await client.send_message("me", source_message)
        await query.answer(
            f"Тестовый пост отправлен в Избранное через аккаунт {account_number}",
            show_alert=True,
        )
    except Exception as exc:
        await query.answer(
            f"Не удалось отправить тестовый пост: {str(exc)}",
            show_alert=True,
        )

@router.callback_query(F.data == "text_add_new")
async def text_add_new_callback(query: CallbackQuery, state: FSMContext):
    """Ask user for a new manual broadcast text."""

    await query.answer()

    config = get_broadcast_config(query.from_user.id)
    if _is_channel_source(config):
        await query.answer(
            "\u0412 \u0440\u0435\u0436\u0438\u043c\u0435 \u043a\u0430\u043d\u0430\u043b\u0430 \u0442\u0435\u043a\u0441\u0442\u044b \u0432\u0440\u0443\u0447\u043d\u0443\u044e \u043d\u0435 \u0440\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u0443\u044e\u0442\u0441\u044f",
            show_alert=True,
        )
        return

    await state.set_state(BroadcastConfigState.waiting_for_text_add)
    await state.update_data(
        edit_message_id=query.message.message_id,
        chat_id=query.message.chat.id,
    )

    text = (
        "\u270d\ufe0f <b>\u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u043d\u043e\u0432\u044b\u0439 \u0442\u0435\u043a\u0441\u0442</b>\n\n"
        "\u041e\u0442\u043f\u0440\u0430\u0432\u044c \u043e\u0434\u043d\u0438\u043c \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435\u043c \u0442\u0435\u043a\u0441\u0442 \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438.\n\n"
        f"\u0422\u0435\u043a\u0443\u0449\u0438\u0439 \u0444\u043e\u0440\u043c\u0430\u0442: <b>{html.escape(config.get('parse_mode', 'HTML'))}</b>."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="\u041d\u0430\u0437\u0430\u0434", callback_data="text_list")]]
    )
    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data.startswith("text_edit_"))
async def text_edit_callback(query: CallbackQuery, state: FSMContext):
    """Ask user for a new body for an existing manual text."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    if _is_channel_source(config):
        await query.answer(
            "\u041f\u043e\u0441\u0442\u044b \u043a\u0430\u043d\u0430\u043b\u0430 \u0440\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u0443\u044e\u0442\u0441\u044f \u0442\u043e\u043b\u044c\u043a\u043e \u0432 \u0441\u0430\u043c\u043e\u043c \u043a\u0430\u043d\u0430\u043b\u0435",
            show_alert=True,
        )
        return

    try:
        text_index = int(query.data.split("_")[2])
        texts = config.get("texts") or []
        if text_index >= len(texts):
            await query.answer("\u0422\u0435\u043a\u0441\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d", show_alert=True)
            return

        current_text = str(texts[text_index])
        parse_mode = config.get("parse_mode", "HTML")

        await state.set_state(BroadcastConfigState.waiting_for_text_edit)
        await state.update_data(
            edit_message_id=query.message.message_id,
            chat_id=query.message.chat.id,
            text_index=text_index,
        )

        preview_text = current_text
        suffix = ""
        if len(preview_text) > 3500:
            preview_text = preview_text[:3500]
            suffix = f"\n<i>... \u043e\u0431\u0440\u0435\u0437\u0430\u043d\u043e, \u0432\u0441\u0435\u0433\u043e {len(current_text)} \u0441\u0438\u043c\u0432\u043e\u043b\u043e\u0432</i>"

        text = (
            f"\u270f\ufe0f <b>\u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435 \u0442\u0435\u043a\u0441\u0442\u0430 #{text_index + 1}</b>\n\n"
            f"\u0424\u043e\u0440\u043c\u0430\u0442: <b>{html.escape(parse_mode)}</b>\n\n"
            f"<code>{html.escape(preview_text)}</code>{suffix}\n\n"
            "\u041e\u0442\u043f\u0440\u0430\u0432\u044c \u043d\u043e\u0432\u044b\u0439 \u0432\u0430\u0440\u0438\u0430\u043d\u0442 \u0442\u0435\u043a\u0441\u0442\u0430 \u043e\u0434\u043d\u0438\u043c \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435\u043c."
        )

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="\u041d\u0430\u0437\u0430\u0434",
                        callback_data=f"text_view_{text_index}",
                    )
                ]
            ]
        )
        await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except (ValueError, IndexError):
        await query.answer("\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0442\u043a\u0440\u044b\u0442\u044c \u0442\u0435\u043a\u0441\u0442", show_alert=True)

@router.callback_query(F.data.startswith("text_delete_"))
async def text_delete_callback(query: CallbackQuery, state: FSMContext):
    """Delete a manual broadcast text."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)

    if _is_channel_source(config):
        await query.answer(
            "\u041f\u043e\u0441\u0442\u044b \u043a\u0430\u043d\u0430\u043b\u0430 \u0443\u0434\u0430\u043b\u044f\u044e\u0442\u0441\u044f \u0442\u043e\u043b\u044c\u043a\u043e \u0432 \u0441\u0430\u043c\u043e\u043c \u043a\u0430\u043d\u0430\u043b\u0435",
            show_alert=True,
        )
        return

    try:
        text_index = int(query.data.split("_")[2])
        texts = list(config.get("texts") or [])
        if text_index >= len(texts):
            await query.answer("\u0422\u0435\u043a\u0441\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d", show_alert=True)
            return

        texts.pop(text_index)
        config["texts"] = texts
        if text_index >= len(texts):
            config["text_index"] = max(len(texts) - 1, 0)

        save_broadcast_config_with_profile(user_id, config)
        await query.answer("\u0422\u0435\u043a\u0441\u0442 \u0443\u0434\u0430\u043b\u0435\u043d")

        kb = build_texts_keyboard(config["texts"], back_callback="bc_text")
        await query.message.edit_text(
            _build_text_list_info(config),
            reply_markup=kb,
            parse_mode="HTML",
        )
    except (ValueError, IndexError):
        await query.answer("\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0443\u0434\u0430\u043b\u0438\u0442\u044c \u0442\u0435\u043a\u0441\u0442", show_alert=True)

@router.callback_query(F.data == "text_mode_toggle")
async def text_mode_toggle_callback(query: CallbackQuery, state: FSMContext):
    """Toggle random/sequential selection for current content source."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    if count_source_items(config) == 0:
        await query.answer("\u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u0434\u043e\u0431\u0430\u0432\u044c \u0432\u0430\u0440\u0438\u0430\u043d\u0442\u044b \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438", show_alert=True)
        return

    config["text_mode"] = "sequence" if config.get("text_mode") == "random" else "random"
    config["text_index"] = 0
    save_broadcast_config_with_profile(user_id, config)

    kb = build_text_settings_keyboard(
        config.get("text_source_type", "manual"),
        config.get("text_mode", "random"),
        config.get("parse_mode", "HTML"),
    )
    await query.message.edit_text(
        _build_text_settings_info(config),
        reply_markup=kb,
        parse_mode="HTML",
    )

@router.callback_query(F.data == "text_format_toggle")
async def text_format_toggle_callback(query: CallbackQuery, state: FSMContext):
    """Toggle parse mode for manual texts only."""

    await query.answer()

    user_id = query.from_user.id
    config = get_broadcast_config(user_id)
    if _is_channel_source(config):
        await query.answer(
            "\u0414\u043b\u044f \u043f\u043e\u0441\u0442\u043e\u0432 \u0438\u0437 \u043a\u0430\u043d\u0430\u043b\u0430 \u0444\u043e\u0440\u043c\u0430\u0442 \u043f\u0435\u0440\u0435\u043a\u043b\u044e\u0447\u0430\u0442\u044c \u043d\u0435 \u043d\u0443\u0436\u043d\u043e",
            show_alert=True,
        )
        return

    config["parse_mode"] = "Markdown" if config.get("parse_mode") == "HTML" else "HTML"
    save_broadcast_config_with_profile(user_id, config)

    kb = build_text_settings_keyboard(
        config.get("text_source_type", "manual"),
        config.get("text_mode", "random"),
        config.get("parse_mode", "HTML"),
    )
    await query.message.edit_text(
        _build_text_settings_info(config),
        reply_markup=kb,
        parse_mode="HTML",
    )

@router.callback_query(F.data == "bc_quantity")
async def bc_quantity_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    await state.set_state(BroadcastConfigState.waiting_for_count)

    await state.update_data(
        edit_message_id=query.message.message_id, chat_id=query.message.chat.id
    )

    config = get_broadcast_config(query.from_user.id)

    text = (
        "\U0001f522 <b>\u041e\u0411\u0429\u0415\u0415 \u041a\u041e\u041b-\u0412\u041e \u0421\u041e\u041e\u0411\u0429\u0415\u041d\u0418\u0419</b>\n\n"
        f"\u0422\u0435\u043a\u0443\u0449\u0435\u0435: {config.get('count', 0)}\n\n"
        "\u0412\u0432\u0435\u0434\u0438 \u043d\u043e\u0432\u043e\u0435 \u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u043e\u0442 1 \u0434\u043e 1000:"
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

@router.callback_query(F.data == "bc_interval")
async def bc_interval_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    await state.set_state(BroadcastConfigState.waiting_for_interval)

    await state.update_data(
        edit_message_id=query.message.message_id, chat_id=query.message.chat.id
    )

    config = get_broadcast_config(query.from_user.id)

    current_interval = config.get("interval", "30-90")

    text = (
        "\u23f1\ufe0f <b>\u0418\u041d\u0422\u0415\u0420\u0412\u0410\u041b \u0414\u041b\u042f \u041a\u0410\u0416\u0414\u041e\u0413\u041e \u0427\u0410\u0422\u0410</b>\n\n"
        f"\u0422\u0435\u043a\u0443\u0449\u0438\u0439: {current_interval} \u043c\u0438\u043d\n\n"
        "\u041f\u043e\u0441\u043b\u0435 \u043a\u0430\u0436\u0434\u043e\u0439 \u043e\u0442\u043f\u0440\u0430\u0432\u043a\u0438 \u0431\u043e\u0442 \u0437\u0430\u043d\u043e\u0432\u043e \u043d\u0430\u0437\u043d\u0430\u0447\u0430\u0435\u0442 \u044d\u0442\u043e\u0442 \u0438\u043d\u0442\u0435\u0440\u0432\u0430\u043b \u0438\u043c\u0435\u043d\u043d\u043e \u0434\u043b\u044f \u0442\u043e\u0433\u043e \u0447\u0430\u0442\u0430, \u043a\u0443\u0434\u0430 \u0442\u043e\u043b\u044c\u043a\u043e \u0447\u0442\u043e \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u043b.\n"
        "\u0412\u0432\u0435\u0434\u0438 \u043e\u0434\u043d\u043e \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d.\n"
        "\u041f\u0440\u0438\u043c\u0435\u0440\u044b: <code>15</code> \u0438\u043b\u0438 <code>10-30</code>"
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

@router.callback_query(F.data == "bc_batch_pause")
async def bc_batch_pause_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    await state.set_state(BroadcastConfigState.waiting_for_chat_pause)

    await state.update_data(
        edit_message_id=query.message.message_id, chat_id=query.message.chat.id
    )

    config = get_broadcast_config(query.from_user.id)

    current_pause = config.get("chat_pause", "20-60")

    text = (
        "\u26a1 <b>\u0422\u0415\u041c\u041f</b>\n\n"
        "\u042d\u0442\u043e \u043c\u0438\u043d\u0438\u043c\u0430\u043b\u044c\u043d\u0430\u044f \u043f\u0430\u0443\u0437\u0430 \u043c\u0435\u0436\u0434\u0443 \u043b\u044e\u0431\u044b\u043c\u0438 \u0434\u0432\u0443\u043c\u044f \u043e\u0442\u043f\u0440\u0430\u0432\u043a\u0430\u043c\u0438.\n"
        "\u0415\u0441\u043b\u0438 \u0434\u0432\u0430 \u0447\u0430\u0442\u0430 \u0433\u043e\u0442\u043e\u0432\u044b \u043f\u043e\u0447\u0442\u0438 \u043e\u0434\u043d\u043e\u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e, \u0438\u043c\u0435\u043d\u043d\u043e \u0442\u0435\u043c\u043f \u0440\u0430\u0437\u0434\u0432\u0438\u043d\u0435\u0442 \u0438\u0445 \u043f\u043e \u0432\u0440\u0435\u043c\u0435\u043d\u0438.\n\n"
        f"\u0422\u0435\u043a\u0443\u0449\u0438\u0439: <b>{current_pause}</b> \u0441\u0435\u043a\n\n"
        "\u0412\u0432\u0435\u0434\u0438 \u043e\u0434\u043d\u043e \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d:\n"
        "\u2022 <code>2</code>\n"
        "\u2022 <code>1-3</code>\n"
        f"\u041c\u0430\u043a\u0441\u0438\u043c\u0443\u043c: <code>{CHAT_PAUSE_MAX_SECONDS}</code> \u0441\u0435\u043a"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=CANCEL_TEXT,
                    callback_data="bc_cancel_tempo",
                )
            ]
        ]
    )

    await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "bc_cancel_tempo")
async def bc_cancel_tempo_callback(query: CallbackQuery, state: FSMContext):

    await query.answer()

    await state.clear()

    user_id = query.from_user.id

    await show_broadcast_menu(query, user_id, is_edit=True)

@router.message(BroadcastConfigState.waiting_for_source_channel)
async def process_source_channel_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    source_ref = normalize_channel_reference(message.text)
    if not source_ref:
        await message.answer("Укажи ссылку, @username или ID канала")
        return

    try:
        source_data = await _load_channel_source_for_user(user_id, source_ref)
    except Exception as exc:
        await message.answer(f"❌ Не удалось загрузить посты: {exc}")
        return

    config = get_broadcast_config(user_id)
    config["text_source_type"] = "channel"
    config.update(source_data)
    config["text_index"] = 0
    save_broadcast_config_with_profile(user_id, config)

    data = await state.get_data()
    chat_id = data.get("chat_id")
    edit_message_id = data.get("edit_message_id")
    await state.clear()

    try:
        await message.delete()
    except Exception:
        pass

    kb = build_text_settings_keyboard(
        config.get("text_source_type", "manual"),
        config.get("text_mode", "random"),
        config.get("parse_mode", "HTML"),
    )
    info = _build_text_settings_info(config)
    if edit_message_id and chat_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id,
                message_id=edit_message_id,
                text=info,
                reply_markup=kb,
                parse_mode="HTML",
            )
            return
        except Exception:
            pass

    await message.answer(info, reply_markup=kb, parse_mode="HTML")

@router.message(BroadcastConfigState.waiting_for_text_add)
async def process_text_add(message: Message, state: FSMContext):
    """Handle process text add."""

    user_id = message.from_user.id

    if message.text == CANCEL_TEXT:
        await state.clear()
        config = get_broadcast_config(user_id)
        kb = build_texts_keyboard(config["texts"], back_callback="bc_text")
        info = _build_text_list_info(config)

        data = await state.get_data()
        chat_id = data.get("chat_id")
        edit_message_id = data.get("edit_message_id")

        if edit_message_id and chat_id:
            try:
                await message.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=edit_message_id,
                    text=info,
                    reply_markup=kb,
                    parse_mode="HTML",
                )
            except Exception:
                await message.answer(info, reply_markup=kb, parse_mode="HTML")
        return

    config = get_broadcast_config(user_id)
    config["texts"].append(message.text)
    save_broadcast_config_with_profile(user_id, config)

    await state.clear()

    try:
        await message.delete()
    except Exception:
        pass

    kb = build_texts_keyboard(config["texts"], back_callback="bc_text")
    info = _build_text_list_info(config)

    data = await state.get_data()
    chat_id = data.get("chat_id")
    edit_message_id = data.get("edit_message_id")

    if edit_message_id and chat_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id,
                message_id=edit_message_id,
                text=info,
                reply_markup=kb,
                parse_mode="HTML",
            )
        except Exception:
            await message.answer(info, reply_markup=kb, parse_mode="HTML")
    else:
        await message.answer(info, reply_markup=kb, parse_mode="HTML")

@router.message(BroadcastConfigState.waiting_for_text_edit)
async def process_text_edit(message: Message, state: FSMContext):
    """Handle process text edit."""

    user_id = message.from_user.id

    if message.text == CANCEL_TEXT:
        data = await state.get_data()
        text_index = data.get("text_index", 0)
        await state.clear()

        config = get_broadcast_config(user_id)
        texts = config.get("texts") or []
        if texts:
            text_index = min(text_index, len(texts) - 1)
            current_text = str(texts[text_index])
            parse_mode = config.get("parse_mode", "HTML")
            preview_text = html.escape(current_text[:3500])
            suffix = ""
            if len(current_text) > 3500:
                suffix = f"\n<i>... обрезано, всего {len(current_text)} символов</i>"

            info = (
                f"✏️ <b>Текст #{text_index + 1}</b>\n\n"
                f"Формат: <b>{html.escape(parse_mode)}</b>\n"
                f"{'-' * 24}\n"
                f"<code>{preview_text}</code>{suffix}\n"
                f"{'-' * 24}"
            )
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="Изменить",
                            callback_data=f"text_edit_{text_index}",
                        ),
                        InlineKeyboardButton(
                            text="Удалить",
                            callback_data=f"text_delete_{text_index}",
                        ),
                    ],
                    [InlineKeyboardButton(text="Назад", callback_data="text_list")],
                ]
            )
        else:
            info = _build_text_list_info(config)
            kb = build_texts_keyboard(config["texts"], back_callback="bc_text")

        chat_id = data.get("chat_id")
        edit_message_id = data.get("edit_message_id")

        if edit_message_id and chat_id:
            try:
                await message.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=edit_message_id,
                    text=info,
                    reply_markup=kb,
                    parse_mode="HTML",
                )
            except Exception:
                await message.answer(info, reply_markup=kb, parse_mode="HTML")
        return

    data = await state.get_data()
    text_index = data.get("text_index", 0)
    config = get_broadcast_config(user_id)

    if text_index < len(config["texts"]):
        config["texts"][text_index] = message.text
        save_broadcast_config_with_profile(user_id, config)

    await state.clear()

    try:
        await message.delete()
    except Exception:
        pass

    texts = config.get("texts") or []
    if texts:
        text_index = min(text_index, len(texts) - 1)
        current_text = str(texts[text_index])
        parse_mode = config.get("parse_mode", "HTML")
        preview_text = html.escape(current_text[:3500])
        suffix = ""
        if len(current_text) > 3500:
            suffix = f"\n<i>... обрезано, всего {len(current_text)} символов</i>"

        info = (
            f"✏️ <b>Текст #{text_index + 1}</b>\n\n"
            f"Формат: <b>{html.escape(parse_mode)}</b>\n"
            f"{'-' * 24}\n"
            f"<code>{preview_text}</code>{suffix}\n"
            f"{'-' * 24}"
        )
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Изменить",
                        callback_data=f"text_edit_{text_index}",
                    ),
                    InlineKeyboardButton(
                        text="Удалить",
                        callback_data=f"text_delete_{text_index}",
                    ),
                ],
                [InlineKeyboardButton(text="Назад", callback_data="text_list")],
            ]
        )
    else:
        info = _build_text_list_info(config)
        kb = build_texts_keyboard(config["texts"], back_callback="bc_text")

    chat_id = data.get("chat_id")
    edit_message_id = data.get("edit_message_id")

    if edit_message_id and chat_id:
        try:
            await message.bot.edit_message_text(
                chat_id=chat_id,
                message_id=edit_message_id,
                text=info,
                reply_markup=kb,
                parse_mode="HTML",
            )
        except Exception:
            await message.answer(info, reply_markup=kb, parse_mode="HTML")
    else:
        await message.answer(info, reply_markup=kb, parse_mode="HTML")

@router.message(F.text == COUNT_BUTTON_TEXT)
async def broadcast_count_button(message: Message, state: FSMContext):
    """Handle broadcast count button."""

    user_id = message.from_user.id

    config = get_broadcast_config(user_id)

    await state.update_data(previous_menu="broadcast")

    await state.set_state(BroadcastConfigState.waiting_for_count)

    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=CANCEL_TEXT)]],
        resize_keyboard=True,
    )

    await message.answer(
        (
            "\U0001f522 <b>\u041e\u0411\u0429\u0415\u0415 \u041a\u041e\u041b-\u0412\u041e \u0421\u041e\u041e\u0411\u0429\u0415\u041d\u0418\u0419</b>\n\n"
            f"\u0422\u0435\u043a\u0443\u0449\u0435\u0435: {config.get('count', 0)}\n\n"
            "\u041e\u0442\u043f\u0440\u0430\u0432\u044c \u0447\u0438\u0441\u043b\u043e \u043e\u0442 1 \u0434\u043e 1000"
        ),
        reply_markup=keyboard,
        parse_mode="HTML",
    )

@router.message(BroadcastConfigState.waiting_for_count)
async def process_broadcast_count(message: Message, state: FSMContext):
    """Handle process broadcast count."""

    user_id = message.from_user.id

  # cleaned comment

    if message.text == CANCEL_TEXT:
        await return_to_previous_menu(message, state)

        return

    try:
        count = int(message.text)

        if count < 1 or count > 1000:
            await message.answer("\u274c \u041a\u043e\u043b-\u0432\u043e \u0434\u043e\u043b\u0436\u043d\u043e \u0431\u044b\u0442\u044c \u043e\u0442 1 \u0434\u043e 1000")

            return

        config = get_broadcast_config(user_id)

        config["count"] = count

        save_broadcast_config_with_profile(user_id, config)

        data = await state.get_data()

        edit_broadcast_id = data.get("edit_broadcast_id")
        edit_group_id = data.get("edit_group_id")
        edit_message_id = data.get("edit_message_id")

        chat_id = data.get("chat_id")

        if edit_broadcast_id in active_broadcasts:
            await update_broadcast_fields(
                edit_broadcast_id,
                count=count,
                planned_count=count,
            )
        elif edit_group_id is not None:
            for bid, broadcast in list(active_broadcasts.items()):
                if (
                    broadcast.get("group_id") == edit_group_id
                    and broadcast.get("user_id") == user_id
                    and broadcast.get("status") in ("running", "paused")
                ):
                    await update_broadcast_fields(
                        bid,
                        count=count,
                        planned_count=count,
                    )

        await state.clear()

  # cleaned comment

        try:
            await message.delete()

        except Exception:
            pass

        # Refresh the same menu message or send a new one if editing fails.

        chats = get_broadcast_chats(user_id)

        if edit_message_id and chat_id and edit_group_id is not None:
            try:
                if await _edit_group_detail_message(
                    message,
                    user_id,
                    edit_group_id,
                    chat_id=chat_id,
                    message_id=edit_message_id,
                ):
                    return
            except Exception as e:
                print(f"Group detail refresh failed after count update: {e}")
                await message.answer(
                    "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043c\u0435\u043d\u044e \u0433\u0440\u0443\u043f\u043f\u044b"
                )
                return

        if edit_message_id and chat_id:
            try:
                info = build_broadcast_menu_text(
                    config, chats, active_broadcasts, user_id
                )

                kb = build_broadcast_keyboard(
                    include_active=False,
                    user_id=user_id,
                    active_broadcasts=active_broadcasts,
                    back_callback="delete_bc_menu",
                )

                await message.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=edit_message_id,
                    text=info,
                    reply_markup=kb,
                    parse_mode="HTML",
                )

            except Exception as e:
                print(
                    f"Broadcast menu refresh error: {e}"
                )

                import traceback

                traceback.print_exc()

                await message.answer(
                    "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043c\u0435\u043d\u044e"
                )

        else:
            await show_broadcast_menu(message, message.from_user.id, is_edit=False)

    except ValueError:
        await message.answer("\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u043e")

@router.message(F.text == INTERVAL_BUTTON_TEXT)
async def broadcast_interval_button(message: Message, state: FSMContext):
    """Handle the interval button."""

    user_id = message.from_user.id

    config = get_broadcast_config(user_id)

    await state.update_data(previous_menu="broadcast")

    await state.set_state(BroadcastConfigState.waiting_for_interval)

    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=CANCEL_TEXT)]],
        resize_keyboard=True,
    )

    await message.answer(
        (
            "\u23f1\ufe0f <b>\u0418\u041d\u0422\u0415\u0420\u0412\u0410\u041b \u0414\u041b\u042f \u041a\u0410\u0416\u0414\u041e\u0413\u041e \u0427\u0410\u0422\u0410</b>\n\n"
            f"\u0422\u0435\u043a\u0443\u0449\u0438\u0439: {config.get('interval', 0)} \u043c\u0438\u043d\n\n"
            "\u041f\u043e\u0441\u043b\u0435 \u043a\u0430\u0436\u0434\u043e\u0439 \u043e\u0442\u043f\u0440\u0430\u0432\u043a\u0438 \u0431\u043e\u0442 \u0437\u0430\u043d\u043e\u0432\u043e \u0432\u044b\u0431\u0438\u0440\u0430\u0435\u0442 \u044d\u0442\u043e\u0442 \u0438\u043d\u0442\u0435\u0440\u0432\u0430\u043b \u0434\u043b\u044f \u043a\u043e\u043d\u043a\u0440\u0435\u0442\u043d\u043e\u0433\u043e \u0447\u0430\u0442\u0430.\n"
            "\u041e\u0442\u043f\u0440\u0430\u0432\u044c \u043e\u0434\u043d\u043e \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d.\n"
            "\u041f\u0440\u0438\u043c\u0435\u0440\u044b: <code>15</code> \u0438\u043b\u0438 <code>10-30</code>"
        ),
        reply_markup=keyboard,
        parse_mode="HTML",
    )

@router.message(BroadcastConfigState.waiting_for_interval)
async def process_broadcast_interval(message: Message, state: FSMContext):
    """Handle process broadcast interval."""

    user_id = message.from_user.id

    # If this is not the cancel button, expect a number or a range.

    if message.text == CANCEL_TEXT:
        await return_to_previous_menu(message, state)

        return

    try:
        text = message.text.strip()

        # Accept either a single number or a min-max range.

        if "-" in text:
            # Range format: min-max.

            parts = text.split("-")

            if len(parts) != 2:
                await message.answer(
                    "\u274c \u041d\u0435\u0432\u0435\u0440\u043d\u044b\u0439 \u0444\u043e\u0440\u043c\u0430\u0442. \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439: 10-30 \u0438\u043b\u0438 15"
                )

                return

            try:
                min_interval = int(parts[0].strip())

                max_interval = int(parts[1].strip())

                if min_interval < 1 or max_interval < 1 or min_interval > max_interval:
                    await message.answer(
                        "\u274c \u0417\u043d\u0430\u0447\u0435\u043d\u0438\u044f \u0434\u043e\u043b\u0436\u043d\u044b \u0431\u044b\u0442\u044c \u043f\u043e\u043b\u043e\u0436\u0438\u0442\u0435\u043b\u044c\u043d\u044b\u043c\u0438, \u0438 min \u043d\u0435 \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u0431\u043e\u043b\u044c\u0448\u0435 max"
                    )

                    return

                if min_interval > 480 or max_interval > 480:
                    await message.answer(
                        "\u274c \u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b \u043d\u0435 \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u0431\u043e\u043b\u044c\u0448\u0435 480 \u043c\u0438\u043d\u0443\u0442 (8 \u0447\u0430\u0441\u043e\u0432)"
                    )

                    return

                interval_value = text  # Keep the range as a raw string.

            except ValueError:
                await message.answer(
                    "\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u0430 \u0432 \u0444\u043e\u0440\u043c\u0430\u0442\u0435: 10-30"
                )

                return

        else:
            # Single number.

            try:
                interval_int = int(text)

                if interval_int < 1 or interval_int > 480:
                    await message.answer(
                        "\u274c \u0418\u043d\u0442\u0435\u0440\u0432\u0430\u043b \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u043e\u0442 1 \u0434\u043e 480 \u043c\u0438\u043d\u0443\u0442"
                    )

                    return

                interval_value = text

            except ValueError:
                await message.answer(
                    "\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d (\u043d\u0430\u043f\u0440\u0438\u043c\u0435\u0440 10-30)"
                )

                return

        # Save the config.

        config = get_broadcast_config(user_id)

        config["interval"] = interval_value

        save_broadcast_config_with_profile(user_id, config)

        data = await state.get_data()

        edit_broadcast_id = data.get("edit_broadcast_id")
        edit_group_id = data.get("edit_group_id")
        edit_message_id = data.get("edit_message_id")

        chat_id = data.get("chat_id")

        if edit_broadcast_id in active_broadcasts:
            await update_broadcast_fields(
                edit_broadcast_id,
                interval_minutes=interval_value,
                interval_value=interval_value,
            )
        elif edit_group_id is not None:
            for bid, broadcast in list(active_broadcasts.items()):
                if (
                    broadcast.get("group_id") == edit_group_id
                    and broadcast.get("user_id") == user_id
                    and broadcast.get("status") in ("running", "paused")
                ):
                    await update_broadcast_fields(
                        bid,
                        interval_minutes=interval_value,
                        interval_value=interval_value,
                    )

        await state.clear()

        # Remove the user message.

        try:
            await message.delete()

        except Exception:
            pass

        # Refresh the same menu message or send a new one if editing fails.

        chats = get_broadcast_chats(user_id)

        if edit_message_id and chat_id and edit_group_id is not None:
            try:
                if await _edit_group_detail_message(
                    message,
                    user_id,
                    edit_group_id,
                    chat_id=chat_id,
                    message_id=edit_message_id,
                ):
                    return
            except Exception as e:
                print(f"Group detail refresh failed after interval update: {e}")
                await message.answer(
                    "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043c\u0435\u043d\u044e \u0433\u0440\u0443\u043f\u043f\u044b"
                )
                return

        if edit_message_id and chat_id:
            try:
                info = build_broadcast_menu_text(
                    config, chats, active_broadcasts, user_id
                )

                kb = build_broadcast_keyboard(
                    include_active=False,
                    user_id=user_id,
                    active_broadcasts=active_broadcasts,
                    back_callback="delete_bc_menu",
                )

                await message.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=edit_message_id,
                    text=info,
                    reply_markup=kb,
                    parse_mode="HTML",
                )

            except Exception as e:
                print(
                    f"Broadcast menu refresh error: {e}"
                )

                import traceback

                traceback.print_exc()

                await message.answer(
                    "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043c\u0435\u043d\u044e"
                )

        else:
            await show_broadcast_menu(message, message.from_user.id, is_edit=False)

    except ValueError:
        await message.answer("\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u043e")

@router.message(BroadcastConfigState.waiting_for_chat_pause)
async def process_broadcast_chat_pause(message: Message, state: FSMContext):
    """Handle the pause between chats input."""

    user_id = message.from_user.id

    # If this is not the cancel button, expect a number or a range.

    if message.text == CANCEL_TEXT:
        await return_to_previous_menu(message, state)

        return

    try:
        text = message.text.strip()

  # cleaned comment

        if "-" in text:
  # cleaned comment

            parts = text.split("-")

            if len(parts) != 2:
                await message.answer(
                    "\u274c \u041d\u0435\u0432\u0435\u0440\u043d\u044b\u0439 \u0444\u043e\u0440\u043c\u0430\u0442. \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439: 1-3 \u0438\u043b\u0438 2"
                )

                return

            try:
                min_pause = int(parts[0].strip())

                max_pause = int(parts[1].strip())

                if min_pause < 1 or max_pause < 1 or min_pause > max_pause:
                    await message.answer(
                        "\u274c \u0417\u043d\u0430\u0447\u0435\u043d\u0438\u044f \u0434\u043e\u043b\u0436\u043d\u044b \u0431\u044b\u0442\u044c \u043f\u043e\u043b\u043e\u0436\u0438\u0442\u0435\u043b\u044c\u043d\u044b\u043c\u0438, \u0438 min \u043d\u0435 \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u0431\u043e\u043b\u044c\u0448\u0435 max"
                    )

                    return

                if min_pause > CHAT_PAUSE_MAX_SECONDS or max_pause > CHAT_PAUSE_MAX_SECONDS:
                    await message.answer(
                        f"\u274c \u0422\u0435\u043c\u043f \u043d\u0435 \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u0431\u043e\u043b\u044c\u0448\u0435 {CHAT_PAUSE_MAX_SECONDS} \u0441\u0435\u043a\u0443\u043d\u0434"
                    )

                    return

                pause_value = text  # cleaned comment

            except ValueError:
                await message.answer(
                    "\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u0430 \u0432 \u0444\u043e\u0440\u043c\u0430\u0442\u0435: 1-3"
                )

                return

        else:
  # cleaned comment

            try:
                pause_int = int(text)

                if pause_int < 1 or pause_int > CHAT_PAUSE_MAX_SECONDS:
                    await message.answer(
                        f"\u274c \u0422\u0435\u043c\u043f \u0434\u043e\u043b\u0436\u0435\u043d \u0431\u044b\u0442\u044c \u043e\u0442 1 \u0434\u043e {CHAT_PAUSE_MAX_SECONDS} \u0441\u0435\u043a\u0443\u043d\u0434"
                    )

                    return

                pause_value = text

            except ValueError:
                await message.answer(
                    "\u274c \u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u043e \u0438\u043b\u0438 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d (\u043d\u0430\u043f\u0440\u0438\u043c\u0435\u0440 1-3)"
                )

                return

  # cleaned comment

        config = get_broadcast_config(user_id)

        config["chat_pause"] = pause_value

        save_broadcast_config_with_profile(user_id, config)

        data = await state.get_data()

        edit_group_id = data.get("edit_group_id")
        edit_message_id = data.get("edit_message_id")

        chat_id = data.get("chat_id")

        await state.clear()

  # cleaned comment

        try:
            await message.delete()

        except Exception:
            pass

  # cleaned comment

        if edit_group_id is not None:
            for bid, broadcast in list(active_broadcasts.items()):
                if (
                    broadcast.get("group_id") == edit_group_id
                    and broadcast.get("user_id") == user_id
                    and broadcast.get("status") in ("running", "paused")
                ):
                    await update_broadcast_fields(bid, chat_pause=pause_value)

        if edit_message_id and chat_id and edit_group_id is not None:
            try:
                if await _edit_group_detail_message(
                    message,
                    user_id,
                    edit_group_id,
                    chat_id=chat_id,
                    message_id=edit_message_id,
                ):
                    return
            except Exception as e:
                print(f"Group detail refresh failed after pace update: {e}")
                await message.answer(
                    "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043c\u0435\u043d\u044e \u0433\u0440\u0443\u043f\u043f\u044b"
                )
                return

        if edit_message_id and chat_id:
            try:
                chats = get_broadcast_chats(user_id)

                info = build_broadcast_menu_text(
                    config, chats, active_broadcasts, user_id
                )

                kb = build_broadcast_keyboard(
                    include_active=False,
                    user_id=user_id,
                    active_broadcasts=active_broadcasts,
                    back_callback="delete_bc_menu",
                )

                await message.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=edit_message_id,
                    text=info,
                    reply_markup=kb,
                    parse_mode="HTML",
                )

            except Exception as e:
                print(f"Error refreshing broadcast menu: {e}")

                await message.answer(
                    "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u043c\u0435\u043d\u044e"
                )

        else:
            await show_broadcast_menu(message, message.from_user.id, is_edit=False)

    except Exception as e:
        print(f"Error processing inter-chat pause: {e}")

        await message.answer(
            "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0441\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c \u0442\u0435\u043c\u043f"
        )

async def return_to_previous_menu(message: Message, state: FSMContext):
    """\u0412\u043e\u0437\u0432\u0440\u0430\u0449\u0430\u0435\u0442 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f \u0432 \u043f\u0440\u0435\u0434\u044b\u0434\u0443\u0449\u0435\u0435 \u043c\u0435\u043d\u044e \u0431\u0435\u0437 \u043b\u0438\u0448\u043d\u0435\u0433\u043e \u0442\u0435\u043a\u0441\u0442\u0430."""

    data = await state.get_data()
    previous_menu = data.get("previous_menu", "broadcast")
    await state.clear()

    if previous_menu == "broadcast":
        await show_broadcast_menu(message, message.from_user.id, is_edit=False)
        return

    if previous_menu == "broadcast_chats":
        await show_broadcast_chats_menu(
            message, message.from_user.id, menu_message_id=data.get("menu_message_id")
        )
        return

    await message.answer(
        "\u0413\u043b\u0430\u0432\u043d\u043e\u0435 \u043c\u0435\u043d\u044e",
        reply_markup=get_main_menu_keyboard(),
    )
