from database import get_account_proxy, get_account_proxy_check_result
from services.broadcast_runtime_service import account_label
from services.channel_post_service import build_text_source_label, count_source_items
from services.operation_guard_service import get_active_operation


def _content_ready(config: dict) -> bool:
    source_type = str(config.get("text_source_type") or "manual")
    if source_type == "channel":
        return bool(config.get("source_channel_ref") and config.get("source_posts"))
    return bool(config.get("texts"))


def build_broadcast_preflight_text(
    user_id: int,
    *,
    config: dict,
    chats: list,
    available_accounts: list[tuple[int, str | None, str | None]],
    active_broadcasts: dict,
) -> str:
    active_count = sum(
        1
        for broadcast in active_broadcasts.values()
        if broadcast.get("user_id") == user_id
        and broadcast.get("status") in ("running", "paused")
    )

    lines = [
        "🚀 <b>Проверка перед запуском</b>",
        "",
        f"Источник текста: <b>{build_text_source_label(config)}</b>",
        f"Вариантов контента: <b>{count_source_items(config)}</b>",
        f"Чатов в списке: <b>{len(chats)}</b>",
        f"Аккаунтов доступно: <b>{len(available_accounts)}</b>",
        f"Уже активных рассылок: <b>{active_count}</b>",
    ]

    problems: list[str] = []
    warnings: list[str] = []

    if not _content_ready(config):
        problems.append("Не настроен текст или источник контента.")
    if not chats:
        problems.append("Список чатов пуст.")
    if not available_accounts:
        problems.append("Нет доступных аккаунтов для запуска.")

    active_operation = get_active_operation(user_id)
    if active_operation:
        problems.append(f"Сейчас выполняется операция: {active_operation}.")

    for account_number, username, first_name in available_accounts:
        proxy = get_account_proxy(user_id, account_number)
        if not proxy:
            continue
        proxy_check = get_account_proxy_check_result(user_id, account_number)
        label = account_label(account_number, username, first_name)
        if not proxy_check:
            warnings.append(f"{label}: прокси ещё не проверялся.")
            continue
        if proxy_check.get("ok") is False:
            warnings.append(f"{label}: последняя проверка прокси завершилась ошибкой.")

    if problems:
        lines.extend(["", "❌ <b>Что мешает запуску</b>"])
        lines.extend([f"• {problem}" for problem in problems])
    else:
        lines.extend(["", "✅ <b>Базовая проверка пройдена</b>"])

    if warnings:
        lines.extend(["", "⚠️ <b>На что стоит посмотреть</b>"])
        lines.extend([f"• {warning}" for warning in warnings[:5]])

    lines.extend(
        [
            "",
            "Ниже выбери аккаунт для запуска.",
        ]
    )
    return "\n".join(lines)
