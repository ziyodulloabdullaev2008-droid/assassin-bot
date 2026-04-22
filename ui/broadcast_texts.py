from services.channel_post_service import (
    build_text_source_label,
    count_source_items,
    enabled_source_posts_count,
    source_channel_title,
    source_posts_total_count,
)

LOGIN_REQUIRED_TEXT = "❌ Сначала войди через /login"
CANCEL_TEXT = "❌ Отменить"
COUNT_BUTTON_TEXT = "Кол-во"
INTERVAL_BUTTON_TEXT = "Интервал"


def is_channel_source(config: dict) -> bool:
    return config.get("text_source_type", "manual") == "channel"


def build_text_settings_info(config: dict) -> str:
    lines = [
        "📝 <b>НАСТРОЙКИ КОНТЕНТА</b>",
        "",
        f"Источник текста: {build_text_source_label(config)}",
        f"Вариантов: {count_source_items(config)}",
        f"Режим: {'Random ✅' if config.get('text_mode') == 'random' else 'No Random ❌'}",
    ]
    if is_channel_source(config):
        total_posts = source_posts_total_count(config.get("source_posts") or [])
        enabled_posts = enabled_source_posts_count(config.get("source_posts") or [])
        lines.append(f"Канал: {source_channel_title(config)}")
        lines.append(f"Постов активно: {enabled_posts}/{total_posts}")
        lines.append(
            "Пересылка: "
            + (
                "показывать источник"
                if config.get("show_forward_source")
                else "скрывать источник"
            )
        )
    else:
        lines.append(f"Формат: {config.get('parse_mode', 'HTML')}")
    return "\n".join(lines)


def build_text_list_info(config: dict) -> str:
    if is_channel_source(config):
        posts = config.get("source_posts") or []
        total_posts = source_posts_total_count(posts)
        enabled_posts = enabled_source_posts_count(posts)
        lines = [
            "📚 <b>ПОСТЫ ИЗ КАНАЛА</b>",
            "",
            f"Канал: {source_channel_title(config)}",
            f"Активно: {enabled_posts}/{total_posts}",
            "",
        ]
        if not total_posts:
            lines.append("Сначала укажи канал-источник и загрузи посты.")
        else:
            lines.append("Выбери пост для просмотра и включения/выключения.")
        return "\n".join(lines)

    count = len(config.get("texts") or [])
    lines = [
        "📚 <b>СПИСОК ТЕКСТОВ</b>",
        "",
        f"Всего текстов: {count}",
        "",
    ]
    if not count:
        lines.append("Еще не добавлено ни одного текста.")
    else:
        lines.append("Выбери текст для просмотра или редактирования.")
    return "\n".join(lines)
