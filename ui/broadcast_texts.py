from services.channel_post_service import (
    build_text_source_label,
    count_source_items,
    source_channel_title,
)


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
        lines.append(f"Канал: {source_channel_title(config)}")
    else:
        lines.append(f"Формат: {config.get('parse_mode', 'HTML')}")
    return "\n".join(lines)


def build_text_list_info(config: dict) -> str:
    if is_channel_source(config):
        count = len(config.get("source_posts") or [])
        lines = [
            "📚 <b>ПОСТЫ ИЗ КАНАЛА</b>",
            "",
            f"Канал: {source_channel_title(config)}",
            f"Постов доступно: {count}",
            "",
        ]
        if not count:
            lines.append("Сначала укажи канал-источник и загрузи посты.")
        else:
            lines.append("Выбери пост для просмотра.")
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
