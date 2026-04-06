from typing import Any


def normalize_channel_reference(value: str | None) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None

    if raw.startswith("@"):
        return raw

    lower = raw.lower()
    if lower.startswith("https://t.me/") or lower.startswith("http://t.me/"):
        tail = raw.split("t.me/", 1)[1].strip("/")
        if tail:
            if tail.startswith("+"):
                return f"https://t.me/{tail}"
            return f"@{tail.split('/', 1)[0]}"

    if lower.startswith("t.me/"):
        tail = raw.split("t.me/", 1)[1].strip("/")
        if tail:
            if tail.startswith("+"):
                return f"https://t.me/{tail}"
            return f"@{tail.split('/', 1)[0]}"

    return raw


def build_source_posts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "message_id": int(item["message_id"]),
            "preview": str(item.get("preview") or ""),
        }
        for item in items
        if item.get("message_id")
    ]


def count_source_items(config: dict) -> int:
    source_type = config.get("text_source_type", "manual")
    if source_type == "channel":
        return len(config.get("source_posts") or [])
    return len(config.get("texts") or [])


def build_text_source_label(config: dict) -> str:
    source_type = config.get("text_source_type", "manual")
    return "Канал" if source_type == "channel" else "Вручную"


def source_channel_title(config: dict) -> str:
    title = (config.get("source_channel_title") or "").strip()
    if title:
        return title
    ref = (config.get("source_channel_ref") or "").strip()
    return ref or "не выбран"


def post_preview_text(preview: str, limit: int = 90) -> str:
    text = " ".join((preview or "").split())
    if not text:
        return "[пустой пост]"
    if len(text) > limit:
        return text[: limit - 1] + "…"
    return text


def format_source_channel_link(config: dict, message_id: int | None = None) -> str | None:
    ref = str(config.get("source_channel_ref") or "").strip()
    if not ref:
        return None

    if ref.startswith("@"):
        base = f"https://t.me/{ref[1:]}"
    elif ref.startswith("https://t.me/"):
        base = ref.rstrip("/")
    else:
        return None

    if message_id:
        return f"{base}/{message_id}"
    return base


def _preview_from_message(message) -> str:
    body = (getattr(message, "message", "") or "").strip()
    if body:
        return body

    if getattr(message, "media", None) is not None:
        return "[media пост]"

    return ""


async def fetch_channel_posts(client, channel_ref: str) -> dict[str, Any]:
    normalized_ref = normalize_channel_reference(channel_ref)
    if not normalized_ref:
        raise ValueError("Источник канала не указан")

    entity = await client.get_entity(normalized_ref)
    title = getattr(entity, "title", None) or getattr(entity, "username", None) or normalized_ref
    resolved_ref = f"@{entity.username}" if getattr(entity, "username", None) else normalized_ref

    posts: list[dict[str, Any]] = []
    async for message in client.iter_messages(entity, reverse=True):
        if getattr(message, "action", None) is not None:
            continue

        preview = _preview_from_message(message)
        if not preview:
            continue

        posts.append(
            {
                "message_id": int(message.id),
                "preview": post_preview_text(preview),
            }
        )

    return {
        "source_channel_ref": resolved_ref,
        "source_channel_title": str(title),
        "source_posts": build_source_posts(posts),
    }
