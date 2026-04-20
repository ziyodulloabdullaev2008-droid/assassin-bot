import asyncio
from typing import Any
from urllib.parse import urlparse
from telethon.utils import get_peer_id
from services.mention_utils import normalize_chat_id


def normalize_channel_reference(value: str | None) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None

    if raw.startswith("@"):
        return raw

    lower = raw.lower()
    if (
        lower.startswith("https://t.me/")
        or lower.startswith("http://t.me/")
        or lower.startswith("t.me/")
    ):
        normalized_url = raw if "://" in raw else f"https://{raw}"
        parsed = urlparse(normalized_url)
        parts = [part for part in parsed.path.split("/") if part]
        if parts:
            head = parts[0]
            if head == "c" and len(parts) >= 2 and parts[1].isdigit():
                return f"-100{parts[1]}"
            if head == "joinchat" and len(parts) >= 2:
                return f"https://t.me/joinchat/{parts[1]}"
            if head.startswith("+"):
                return f"https://t.me/{head}"
            return f"@{head}"

    return raw


def parse_numeric_reference(value: str | int | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lstrip("-").isdigit():
        return int(text)
    return None


def _entity_reference_candidates(entity) -> set[int]:
    candidates: set[int] = set()
    entity_id = getattr(entity, "id", None)
    if entity_id is None:
        return candidates

    entity_id = int(entity_id)
    candidates.add(entity_id)
    candidates.add(normalize_chat_id(entity_id))
    try:
        full_peer_id = int(get_peer_id(entity))
        candidates.add(full_peer_id)
        candidates.add(normalize_chat_id(full_peer_id))
    except Exception:
        pass

    if entity_id > 0:
        candidates.add(-entity_id)
        candidates.add(int(f"-100{entity_id}"))

    return candidates


async def _find_entity_in_dialogs(client, target_candidates: set[int]):
    async for dialog in client.iter_dialogs():
        entity = getattr(dialog, "entity", None)
        if entity is None:
            continue
        entity_candidates = _entity_reference_candidates(entity)
        if entity_candidates & target_candidates:
            return entity
    return None


async def resolve_entity_reference(client, reference: str | int):
    normalized_ref = (
        normalize_channel_reference(reference)
        if isinstance(reference, str)
        else reference
    )

    lookup_timed_out = False
    try:
        return await asyncio.wait_for(client.get_entity(normalized_ref), timeout=12.0)
    except asyncio.TimeoutError:
        lookup_timed_out = True
    except Exception:
        pass

    numeric_ref = parse_numeric_reference(normalized_ref)
    if numeric_ref is None:
        if lookup_timed_out:
            raise TimeoutError(f"Telegram timed out while resolving chat: {reference}")
        raise ValueError(f"Chat or channel not found for reference: {reference}")

    target_candidates = {
        int(numeric_ref),
        normalize_chat_id(numeric_ref),
    }

    try:
        entity = await asyncio.wait_for(
            _find_entity_in_dialogs(client, target_candidates), timeout=15.0
        )
    except asyncio.TimeoutError as exc:
        raise TimeoutError(
            f"Telegram timed out while scanning dialogs for chat: {reference}"
        ) from exc
    if entity is not None:
        return entity

    raise ValueError(f"Chat or channel not found for reference: {reference}")


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

    entity = await resolve_entity_reference(client, normalized_ref)
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
