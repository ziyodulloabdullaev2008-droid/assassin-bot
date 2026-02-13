import asyncio
import json
import random
import re
from pathlib import Path
from typing import List, Optional, Tuple

from telethon.errors import UserAlreadyParticipantError, UserNotParticipantError
from telethon.tl.functions.channels import JoinChannelRequest, GetParticipantRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

from core.state import app_state
from services.user_paths import BASE_DIR, joins_settings_path


_KEYWORDS = ["\u043f\u043e\u0434\u043f\u0438\u0441\u0430\u0442\u044c\u0441\u044f", "\u0432\u0441\u0442\u0443\u043f\u0438\u0442\u044c", "\u043d\u0435\u043e\u0431\u0445\u043e\u0434\u0438\u043c\u043e"]
DEFAULT_PER_TARGET_DELAY_MIN = 7
DEFAULT_PER_TARGET_DELAY_MAX = 15
DEFAULT_BETWEEN_CHATS_DELAY_MIN = 20
DEFAULT_BETWEEN_CHATS_DELAY_MAX = 30


def _get_lock(user_id: int) -> asyncio.Lock:
    if user_id not in app_state.joins_lock:
        app_state.joins_lock[user_id] = asyncio.Lock()
    return app_state.joins_lock[user_id]


def _get_queue(user_id: int) -> list:
    if user_id not in app_state.joins_queue:
        app_state.joins_queue[user_id] = []
    return app_state.joins_queue[user_id]


def _get_seen(user_id: int) -> set:
    if user_id not in app_state.joins_seen:
        app_state.joins_seen[user_id] = set()
    return app_state.joins_seen[user_id]


def set_enabled(user_id: int, enabled: bool) -> None:
    app_state.joins_enabled[user_id] = enabled
    _save_settings(user_id)
    if enabled:
        _ensure_worker(user_id)


def is_enabled(user_id: int) -> bool:
    return app_state.joins_enabled.get(user_id, False)


def set_target_accounts(user_id: int, account_numbers: Optional[List[int]]) -> None:
    if account_numbers is None:
        app_state.joins_target_accounts[user_id] = set()
    else:
        app_state.joins_target_accounts[user_id] = set(account_numbers)
    _save_settings(user_id)


def get_target_accounts(user_id: int) -> Optional[set]:
    return app_state.joins_target_accounts.get(user_id, set())


def _normalize_range(min_value: int, max_value: int, default_min: int, default_max: int) -> tuple[int, int]:
    try:
        min_value = int(min_value)
        max_value = int(max_value)
    except Exception:
        return default_min, default_max
    min_value = max(1, min_value)
    max_value = max(1, max_value)
    if min_value > max_value:
        min_value, max_value = max_value, min_value
    return min_value, max_value


def get_delay_config(user_id: int) -> dict:
    cfg = app_state.joins_delay_config.get(user_id) or {}
    per_min, per_max = _normalize_range(
        cfg.get("per_target_min", DEFAULT_PER_TARGET_DELAY_MIN),
        cfg.get("per_target_max", DEFAULT_PER_TARGET_DELAY_MAX),
        DEFAULT_PER_TARGET_DELAY_MIN,
        DEFAULT_PER_TARGET_DELAY_MAX,
    )
    between_min, between_max = _normalize_range(
        cfg.get("between_chats_min", DEFAULT_BETWEEN_CHATS_DELAY_MIN),
        cfg.get("between_chats_max", DEFAULT_BETWEEN_CHATS_DELAY_MAX),
        DEFAULT_BETWEEN_CHATS_DELAY_MIN,
        DEFAULT_BETWEEN_CHATS_DELAY_MAX,
    )
    normalized = {
        "per_target_min": per_min,
        "per_target_max": per_max,
        "between_chats_min": between_min,
        "between_chats_max": between_max,
    }
    app_state.joins_delay_config[user_id] = normalized
    return dict(normalized)


def set_delay_config(
    user_id: int,
    *,
    per_target_min: Optional[int] = None,
    per_target_max: Optional[int] = None,
    between_chats_min: Optional[int] = None,
    between_chats_max: Optional[int] = None,
) -> dict:
    cfg = get_delay_config(user_id)
    if per_target_min is not None:
        cfg["per_target_min"] = int(per_target_min)
    if per_target_max is not None:
        cfg["per_target_max"] = int(per_target_max)
    if between_chats_min is not None:
        cfg["between_chats_min"] = int(between_chats_min)
    if between_chats_max is not None:
        cfg["between_chats_max"] = int(between_chats_max)

    per_min, per_max = _normalize_range(
        cfg["per_target_min"],
        cfg["per_target_max"],
        DEFAULT_PER_TARGET_DELAY_MIN,
        DEFAULT_PER_TARGET_DELAY_MAX,
    )
    between_min, between_max = _normalize_range(
        cfg["between_chats_min"],
        cfg["between_chats_max"],
        DEFAULT_BETWEEN_CHATS_DELAY_MIN,
        DEFAULT_BETWEEN_CHATS_DELAY_MAX,
    )
    normalized = {
        "per_target_min": per_min,
        "per_target_max": per_max,
        "between_chats_min": between_min,
        "between_chats_max": between_max,
    }
    app_state.joins_delay_config[user_id] = normalized
    _save_settings(user_id)
    return dict(normalized)


def _save_settings(user_id: int) -> None:
    path = joins_settings_path(user_id)
    delay_cfg = get_delay_config(user_id)
    payload = {
        "enabled": bool(app_state.joins_enabled.get(user_id, False)),
        "target_accounts": sorted(list(app_state.joins_target_accounts.get(user_id, set()))),
        "delay": delay_cfg,
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def load_all_settings() -> None:
    # users/*/joins_settings.json
    for path in BASE_DIR.glob("*/joins_settings.json"):
        if not path.is_file():
            continue
        _load_settings_file(path)
    # legacy fallback in project root (joins_settings_<user_id>.json)
    for path in Path(__file__).resolve().parent.parent.glob("joins_settings_*.json"):
        if not path.is_file():
            continue
        _load_settings_file(path)


def _load_settings_file(path: Path) -> None:
    user_id: Optional[int] = None
    try:
        if path.parent.name.isdigit():
            user_id = int(path.parent.name)
        elif path.stem.startswith("joins_settings_"):
            user_id = int(path.stem.replace("joins_settings_", ""))
        if not user_id:
            return

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        enabled = bool(data.get("enabled", False))
        targets = data.get("target_accounts") or []
        clean_targets = set()
        for item in targets:
            try:
                clean_targets.add(int(item))
            except Exception:
                continue

        app_state.joins_enabled[user_id] = enabled
        app_state.joins_target_accounts[user_id] = clean_targets
        delay_raw = data.get("delay") or {}
        per_min, per_max = _normalize_range(
            delay_raw.get("per_target_min", DEFAULT_PER_TARGET_DELAY_MIN),
            delay_raw.get("per_target_max", DEFAULT_PER_TARGET_DELAY_MAX),
            DEFAULT_PER_TARGET_DELAY_MIN,
            DEFAULT_PER_TARGET_DELAY_MAX,
        )
        between_min, between_max = _normalize_range(
            delay_raw.get("between_chats_min", DEFAULT_BETWEEN_CHATS_DELAY_MIN),
            delay_raw.get("between_chats_max", DEFAULT_BETWEEN_CHATS_DELAY_MAX),
            DEFAULT_BETWEEN_CHATS_DELAY_MIN,
            DEFAULT_BETWEEN_CHATS_DELAY_MAX,
        )
        app_state.joins_delay_config[user_id] = {
            "per_target_min": per_min,
            "per_target_max": per_max,
            "between_chats_min": between_min,
            "between_chats_max": between_max,
        }

        if enabled:
            _ensure_worker(user_id)
    except Exception:
        return


def extract_join_links(text: str) -> Tuple[List[str], List[str]]:
    links = []
    usernames = []
    if not text:
        return links, usernames
    for match in re.findall(r"(https?://t\.me/\+[-_\w]+)", text, flags=re.IGNORECASE):
        links.append(match)
    for match in re.findall(r"(https?://t\.me/joinchat/[-_\w]+)", text, flags=re.IGNORECASE):
        links.append(match)
    for match in re.findall(r"@([a-zA-Z0-9_]{4,})", text):
        usernames.append(match)
    return links, usernames


def message_has_keywords(text: str) -> bool:
    if not text:
        return False
    lower = text.lower()
    return any(word in lower for word in _KEYWORDS)


def should_enqueue_from_error(error_str: str) -> bool:
    if not error_str:
        return False
    lower = error_str.lower()
    triggers = [
        "usernotparticipant",
        "not a participant",
        "chatwriteforbidden",
        "forbidden",
        "join",
        "participant",
    ]
    return any(t in lower for t in triggers)


async def enqueue_join(
    user_id: int,
    chat_id: Optional[int] = None,
    links: Optional[List[str]] = None,
    usernames: Optional[List[str]] = None,
) -> None:
    if not is_enabled(user_id):
        return

    links = links or []
    usernames = usernames or []
    queue = _get_queue(user_id)
    seen = _get_seen(user_id)

    async with _get_lock(user_id):
        if not links and not usernames and chat_id is None:
            return

        key = (chat_id, tuple(sorted(links)), tuple(sorted(usernames)))
        if key in seen:
            return
        seen.add(key)
        queue.append({
            "chat_id": chat_id,
            "links": links,
            "usernames": usernames,
        })

    _ensure_worker(user_id)


def _ensure_worker(user_id: int) -> None:
    task = app_state.joins_task.get(user_id)
    if task and not task.done():
        return
    app_state.joins_task[user_id] = asyncio.create_task(_join_worker(user_id))


async def _join_worker(user_id: int) -> None:
    while is_enabled(user_id):
        queue = _get_queue(user_id)
        if not queue:
            await asyncio.sleep(2)
            continue

        async with _get_lock(user_id):
            if not queue:
                continue
            req = queue.pop(0)

        target_accounts = get_target_accounts(user_id)
        if not target_accounts:
            if user_id not in app_state.user_authenticated:
                await asyncio.sleep(2)
                continue
            target_accounts = set(app_state.user_authenticated[user_id].keys())

        for acc_num in list(target_accounts):
            client = app_state.user_authenticated.get(user_id, {}).get(acc_num)
            if not client:
                continue
            await _handle_join_request(client, req, user_id)

        # Requests from different chats are processed strictly one-by-one
        # with long cooldown to reduce spam blocks.
        cfg = get_delay_config(user_id)
        await asyncio.sleep(random.uniform(cfg["between_chats_min"], cfg["between_chats_max"]))


async def _handle_join_request(client, req: dict, user_id: int) -> None:
    links = _unique_preserve_order(req.get("links") or [])
    usernames = _unique_preserve_order(req.get("usernames") or [])
    chat_id = req.get("chat_id")
    targets_count = len(links) + len(usernames) + (1 if chat_id is not None else 0)
    step = 0
    cfg = get_delay_config(user_id)
    per_min = cfg["per_target_min"]
    per_max = cfg["per_target_max"]

    # Try links from buttons/text. Each target gets its own delay.
    for link in links:
        step += 1
        invite_hash = _extract_invite_hash(link)
        if invite_hash:
            try:
                try:
                    entity = await client.get_entity(link)
                    if await _is_already_participant(client, entity):
                        pass
                    else:
                        await client(ImportChatInviteRequest(invite_hash))
                except Exception:
                    await client(ImportChatInviteRequest(invite_hash))
            except UserAlreadyParticipantError:
                pass
            except Exception:
                pass
        else:
            # Public links like https://t.me/channel_name
            try:
                entity = await client.get_entity(link)
                if await _is_already_participant(client, entity):
                    pass
                else:
                    await client(JoinChannelRequest(entity))
            except UserAlreadyParticipantError:
                pass
            except Exception:
                pass

        if step < targets_count:
            await asyncio.sleep(random.uniform(per_min, per_max))

    # Try usernames
    for username in usernames:
        step += 1
        try:
            entity = await client.get_entity(username)
            if await _is_already_participant(client, entity):
                pass
            else:
                await client(JoinChannelRequest(entity))
        except UserAlreadyParticipantError:
            pass
        except Exception:
            pass

        if step < targets_count:
            await asyncio.sleep(random.uniform(per_min, per_max))

    # Try by chat_id
    if chat_id is not None:
        step += 1
        try:
            entity = await client.get_entity(chat_id)
            if await _is_already_participant(client, entity):
                pass
            else:
                await client(JoinChannelRequest(entity))
        except UserAlreadyParticipantError:
            pass
        except Exception:
            pass

        if step < targets_count:
            await asyncio.sleep(random.uniform(per_min, per_max))


def _unique_preserve_order(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        key = (item or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _extract_invite_hash(link: str) -> Optional[str]:
    m = re.search(r"/\+([-_\w]+)$", link)
    if m:
        return m.group(1)
    m = re.search(r"/joinchat/([-_\w]+)$", link)
    if m:
        return m.group(1)
    return None


async def _is_already_participant(client, entity) -> bool:
    try:
        await client.get_permissions(entity, "me")
        return True
    except UserNotParticipantError:
        return False
    except Exception:
        try:
            me = await client.get_input_entity("me")
            await client(GetParticipantRequest(channel=entity, participant=me))
            return True
        except UserNotParticipantError:
            return False
        except Exception:
            return False
