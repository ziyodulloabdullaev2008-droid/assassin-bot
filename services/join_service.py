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


def _save_settings(user_id: int) -> None:
    path = joins_settings_path(user_id)
    payload = {
        "enabled": bool(app_state.joins_enabled.get(user_id, False)),
        "target_accounts": sorted(list(app_state.joins_target_accounts.get(user_id, set()))),
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
            await _handle_join_request(client, req)
            await asyncio.sleep(random.uniform(10, 15))

        await asyncio.sleep(random.uniform(5, 10))


async def _handle_join_request(client, req: dict) -> None:
    links = req.get("links") or []
    usernames = req.get("usernames") or []
    chat_id = req.get("chat_id")

    # Try invite links first
    for link in links:
        invite_hash = _extract_invite_hash(link)
        if invite_hash:
            try:
                try:
                    entity = await client.get_entity(link)
                    if await _is_already_participant(client, entity):
                        return
                except Exception:
                    pass
                await client(ImportChatInviteRequest(invite_hash))
                return
            except UserAlreadyParticipantError:
                return
            except Exception:
                continue

    # Try usernames
    for username in usernames:
        try:
            entity = await client.get_entity(username)
            if await _is_already_participant(client, entity):
                return
            await client(JoinChannelRequest(entity))
            return
        except UserAlreadyParticipantError:
            return
        except Exception:
            continue

    # Try by chat_id
    if chat_id is not None:
        try:
            entity = await client.get_entity(chat_id)
            if await _is_already_participant(client, entity):
                return
            await client(JoinChannelRequest(entity))
            return
        except UserAlreadyParticipantError:
            return
        except Exception:
            return


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
