from __future__ import annotations



import json

from pathlib import Path



from core.state import app_state

from database import (

    get_broadcast_chats,

    add_broadcast_chat,

    remove_broadcast_chat,

    save_broadcast_config,

)

from services.broadcast_config_service import get_broadcast_config



from services.user_paths import broadcast_profiles_path, user_broadcast_dir


LEGACY_CONFIG_FILE = Path(__file__).resolve().parent.parent / "broadcast_profiles.json"


def _load_user_store(user_id: int) -> dict:
    user_broadcast_dir(user_id)
    config_file = broadcast_profiles_path(user_id)

    if config_file.exists():
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"active_id": 0, "next_id": 1, "configs": {}}

    # Миграция из старого общего файла
    if LEGACY_CONFIG_FILE.exists():
        try:
            with open(LEGACY_CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            store = data.get("users", {}).get(str(user_id))
            if store:
                with open(config_file, "w", encoding="utf-8") as f:
                    json.dump(store, f, ensure_ascii=False, indent=2)
                return store
        except Exception:
            pass

    return {"active_id": 0, "next_id": 1, "configs": {}}


def _save_user_store(user_id: int, store: dict) -> None:
    user_broadcast_dir(user_id)
    config_file = broadcast_profiles_path(user_id)
    with open(config_file, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2)


def list_configs(user_id: int) -> list[tuple[int, str, bool]]:

    store = _load_user_store(user_id)

    active_id = int(store.get("active_id", 0))

    items = [(0, "По умолчанию", active_id == 0)]

    for cid_str, cfg in sorted(store.get("configs", {}).items(), key=lambda x: int(x[0])):

        cid = int(cid_str)

        name = cfg.get("name", f"Конфиг {cid}")

        items.append((cid, name, cid == active_id))

    return items





def get_active_config_id(user_id: int) -> int:

    store = _load_user_store(user_id)

    return int(store.get("active_id", 0))





def _snapshot_current(user_id: int) -> dict:

    config = get_broadcast_config(user_id)

    chats = get_broadcast_chats(user_id)

    return {"config": config, "chats": chats}





def _apply_snapshot(user_id: int, config: dict, chats: list[tuple]) -> None:

    save_broadcast_config(user_id, config)

    app_state.broadcast_config[user_id] = config

    existing = get_broadcast_chats(user_id)

    for chat_id, _ in existing:

        remove_broadcast_chat(user_id, chat_id)

    for chat_id, chat_name in chats:

        add_broadcast_chat(user_id, chat_id, chat_name)





def ensure_active_config(user_id: int) -> int:

    store = _load_user_store(user_id)

    active_id = int(store.get("active_id", 0))

    if active_id != 0:

        return active_id



    snap = _snapshot_current(user_id)

    next_id = int(store.get("next_id", 1))

    name = f"Конфиг {next_id}"

    store["configs"][str(next_id)] = {

        "name": name,

        "config": snap["config"],

        "chats": snap["chats"],

    }

    store["active_id"] = next_id

    store["next_id"] = next_id + 1

    _save_user_store(user_id, store)

    return next_id





def sync_active_config_from_db(user_id: int) -> None:

    store = _load_user_store(user_id)

    active_id = int(store.get("active_id", 0))

    if active_id == 0:

        return

    snap = _snapshot_current(user_id)

    cfg = store.get("configs", {}).get(str(active_id))

    if cfg is None:

        return

    cfg["config"] = snap["config"]

    cfg["chats"] = snap["chats"]

    store["configs"][str(active_id)] = cfg

    _save_user_store(user_id, store)





def set_active_config(user_id: int, config_id: int) -> None:

    store = _load_user_store(user_id)

    if config_id == 0:

        default_config = {

            "texts": [],

            "text_mode": "random",

            "text_index": 0,

            "count": 1,

            "interval": 1,

            "parse_mode": "HTML",

            "chat_pause": "1-3",

        }

        _apply_snapshot(user_id, default_config, [])

        store["active_id"] = 0

        _save_user_store(user_id, store)

        return



    cfg = store.get("configs", {}).get(str(config_id))

    if not cfg:

        return

    _apply_snapshot(user_id, cfg.get("config", {}), cfg.get("chats", []))

    store["active_id"] = config_id

    _save_user_store(user_id, store)





def rename_config(user_id: int, config_id: int, new_name: str) -> bool:

    store = _load_user_store(user_id)

    cfg = store.get("configs", {}).get(str(config_id))

    if not cfg:

        return False

    cfg["name"] = new_name

    store["configs"][str(config_id)] = cfg

    _save_user_store(user_id, store)

    return True





def delete_config(user_id: int, config_id: int) -> bool:

    store = _load_user_store(user_id)

    if str(config_id) not in store.get("configs", {}):

        return False

    del store["configs"][str(config_id)]

    if int(store.get("active_id", 0)) == config_id:

        store["active_id"] = 0

    _save_user_store(user_id, store)

    return True





def get_config_detail(user_id: int, config_id: int) -> dict | None:

    store = _load_user_store(user_id)

    if config_id == 0:

        default_config = {

            "texts": [],

            "text_mode": "random",

            "text_index": 0,

            "count": 1,

            "interval": 1,

            "parse_mode": "HTML",

            "chat_pause": "1-3",

        }

        return {"name": "По умолчанию", "config": default_config, "chats": []}

    return store.get("configs", {}).get(str(config_id))





def export_config_payload(user_id: int, config_id: int, include_chats: bool) -> dict | None:

    cfg = get_config_detail(user_id, config_id)

    if not cfg:

        return None

    payload = {

        "version": 1,

        "name": cfg.get("name", "Конфиг"),

        "config": cfg.get("config", {}),

        "include_chats": include_chats,

    }

    if include_chats:

        payload["chats"] = cfg.get("chats", [])

    return payload





def import_config_payload(user_id: int, payload: dict) -> int | None:

    if not isinstance(payload, dict):

        return None

    name = payload.get("name") or "Конфиг"

    config = payload.get("config")

    if not isinstance(config, dict):

        return None

    chats_raw = payload.get("chats", [])

    chats = []

    if isinstance(chats_raw, list):

        for item in chats_raw:

            if not isinstance(item, (list, tuple)) or len(item) < 2:

                continue

            chat_id_raw, chat_name_raw = item[0], item[1]

            try:

                chat_id = int(chat_id_raw)

            except (TypeError, ValueError):

                continue

            chat_name = str(chat_name_raw)

            chats.append((chat_id, chat_name))



    store = _load_user_store(user_id)

    next_id = int(store.get("next_id", 1))

    store["configs"][str(next_id)] = {

        "name": name,

        "config": config,

        "chats": chats,

    }

    store["next_id"] = next_id + 1

    _save_user_store(user_id, store)

    return next_id



