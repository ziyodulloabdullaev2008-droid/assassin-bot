from __future__ import annotations

import os
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_RUNTIME_DIR = ROOT_DIR / "runtime"


def _resolve_runtime_dir() -> Path:
    env_value = (os.getenv("ASSASSIN_RUNTIME_DIR") or "").strip()
    if env_value:
        return Path(env_value).expanduser().resolve()

    if DEFAULT_RUNTIME_DIR.exists():
        return DEFAULT_RUNTIME_DIR.resolve()

    return ROOT_DIR


RUNTIME_DIR = _resolve_runtime_dir()
BASE_DIR = RUNTIME_DIR / "users"
LOGS_DIR = RUNTIME_DIR / "logs"
COMMON_DIR = BASE_DIR / "_common"

_CONFIG_ENV = (os.getenv("ASSASSIN_CONFIG_PATH") or "").strip()
CONFIG_PATH = (
    Path(_CONFIG_ENV).expanduser().resolve()
    if _CONFIG_ENV
    else (
        (RUNTIME_DIR / "config.local.json")
        if (RUNTIME_DIR / "config.local.json").exists()
        else (ROOT_DIR / "config.local.json")
    )
)
CONFIG_EXAMPLE_PATH = (
    (RUNTIME_DIR / "config.local.example.json")
    if (RUNTIME_DIR / "config.local.example.json").exists()
    else (ROOT_DIR / "config.local.example.json")
)
LEGACY_BROADCAST_CONFIGS_PATH = RUNTIME_DIR / "broadcast_configs.json"

BASE_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
COMMON_DIR.mkdir(parents=True, exist_ok=True)


def ensure_runtime_dir() -> Path:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    COMMON_DIR.mkdir(parents=True, exist_ok=True)
    return RUNTIME_DIR


def ensure_user_dir(user_id: int) -> Path:
    user_dir = BASE_DIR / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    return user_dir


def user_sessions_dir(user_id: int) -> Path:
    sessions_dir = ensure_user_dir(user_id) / "sessions"
    sessions_dir.mkdir(parents=True, exist_ok=True)
    return sessions_dir


def user_broadcast_dir(user_id: int) -> Path:
    broadcast_dir = ensure_user_dir(user_id) / "broadcast"
    broadcast_dir.mkdir(parents=True, exist_ok=True)
    return broadcast_dir


def user_events_dir(user_id: int) -> Path:
    events_dir = ensure_user_dir(user_id) / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    return events_dir


def session_base_path(user_id: int, account_number: int) -> Path:
    return user_sessions_dir(user_id) / f"session_{user_id}_{account_number}"


def temp_session_base_path(user_id: int, login_id: int | str) -> Path:
    return user_sessions_dir(user_id) / f"temp_session_{user_id}_{login_id}"


def broadcast_config_path(user_id: int) -> Path:
    return user_broadcast_dir(user_id) / "broadcast_config.json"


def broadcast_config_backup_path(user_id: int) -> Path:
    return user_broadcast_dir(user_id) / "broadcast_config.json.backup"


def broadcast_profiles_path(user_id: int) -> Path:
    return user_broadcast_dir(user_id) / "broadcast_profiles.json"


def active_broadcasts_path(user_id: int) -> Path:
    return user_broadcast_dir(user_id) / "active_broadcasts.json"


def account_events_path(user_id: int) -> Path:
    return user_events_dir(user_id) / "account_events.jsonl"
