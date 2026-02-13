from __future__ import annotations

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent / "users"


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
