import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from core.state import app_state
from services.user_paths import BASE_DIR, active_broadcasts_path


ACTIVE_STATUSES = {"running", "paused"}
FINISHED_STATUSES = {"completed", "error", "cancelled", "stopped"}


def _serialize_broadcast(payload: Dict[str, Any]) -> Dict[str, Any]:
    serialized = dict(payload)
    start_time = serialized.get("start_time")
    if isinstance(start_time, datetime):
        serialized["start_time"] = start_time.isoformat()
    return serialized


def _deserialize_broadcast(payload: Dict[str, Any]) -> Dict[str, Any]:
    restored = dict(payload)
    start_time = restored.get("start_time")
    if isinstance(start_time, str):
        try:
            restored["start_time"] = datetime.fromisoformat(start_time)
        except ValueError:
            restored["start_time"] = datetime.now(timezone.utc)

    if restored.get("status") in ACTIVE_STATUSES:
        restored["status"] = "paused"
        restored["resume_required"] = True

    return restored


def _iter_user_ids_with_storage() -> set[int]:
    user_ids: set[int] = {broadcast["user_id"] for broadcast in app_state.active_broadcasts.values()}

    if BASE_DIR.exists():
        for child in BASE_DIR.iterdir():
            if child.is_dir() and child.name.isdigit():
                user_ids.add(int(child.name))

    return user_ids


def _save_user_broadcasts(user_id: int) -> None:
    path = active_broadcasts_path(user_id)
    payload = {
        str(bid): _serialize_broadcast(broadcast)
        for bid, broadcast in app_state.active_broadcasts.items()
        if broadcast.get("user_id") == user_id
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def save_all_broadcasts() -> None:
    for user_id in _iter_user_ids_with_storage():
        _save_user_broadcasts(user_id)


def load_persisted_broadcasts() -> int:
    restored_count = 0

    for user_dir in sorted(BASE_DIR.iterdir()) if BASE_DIR.exists() else []:
        if not user_dir.is_dir() or not user_dir.name.isdigit():
            continue

        path = active_broadcasts_path(int(user_dir.name))
        if not path.exists():
            continue

        try:
            raw_payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        if not isinstance(raw_payload, dict):
            continue

        for bid_text, payload in raw_payload.items():
            if not isinstance(payload, dict):
                continue

            try:
                bid = int(bid_text)
            except (TypeError, ValueError):
                continue

            app_state.active_broadcasts[bid] = _deserialize_broadcast(payload)
            restored_count += 1

    return restored_count


def next_broadcast_id() -> int:
    """Возвращает следующий ID рассылки."""
    if app_state.active_broadcasts:
        return max(app_state.active_broadcasts.keys()) + 1
    return 1


def create_broadcast(bid: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Создает запись рассылки."""
    app_state.active_broadcasts[bid] = payload
    _save_user_broadcasts(payload["user_id"])
    return app_state.active_broadcasts[bid]


def get_broadcast(bid: int) -> Optional[Dict[str, Any]]:
    return app_state.active_broadcasts.get(bid)


def get_broadcast_task(bid: int):
    return app_state.broadcast_tasks.get(bid)


def register_broadcast_task(bid: int, task) -> None:
    app_state.broadcast_tasks[bid] = task


def discard_broadcast_task(bid: int) -> None:
    app_state.broadcast_tasks.pop(bid, None)


def list_user_broadcasts(
    user_id: int, statuses: Optional[Tuple[str, ...]] = None
) -> Dict[int, Dict[str, Any]]:
    if statuses:
        return {
            bid: b
            for bid, b in app_state.active_broadcasts.items()
            if b["user_id"] == user_id and b["status"] in statuses
        }
    return {
        bid: b
        for bid, b in app_state.active_broadcasts.items()
        if b["user_id"] == user_id
    }


async def set_status(bid: int, status: str) -> None:
    async with app_state.broadcast_update_lock:
        if bid in app_state.active_broadcasts:
            app_state.active_broadcasts[bid]["status"] = status
            if status != "paused":
                app_state.active_broadcasts[bid].pop("resume_required", None)
            _save_user_broadcasts(app_state.active_broadcasts[bid]["user_id"])


async def update_progress(
    bid: int, sent_chats: int, planned_count: Optional[int] = None
) -> None:
    async with app_state.broadcast_update_lock:
        if bid in app_state.active_broadcasts:
            broadcast = app_state.active_broadcasts[bid]
            broadcast["sent_chats"] = sent_chats
            if planned_count is not None:
                broadcast["planned_count"] = planned_count
            _save_user_broadcasts(broadcast["user_id"])


async def update_broadcast_fields(bid: int, **fields: Any) -> None:
    async with app_state.broadcast_update_lock:
        if bid in app_state.active_broadcasts:
            broadcast = app_state.active_broadcasts[bid]
            broadcast.update(fields)
            _save_user_broadcasts(broadcast["user_id"])


async def mark_error(
    bid: int, error_message: str, error_type: str = "send_failed"
) -> None:
    async with app_state.broadcast_update_lock:
        if bid in app_state.active_broadcasts:
            broadcast = app_state.active_broadcasts[bid]
            broadcast["status"] = "error"
            broadcast["error_message"] = error_message
            broadcast["error_type"] = error_type
            _save_user_broadcasts(broadcast["user_id"])


def delete_broadcast(bid: int) -> None:
    broadcast = app_state.active_broadcasts.pop(bid, None)
    discard_broadcast_task(bid)
    if broadcast:
        _save_user_broadcasts(broadcast["user_id"])


def cleanup_old_broadcasts(max_age_minutes: int = 120) -> int:
    """Удалить завершенные/ошибочные рассылки из памяти для предотвращения утечки."""
    current_time = datetime.now(timezone.utc)
    to_delete = []

    for bid, broadcast in app_state.active_broadcasts.items():
        if broadcast["status"] in FINISHED_STATUSES:
            if "start_time" in broadcast:
                start_time = broadcast["start_time"]
                if isinstance(start_time, str):
                    try:
                        start_time = datetime.fromisoformat(start_time)
                    except ValueError:
                        start_time = current_time
                age = (current_time - start_time).total_seconds() / 60
                if age > max_age_minutes:
                    to_delete.append(bid)
            else:
                to_delete.append(bid)

    for bid in to_delete:
        delete_broadcast(bid)

    return len(to_delete)
