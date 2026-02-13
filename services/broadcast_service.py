import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple

from core.state import app_state


def next_broadcast_id() -> int:
    """Возвращает следующий ID рассылки."""
    if app_state.active_broadcasts:
        return max(app_state.active_broadcasts.keys()) + 1
    return 1


def create_broadcast(bid: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Создает запись рассылки."""
    app_state.active_broadcasts[bid] = payload
    return app_state.active_broadcasts[bid]


def get_broadcast(bid: int) -> Optional[Dict[str, Any]]:
    return app_state.active_broadcasts.get(bid)


def list_user_broadcasts(user_id: int, statuses: Optional[Tuple[str, ...]] = None) -> Dict[int, Dict[str, Any]]:
    if statuses:
        return {bid: b for bid, b in app_state.active_broadcasts.items() if b["user_id"] == user_id and b["status"] in statuses}
    return {bid: b for bid, b in app_state.active_broadcasts.items() if b["user_id"] == user_id}


async def set_status(bid: int, status: str) -> None:
    async with app_state.broadcast_update_lock:
        if bid in app_state.active_broadcasts:
            app_state.active_broadcasts[bid]["status"] = status


async def update_progress(bid: int, sent_chats: int, planned_count: Optional[int] = None) -> None:
    async with app_state.broadcast_update_lock:
        if bid in app_state.active_broadcasts:
            app_state.active_broadcasts[bid]["sent_chats"] = sent_chats
            if planned_count is not None:
                app_state.active_broadcasts[bid]["planned_count"] = planned_count


async def mark_error(bid: int, error_message: str, error_type: str = "send_failed") -> None:
    async with app_state.broadcast_update_lock:
        if bid in app_state.active_broadcasts:
            b = app_state.active_broadcasts[bid]
            b["status"] = "error"
            b["error_message"] = error_message
            b["error_type"] = error_type


def cleanup_old_broadcasts(max_age_minutes: int = 120) -> int:
    """Удалить завершенные/ошибочные рассылки из памяти для предотвращения утечки."""
    current_time = datetime.now(timezone.utc)
    to_delete = []

    for bid, broadcast in app_state.active_broadcasts.items():
        if broadcast["status"] in ("completed", "error", "cancelled", "stopped"):
            if "start_time" in broadcast:
                age = (current_time - broadcast["start_time"]).total_seconds() / 60
                if age > max_age_minutes:
                    to_delete.append(bid)
            else:
                to_delete.append(bid)

    for bid in to_delete:
        del app_state.active_broadcasts[bid]

    return len(to_delete)
