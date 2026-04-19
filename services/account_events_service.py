import json
from datetime import datetime, timezone

from services.user_paths import account_events_path


MAX_EVENT_LINES = 500


def append_account_event(
    user_id: int,
    *,
    text: str,
    account_number: int | None = None,
    kind: str = "info",
    level: str = "info",
    broadcast_id: int | None = None,
) -> None:
    path = account_events_path(user_id)
    event = {
        "ts": datetime.now(timezone.utc).timestamp(),
        "account_number": account_number,
        "kind": kind,
        "level": level,
        "broadcast_id": broadcast_id,
        "text": text.strip(),
    }

    existing_lines: list[str] = []
    if path.exists():
        try:
            existing_lines = path.read_text(encoding="utf-8").splitlines()
        except Exception:
            existing_lines = []

    existing_lines.append(json.dumps(event, ensure_ascii=False))
    trimmed_lines = existing_lines[-MAX_EVENT_LINES:]
    path.write_text("\n".join(trimmed_lines) + "\n", encoding="utf-8")


def get_recent_account_events(
    user_id: int,
    *,
    account_number: int | None = None,
    limit: int = 5,
) -> list[dict]:
    path = account_events_path(user_id)
    if not path.exists():
        return []

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []

    events: list[dict] = []
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except Exception:
            continue
        if account_number is not None and event.get("account_number") not in (
            None,
            account_number,
        ):
            continue
        events.append(event)
        if len(events) >= limit:
            break

    return events


def format_recent_account_events(events: list[dict]) -> list[str]:
    formatted: list[str] = []
    for event in events:
        timestamp = event.get("ts")
        if timestamp:
            dt = datetime.fromtimestamp(float(timestamp), tz=timezone.utc).astimezone()
            time_text = dt.strftime("%d.%m %H:%M")
        else:
            time_text = "--:--"

        text = str(event.get("text") or "").strip() or "-"
        formatted.append(f"{time_text} — {text}")

    return formatted
