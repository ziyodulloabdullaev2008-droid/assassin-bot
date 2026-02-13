import asyncio
from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class AppState:
    # Locks
    user_authenticated_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    broadcast_update_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    mention_update_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # In-memory session/client caches
    user_clients: Dict[int, Any] = field(default_factory=dict)
    user_hashes: Dict[int, str] = field(default_factory=dict)
    user_code_input: Dict[int, str] = field(default_factory=dict)
    user_authenticated: Dict[int, Dict[int, Any]] = field(default_factory=dict)

    # Last dialogs cache
    user_last_dialogs: Dict[int, set] = field(default_factory=dict)

    # Excel files cache
    user_chats_files: Dict[int, Dict[int, bytes]] = field(default_factory=dict)

    # Broadcasts
    active_broadcasts: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    broadcast_config: Dict[int, Dict[str, Any]] = field(default_factory=dict)

    # Mention monitors
    mention_monitors: Dict[int, Dict[int, asyncio.Task]] = field(default_factory=dict)

    # Auto-join system
    joins_enabled: Dict[int, bool] = field(default_factory=dict)
    joins_queue: Dict[int, list] = field(default_factory=dict)
    joins_lock: Dict[int, asyncio.Lock] = field(default_factory=dict)
    joins_task: Dict[int, asyncio.Task] = field(default_factory=dict)
    joins_seen: Dict[int, set] = field(default_factory=dict)
    joins_target_accounts: Dict[int, set] = field(default_factory=dict)
    joins_delay_config: Dict[int, Dict[str, int]] = field(default_factory=dict)


app_state = AppState()
