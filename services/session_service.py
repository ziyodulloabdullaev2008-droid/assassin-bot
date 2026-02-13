import asyncio
from pathlib import Path
from typing import Optional

from telethon import TelegramClient

from core.logging import get_logger
from core.state import app_state
from database import (
    add_or_update_user,
    add_user_account_with_number,
    get_all_users,
    get_user_accounts,
)
from services.user_paths import BASE_DIR, session_base_path, user_sessions_dir

logger = get_logger("session_service")


async def recover_sessions_from_files(api_id: int, api_hash: str) -> bool:
    """Восстанавливает данные аккаунтов из файлов сессий если БД пуста."""
    logger.info("Проверяю восстановление сессий из файлов...")
    session_files = list(BASE_DIR.glob("*/sessions/session_*.session"))
    session_files.extend(list(Path(__file__).resolve().parent.parent.glob("session_*.session")))
    if not session_files:
        return False

    recovered = 0
    seen_accounts = set()
    for session_path in session_files:
        try:
            filename = session_path.stem
            if not filename.startswith("session_"):
                continue

            parts = filename.replace("session_", "").split("_")
            if len(parts) != 2:
                continue

            user_id = int(parts[0])
            account_number = int(parts[1])
            key = (user_id, account_number)
            if key in seen_accounts:
                continue
            seen_accounts.add(key)

            client = TelegramClient(str(session_path), api_id, api_hash)
            try:
                await asyncio.wait_for(client.connect(), timeout=5.0)
                if await client.is_user_authorized():
                    me = await client.get_me()
                    add_or_update_user(user_id, me.username or "unknown", me.first_name or "User")
                    add_user_account_with_number(
                        user_id,
                        account_number,
                        me.id,
                        me.username or "unknown",
                        me.first_name or "User",
                        None,
                    )
                    async with app_state.user_authenticated_lock:
                        app_state.user_authenticated.setdefault(user_id, {})
                        app_state.user_authenticated[user_id][account_number] = client
                    recovered += 1
                    logger.info("Восстановлен аккаунт %s_%s", user_id, account_number)
                else:
                    await client.disconnect()
            except asyncio.TimeoutError:
                await _safe_disconnect(client)
            except Exception as exc:
                logger.warning("Ошибка восстановления сессии %s: %s", session_path.name, exc)
                await _safe_disconnect(client)
        except Exception as exc:
            logger.warning("Ошибка обработки файла %s: %s", session_path.name, exc)

    if recovered:
        logger.info("Восстановлено аккаунтов: %s", recovered)
    return recovered > 0


async def load_saved_sessions(api_id: int, api_hash: str, on_loaded: Optional[callable] = None) -> None:
    """Загружает сохраненные сессии при старте."""
    users = get_all_users()
    logger.info("Найдено пользователей в БД: %s", len(users))

    tasks = []
    for user_id, _, _, _ in users:
        accounts = get_user_accounts(user_id)
        if not accounts:
            continue
        for account_number, _, _, _, is_active in accounts:
            if not is_active:
                continue
            tasks.append(_load_single_session(api_id, api_hash, user_id, account_number))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Загружены аккаунты из БД")

    if on_loaded:
        await on_loaded()


async def _load_single_session(api_id: int, api_hash: str, user_id: int, account_number: int) -> None:
    try:
        # Skip duplicates when account is already restored/loaded in memory.
        existing = app_state.user_authenticated.get(user_id, {}).get(account_number)
        if existing:
            try:
                if existing.is_connected():
                    return
            except Exception:
                return

        accounts = get_user_accounts(user_id)
        account_info = next((a for a in accounts if a[0] == account_number), None)
        if not account_info:
            return
        if len(account_info) >= 5 and not account_info[4]:
            return

        session_file = session_base_path(user_id, account_number)
        session_file_with_ext = Path(f"{session_file}.session")

        if not session_file_with_ext.exists():
            old_session_file = (
                Path(__file__).resolve().parent.parent
                / f"session_{user_id}_{account_number}"
            )
            old_session_file_with_ext = Path(f"{old_session_file}.session")
            if old_session_file_with_ext.exists():
                try:
                    user_sessions_dir(user_id)
                    old_session_file_with_ext.rename(session_file_with_ext)
                except Exception:
                    session_file = old_session_file
                    session_file_with_ext = old_session_file_with_ext

        if not session_file_with_ext.exists():
            return

        client = TelegramClient(str(session_file), api_id, api_hash)
        try:
            await asyncio.wait_for(client.connect(), timeout=5.0)
            if await client.is_user_authorized():
                async with app_state.user_authenticated_lock:
                    app_state.user_authenticated.setdefault(user_id, {})
                    app_state.user_authenticated[user_id][account_number] = client
            else:
                await client.disconnect()
        except asyncio.TimeoutError:
            await _safe_disconnect(client)
        except Exception as exc:
            if "database is locked" not in str(exc).lower():
                logger.warning("Ошибка загрузки сессии %s_%s: %s", user_id, account_number, exc)
            await _safe_disconnect(client)
    except Exception as exc:
        if "database is locked" not in str(exc).lower():
            logger.warning("Ошибка загрузки одной сессии: %s", exc)


async def _safe_disconnect(client: TelegramClient) -> None:
    try:
        await client.disconnect()
    except Exception:
        pass
