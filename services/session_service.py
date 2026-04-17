import asyncio
import shutil
import time
from pathlib import Path
from typing import Optional

from telethon import TelegramClient

from core.logging import get_logger
from core.state import app_state
from database import (
    add_or_update_user,
    add_user_account_with_number,
    get_account_proxy,
    get_all_users,
    get_user_accounts,
)
from services.proxy_service import build_session_candidates, build_telegram_client
from services.user_paths import BASE_DIR

logger = get_logger("session_service")


async def recover_sessions_from_files(api_id: int, api_hash: str) -> bool:
    """Восстанавливает данные аккаунтов из файлов сессий если БД пуста."""
    logger.info("Проверяю восстановление сессий из файлов...")
    users = get_all_users()
    if users:
        # Normal startup path: accounts are loaded from DB.
        # Recovery from raw session files is only for empty DB/bootstrap case.
        logger.info(
            "Пропускаю восстановление из файлов: БД уже содержит пользователей (%s)",
            len(users),
        )
        return False

    session_files = list(BASE_DIR.glob("*/sessions/session_*.session"))
    session_files.extend(
        list(Path(__file__).resolve().parent.parent.glob("session_*.session"))
    )
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

            client = build_telegram_client(
                session_path,
                api_id,
                api_hash,
                get_account_proxy(user_id, account_number),
            )
            try:
                await asyncio.wait_for(client.connect(), timeout=5.0)
                if await client.is_user_authorized():
                    me = await client.get_me()
                    add_or_update_user(
                        user_id, me.username or "unknown", me.first_name or "User"
                    )
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
                logger.warning(
                    "Ошибка восстановления сессии %s: %s", session_path.name, exc
                )
                await _safe_disconnect(client)
        except Exception as exc:
            logger.warning("Ошибка обработки файла %s: %s", session_path.name, exc)

    if recovered:
        logger.info("Восстановлено аккаунтов: %s", recovered)
    return recovered > 0


async def load_saved_sessions(
    api_id: int,
    api_hash: str,
    on_loaded: Optional[callable] = None,
    connect_on_start: bool = True,
) -> None:
    """Загружает сохраненные сессии при старте.

    connect_on_start=False: ленивый режим, клиенты создаются без connect().
    """
    users = get_all_users()
    logger.info("Найдено пользователей в БД: %s", len(users))

    accounts_to_load = []
    for user_id, _, _, _ in users:
        accounts = get_user_accounts(user_id)
        if not accounts:
            continue
        for account_number, _, _, _, is_active in accounts:
            if not is_active:
                continue
            accounts_to_load.append((user_id, account_number))

    if accounts_to_load:
        loaded = 0
        for user_id, account_number in accounts_to_load:
            ok = await _load_single_session(
                api_id,
                api_hash,
                user_id,
                account_number,
                connect_on_start=connect_on_start,
            )
            if ok:
                loaded += 1
            await asyncio.sleep(0.15)
        logger.info("Загружены аккаунты из БД: %s/%s", loaded, len(accounts_to_load))

    if on_loaded:
        await on_loaded()


async def ensure_connected_client(
    user_id: int,
    account_number: int,
    *,
    api_id: int | None = None,
    api_hash: str | None = None,
    connect_timeout: float = 6.0,
):
    """Return an authorized client, reconnecting or reloading it when possible."""
    client = app_state.user_authenticated.get(user_id, {}).get(account_number)

    if client is None and api_id and api_hash:
        loaded = await _load_single_session(
            api_id,
            api_hash,
            user_id,
            account_number,
            connect_on_start=True,
        )
        if loaded:
            client = app_state.user_authenticated.get(user_id, {}).get(account_number)

    if client is None:
        return None

    try:
        if not client.is_connected():
            await asyncio.wait_for(client.connect(), timeout=connect_timeout)
        if await client.is_user_authorized():
            return client
    except Exception:
        pass

    await _safe_disconnect(client)

    async with app_state.user_authenticated_lock:
        user_clients = app_state.user_authenticated.get(user_id, {})
        if user_clients.get(account_number) is client:
            user_clients.pop(account_number, None)
            if not user_clients:
                app_state.user_authenticated.pop(user_id, None)

    if not api_id or not api_hash:
        return None

    loaded = await _load_single_session(
        api_id,
        api_hash,
        user_id,
        account_number,
        connect_on_start=True,
    )
    if not loaded:
        return None

    return app_state.user_authenticated.get(user_id, {}).get(account_number)


async def drop_cached_client(user_id: int, account_number: int) -> None:
    client = app_state.user_authenticated.get(user_id, {}).get(account_number)
    if client:
        await _safe_disconnect(client)

    async with app_state.user_authenticated_lock:
        user_clients = app_state.user_authenticated.get(user_id, {})
        user_clients.pop(account_number, None)
        if not user_clients:
            app_state.user_authenticated.pop(user_id, None)


async def _load_single_session(
    api_id: int,
    api_hash: str,
    user_id: int,
    account_number: int,
    *,
    connect_on_start: bool = True,
) -> bool:
    try:
        # Skip duplicates when account is already restored/loaded in memory.
        existing = app_state.user_authenticated.get(user_id, {}).get(account_number)
        if existing:
            try:
                if existing.is_connected():
                    return True
            except Exception:
                return True

        accounts = get_user_accounts(user_id)
        account_info = next((a for a in accounts if a[0] == account_number), None)
        if not account_info:
            return False
        if len(account_info) >= 5 and not account_info[4]:
            return False

        candidates = build_session_candidates(user_id, account_number)
        if not candidates:
            logger.warning("Файл сессии не найден для %s_%s", user_id, account_number)
            return False

        proxy_settings = get_account_proxy(user_id, account_number)

        for session_file in candidates:
            if not connect_on_start:
                try:
                    client = build_telegram_client(
                        session_file,
                        api_id,
                        api_hash,
                        proxy_settings,
                    )
                    async with app_state.user_authenticated_lock:
                        app_state.user_authenticated.setdefault(user_id, {})
                        app_state.user_authenticated[user_id][account_number] = client
                    return True
                except Exception as exc:
                    logger.warning(
                        "Ошибка ленивой загрузки сессии %s_%s: %s",
                        user_id,
                        account_number,
                        exc,
                    )
                    continue

            for attempt in range(2):
                client = build_telegram_client(
                    session_file,
                    api_id,
                    api_hash,
                    proxy_settings,
                )
                try:
                    await asyncio.wait_for(client.connect(), timeout=6.0)
                    if await client.is_user_authorized():
                        async with app_state.user_authenticated_lock:
                            app_state.user_authenticated.setdefault(user_id, {})
                            app_state.user_authenticated[user_id][account_number] = (
                                client
                            )
                        return True
                    await client.disconnect()
                    if attempt == 1:
                        logger.warning(
                            "Сессия не авторизована: %s (для аккаунта %s_%s)",
                            f"{session_file}.session",
                            user_id,
                            account_number,
                        )
                    break
                except asyncio.TimeoutError:
                    await _safe_disconnect(client)
                    if attempt < 1:
                        await asyncio.sleep(0.6)
                        continue
                    logger.warning(
                        "Таймаут загрузки сессии %s_%s", user_id, account_number
                    )
                    break
                except Exception as exc:
                    exc_str = str(exc).lower()
                    await _safe_disconnect(client)
                    if "database is locked" in exc_str:
                        cloned_ok = await _try_load_from_clone(
                            session_file,
                            api_id,
                            api_hash,
                            user_id,
                            account_number,
                            proxy_settings,
                        )
                        if cloned_ok:
                            return True
                        if attempt < 1:
                            await asyncio.sleep(0.6)
                            continue
                        logger.warning(
                            "Сессия %s_%s заблокирована (database is locked). "
                            "Проверь, не запущен ли второй процесс бота.",
                            user_id,
                            account_number,
                        )
                        break
                    logger.warning(
                        "Ошибка загрузки сессии %s_%s: %s", user_id, account_number, exc
                    )
                    break
    except Exception as exc:
        if "database is locked" not in str(exc).lower():
            logger.warning("Ошибка загрузки одной сессии: %s", exc)
    return False


async def _safe_disconnect(client: TelegramClient) -> None:
    try:
        await client.disconnect()
    except Exception:
        pass


async def _try_load_from_clone(
    session_file: Path,
    api_id: int,
    api_hash: str,
    user_id: int,
    account_number: int,
    proxy_settings: dict | None,
) -> bool:
    src = Path(f"{session_file}.session")
    if not src.exists():
        return False

    clone_base = src.with_name(f"{src.stem}_startup_clone_{int(time.time())}")
    clone_session = Path(f"{clone_base}.session")
    client = None
    try:
        shutil.copy2(src, clone_session)
        client = build_telegram_client(clone_base, api_id, api_hash, proxy_settings)
        await asyncio.wait_for(client.connect(), timeout=6.0)
        if not await client.is_user_authorized():
            await _safe_disconnect(client)
            return False

        async with app_state.user_authenticated_lock:
            app_state.user_authenticated.setdefault(user_id, {})
            app_state.user_authenticated[user_id][account_number] = client

        logger.warning(
            "Сессия %s_%s загружена из clone-файла из-за lock оригинала",
            user_id,
            account_number,
        )
        return True
    except Exception:
        if client:
            await _safe_disconnect(client)
        return False
