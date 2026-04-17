from __future__ import annotations

import asyncio
import importlib.util
import shutil
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from telethon import TelegramClient
from telethon.network.connection.tcpmtproxy import (
    ConnectionTcpMTProxyIntermediate,
    ConnectionTcpMTProxyRandomizedIntermediate,
)

from services.user_paths import session_base_path


ProxySettings = dict[str, Any]


def parse_proxy_input(raw_value: str) -> ProxySettings:
    value = (raw_value or "").strip()
    if not value:
        raise ValueError("Прокси пустой.")

    if "t.me/proxy" in value.lower():
        return _parse_mtproto_url(value)

    parts = [part.strip() for part in value.split(":")]
    if len(parts) == 2:
        host, port = parts
        return {
            "type": "socks5",
            "host": _validate_host(host),
            "port": _validate_port(port),
            "username": None,
            "password": None,
        }

    if len(parts) == 3 and _looks_like_mtproto_secret(parts[2]):
        host, port, secret = parts
        transport = _detect_mtproto_transport(secret)
        if transport == "fake_tls":
            raise ValueError(
                "MTProto proxy с secret вида 'ee...' (fake TLS / доменный secret) "
                "работает в Telegram Desktop, но не поддерживается Telethon в этом боте. "
                "Используй SOCKS5 или обычный MTProto без доменного secret."
            )
        return {
            "type": "mtproto",
            "host": _validate_host(host),
            "port": _validate_port(port),
            "secret": _normalize_secret(secret),
            "transport": transport,
        }

    if len(parts) == 4:
        host, port, username, password = parts
        if not username or not password:
            raise ValueError("Логин и пароль прокси не должны быть пустыми.")
        return {
            "type": "socks5",
            "host": _validate_host(host),
            "port": _validate_port(port),
            "username": username,
            "password": password,
        }

    raise ValueError(
        "Не понял формат прокси. Поддерживаются host:port, "
        "host:port:login:password, host:port:secret и ссылка t.me/proxy."
    )


def build_telegram_client(
    session_path: str | Path,
    api_id: int,
    api_hash: str,
    proxy_settings: ProxySettings | None = None,
) -> TelegramClient:
    proxy = normalize_proxy_settings(proxy_settings)
    kwargs: dict[str, Any] = {}

    if proxy:
        proxy_type = proxy["type"]
        if proxy_type == "socks5":
            if not _has_socks_support():
                raise RuntimeError(
                    "Для SOCKS5 не найден пакет PySocks/python-socks. "
                    "Установи зависимости из requirements.txt."
                )
            kwargs["proxy"] = (
                "socks5",
                proxy["host"],
                int(proxy["port"]),
                True,
                proxy.get("username"),
                proxy.get("password"),
            )
        elif proxy_type == "mtproto":
            transport = str(proxy.get("transport") or "intermediate")
            if transport == "fake_tls":
                raise RuntimeError(
                    "MTProto proxy с secret вида 'ee...' (fake TLS / доменный secret) "
                    "Telethon в этом боте не поддерживает. Используй SOCKS5 или обычный MTProto."
                )
            kwargs["connection"] = (
                ConnectionTcpMTProxyRandomizedIntermediate
                if transport == "randomized_intermediate"
                else ConnectionTcpMTProxyIntermediate
            )
            kwargs["proxy"] = (
                proxy["host"],
                int(proxy["port"]),
                proxy["secret"],
            )
        else:
            raise ValueError(f"Неизвестный тип прокси: {proxy_type}")

    return TelegramClient(str(session_path), api_id, api_hash, **kwargs)


def normalize_proxy_settings(proxy_settings: ProxySettings | None) -> ProxySettings | None:
    if not proxy_settings:
        return None

    proxy_type = str(proxy_settings.get("type") or "").strip().lower()
    if proxy_type not in {"socks5", "mtproto"}:
        return None

    host = _validate_host(str(proxy_settings.get("host") or ""))
    port = _validate_port(proxy_settings.get("port"))

    if proxy_type == "socks5":
        username = _empty_to_none(proxy_settings.get("username"))
        password = _empty_to_none(proxy_settings.get("password"))
        return {
            "type": "socks5",
            "host": host,
            "port": port,
            "username": username,
            "password": password,
        }

    secret = _normalize_secret(str(proxy_settings.get("secret") or ""))
    transport = _detect_mtproto_transport(secret)
    if transport == "fake_tls":
        raise ValueError(
            "MTProto proxy с secret вида 'ee...' (fake TLS / доменный secret) "
            "не поддерживается Telethon в этом боте."
        )
    return {
        "type": "mtproto",
        "host": host,
        "port": port,
        "secret": secret,
        "transport": transport,
    }


def format_proxy_summary(proxy_settings: ProxySettings | None) -> str:
    proxy = normalize_proxy_settings(proxy_settings)
    if not proxy:
        return "Прокси не задан."

    lines = [
        f"Тип: {'MTProto' if proxy['type'] == 'mtproto' else 'SOCKS5'}",
        f"Сервер: {proxy['host']}",
        f"Порт: {proxy['port']}",
    ]

    if proxy["type"] == "mtproto":
        lines.append(f"Ключ: {mask_secret(proxy['secret'])}")
    elif proxy.get("username"):
        lines.append(f"Логин: {proxy['username']}")
        lines.append("Авторизация: включена")
    else:
        lines.append("Авторизация: не нужна")

    return "\n".join(lines)


def mask_secret(value: str | None) -> str:
    if not value:
        return "—"
    if len(value) <= 12:
        return value
    return f"{value[:6]}...{value[-6:]}"


async def test_session_proxy(
    user_id: int,
    account_number: int,
    api_id: int,
    api_hash: str,
    proxy_settings: ProxySettings | None,
    *,
    timeout: float = 8.0,
) -> tuple[bool, str, int | None]:
    candidates = build_session_candidates(user_id, account_number)
    if not candidates:
        return False, "Файл сессии не найден.", None

    session_file = candidates[0]
    src = Path(f"{session_file}.session")
    clone_base = src.with_name(f"{src.stem}_proxy_check_{int(time.time())}")
    clone_session = Path(f"{clone_base}.session")
    client = None

    try:
        shutil.copy2(src, clone_session)
        client = build_telegram_client(clone_base, api_id, api_hash, proxy_settings)
        started_at = time.perf_counter()
        await asyncio.wait_for(client.connect(), timeout=timeout)
        ping_ms = int((time.perf_counter() - started_at) * 1000)
        if not await client.is_user_authorized():
            return False, "Сессия не авторизована.", ping_ms
        return True, "Подключение прошло успешно.", ping_ms
    except asyncio.TimeoutError:
        return False, "Таймаут подключения.", None
    except Exception as exc:
        return False, str(exc), None
    finally:
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        for extra in (clone_session, Path(f"{clone_base}.session-journal")):
            try:
                if extra.exists():
                    extra.unlink()
            except Exception:
                pass


def build_session_candidates(user_id: int, account_number: int) -> list[Path]:
    candidates: list[Path] = []
    seen: set[str] = set()

    def add_base(path_base: Path) -> None:
        key = str(path_base)
        if key in seen:
            return
        if Path(f"{path_base}.session").exists():
            candidates.append(path_base)
            seen.add(key)

    add_base(session_base_path(user_id, account_number))
    add_base(
        Path(__file__).resolve().parent.parent / f"session_{user_id}_{account_number}"
    )

    user_sessions_dir = session_base_path(user_id, account_number).parent
    for path in user_sessions_dir.glob(f"session_{user_id}_*.session"):
        add_base(path.with_suffix(""))

    for path in Path(__file__).resolve().parent.parent.glob(f"session_{user_id}_*.session"):
        add_base(path.with_suffix(""))

    return candidates


def _parse_mtproto_url(value: str) -> ProxySettings:
    parsed = urlparse(value)
    query = parse_qs(parsed.query)
    host = _validate_host(query.get("server", [""])[0])
    port = _validate_port(query.get("port", [""])[0])
    secret = _normalize_secret(query.get("secret", [""])[0])
    transport = _detect_mtproto_transport(secret)
    if transport == "fake_tls":
        raise ValueError(
            "MTProto proxy по ссылке t.me/proxy использует secret вида 'ee...' "
            "(fake TLS / доменный secret). Telegram Desktop его понимает, "
            "а Telethon в этом боте нет. Используй SOCKS5 или обычный MTProto."
        )
    return {
        "type": "mtproto",
        "host": host,
        "port": port,
        "secret": secret,
        "transport": transport,
    }


def _validate_host(host: str) -> str:
    normalized = (host or "").strip()
    if not normalized:
        raise ValueError("Не указан сервер прокси.")
    return normalized


def _validate_port(port: Any) -> int:
    try:
        value = int(str(port).strip())
    except Exception as exc:
        raise ValueError("Порт прокси должен быть числом.") from exc

    if not 1 <= value <= 65535:
        raise ValueError("Порт прокси должен быть в диапазоне 1-65535.")
    return value


def _normalize_secret(secret: str) -> str:
    normalized = (secret or "").strip()
    if not normalized:
        raise ValueError("Не указан ключ MTProto.")
    return normalized


def _looks_like_mtproto_secret(value: str) -> bool:
    normalized = (value or "").strip()
    if len(normalized) < 8:
        return False
    lowered = normalized.lower()
    return all(ch in "0123456789abcdef" for ch in lowered)


def _detect_mtproto_transport(secret: str) -> str:
    normalized = (secret or "").strip().lower()
    if normalized.startswith("ee"):
        return "fake_tls"
    if normalized.startswith("dd"):
        return "randomized_intermediate"
    return "intermediate"


def _empty_to_none(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _has_socks_support() -> bool:
    return bool(
        importlib.util.find_spec("python_socks") or importlib.util.find_spec("socks")
    )
