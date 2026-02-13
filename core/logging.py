import asyncio
import logging
import random
import re
from typing import Optional


_TELEGRAM_LOG_QUEUE: Optional[asyncio.Queue] = None
_TELEGRAM_LOG_TASK: Optional[asyncio.Task] = None
_TELEGRAM_LOG_HANDLER: Optional[logging.Handler] = None


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    # Reduce noisy Telethon informational chatter in all outputs.
    logging.getLogger("telethon.network.mtprotosender").setLevel(logging.WARNING)
    logging.getLogger("telethon.client.updates").setLevel(logging.WARNING)
    # Keep aiogram only on warnings/errors to avoid per-update spam.
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)
    logging.getLogger("aiogram.dispatcher").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def _should_skip_record(record: logging.LogRecord) -> bool:
    name = (record.name or "").lower()
    if name.startswith("telethon.network.mtprotosender"):
        return True
    if name.startswith("telethon.client.updates"):
        return True
    return False


def _redact_sensitive(text: str) -> str:
    if not text:
        return text
    # Bot token pattern: 123456789:AA...
    text = re.sub(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b", "[REDACTED_TOKEN]", text)
    # Generic API hash-style 32-64 hex symbols.
    text = re.sub(r"\b[a-fA-F0-9]{32,64}\b", "[REDACTED_HASH]", text)
    return text


def _split_chunks(text: str, max_len: int = 3800) -> list[str]:
    if len(text) <= max_len:
        return [text]
    chunks = []
    current = []
    current_len = 0
    for line in text.splitlines(keepends=True):
        if current_len + len(line) > max_len and current:
            chunks.append("".join(current))
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += len(line)
    if current:
        chunks.append("".join(current))
    return chunks


class _TelegramQueueLogHandler(logging.Handler):
    def __init__(self, queue: asyncio.Queue):
        super().__init__(level=logging.INFO)
        self.queue = queue
        self.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        if _should_skip_record(record):
            return
        try:
            msg = self.format(record)
            msg = _redact_sensitive(msg)
            for chunk in _split_chunks(msg):
                try:
                    self.queue.put_nowait(chunk)
                except asyncio.QueueFull:
                    # Drop when overloaded to avoid blocking the app.
                    return
        except Exception:
            return


async def _telegram_log_worker(bot, chat_id: int, queue: asyncio.Queue) -> None:
    while True:
        text = await queue.get()
        try:
            await bot.send_message(chat_id, text, disable_web_page_preview=True)
        except Exception:
            pass
        await asyncio.sleep(random.uniform(0.5, 2.0))


async def start_telegram_log_forwarding(bot, chat_id: Optional[int]) -> None:
    global _TELEGRAM_LOG_QUEUE, _TELEGRAM_LOG_TASK, _TELEGRAM_LOG_HANDLER
    if not chat_id:
        return
    if _TELEGRAM_LOG_TASK and not _TELEGRAM_LOG_TASK.done():
        return

    _TELEGRAM_LOG_QUEUE = asyncio.Queue(maxsize=800)
    _TELEGRAM_LOG_HANDLER = _TelegramQueueLogHandler(_TELEGRAM_LOG_QUEUE)
    logging.getLogger().addHandler(_TELEGRAM_LOG_HANDLER)
    _TELEGRAM_LOG_TASK = asyncio.create_task(_telegram_log_worker(bot, chat_id, _TELEGRAM_LOG_QUEUE))


async def stop_telegram_log_forwarding() -> None:
    global _TELEGRAM_LOG_TASK, _TELEGRAM_LOG_HANDLER
    if _TELEGRAM_LOG_TASK and not _TELEGRAM_LOG_TASK.done():
        _TELEGRAM_LOG_TASK.cancel()
        try:
            await _TELEGRAM_LOG_TASK
        except Exception:
            pass
    _TELEGRAM_LOG_TASK = None

    if _TELEGRAM_LOG_HANDLER:
        try:
            logging.getLogger().removeHandler(_TELEGRAM_LOG_HANDLER)
        except Exception:
            pass
    _TELEGRAM_LOG_HANDLER = None
