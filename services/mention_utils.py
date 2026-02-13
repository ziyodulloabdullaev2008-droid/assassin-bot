import asyncio


def normalize_chat_id(chat_id: int) -> int:
    """Нормализует ID чата в стандартный формат."""
    chat_id = int(chat_id)
    if chat_id < 0:
        if str(chat_id).startswith("-100"):
            return int(str(chat_id)[4:])
        return abs(chat_id)
    return chat_id


async def delete_message_after_delay(message, delay: int = 2) -> None:
    try:
        await asyncio.sleep(delay)
        await message.delete()
    except Exception:
        pass
