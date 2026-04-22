import html

from core.config import ADMIN_ID
from core.state import app_state


def _user_identity_lines(
    *,
    user_id: int,
    username: str | None = None,
    first_name: str | None = None,
) -> list[str]:
    lines = [f"👤 Пользователь: <code>{user_id}</code>"]
    if first_name:
        lines.append(f"Имя: <b>{html.escape(first_name)}</b>")
    if username:
        lines.append(f"Username: @{html.escape(username.lstrip('@'))}")
    return lines


async def send_admin_event(title: str, lines: list[str]) -> None:
    if not ADMIN_ID:
        return

    bot = app_state.bot
    if not bot:
        return

    text = "\n".join([title, "", *[line for line in lines if line]])
    try:
        await bot.send_message(
            ADMIN_ID,
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        pass


async def notify_new_bot_user(
    *,
    user_id: int,
    username: str | None = None,
    first_name: str | None = None,
    source: str,
) -> None:
    await send_admin_event(
        "🆕 <b>Новый пользователь в боте</b>",
        [
            *_user_identity_lines(
                user_id=user_id,
                username=username,
                first_name=first_name,
            ),
            f"Источник: <b>{html.escape(source)}</b>",
        ],
    )
