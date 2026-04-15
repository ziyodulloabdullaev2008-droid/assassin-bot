import json
import os

from services.user_paths import CONFIG_PATH


LOCAL_CONFIG_PATH = CONFIG_PATH


def _load_local_config() -> dict:
    if not LOCAL_CONFIG_PATH.exists():
        return {}

    try:
        raw_config = json.loads(LOCAL_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read local config file: {LOCAL_CONFIG_PATH}"
        ) from exc

    if not isinstance(raw_config, dict):
        raise RuntimeError(
            f"Local config file must contain a JSON object: {LOCAL_CONFIG_PATH}"
        )

    return raw_config


_LOCAL_CONFIG = _load_local_config()


def _get_config_value(
    name: str,
    *,
    cast=str,
    required: bool = True,
    default=None,
):
    value = os.getenv(name)
    if value is None:
        value = _LOCAL_CONFIG.get(name)

    if value in (None, ""):
        if required:
            raise RuntimeError(
                f"Missing required config value '{name}'. "
                f"Set it in environment variables or {LOCAL_CONFIG_PATH.name}."
            )
        return default

    try:
        return cast(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Invalid value for config '{name}': {value!r}") from exc


TOKEN = _get_config_value("TOKEN")
ADMIN_ID = _get_config_value("ADMIN_ID", cast=int, required=False, default=0)
API_ID = _get_config_value("API_ID", cast=int)
API_HASH = _get_config_value("API_HASH")
DEV_LOG_CHAT_ID = _get_config_value(
    "DEV_LOG_CHAT_ID",
    cast=int,
    required=False,
    default=0,
)
