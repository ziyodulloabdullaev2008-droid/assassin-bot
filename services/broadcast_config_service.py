from core.state import app_state
from database import load_broadcast_config, save_broadcast_config
from pathlib import Path
import json


def load_broadcast_configs() -> None:
    """Загружает сохранённые конфиги рассылки при старте."""
    config_dir = Path(__file__).resolve().parent.parent
    config_file = config_dir / "broadcast_configs.json"

    if config_file.exists():
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                all_configs = json.load(f)
                for user_id_str, config in all_configs.items():
                    user_id = int(user_id_str)
                    app_state.broadcast_config[user_id] = config
        except Exception:
            pass


def get_broadcast_config(user_id: int) -> dict:
    """Получить конфиг рассылки для пользователя."""
    if user_id not in app_state.broadcast_config:
        config = load_broadcast_config(user_id)
        app_state.broadcast_config[user_id] = config

    config = app_state.broadcast_config[user_id]

    if "text" in config and "texts" not in config:
        if config["text"]:
            config["texts"] = [config["text"]]
        else:
            config["texts"] = []
        del config["text"]

    if "texts" not in config:
        config["texts"] = []
    if "text_mode" not in config:
        config["text_mode"] = "random"
    if "text_index" not in config:
        config["text_index"] = 0
    if "count" not in config:
        config["count"] = 1
    if "interval" not in config:
        config["interval"] = 1
    if "parse_mode" not in config:
        config["parse_mode"] = "HTML"
    if "chat_pause" not in config:
        config["chat_pause"] = "1-3"
    if "plan_limit_count" not in config:
        config["plan_limit_count"] = 0
    if "plan_limit_rest" not in config:
        config["plan_limit_rest"] = 0

    return config


def save_and_get_broadcast_config(user_id: int) -> dict:
    """Получить конфиг рассылки и сохранить в файл."""
    config = get_broadcast_config(user_id)
    save_broadcast_config(user_id, config)
    return config
