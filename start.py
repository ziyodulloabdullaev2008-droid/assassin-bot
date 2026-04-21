import json
import runpy
import subprocess
import sys
from pathlib import Path

from services.user_paths import CONFIG_EXAMPLE_PATH, CONFIG_PATH, ensure_runtime_dir

ROOT = Path(__file__).resolve().parent
REQUIREMENTS_FILE = ROOT / "requirements.txt"
CONFIG_FILE = CONFIG_PATH
CONFIG_EXAMPLE_FILE = CONFIG_EXAMPLE_PATH

REQUIRED_MODULES = (
    "aiogram",
    "telethon",
    "openpyxl",
)

PLACEHOLDER_MARKERS = (
    "replace-me",
    "123456:test-token",
    "123456",
)


def _print(message: str) -> None:
    print(f"[start] {message}")


def ensure_dependencies() -> None:
    missing = []
    for module_name in REQUIRED_MODULES:
        try:
            __import__(module_name)
        except ImportError:
            missing.append(module_name)

    if not missing:
        return

    _print(f"Installing missing packages: {', '.join(missing)}")
    in_virtualenv = sys.prefix != getattr(sys, "base_prefix", sys.prefix)
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "-r",
        str(REQUIREMENTS_FILE),
    ]
    if not in_virtualenv:
        command.insert(4, "--user")
    result = subprocess.run(command, cwd=str(ROOT))
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def ensure_local_config() -> None:
    ensure_runtime_dir()

    if not CONFIG_FILE.exists():
        if not CONFIG_EXAMPLE_FILE.exists():
            raise SystemExit("config.local.example.json is missing")

        CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_FILE.write_text(
            CONFIG_EXAMPLE_FILE.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        _print(f"Created {CONFIG_FILE} from {CONFIG_EXAMPLE_FILE}")
        _print("Fill in your real TOKEN / API_ID / API_HASH and run again")
        raise SystemExit(1)

    try:
        config = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"Failed to read {CONFIG_FILE.name}: {exc}") from exc

    invalid_keys = []
    for key in ("TOKEN", "API_ID", "API_HASH"):
        value = str(config.get(key, "")).strip()
        if not value or any(marker in value for marker in PLACEHOLDER_MARKERS):
            invalid_keys.append(key)

    if invalid_keys:
        _print(
            f"Update {CONFIG_FILE.name} before launch. Missing or placeholder: "
            + ", ".join(invalid_keys)
        )
        raise SystemExit(1)


def main() -> None:
    ensure_local_config()
    ensure_dependencies()

    _print("Launching bot")
    runpy.run_path(str(ROOT / "bot.py"), run_name="__main__")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        _print("Stopped")
