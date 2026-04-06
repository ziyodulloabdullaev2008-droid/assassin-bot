import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DIST_DIR = ROOT / "dist" / "server_bundle"

INCLUDE_FILES = [
    "bot.py",
    "database.py",
    "requirements.txt",
    "start.py",
    "start.bat",
    "start.ps1",
    "run.sh",
    "config.local.json",
    "config.local.example.json",
    "DEPLOY_SERVER.md",
]

INCLUDE_DIRS = [
    "core",
    "handlers",
    "services",
    "ui",
    "users",
    "deploy",
]

SKIP_DIR_NAMES = {
    "__pycache__",
    ".git",
    ".venv",
    "tests",
    "dist",
}

SKIP_FILE_SUFFIXES = {
    ".pyc",
    ".pyo",
}

SKIP_FILE_NAMES = {
    ".DS_Store",
}

SKIP_FILE_ENDINGS = (
    ".session-journal",
    ".json.backup",
)


def _should_skip_file(path: Path) -> bool:
    if path.name in SKIP_FILE_NAMES:
        return True
    if path.suffix in SKIP_FILE_SUFFIXES:
        return True
    return any(path.name.endswith(ending) for ending in SKIP_FILE_ENDINGS)


def _copy_tree(src: Path, dst: Path) -> None:
    for item in src.iterdir():
        if item.name in SKIP_DIR_NAMES:
            continue

        target = dst / item.name
        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            _copy_tree(item, target)
            continue

        if _should_skip_file(item):
            continue

        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item, target)


def main() -> None:
    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR)

    DIST_DIR.mkdir(parents=True, exist_ok=True)

    for rel_path in INCLUDE_FILES:
        src = ROOT / rel_path
        if not src.exists():
            continue
        dst = DIST_DIR / rel_path
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    for rel_path in INCLUDE_DIRS:
        src = ROOT / rel_path
        if not src.exists():
            continue
        dst = DIST_DIR / rel_path
        dst.mkdir(parents=True, exist_ok=True)
        _copy_tree(src, dst)

    print(f"Server bundle prepared: {DIST_DIR}")


if __name__ == "__main__":
    main()
