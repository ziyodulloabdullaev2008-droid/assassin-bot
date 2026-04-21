from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
FILE_PATTERNS = ("*.py", "*.json", "*.md")
SUSPICIOUS_TOKENS = (
    "Рђ",
    "РЎ",
    "Рќ",
    "СЃ",
    "С‚",
    "вЂ",
    "Ð",
    "Ñ",
    "??? ?????",
)
SKIP_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    "dist",
}


def should_skip(path: Path) -> bool:
    return path.name == "check_mojibake.py" or any(part in SKIP_PARTS for part in path.parts)


def iter_files() -> list[Path]:
    files: list[Path] = []
    for pattern in FILE_PATTERNS:
        files.extend(ROOT.rglob(pattern))
    return sorted(path for path in files if not should_skip(path))


def scan_file(path: Path) -> list[tuple[int, str]]:
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return []

    findings: list[tuple[int, str]] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        if any(token in line for token in SUSPICIOUS_TOKENS):
            findings.append((line_no, line.strip()))
    return findings


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    except Exception:
        pass

    bad = []
    for path in iter_files():
        findings = scan_file(path)
        if findings:
            bad.append((path, findings))

    if not bad:
        print("OK: no suspicious mojibake fragments found.")
        return 0

    print("Suspicious text fragments found:")
    for path, findings in bad:
        for line_no, line in findings[:10]:
            print(f"- {path.relative_to(ROOT)}:{line_no}: {line}")
        if len(findings) > 10:
            print(f"- {path.relative_to(ROOT)}: ... and {len(findings) - 10} more")
    return 1


if __name__ == "__main__":
    sys.exit(main())
