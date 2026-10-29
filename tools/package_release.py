#!/usr/bin/env python3
"""Build a source release from an explicit allowlist after offline checks."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from mra import __version__
PATTERNS = [rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----", rb"\bsk-(?:proj-|or-v1-)?[A-Za-z0-9_-]{24,}",
            rb"\bgithub_pat_[A-Za-z0-9_]{20,}", rb"\bgh[pousr]_[A-Za-z0-9]{20,}"]


def release_files():
    names = [line.strip() for line in (ROOT / "RELEASE_FILES.txt").read_text().splitlines() if line.strip() and not line.startswith("#")]
    if len(names) != len(set(names)):
        raise ValueError("повтор файла в RELEASE_FILES.txt")
    for name in names:
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts or any(p in {".git", "data", "private", "__pycache__"} for p in path.parts):
            raise ValueError(f"недопустимый путь в составе: {name}")
        local = ROOT / name
        if local.is_symlink() or not local.is_file() or not local.resolve().is_relative_to(ROOT.resolve()):
            raise ValueError(f"файл отсутствует или ведёт за пределы релиза: {name}")
        if local.suffix in {".db", ".sqlite3", ".sqlite", ".key", ".pem"} or local.name.startswith(".env"):
            raise ValueError(f"частный файл в составе: {name}")
        data = local.read_bytes()
        if any(re.search(pattern, data) for pattern in PATTERNS):
            raise ValueError(f"обнаружен явный формат ключа в файле {name}; значение не выводится")
    return names


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    names = release_files()
    subprocess.run([sys.executable, "-B", str(ROOT / "tools/check.py")], cwd=ROOT, check=True)
    manifest = {"version": __version__, "algorithm": "sha256",
                "files": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in names}}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    prefix = f"marketplace-reply-assistant-{__version__}/"
    with zipfile.ZipFile(args.output, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in names:
            archive.write(ROOT / name, prefix + name)
        archive.writestr(prefix + "source-manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"archive": str(args.output.resolve()), "files": len(names),
                      "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
