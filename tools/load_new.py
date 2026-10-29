"""Compatibility entry point for the legacy collector's read-only input database."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mra.importing import article_from_hints
from mra.cli import main

if __name__ == "__main__":
    raise SystemExit(main(["import-legacy", *sys.argv[1:]]))
