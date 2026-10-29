#!/usr/bin/env python3
"""Run offline unit and integration checks with all test temporary files inside this checkout."""

from __future__ import annotations

import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
import urllib.request
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.dont_write_bytecode = True
os.environ["MRA_DISABLE_LLM"] = "1"
os.environ["MRA_IGNORE_RUNTIME"] = "1"


def main():
    with tempfile.TemporaryDirectory(prefix=".check-", dir=ROOT) as temporary:
        previous = tempfile.tempdir
        tempfile.tempdir = temporary
        try:
            with patch.object(urllib.request, "urlopen", side_effect=AssertionError("network forbidden")), \
                    patch.object(socket, "create_connection", side_effect=AssertionError("network forbidden")), \
                    patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")):
                suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"))
                result = unittest.TextTestRunner(verbosity=2).run(suite)
        finally:
            tempfile.tempdir = previous
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
