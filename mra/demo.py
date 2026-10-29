"""Deterministic synthetic demonstration. It never contacts a model or marketplace."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from .generate import GenerationResult
from .importing import import_snapshot
from .pipeline import Pipeline
from .store import Store


def run_demo() -> dict:
    root = Path(__file__).resolve().parents[1]
    snapshot = json.loads((root / "examples/snapshot.json").read_text(encoding="utf-8"))
    store = Store(Path(":memory:"))
    try:
        counts = import_snapshot(store, snapshot)

        def fixture_reply(_message, _article, material, **_kwargs):
            return GenerationResult(text="Демонстрационный ответ: это синтетический товар для проверки установки.",
                                    model="synthetic-fixture", source="demo", decision="answer", basis=tuple(material[:1]))

        with patch("mra.generate.RULES_FILE", root / "examples/rules.example.md"):
            pipeline = Pipeline(store, generator=fixture_reply)
            message_id = store.messages_by_state("new")[0][0]
            outcome = pipeline.process(message_id)
            draft_id = pipeline.approve(message_id, actor="demo_script")
        if outcome.state != "drafted" or not draft_id or store.outbox_count() != 1:
            raise ValueError("демонстрационный цикл не завершился")
        return {"mode": "synthetic-offline", "imported": counts, "message_id": message_id,
                "state": store.get_message(message_id)[11], "outbox": "held", "network_calls": 0}
    finally:
        store.close()
