"""Tests for the embeddings-local sidecar.

These tests NEVER download the real mmlw model and NEVER import torch /
sentence-transformers. Every test monkeypatches ``server.get_model`` to a fake
encoder, so the contract (POST /embed → 1024-dim vectors, "passage: " prefix,
length cap, batching, lazy load) is exercised with just fastapi + a fake.

Mirrors bagent/tests/test_api.py style: TestClient, builtin monkeypatch,
imports the subfolder's top-level ``server`` module.
"""

import pathlib
import sys

# Make the sidecar's top-level server.py importable regardless of cwd.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import server  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from server import app  # noqa: E402

client = TestClient(app)


class FakeEncoder:
    """Stand-in for SentenceTransformer.

    Records every list of strings handed to ``.encode`` so prefix / cap /
    batch assertions can inspect what the handler actually sent. Returns a
    deterministic 1024-wide python list per input (no numpy required, no
    model download).
    """

    DIM = 1024

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def encode(self, inputs, **kwargs):
        recorded = list(inputs)
        self.calls.append(recorded)
        return [[0.0] * self.DIM for _ in recorded]

    @property
    def seen_inputs(self) -> list[str]:
        flat: list[str] = []
        for call in self.calls:
            flat.extend(call)
        return flat


def _install_fake(monkeypatch) -> FakeEncoder:
    """Reset the lazy singleton and force get_model to return a fresh fake."""
    monkeypatch.setattr(server, "_model", None, raising=False)
    fake = FakeEncoder()

    def _fake_get_model():
        # Mirror the real lazy singleton: populate the module global so
        # /health's model_loaded flips True after the first /embed.
        server._model = fake
        return fake

    monkeypatch.setattr(server, "get_model", _fake_get_model)
    return fake


def test_embed_returns_1024_dim_per_input(monkeypatch):
    """POST /embed → one 1024-float JSON-serializable vector per input."""
    _install_fake(monkeypatch)
    resp = client.post(
        "/embed",
        json={"inputs": ["Botoks 1 okolica", "Manicure hybrydowy"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["model"] == "mmlw-e5-large"
    assert body["dim"] == 1024
    assert len(body["embeddings"]) == 2
    for vec in body["embeddings"]:
        assert len(vec) == 1024
        assert all(isinstance(x, float) for x in vec)
    # The whole body must be plain-JSON-serializable (no numpy scalars).
    import json

    json.dumps(body)


def test_passage_prefix_applied(monkeypatch):
    """Every string the encoder saw is 'passage: '-prefixed + keeps the text."""
    fake = _install_fake(monkeypatch)
    client.post("/embed", json={"inputs": ["Botoks 1 okolica"]})
    seen = fake.seen_inputs
    assert seen, "encoder was never called"
    for s in seen:
        assert s.startswith("passage: ")
    assert any("Botoks 1 okolica" in s for s in seen)
    assert "passage: Botoks 1 okolica" in seen


def test_batching_for_many_inputs(monkeypatch):
    """>BATCH_SIZE inputs → that many vectors; explicit batching loop ran."""
    fake = _install_fake(monkeypatch)
    n = server.BATCH_SIZE + 6  # 70 when BATCH_SIZE == 64
    inputs = [f"usluga {i}" for i in range(n)]
    resp = client.post("/embed", json={"inputs": inputs})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["embeddings"]) == n
    for vec in body["embeddings"]:
        assert len(vec) == 1024
    # The handler batches explicitly → encoder called more than once for >cap.
    assert len(fake.calls) > 1


def test_input_length_cap(monkeypatch):
    """Over-long input is capped before encoding (cap + prefix length)."""
    fake = _install_fake(monkeypatch)
    long_input = "a" * 5000
    resp = client.post("/embed", json={"inputs": [long_input]})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["embeddings"]) == 1
    assert len(body["embeddings"][0]) == 1024
    seen = fake.seen_inputs
    assert len(seen) == 1
    cap = server.MAX_INPUT_CHARS + len(server.PASSAGE_PREFIX)
    assert len(seen[0]) <= cap
    assert seen[0].startswith("passage: ")


def test_empty_inputs(monkeypatch):
    """Empty inputs → empty embeddings, encoder NOT called, model NOT loaded."""
    fake = _install_fake(monkeypatch)
    resp = client.post("/embed", json={"inputs": []})
    assert resp.status_code == 200
    body = resp.json()
    assert body["embeddings"] == []
    assert body["model"] == "mmlw-e5-large"
    assert body["dim"] == 1024
    assert fake.calls == []  # encoder never touched
    assert server._model is None  # model never loaded


def test_health_lazy(monkeypatch):
    """GET /health reflects lazy load: False before /embed, True after."""
    _install_fake(monkeypatch)
    # Force the unloaded state explicitly.
    monkeypatch.setattr(server, "_model", None, raising=False)

    before = client.get("/health")
    assert before.status_code == 200
    assert before.json() == {"status": "ok", "model_loaded": False}

    client.post("/embed", json={"inputs": ["Manicure hybrydowy"]})

    after = client.get("/health")
    assert after.status_code == 200
    after_body = after.json()
    assert after_body["status"] == "ok"
    assert after_body["model_loaded"] is True
