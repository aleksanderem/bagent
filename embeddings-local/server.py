"""embeddings-local — quota-proof local embedding sidecar.

Serves sdadas/mmlw-e5-large (1024-dim, Polish) over HTTP as a fallback for
BeautyAudit's pipeline when OpenAI embeddings are exhausted. Binds localhost
only (called by the bagent worker over 127.0.0.1) — NOT publicly exposed, so
no API-key auth here. Domain: booksyaudit.pl.

torch / sentence-transformers are imported LAZILY inside get_model() only, so
this module imports (and its tests run) with just fastapi + a fake encoder —
the multi-GB model is never loaded at import time.
"""

from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Model + request constants.
# ---------------------------------------------------------------------------
MODEL_NAME = "sdadas/mmlw-e5-large"  # HuggingFace id; downloaded at first load
MODEL_LABEL = "mmlw-e5-large"  # short tag returned in responses / used as space id
DIM = 1024  # mmlw-e5-large embedding width
MAX_INPUT_CHARS = 1500  # cap user content before prefixing (mirrors ingest path)
BATCH_SIZE = 64  # explicit encode batch; mmlw 8K ctx, our inputs are short
PASSAGE_PREFIX = "passage: "  # E5 prefix — same on both sides keeps cosine meaningful

# Lazy singleton. Populated on first real get_model() call; tests monkeypatch
# get_model and set this directly, so the real model never loads under test.
_model = None

app = FastAPI(title="embeddings-local", version="0.1.0")


def get_model():
    """Lazy singleton SentenceTransformer loader.

    torch and sentence-transformers are imported HERE (never at module top) so
    the heavy deps stay out of import time and out of the test path. Tests
    monkeypatch this function to return a fake encoder.
    """
    global _model
    if _model is None:
        import os

        import torch
        from sentence_transformers import SentenceTransformer

        # Leave headroom for the main worker on the shared CPU box (tytan).
        torch.set_num_threads(min(6, os.cpu_count() or 1))
        _model = SentenceTransformer(MODEL_NAME)
    return _model


class EmbedRequest(BaseModel):
    inputs: list[str]


@app.post("/embed")
def embed(request: EmbedRequest) -> dict:
    """Embed each input into a 1024-dim float vector.

    Empty inputs short-circuit (no model load, no encode). Otherwise every
    input is capped to MAX_INPUT_CHARS and "passage: "-prefixed BEFORE encoding;
    encoding runs in explicit BATCH_SIZE chunks; output is converted to plain
    python float lists so the JSON response carries no numpy scalars.
    """
    if not request.inputs:
        return {"embeddings": [], "model": MODEL_LABEL, "dim": DIM}

    prefixed = [PASSAGE_PREFIX + (s or "")[:MAX_INPUT_CHARS] for s in request.inputs]

    model = get_model()
    vectors: list[list[float]] = []
    for i in range(0, len(prefixed), BATCH_SIZE):
        chunk = prefixed[i : i + BATCH_SIZE]
        out = model.encode(chunk)
        for vec in out:
            vectors.append([float(x) for x in vec])

    return {"embeddings": vectors, "model": MODEL_LABEL, "dim": DIM}


@app.get("/health")
def health() -> dict:
    """Liveness + lazy-load state. model_loaded reflects whether the singleton
    has been constructed yet."""
    return {"status": "ok", "model_loaded": _model is not None}
