import logging
import os
import threading
import time
from typing import List, Optional

import numpy as np
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


MODEL_NAME = os.getenv("EMBEDDER_MODEL", "jinaai/jina-embeddings-v5-text-nano")
DIMENSION = int(os.getenv("EMBEDDER_DIMENSION", "512"))
DEVICE = os.getenv("EMBEDDER_DEVICE", "cpu")
BACKEND = os.getenv("EMBEDDER_BACKEND", "auto").strip().lower()
TASK = os.getenv("EMBEDDER_TASK", "retrieval").strip()


class EncodeRequest(BaseModel):
    prompts: List[str] = Field(default_factory=list)
    dimension: Optional[int] = None
    task_description: Optional[str] = None


class EncodeResponse(BaseModel):
    embeddings: List[List[float]]
    dimension: int
    count: int


app = FastAPI(title="openplotva-memory-embedder")
logger = logging.getLogger("uvicorn.error")
_model = None
_model_lock = threading.Lock()
_backend = ""


@app.middleware("http")
async def log_request_duration(request: Request, call_next):
    start = time.perf_counter()
    response = None
    try:
        response = await call_next(request)
        return response
    finally:
        duration = time.perf_counter() - start
        status_code = response.status_code if response is not None else 500
        logger.info(
            "request completed method=%s path=%s status=%s duration_seconds=%.3f",
            request.method,
            request.url.path,
            status_code,
            duration,
        )


def _load_fastembed():
    from fastembed import TextEmbedding

    return TextEmbedding(model_name=MODEL_NAME)


def _load_sentence_transformers():
    from sentence_transformers import SentenceTransformer

    kwargs = _sentence_transformer_kwargs(with_task=True)
    try:
        return SentenceTransformer(MODEL_NAME, **kwargs)
    except (TypeError, ValueError) as exc:
        if TASK and "default_task" in str(exc):
            return SentenceTransformer(
                MODEL_NAME,
                **_sentence_transformer_kwargs(with_task=False),
            )
        raise


def _sentence_transformer_kwargs(with_task: bool):
    kwargs = {"trust_remote_code": True}
    if with_task and TASK:
        kwargs["model_kwargs"] = {"default_task": TASK}
    if DEVICE and DEVICE.lower() != "cpu":
        kwargs["device"] = DEVICE
    return kwargs


def model():
    global _model, _backend
    if _model is not None:
        return _model

    with _model_lock:
        if _model is not None:
            return _model

        if BACKEND in ("auto", "fastembed"):
            try:
                _model = _load_fastembed()
                _backend = "fastembed"
                return _model
            except Exception:
                if BACKEND == "fastembed":
                    raise

        _model = _load_sentence_transformers()
        _backend = "sentence-transformers"
    return _model


def normalize_and_crop(vector, dimension: int) -> List[float]:
    arr = np.asarray(vector, dtype=np.float32)
    if dimension > 0 and arr.shape[0] > dimension:
        arr = arr[:dimension]
    if dimension > 0 and arr.shape[0] < dimension:
        arr = np.pad(arr, (0, dimension - arr.shape[0]))
    norm = float(np.linalg.norm(arr))
    if norm > 0:
        arr = arr / norm
    return arr.astype(np.float32).tolist()


@app.get("/health")
def health():
    error = ""
    try:
        model()
        loaded = True
    except Exception as exc:
        loaded = False
        error = str(exc)
    payload = {
        "status": "ok" if loaded else "loading_error",
        "model_loaded": loaded,
        "device": DEVICE,
        "backend": _backend or BACKEND,
        "task": TASK,
        "supported_dimensions": [DIMENSION],
    }
    if error:
        payload["error"] = error
    if not loaded:
        return JSONResponse(status_code=503, content=payload)
    return payload


@app.post("/encode", response_model=EncodeResponse)
def encode(req: EncodeRequest):
    prompts = [p.strip() for p in req.prompts if p and p.strip()]
    if not prompts:
        raise HTTPException(status_code=400, detail="prompts cannot be empty")

    dimension = req.dimension or DIMENSION
    try:
        encoder = model()
        if _backend == "fastembed":
            raw_embeddings = list(encoder.embed(prompts))
        else:
            raw_embeddings = encoder.encode(
                prompts,
                normalize_embeddings=False,
                show_progress_bar=False,
            )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    embeddings = [normalize_and_crop(v, dimension) for v in raw_embeddings]
    return EncodeResponse(
        embeddings=embeddings,
        dimension=dimension,
        count=len(embeddings),
    )
