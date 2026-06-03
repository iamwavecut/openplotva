import logging
import os
import threading
import time

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


MODEL_NAME = os.getenv("TOKEN_ESTIMATOR_MODEL", "google/gemma-4-26B-A4B-it").strip()
MAX_TEXT_CHARS = int(os.getenv("TOKEN_ESTIMATOR_MAX_TEXT_CHARS", "1000000"))


class EstimateRequest(BaseModel):
    text: str = Field(default="")
    model: str | None = None
    add_special_tokens: bool = False


class EstimateResponse(BaseModel):
    tokens: int
    model: str


app = FastAPI(title="openplotva-token-estimator")
logger = logging.getLogger("uvicorn.error")
_tokenizer = None
_tokenizer_lock = threading.Lock()


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


def tokenizer():
    global _tokenizer
    if _tokenizer is not None:
        return _tokenizer

    with _tokenizer_lock:
        if _tokenizer is not None:
            return _tokenizer

        if not MODEL_NAME:
            raise RuntimeError("TOKEN_ESTIMATOR_MODEL must not be empty")

        from transformers import AutoTokenizer

        _tokenizer = AutoTokenizer.from_pretrained(
            MODEL_NAME,
            use_fast=True,
            trust_remote_code=True,
            extra_special_tokens={},
        )
        return _tokenizer


@app.get("/health")
def health():
    error = ""
    try:
        tokenizer()
        loaded = True
    except Exception as exc:
        loaded = False
        error = str(exc)

    payload = {
        "status": "ok" if loaded else "loading_error",
        "model": MODEL_NAME,
        "model_loaded": loaded,
    }
    if error:
        payload["error"] = error
    if not loaded:
        return JSONResponse(status_code=503, content=payload)
    return payload


@app.post("/estimate", response_model=EstimateResponse)
def estimate(req: EstimateRequest):
    text = req.text or ""
    if not text.strip():
        return EstimateResponse(tokens=0, model=MODEL_NAME)
    if len(text) > MAX_TEXT_CHARS:
        raise HTTPException(status_code=413, detail="text is too large")
    if req.model and req.model.strip() and req.model.strip() != MODEL_NAME:
        raise HTTPException(status_code=400, detail=f"only model {MODEL_NAME!r} is loaded")

    try:
        ids = tokenizer().encode(text, add_special_tokens=req.add_special_tokens)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return EstimateResponse(tokens=len(ids), model=MODEL_NAME)
