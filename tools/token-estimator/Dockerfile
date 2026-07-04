FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HF_HOME=/models \
    TOKENIZERS_PARALLELISM=false \
    TRANSFORMERS_VERBOSITY=error

WORKDIR /app

RUN addgroup --system app && \
    adduser --system --ingroup app --home /app app && \
    mkdir -p /models && \
    chown -R app:app /app /models

RUN pip install --no-cache-dir \
    fastapi==0.115.6 \
    uvicorn[standard]==0.34.0 \
    transformers==4.57.6 \
    tokenizers==0.22.1 \
    huggingface-hub==0.36.0 \
    protobuf==5.29.3

COPY --chown=app:app app.py /app/app.py

EXPOSE 12600

USER app

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "12600"]
