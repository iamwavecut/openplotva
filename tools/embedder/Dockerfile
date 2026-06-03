FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HF_HOME=/models \
    TRANSFORMERS_CACHE=/models \
    SENTENCE_TRANSFORMERS_HOME=/models

WORKDIR /app

RUN pip install --no-cache-dir \
    --index-url https://download.pytorch.org/whl/cpu \
    torch==2.11.0

RUN pip install --no-cache-dir \
    fastapi==0.115.6 \
    uvicorn[standard]==0.34.0 \
    numpy==2.2.1 \
    fastembed==0.5.0 \
    peft==0.17.1 \
    sentence-transformers==5.1.2 \
    transformers==4.57.6

COPY app.py /app/app.py

EXPOSE 12500

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "12500"]
