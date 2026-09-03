FROM python:3.12-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app
COPY src ./src

USER 65532:65532
ENTRYPOINT ["python", "-m", "sandbox_manager"]
