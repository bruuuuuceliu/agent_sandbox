FROM python:3.12-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY sandbox_manager ./sandbox_manager

USER 65532:65532
ENTRYPOINT ["python", "-m", "sandbox_manager"]
