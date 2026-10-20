# Stdlib-only: no pip install, nothing to pin beyond the base image.
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY runpod_pool ./runpod_pool

# Matches runAsUser in deploy/controller.yaml.
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["python", "-m", "runpod_pool"]
