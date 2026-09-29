FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY polycollect ./polycollect
RUN pip install --no-cache-dir .

ENV DATA_DIR=/data
VOLUME ["/data"]

ENTRYPOINT ["polycollect"]
