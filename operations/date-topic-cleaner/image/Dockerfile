FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /usr/local/bin/uv

# 패키지명이 app 이므로 WORKDIR 은 겹치지 않게 /src 로 둔다
WORKDIR /src

# 의존성 먼저 설치해 레이어 캐시 활용
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY app/ ./app/
RUN uv sync --frozen --no-dev

ENV PATH="/src/.venv/bin:$PATH"

ENTRYPOINT ["python", "-m", "app"]
