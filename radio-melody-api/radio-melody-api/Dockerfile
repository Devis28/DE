FROM python:3.11-slim
WORKDIR /app

# skopíruj zdrojáky
COPY app.py melody_core.py scrape_melody.py ./

# závislosti
RUN pip install --no-cache-dir \
    fastapi uvicorn[standard] requests beautifulsoup4 apscheduler cloudscraper

# defaulty (na Fly.io sa prebijú hodnotami z fly.toml)
ENV OUT_PATH=/data/playlist.json
ENV PLAYLIST_LIMIT=0
ENV SCRAPE_EVERY_S=180
ENV PUSH_INTERVAL=10
ENV PYTHONUNBUFFERED=1

VOLUME ["/data"]
EXPOSE 8080

# 1 worker stačí; ak by si chcel viac: --workers 2
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
