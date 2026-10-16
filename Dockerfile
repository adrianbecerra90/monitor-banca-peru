# Monitor de banca · Perú — imagen para publicarlo en un servidor (Railway, Render, Fly.io, un VPS…)
FROM python:3.12-slim
WORKDIR /app
COPY server.py index.html login.html config.json ejemplos.json ./
# Ruta gratuita de TikTok y YouTube (sin claves ni créditos)
RUN pip install --no-cache-dir "yt-dlp[default,curl-cffi]"
# Los datos viven en un disco persistente montado en /data; las claves se definen como variables secretas del servicio
ENV HOST=0.0.0.0 \
    DATA_DIR=/data \
    TRUST_PROXY=1 \
    PYTHONUNBUFFERED=1
RUN mkdir -p /data
EXPOSE 8765
CMD ["python", "server.py"]
