# Monitor de banca · Perú — imagen para publicarlo en un servidor (Railway, Render, Fly.io, un VPS…)
FROM python:3.12-slim
WORKDIR /app
# Primero las dependencias y después el código: así un cambio de código reutiliza la capa ya instalada y la
# actualización tarda segundos (antes reinstalaba yt-dlp, curl-cffi y telethon en cada despliegue: ~10 min en el
# servidor). Para actualizar yt-dlp (TikTok y YouTube cambian a menudo) basta con cambiar esta fecha.
ARG DEPENDENCIAS=2026-11-20
# Ruta gratuita de TikTok y YouTube (sin claves ni créditos)
RUN echo "dependencias $DEPENDENCIAS" && pip install --no-cache-dir "yt-dlp[default,curl-cffi]" telethon
COPY server.py index.html login.html config.json ejemplos.json ./
# Los datos viven en un disco persistente montado en /data; las claves se definen como variables secretas del servicio
ENV HOST=0.0.0.0 \
    DATA_DIR=/data \
    TRUST_PROXY=1 \
    PYTHONUNBUFFERED=1
RUN mkdir -p /data
EXPOSE 8765
CMD ["python", "server.py"]
