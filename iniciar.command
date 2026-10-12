#!/bin/bash
cd "$(dirname "$0")"
(sleep 2 && open http://localhost:8765) &
# Usa el entorno con yt-dlp y pysentimiento si existe; si no, el Python del sistema (funciona sin ellos)
if [ -x .venv/bin/python ]; then .venv/bin/python server.py; else python3 server.py; fi
