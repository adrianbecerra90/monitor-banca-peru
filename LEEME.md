# Monitor de banca · Perú

Panel local que vigila redes sociales y noticias buscando quejas de clientes de
**BCP (ViaBCP, Yape), Interbank, BBVA Perú, Scotiabank Perú, Banco de la Nación y la SBS**.
Clasifica cada mención por **sentimiento (positivo / neutro / negativo)**, si es queja, tipo de problema y gravedad.

- **Tiempo real:** cada fuente se consulta en su intervalo y las publicaciones nuevas aparecen al instante en el panel (indicador EN VIVO).
- **Histórico:** todo se guarda en `quejas.db`; desde el panel ("Carga histórica") puedes traer meses anteriores por rango de fechas.
- **Granularidad:** automática o fija (5 min, 15 min, hora, día, semana), siempre en hora de Lima. Clic en una barra del gráfico para ampliar ese intervalo; "Deshacer zoom" para volver. Mapa de calor día × hora de las menciones negativas.
- **X:** las respuestas de las cuentas oficiales (p. ej. @interbank respondiendo a un cliente) se separan como "respuesta oficial" y no cuentan como queja.
- **Filtros:** periodo (1 h a todo, o fechas exactas), entidad, red, sentimiento, tipo (comentarios a cuentas oficiales / publicaciones / noticias), categoría, gravedad y texto. Los filtros quedan en la URL: guárdala como marcador para volver a la misma vista.

## Arranque

Doble clic en `iniciar.command`, o desde Terminal:

```bash
python3 server.py
```

Abre http://localhost:8765 . No necesita instalar nada (Python 3.9+).

## Fuentes y cómo se obtienen

| Red | Método | Clave |
|---|---|---|
| Noticias | Google News Perú, Bing News, RSS de RPP, La República, Gestión, El Comercio, Correo, Infobae | — |
| YouTube | API oficial: búsqueda + comentarios. Sin clave: scraping de resultados de búsqueda (solo títulos/descripciones) | `YOUTUBE_API_KEY` (gratis) |
| X / Twitter | **Sin clave: Nitter** (espejo público de X; búsquedas, menciones y respuestas a las cuentas oficiales, tiempo real cada 3 min e histórico paginando hacia atrás). Respaldo de pago: Apify | opcional: `APIFY_TOKEN` |
| Facebook | Comentarios de las últimas publicaciones de las páginas oficiales (Apify) | `APIFY_TOKEN` |
| Instagram | Comentarios de las últimas publicaciones de las cuentas oficiales (Apify) | `APIFY_TOKEN` |
| TikTok | Búsqueda por palabra clave + comentarios en las cuentas oficiales (Apify) | `APIFY_TOKEN` |
| Bluesky | API pública | — |

### Rutas gratuitas de TikTok y YouTube (opcional, recomendado)

```bash
/opt/homebrew/bin/python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

`iniciar.command` usa `.venv` automáticamente si existe (en Docker, yt-dlp ya viene instalado). Con esto:

- **TikTok gratis:** yt-dlp lista los últimos videos de cada cuenta oficial y el monitor lee sus comentarios públicos
  (sin cuenta ni créditos). Apify queda solo de respaldo si esta ruta falla.
  Si TikTok no deja listar una cuenta, se siguen sus videos de los últimos 7 días ya conocidos.
- **YouTube gratis:** yt-dlp busca videos de la semana cuyo título nombra a la entidad y trae sus comentarios más
  recientes; funciona en paralelo con la API oficial.

Sin `.venv` el monitor funciona igual, solo con Python.

Copia `.env.example` como `.env` y pega las claves que tengas. Reinicia el servidor.

### Cómo obtener cada clave (tú mismo, ~2 min cada una)

1. **Apify** (X, Facebook, Instagram, TikTok): crea cuenta en https://console.apify.com/sign-up →
   *Settings → API & Integrations* (https://console.apify.com/settings/integrations) → copia el *Personal API token*.
2. **YouTube**: en https://console.cloud.google.com/ crea un proyecto → *APIs y servicios → Biblioteca* →
   activa **YouTube Data API v3** → *Credenciales → Crear credenciales → Clave de API*.
   Cuota gratuita: 10.000 unidades/día (el monitor usa ~2.500).

**Histórico por fuente:** Noticias (Google News, años hacia atrás), YouTube con clave, Bluesky,
X (Nitter) y Facebook/Instagram/TikTok vía los proveedores de pago (cuantos más meses, más coste).

**Por qué Apify:** Facebook, Instagram y TikTok no ofrecen búsqueda pública por API,
y sin sesión iniciada bloquean el scraping directo. Apify mantiene scrapers que sí
funcionan (pago por uso, unos US$0.3–2 por cada 1.000 resultados; trae US$5/mes gratis).
Para controlar el gasto, ajusta `intervalos_minutos` y `apify.comentarios_por_post`
en `config.json`.

## Personalizar (`config.json`)

- `bancos`: términos de búsqueda y alias de cada banco. Los alias en `ambiguos`
  (p. ej. "BBVA" o "BCP" a secas) solo cuentan si hay contexto peruano (`contexto_peru`:
  soles, Yape, Lima…) o el `contexto` propio de la entidad (p. ej. "hackeo", "AFP" para la
  SBS), y nunca si aparece una palabra de `contexto_excluir` (Ecuador, México, kpop…),
  salvo que el texto nombre a Perú. Todo esto se edita en el panel (Configuración →
  Entidades), que además sugiere exclusiones a partir de tus correcciones.
- Antes de subir un cambio de reglas, corre el banco de pruebas: `python3 pruebas/probar_reglas.py`.
  Cuando encuentres un error de asignación, agrégalo primero a `pruebas/casos_reglas.json`.
- `cuentas_oficiales`: URLs de las páginas de Facebook, Instagram y TikTok de cada banco.
  **Revísalas**: comprueba que cada una tenga la insignia azul y corrige las que no coincidan.
- `intervalos_minutos`: cada cuánto se consulta cada fuente.
- `noticias_rss`: añade o quita medios.

El léxico de quejas (peruanismos incluidos: "malogrado", "qué tal raza", "rateros",
"no me devuelven"…) está en `server.py`, en `CATEGORIAS` y `NEGATIVO`. Al reiniciar,
todo el histórico se reclasifica con las reglas nuevas.

## Panel

- Indicadores, quejas por banco y categoría, evolución por hora.
- Filtros por banco, red, categoría, gravedad y texto. Exportación a CSV (Excel).
- Alerta de **pico**: 5 o más quejas en una hora y al menos 3 veces el promedio horario
  de las 48 h previas (posible caída o incidente).
- Avisos de escritorio opcionales para quejas graves (fraude, clonación, cuentas bloqueadas, caídas).

Los datos se guardan en `quejas.db` (SQLite).

## Seguridad

Revisión de desarrollo seguro aplicada (octubre 2026):

- **Solo local por defecto:** escucha en `127.0.0.1`. Para publicarlo (`HOST=0.0.0.0`) exige al menos una cuenta de
  administrador (o `ADMIN_USUARIO`/`ADMIN_CLAVE`) y no arranca sin ella.
- **DNS rebinding:** solo responde a los nombres de host esperados (`localhost`, `127.0.0.1` y `HOSTS_PERMITIDOS`).
- **CSRF:** las acciones (consultar ahora, carga histórica) exigen una cabecera propia del panel y origen propio;
  una web maliciosa abierta en tu navegador no puede dispararlas ni gastar tu crédito de Apify.
- **XSS:** todo el contenido externo se escapa al mostrarse; solo se aceptan enlaces `http(s)`; política de
  seguridad de contenido (CSP), `X-Frame-Options`, `nosniff` y `Referrer-Policy: no-referrer`.
- **Inyección SQL:** todas las consultas usan parámetros.
- **Inyección en CSV:** las celdas que empiezan por `= + - @` se neutralizan para que Excel no ejecute fórmulas.
- **Inyección de instrucciones a la IA:** el prompt trata los textos como datos no confiables y la respuesta se
  valida contra valores permitidos.
- **Secretos:** `.env` y la base de datos con permisos `600` (solo tu usuario); `.gitignore` evita subirlos a un repositorio.
- **Robustez:** límite de 25 MB por respuesta externa, máximo 20 conexiones en vivo, validación de parámetros y
  rango máximo de 3 años para cargas históricas.

- **Inicio de sesión y roles:** cuentas con contraseña cifrada (PBKDF2-SHA256), sesiones de 12 h en cookie
  `HttpOnly`/`SameSite=Strict`, bloqueo tras 5 intentos fallidos por usuario (20 por IP), registro de accesos y dos
  roles: **Administrador** (todo) y **Lector** (ver y descargar). Se gestionan en ⚙ Configuración → Accesos.

**Datos personales:** se guardan publicaciones públicas con nombre de usuario. Define cuánto tiempo conservarlas
y quién accede al panel conforme a la Ley N.º 29733 de Protección de Datos Personales.


## Acceso

La primera vez que abras el monitor te pedirá **crear la cuenta de administrador** (solo se permite desde el mismo
equipo donde está instalado). Después, en **⚙ Configuración → Accesos** puedes crear cuentas para tu equipo.

## Publicarlo en internet (Railway)

El repositorio ya incluye un `Dockerfile`. Pasos:

1. Entra en https://railway.com con tu cuenta de GitHub y crea un proyecto: **New Project → Deploy from GitHub repo →
   monitor-banca-peru**.
2. En el servicio, **Settings → Volumes → Add Volume** con ruta de montaje **`/data`** (ahí se guardan la base de
   datos, el archivo y la configuración; sin volumen se perderían en cada despliegue).
3. En **Variables**, añade como secretas:
   - `ADMIN_USUARIO` y `ADMIN_CLAVE` (tu primer administrador; mínimo 10 caracteres con letras y números),
   - las claves que uses: `APIFY_TOKEN`, `NVIDIA_API_KEY`, `OPENROUTER_API_KEY`,
     `YOUTUBE_API_KEY`.
4. **Settings → Networking → Generate Domain**. Railway te da una dirección `https://….up.railway.app`; ese es tu
   monitor en internet (con HTTPS y login).
5. Cada vez que se suba un cambio al repositorio, Railway lo vuelve a publicar solo.

Notas: el plan Hobby de Railway cuesta unos US$5/mes. Desde servidores en la nube, Nitter (X) y Google Trends pueden
bloquear más que desde una conexión doméstica; el monitor usa rutas de respaldo.

## Ciberseguridad (CSIRT)

La tarjeta «Ciberseguridad · alertas» junta, filtrado al sistema financiero, seguros, AFP y sus proveedores en Perú:
cuentas referentes en X (@alph4_SEC, @peruhacking y otras), ransomware.live, canales públicos de Telegram
(RansomFeed, Ransomlook), dominios que imitan a las entidades (crt.sh), URLhaus y ThreatFox (abuse.ch) e
infostealers por entidad (Hudson Rock, solo conteos).

**Telegram con sesión** (canales privados o por invitación, p. ej. los de grupos hacktivistas peruanos):

1. Crea o usa una **cuenta de Telegram dedicada** del CSIRT (no la personal) y únete con ella a los canales.
2. En https://my.telegram.org → *API development tools* crea una app y pega `TELEGRAM_API_ID` y `TELEGRAM_API_HASH` en `.env`.
3. Ejecuta `.venv/bin/python server.py --telegram-login` (pide teléfono y código). La sesión queda en `.env` y en el portapapeles.
4. En Railway agrega `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` y `TELEGRAM_SESSION`.

El monitor solo lee el **texto** de los mensajes (nunca descarga archivos ni abre enlaces), guarda únicamente lo que
toca al SF peruano y **enmascara datos personales** (correos, DNI, teléfonos, tarjetas, claves) antes de guardar.
Para limitarlo a ciertos canales, pon sus nombres en `config.json` → `ciber.canales_sesion`.
