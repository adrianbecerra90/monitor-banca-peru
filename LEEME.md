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
| X / Twitter | **Sin clave: Nitter** (espejo público de X; búsquedas, menciones y respuestas a las cuentas oficiales, tiempo real cada 3 min e histórico paginando hacia atrás). Con clave: API oficial o Apify (más robusto) | opcional: `X_BEARER_TOKEN` o `APIFY_TOKEN` |
| Facebook | Comentarios de las últimas publicaciones de las páginas oficiales (Apify) | `APIFY_TOKEN` |
| Instagram | Comentarios de las últimas publicaciones de las cuentas oficiales (Apify) | `APIFY_TOKEN` |
| TikTok | Búsqueda por palabra clave + comentarios en las cuentas oficiales (Apify) | `APIFY_TOKEN` |
| Bluesky | API pública | — |

Copia `.env.example` como `.env` y pega las claves que tengas. Reinicia el servidor.

### Cómo obtener cada clave (tú mismo, ~2 min cada una)

1. **Apify** (X, Facebook, Instagram, TikTok): crea cuenta en https://console.apify.com/sign-up →
   *Settings → API & Integrations* (https://console.apify.com/settings/integrations) → copia el *Personal API token*.
2. **YouTube**: en https://console.cloud.google.com/ crea un proyecto → *APIs y servicios → Biblioteca* →
   activa **YouTube Data API v3** → *Credenciales → Crear credenciales → Clave de API*.
   Cuota gratuita: 10.000 unidades/día (el monitor usa ~2.500).
3. **X (opcional)**: https://developer.x.com/en/portal/dashboard → tu app → *Keys and tokens* → *Bearer Token*.
   Requiere plan de pago (Basic). Si no lo tienes, X funciona igual vía Apify.

**Histórico por fuente:** Noticias (Google News, años hacia atrás), YouTube con clave, Bluesky,
X/Facebook/Instagram/TikTok vía Apify (cuantos más meses, más coste). La API oficial de X solo
cubre los últimos 7 días; para histórico de X se usa Apify.

**Por qué Apify:** Facebook, Instagram y TikTok no ofrecen búsqueda pública por API,
y sin sesión iniciada bloquean el scraping directo. Apify mantiene scrapers que sí
funcionan (pago por uso, unos US$0.3–2 por cada 1.000 resultados; trae US$5/mes gratis).
Para controlar el gasto, ajusta `intervalos_minutos` y `apify.comentarios_por_post`
en `config.json`.

## Personalizar (`config.json`)

- `bancos`: términos de búsqueda y alias de cada banco. Los alias en `ambiguos`
  (p. ej. "BBVA" o "BCP" a secas) solo cuentan si hay contexto peruano (soles, Yape,
  SBS, Lima…), para no mezclar con BBVA España/México o el Banco Central del Paraguay.
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

- **Solo local por defecto:** escucha en `127.0.0.1`. Para escuchar fuera del equipo (`HOST=0.0.0.0`) el servidor
  exige `PANEL_USUARIO` y `PANEL_CLAVE` (≥ 12 caracteres) y no arranca sin ellos.
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

**Para publicarlo en internet:** pon delante un proxy con HTTPS (Caddy o Nginx con Let's Encrypt), define
`PANEL_USUARIO`, `PANEL_CLAVE` y `HOSTS_PERMITIDOS`, y no expongas el puerto 8765 directamente.

**Datos personales:** se guardan publicaciones públicas con nombre de usuario. Define cuánto tiempo conservarlas
y quién accede al panel conforme a la Ley N.º 29733 de Protección de Datos Personales.
