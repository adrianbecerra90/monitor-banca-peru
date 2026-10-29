"""Banco de pruebas de las reglas: corre los casos fijos (casos_reglas.json) y mide las reglas contra las correcciones
del analista (ejemplos.json). Úsalo antes de subir un cambio de reglas:

    python3 pruebas/probar_reglas.py

Sale con código 1 si algún caso fijo falla. Usa una copia temporal de config.json (no toca los datos del monitor)."""
import importlib.util
import json
import os
import shutil
import sys
import tempfile

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(RAIZ)
tmp = tempfile.mkdtemp(prefix="pruebas-reglas-")
shutil.copy(os.path.join(RAIZ, "config.json"), os.path.join(tmp, "config.json"))
os.environ["DATA_DIR"] = tmp
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("srv", os.path.join(RAIZ, "server.py"))
srv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(srv)

casos = json.load(open(os.path.join(RAIZ, "pruebas", "casos_reglas.json"), encoding="utf-8"))
fallos = []
for c in casos["entidades"]:
    r = srv.detectar_bancos(c["texto"], c.get("es_peru", False))
    if sorted(r) != sorted(c["espera"]):
        fallos.append(f"entidad · «{c['texto'][:70]}» → {r}, se esperaba {c['espera']}")
for c in casos["cita"]:
    r = srv.detectar_bancos(c["texto"] + " " + c["contexto"], False)
    if sorted(r) != sorted(c["espera"]):
        fallos.append(f"cita · «{c['texto'][:70]}» → {r}, se esperaba {c['espera']}")
for c in casos["ciber"]:
    ev = srv.evaluar_alerta_ciber(c["texto"], c.get("cuenta_peruana", False))
    r = None if ev is None else sorted(ev["entidades"])
    if r != (None if c["espera"] is None else sorted(c["espera"])):
        fallos.append(f"ciber · «{c['texto'][:70]}» → {ev}, se esperaba {c['espera']}")
# Regla de interrupción y de patrones del reporte (casos sintéticos: cuándo es una caída y cuándo son casos aislados)
from datetime import datetime, timedelta, timezone
H = datetime(2026, 11, 3, 20, 30, tzinfo=timezone.utc)


def fila(min_antes, autor, texto="no puedo entrar a la app", tipo="", cats=("App / canales caídos",), queja=1,
         fuente="X", b="Interbank"):
    return {"id": autor + str(min_antes), "_bancos": [b], "_cats": set(cats), "_meta": {"tipo": tipo} if tipo else {},
            "es_queja": queja, "_autor": autor, "texto": texto, "fecha": srv.iso(H - timedelta(minutes=min_antes)),
            "fuente": fuente}


interrupciones = [
    ("2 quejas sueltas", [fila(200, "a"), fila(20, "b")], "aislado"),
    ("5 quejas repartidas en 5 h", [fila(300 - i * 60, "u%d" % i) for i in range(5)], "aislado"),
    ("4 usuarios en 40 min", [fila(50 - i * 10, "u%d" % i) for i in range(4)], "Grave"),
    ("3 usuarios en 1 h + prensa", [fila(50, "a"), fila(40, "b"), fila(30, "c"),
                                    fila(25, "rpp", "Se cae la app de Interbank", "noticia", fuente="Noticias")], "Crítico"),
    ("3 usuarios en 2 redes", [fila(50, "a"), fila(40, "b", fuente="Facebook"), fila(30, "c")], "Crítico"),
    ("7 usuarios en 1 h", [fila(55 - i * 7, "u%d" % i) for i in range(7)], "Crítico"),
    ("comunicado oficial", [fila(30, "interbank", "Estamos presentando intermitencia en nuestra app", "oficial", queja=0)],
     "Crítico"),
    ("mantenimiento anunciado + 4 usuarios",
     [fila(60, "interbank", "Hoy realizaremos un mantenimiento programado de nuestra app", "oficial", queja=0)]
     + [fila(50 - i * 10, "u%d" % i) for i in range(4)], "Normal"),
    ("un usuario repite 6 veces", [fila(50 - i * 5, "mismo") for i in range(6)], "aislado"),
]
for nombre, filas_, espera in interrupciones:
    v = srv.evaluar_interrupcion("Interbank", filas_, H)
    if (v or {}).get("nivel") != espera:
        fallos.append(f"interrupción · {nombre} → {v and v['nivel']}, se esperaba {espera}")
fraudes = [fila(100 - i * 10, "u%d" % i, t, cats=("Fraude / seguridad",), b="Yape") for i, t in enumerate(
    ["me cobraron con un QR falso", "pagué con QR y era falso", "QR trucado", "me robaron por un link", "cargo no reconocido"])]
if (srv.evaluar_patron("Yape", fraudes, "Fraude / seguridad", "Fraudes") or {}).get("nivel") != "En seguimiento" or \
        srv.evaluar_patron("Yape", fraudes[:2], "Fraude / seguridad", "Fraudes") is not None:
    fallos.append("patrón · fraude: 5 usuarios debe ser 'En seguimiento' y 2 usuarios, sin patrón")
for x, ref, espera in [(2, 1, "="), (4, 1, "↑"), (96, 80, "="), (96, 60, "↑"), (5, 20, "↓")]:
    if srv._flecha(x, ref) != espera:
        fallos.append(f"flecha · {x} frente a {ref} → {srv._flecha(x, ref)}, se esperaba {espera}")
total = len(casos["entidades"]) + len(casos["cita"]) + len(casos["ciber"]) + len(interrupciones) + 1 + 5
print(f"Casos fijos: {total - len(fallos)}/{total} correctos")
for f in fallos:
    print("  ✗", f)

# Contra las correcciones del analista: ¿las reglas (sin IA) coinciden con lo que él decidió?
try:
    ejemplos = json.load(open(os.path.join(RAIZ, "ejemplos.json"), encoding="utf-8"))
except (OSError, ValueError):
    ejemplos = []
if ejemplos:
    rel = [e for e in ejemplos if e.get("relevante") and e.get("entidades")]
    reconoce = sum(1 for e in rel if set(e["entidades"]) & set(srv.detectar_bancos(e["texto"], True)))
    filtra = [e for e in ejemplos if not e.get("relevante")]
    descarta = sum(1 for e in filtra if srv.es_ruido(e["texto"]) or not srv.es_relevante(e["texto"]))
    con_queja = [e for e in rel if "queja" in e]
    acierto_queja = sum(1 for e in con_queja if bool(srv.clasificar(e["texto"])[0]) == bool(e["queja"]))
    print(f"Correcciones del analista ({len(ejemplos)}):")
    print(f"  · entidad reconocida por las reglas en {reconoce}/{len(rel)} relevantes ({100 * reconoce // max(len(rel), 1)}%)")
    print(f"  · queja / no queja igual que el analista en {acierto_queja}/{len(con_queja)} ({100 * acierto_queja // max(len(con_queja), 1)}%)")
    print(f"  · no relevantes que las reglas ya descartan solas: {descarta}/{len(filtra)} (el resto lo filtra la IA)")
shutil.rmtree(tmp, ignore_errors=True)
sys.exit(1 if fallos else 0)
