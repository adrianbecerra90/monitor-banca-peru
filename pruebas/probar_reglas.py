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
total = len(casos["entidades"]) + len(casos["cita"]) + len(casos["ciber"])
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
