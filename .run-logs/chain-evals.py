"""Encadena: por cada prediccion v2 que termina bien, lanza su evaluacion primaria.

El marco NO se inventa. Sale de las evaluaciones primarias historicas:
  evaluation_set_id            8763acda  (unico en las 280 filas primarias)
  information_accretion_set_id 3f50aa55  (regimen lafa, el de las 81 primarias)
  scoring_config_ids           las 7 que cubren las 280 filas
  leakage_role                 AUSENTE. Estas son la rejilla primaria del rung 1,
                               no diagnosticos, asi que no se marcan como probe.
"""
import json, time, urllib.request
from pathlib import Path
from sqlalchemy import create_engine, text
from protea.infrastructure.settings import load_settings

EVAL_SET = "8763acda-c3b3-49f8-a5e1-b0955206ee3a"
IA_SET   = "3f50aa55-0908-4f14-bf34-6317fa19abbb"
SCORING  = json.load(open("/tmp/scoring_configs.json"))
API      = "http://localhost:8000/v1/jobs"

s = load_settings(Path("/home/bioxaxi2/Thesis-laptop/PROTEA"))
e = create_engine(s.db_url)
done = set()

def prediction_sets_ready():
    """Conjuntos de las corridas v2 terminadas con exito y aun sin evaluar."""
    with e.connect() as c:
        return [(str(r[0]), r[1]) for r in c.execute(text("""
            select ps.id, left(j.description,70)
            from job j
            join prediction_set ps on ps.id = (j.payload->>'_prediction_set_id')::uuid
            where 'rung1-recompute-v2' = any(j.tags) and j.status::text='SUCCEEDED'
        """))]

def prediction_sets_by_time():
    """Conjuntos listos para evaluar.

    La condicion es que la CORRIDA haya terminado con exito, no que el conjunto
    tenga filas. Version anterior usaba "tiene filas" y evaluo dos conjuntos a
    medio escribir (ProtST K=3 con 5.192 de 6.216 proteinas, K=30 con 2.471),
    produciendo puntuaciones plausibles sobre poblacion incompleta. Es el mismo
    defecto que la campana lleva persiguiendo: un numero creible calculado sobre
    la poblacion equivocada.
    """
    with e.connect() as c:
        return [(str(r[0]), f"{r[1]} K={r[2]}") for r in c.execute(text("""
            select ps.id, ec.model_name, ps.limit_per_entry
            from prediction_set ps
            join embedding_config ec on ec.id = ps.embedding_config_id
            join job j
              on 'rung1-recompute-v2' = any(j.tags)
             and j.payload->>'embedding_config_id' = ps.embedding_config_id::text
             and (j.payload->>'limit_per_entry')::int = ps.limit_per_entry
             and j.status::text = 'SUCCEEDED'
            where ps.created_at > timestamp '2026-08-18 01:35:00'
              and not exists (select 1 from evaluation_result er
                              where er.prediction_set_id = ps.id)
            order by ps.created_at
        """))]

while True:
    try:
        pending = prediction_sets_by_time()
    except Exception as ex:
        print(f"consulta fallo: {ex}", flush=True); time.sleep(120); continue
    for ps_id, label in pending:
        if ps_id in done:
            continue
        body = {"operation": "batch_rescore_evaluation",
                "queue_name": "protea.evaluations",
                "description": f"rung1 eval v2: {label}",
                "tags": ["rung1-eval-v2", "laptop"],
                "payload": {"evaluation_set_id": EVAL_SET,
                            "information_accretion_set_id": IA_SET,
                            "prediction_set_id": ps_id,
                            "scoring_config_ids": SCORING}}
        try:
            req = urllib.request.Request(API, data=json.dumps(body).encode(),
                                         headers={"Content-Type": "application/json"})
            r = json.load(urllib.request.urlopen(req, timeout=60))
            done.add(ps_id)
            print(f"EVAL lanzada  {label}  job={r['id'][:8]}", flush=True)
        except Exception as ex:
            print(f"EVAL fallo    {label}: {ex}", flush=True)
    # 32 corridas: 12 estrechas mias mas 20 anchas. El tope estaba en 12 y
    # habria dejado los anchos sin evaluar sin emitir nada, que es justo la
    # truncacion silenciosa que venimos persiguiendo.
    if len(done) >= 32:
        print("LAS 32 EVALUACIONES ESTAN LANZADAS", flush=True)
        break
    time.sleep(120)
