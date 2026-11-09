"""Effet du niveau de cohérence (ONE, QUORUM, ALL) sur une écriture (T1) et deux lectures (T3, T4).

Usage (dans le conteneur app) :
    python bench_consistency.py --label 3_noeuds
    docker compose stop cassandra3            # depuis l'hôte
    python bench_consistency.py --label 2_noeuds
    docker compose start cassandra3

Résultat attendu avec RF = 3 :
  3 nœuds : tout réussit ; la latence croît avec le nombre de réplicas attendus (ONE < QUORUM < ALL).
  2 nœuds : ONE et QUORUM (2 réplicas sur 3) réussissent, ALL échoue avec « Unavailable ».
Résultat : RESULTS_DIR/consistency_<label>.csv
"""
import argparse
import csv
import os
import random
import statistics
import time
from collections import Counter
from datetime import datetime, timezone

from cassandra import OperationTimedOut, ReadTimeout, Unavailable, WriteTimeout
from cassandra.cluster import NoHostAvailable
from cassandra.util import uuid_from_time

from db import RESULTS_DIR, connect
from queries import Catalog, Workload

ERRORS = (Unavailable, ReadTimeout, WriteTimeout, OperationTimedOut, NoHostAvailable)
SYNTH_USER = 920_000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True, help="ex. 3_noeuds ou 2_noeuds")
    ap.add_argument("--runs", type=int, default=200)
    args = ap.parse_args()

    s = connect()
    wl = Workload(s)
    cat = Catalog.from_cassandra(s)
    rng = random.Random(11)
    users, tracks = list(cat.user_country), list(cat.tracks)
    written = []  # écoutes de test réellement écrites, annulées à la fin

    def t1(level):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        now = now.replace(microsecond=now.microsecond // 1000 * 1000)
        tid = rng.choice(tracks)
        e = {"user_id": SYNTH_USER, "listened_at": now, "listen_id": uuid_from_time(now), "track_id": tid,
             "ms_played": 180_000, "device": "mobile", "source": "search", "skipped": False, "country": "MA"}
        e.update(cat.tracks[tid])
        wl.t1_record_listen(e, profile=level)
        written.append(e)

    operations = [
        ("T1 écriture", t1),
        ("T3 lecture", lambda level: wl.t3_user_profile(rng.choice(users), profile=level)),
        ("T4 lecture", lambda level: wl.t4_last_listens(rng.choice(users), profile=level)),
    ]
    rows = []
    for level in ["ONE", "QUORUM", "ALL"]:
        for op, fn in operations:
            times, errors = [], Counter()
            for _ in range(args.runs):
                t0 = time.perf_counter()
                try:
                    fn(level)
                    times.append((time.perf_counter() - t0) * 1000)
                except ERRORS as exc:
                    errors[type(exc).__name__] += 1
            times.sort()
            row = {"label": args.label, "level": level, "operation": op, "runs": args.runs,
                   "ok": len(times), "errors": sum(errors.values()),
                   "error_types": ";".join(f"{k}:{v}" for k, v in errors.items()),
                   "p50_ms": round(statistics.median(times), 3) if times else "",
                   "p95_ms": round(times[min(len(times) - 1, round(0.95 * (len(times) - 1)))], 3) if times else ""}
            rows.append(row)
            print(f"{args.label:<10} {level:<7} {op:<12} ok {row['ok']:>4}/{args.runs:<4} "
                  f"p50 {row['p50_ms'] or '-':>8} ms  p95 {row['p95_ms'] or '-':>8} ms  {row['error_types']}")

    # annulation des écritures de test (au niveau par défaut, QUORUM)
    try:
        wl.rollback_counters(written)
        wl.t13_erase_history(SYNTH_USER)
    except ERRORS as exc:
        print(f"Nettoyage impossible maintenant ({type(exc).__name__}) : relancez après le retour du nœud.")

    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, f"consistency_{args.label}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Résultats : {path}")


if __name__ == "__main__":
    main()
