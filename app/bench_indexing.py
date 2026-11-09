"""Effet de l'indexation sur deux requêtes du workload : T14 et A24.

Variantes mesurées :
  T14  table dédiée tracks_by_artist (partition = artiste, tri par le clustering)
  T14  index SAI sur tracks(artist_id), tri par année côté client
  T14  sans index : ALLOW FILTERING (parcours complet de la table tracks)
  A24  deux index SAI (is_public et followers_snapshot), intersection côté serveur
  A24  sans index : ALLOW FILTERING (parcours complet de la table playlists)

Le script supprime puis recrée les index : ne le lancez pas pendant la démonstration.
Usage : python bench_indexing.py [--runs 30]
Résultat : RESULTS_DIR/indexing.csv
"""
import argparse
import csv
import os
import random
import statistics
import time

from db import RESULTS_DIR, connect

INDEXES = {
    "tracks_artist_idx": "CREATE INDEX IF NOT EXISTS tracks_artist_idx ON tracks (artist_id) USING 'sai'",
    "playlists_public_idx": "CREATE INDEX IF NOT EXISTS playlists_public_idx ON playlists (is_public) USING 'sai'",
    "playlists_followers_idx": "CREATE INDEX IF NOT EXISTS playlists_followers_idx "
                               "ON playlists (followers_snapshot) USING 'sai'",
}


def wait_until_queryable(session, statement, params, timeout=300):
    """Un index SAI se construit en arrière-plan : on attend que la requête réussisse 3 fois de suite."""
    deadline, ok = time.time() + timeout, 0
    while ok < 3:
        try:
            list(session.execute(statement, params))
            ok += 1
        except Exception:  # index en construction sur au moins un réplica
            ok = 0
            if time.time() > deadline:
                raise
            time.sleep(2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=30)
    args = ap.parse_args()
    s = connect()
    rng = random.Random(5)
    artists = [r.artist_id for r in s.execute("SELECT artist_id FROM artists", execution_profile="SLOW")]
    rows = []

    def measure(query, variant, statement, params_fn, sort_key=None, runs=args.runs):
        times, n = [], 0
        for i in range(runs + 2):
            params = params_fn()
            t0 = time.perf_counter()
            result = list(s.execute(statement, params, execution_profile="SLOW"))
            if sort_key:
                result.sort(key=sort_key)
            dt = (time.perf_counter() - t0) * 1000
            if i >= 2:  # 2 exécutions d'échauffement
                times.append(dt)
                n = len(result)
        times.sort()
        row = {"query": query, "variant": variant, "runs": runs, "rows": n,
               "p50_ms": round(statistics.median(times), 3),
               "p95_ms": round(times[min(len(times) - 1, round(0.95 * (len(times) - 1)))], 3)}
        rows.append(row)
        print(f"{query:<4} {variant:<44} p50 {row['p50_ms']:9.2f} ms   p95 {row['p95_ms']:9.2f} ms   ({n} lignes)")

    # 1) avec index (état normal du schéma)
    for ddl in INDEXES.values():
        s.execute(ddl)
    q_t14_dedicated = s.prepare("SELECT album_year, album_title, track_id, title FROM tracks_by_artist "
                                "WHERE artist_id = ?")
    q_t14_index = s.prepare("SELECT track_id, title, album FROM tracks WHERE artist_id = ?")
    q_a24_index = s.prepare("SELECT playlist_id, name, followers_snapshot FROM playlists "
                            "WHERE is_public = true AND followers_snapshot > 100")
    wait_until_queryable(s, q_t14_index, (artists[0],))
    wait_until_queryable(s, q_a24_index, ())

    measure("T14", "table dédiée tracks_by_artist", q_t14_dedicated, lambda: (rng.choice(artists),))
    measure("T14", "index SAI sur tracks(artist_id) + tri client", q_t14_index, lambda: (rng.choice(artists),),
            sort_key=lambda r: -r.album.year)
    measure("A24", "2 index SAI (is_public, followers_snapshot)", q_a24_index, lambda: ())

    # 2) sans index : ALLOW FILTERING
    print("Suppression des index…")
    for name in INDEXES:
        s.execute(f"DROP INDEX IF EXISTS {name}")
    q_t14_scan = s.prepare("SELECT track_id, title, album FROM tracks WHERE artist_id = ? ALLOW FILTERING")
    q_a24_scan = s.prepare("SELECT playlist_id, name, followers_snapshot FROM playlists "
                           "WHERE is_public = true AND followers_snapshot > 100 ALLOW FILTERING")
    measure("T14", "sans index : ALLOW FILTERING", q_t14_scan, lambda: (rng.choice(artists),),
            sort_key=lambda r: -r.album.year, runs=max(5, args.runs // 3))
    measure("A24", "sans index : ALLOW FILTERING", q_a24_scan, lambda: (), runs=max(5, args.runs // 3))

    # 3) retour à l'état du schéma
    print("Recréation des index (construction en arrière-plan)…")
    for ddl in INDEXES.values():
        s.execute(ddl)
    wait_until_queryable(s, q_t14_index, (artists[0],))
    wait_until_queryable(s, q_a24_index, ())

    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, "indexing.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Résultats : {path}")


if __name__ == "__main__":
    main()
