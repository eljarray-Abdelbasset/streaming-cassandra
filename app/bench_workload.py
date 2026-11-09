"""Mesure le temps d'exécution des 25 requêtes natives (T1 à T15, A16 à A25).

Usage (dans le conteneur app) :
    python bench_workload.py                 # 100 répétitions des requêtes ponctuelles
    python bench_workload.py --runs 20       # plus rapide

Protocole : 3 exécutions d'échauffement non mesurées, puis N exécutions avec des
paramètres tirés au hasard (pour ne pas mesurer un cache). On retient la médiane
(p50) et le 95e centile (p95). Résultat : RESULTS_DIR/bench_native.csv

Les écritures de test (T1, T2, T6 à T11, T13) utilisent des identifiants réservés
et sont annulées à la fin : le jeu de données reste identique.
"""
import argparse
import csv
import os
import random
import statistics
import time
from datetime import datetime, timezone

from cassandra.util import uuid_from_time

from db import RESULTS_DIR, connect
from queries import Catalog, Workload

SYNTH_USERS = 900_000      # T1 / T2 : utilisateurs fictifs
SYNTH_T11 = 950_000        # T11
SYNTH_T13 = 990_000        # T13


def summarize(times):
    times = sorted(times)
    p95 = times[min(len(times) - 1, round(0.95 * (len(times) - 1)))]
    return {"mean_ms": statistics.fmean(times), "p50_ms": statistics.median(times), "p95_ms": p95,
            "min_ms": times[0], "max_ms": times[-1]}


def size(result):
    if result is None or isinstance(result, bool):
        return 1
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], list):
        return len(result[1])
    if isinstance(result, (list, tuple)):
        return len(result)
    return 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=100, help="répétitions des requêtes ponctuelles")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    runs_point, runs_scan = args.runs, max(5, args.runs // 5)

    s = connect()
    wl = Workload(s)
    cat = Catalog.from_cassandra(s)
    rng = random.Random(args.seed)
    users, tracks = list(cat.user_country), list(cat.tracks)
    countries = sorted(set(cat.user_country.values()))
    artists = sorted({t["artist_id"] for t in cat.tracks.values()})
    years = sorted({t["album_year"] for t in cat.tracks.values()})
    genres = sorted({t["genre"] for t in cat.tracks.values()})
    results, synth_events, synth_users, created_playlists = [], [], set(), []

    def measure(qid, label, make_call, runs, warmup=3):
        for _ in range(warmup):
            make_call()()
        times, rows = [], 0
        for _ in range(runs):
            call = make_call()                      # préparation non mesurée
            t0 = time.perf_counter()
            res = call()
            times.append((time.perf_counter() - t0) * 1000)
            rows = size(res)
        st = summarize(times)
        results.append({"query": qid, "label": label, "category": qid[0], "runs": runs, "rows": rows,
                        **{k: round(v, 3) for k, v in st.items()}})
        print(f"{qid:<4} {label:<46} p50 {st['p50_ms']:9.2f} ms   p95 {st['p95_ms']:9.2f} ms   ({runs} exéc.)")

    def synth_event(user_id):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        now = now.replace(microsecond=now.microsecond // 1000 * 1000)
        tid = rng.choice(tracks)
        e = {"user_id": user_id, "listened_at": now, "listen_id": uuid_from_time(now), "track_id": tid,
             "ms_played": rng.randint(20_000, 240_000), "device": rng.choice(["mobile", "desktop", "web"]),
             "source": rng.choice(["playlist", "album", "search", "radio"]), "skipped": rng.random() < 0.2,
             "country": rng.choice(countries)}
        e.update(cat.tracks[tid])
        synth_events.append(e)
        synth_users.add(user_id)
        return e

    print("Maintenance : recopie des compteurs d'abonnés pour A24…")
    wl.sync_followers_snapshot()
    print(f"\n{'':4} {'Requête':<46} (médiane et 95e centile)")

    # ------------------------------------------------------------ T1 à T15
    measure("T1", "Enregistrer une écoute (1 insert + 8 compteurs)",
            lambda: (lambda e=synth_event(SYNTH_USERS + rng.randint(1, 50)): wl.t1_record_listen(e)), runs_point)

    def make_t2():
        events = [synth_event(SYNTH_USERS + rng.randint(1, 50)) for _ in range(10_000)]
        return lambda: wl.t2_bulk_listens(events)
    measure("T2", "Importer 10 000 écoutes en lot", make_t2, runs=3, warmup=0)

    measure("T3", "Profil d'un utilisateur", lambda: (lambda u=rng.choice(users): wl.t3_user_profile(u)), runs_point)
    measure("T4", "50 dernières écoutes d'un utilisateur",
            lambda: (lambda u=rng.choice(users): wl.t4_last_listens(u)), runs_point)
    measure("T5", "Fiche d'un titre + artiste + album",
            lambda: (lambda t=rng.choice(tracks): wl.t5_track_sheet(t)), runs_point)

    def make_t6():
        owner = rng.choice(users)
        def call():
            pid = wl.t6_create_playlist(owner, "Playlist de test (bench)", True, rng.sample(tracks, 20))
            created_playlists.append(pid)
        return call
    measure("T6", "Créer une playlist (batch LOGGED)", make_t6, runs_point)

    bench_pid = created_playlists[0]
    measure("T7", "Insérer un titre à une position (LWT)",
            lambda: (lambda t=rng.choice(tracks), pos=rng.randint(0, 20): wl.t7_insert_track_at(bench_pid, t, pos)),
            runs_point)

    def make_t8():
        row = s.execute(wl.sel_playlist_tracks, (bench_pid,)).one()
        victim = rng.choice(row.tracks)
        return lambda: wl.t8_remove_track(bench_pid, victim)
    measure("T8", "Retirer un titre d'une playlist (LWT)", make_t8, min(runs_point, 60))

    follower_ids = iter(rng.sample(users, len(users)))
    measure("T9", "S'abonner à une playlist (LWT + compteur)",
            lambda: (lambda u=next(follower_ids): wl.t9_follow_playlist(bench_pid, u)), runs_point)

    t10_user = rng.choice(users)
    original_sub = wl.t3_user_profile(t10_user).subscription
    state = {"sub": original_sub}
    def make_t10():
        state["sub"] = "premium" if state["sub"] != "premium" else "free"
        target = state["sub"]
        return lambda: wl.t10_change_subscription(t10_user, target)
    measure("T10", "Changer l'abonnement (lecture + batch)", make_t10, runs_point)

    measure("T11", "Ajouter un genre préféré (ajout à un set)",
            lambda: (lambda u=SYNTH_T11 + rng.randint(1, 100), g=rng.choice(genres): wl.t11_add_favorite_genre(u, g)),
            runs_point)
    measure("T12", "Playlists d'un utilisateur", lambda: (lambda u=rng.choice(users): wl.t12_user_playlists(u)),
            runs_point)

    t13_counter = iter(range(1, 10**6))
    def make_t13():
        uid = SYNTH_T13 + next(t13_counter)
        events = [synth_event(uid) for _ in range(50)]
        synth_events[-50:] = []          # ces écoutes n'ont pas touché les compteurs
        wl._run_concurrent([(wl.ins_listen, wl._listen_params(e)) for e in events])
        return lambda: wl.t13_erase_history(uid)
    measure("T13", "Effacer l'historique (50 écoutes)", make_t13, min(runs_point, 50))

    measure("T14", "Titres d'un artiste triés par année",
            lambda: (lambda a=rng.choice(artists): wl.t14_artist_tracks(a)), runs_point)
    measure("T15", "Nombre total d'écoutes d'un titre (compteur)",
            lambda: (lambda t=rng.choice(tracks): wl.t15_track_total_listens(t)), runs_point)

    # ------------------------------------------------------------ A16 à A25
    measure("A16", "Écoutes par jour sur 30 jours", lambda: wl.a16_daily_listens, runs_scan)
    measure("A17", "Top 10 des titres de la semaine", lambda: wl.a17_top_tracks_week, runs_scan)
    measure("A18", "Top 10 des artistes d'un pays",
            lambda: (lambda c=rng.choice(countries): wl.a18_top_artists_by_country(c)), runs_scan)
    measure("A19", "Taux de titres sautés par genre", lambda: wl.a19_skip_rate_by_genre, runs_scan)
    measure("A20", "Durée moyenne d'écoute par appareil", lambda: wl.a20_avg_duration_by_device, runs_scan)
    measure("A21", "Utilisateurs par abonnement et pays (GROUP BY)",
            lambda: wl.a21_users_by_subscription_country, runs_scan)
    measure("A22", "Écoutes par heure de la journée", lambda: wl.a22_listens_by_hour, runs_scan)
    measure("A23", "Titres jamais écoutés (différence client)", lambda: wl.a23_never_listened,
            max(5, runs_scan // 2), warmup=1)
    measure("A24", "Playlists publiques > 100 abonnés (SAI)", lambda: wl.a24_popular_public_playlists, runs_scan)
    measure("A25", "Albums d'une année triés par écoutes",
            lambda: (lambda y=rng.choice(years[-15:]): wl.a25_albums_of_year(y)), runs_scan)

    # ------------------------------------------------------------ nettoyage
    print("\nNettoyage des écritures de test…")
    wl.rollback_counters(synth_events)
    for uid in synth_users:
        wl.t13_erase_history(uid)
    for pid in created_playlists:
        row = s.execute("SELECT owner_id, created_at FROM playlists WHERE playlist_id = %s", (pid,)).one()
        s.execute("DELETE FROM playlists_by_user WHERE owner_id = %s AND created_at = %s AND playlist_id = %s",
                  (row.owner_id, row.created_at, pid))
        for table in ("playlists", "playlist_followers", "playlist_stats"):
            s.execute(f"DELETE FROM {table} WHERE playlist_id = %s", (pid,))
    for uid in range(SYNTH_T11 + 1, SYNTH_T11 + 101):
        s.execute("DELETE FROM users WHERE user_id = %s", (uid,))
    wl.t10_change_subscription(t10_user, original_sub)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, "bench_native.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)
    print(f"Résultats : {path}")


if __name__ == "__main__":
    main()
