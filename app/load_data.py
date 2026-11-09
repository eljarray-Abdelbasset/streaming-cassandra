"""Charge les fichiers JSONL produits par generate_data.py dans Cassandra.

Usage (dans le conteneur app) :
    python load_data.py --reset        # vide toutes les tables puis recharge

Requêtes préparées + execute_concurrent (jusqu'à 200 requêtes en vol) : le pilote
« token-aware » envoie chaque écriture directement à un réplica de sa partition.
Les écoutes passent par T2 (import en lot avec compteurs pré-agrégés).
"""
import argparse
import json
import os
import time
from datetime import date, datetime
from uuid import UUID

from cassandra.concurrent import execute_concurrent_with_args

from db import DATA_DIR, connect
from queries import Album, Catalog, Workload

TABLES = ["users", "users_by_sub_country", "artists", "tracks", "tracks_by_artist", "listens_by_user",
          "track_listen_counts", "weekly_track_counts", "country_artist_counts", "yearly_album_counts",
          "daily_listen_counts", "hourly_listen_counts", "genre_stats", "device_stats", "playlists",
          "playlists_by_user", "playlist_followers", "playlist_stats", "meta", "trending_tracks",
          "recommendations_by_user", "similar_artists", "spark_kpis"]


def read_jsonl(name):
    with open(os.path.join(DATA_DIR, name), encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def load(session, label, statement, params, concurrency=200):
    t0 = time.perf_counter()
    execute_concurrent_with_args(session, statement, params, concurrency=concurrency, raise_on_first_error=True)
    dt = time.perf_counter() - t0
    print(f"  {label:<28} {len(params):>8} lignes  {dt:6.1f} s  ({len(params) / dt:,.0f} écritures/s)")


def ts(value):
    return datetime.fromisoformat(value)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="vider toutes les tables avant de charger")
    ap.add_argument("--chunk", type=int, default=20_000, help="taille des lots d'écoutes (T2)")
    args = ap.parse_args()

    s = connect()
    wl = Workload(s)
    if args.reset:
        print("Vidage des tables (TRUNCATE)…")
        for table in TABLES:
            s.execute(f"TRUNCATE {table}", execution_profile="SLOW")
    elif s.execute("SELECT value FROM meta WHERE key = 'loaded_at'").one():
        raise SystemExit("Données déjà chargées : relancez avec --reset (les compteurs ne sont pas idempotents).")

    t_start = time.perf_counter()
    print("Chargement des référentiels")
    users = read_jsonl("users.jsonl")
    load(s, "users", s.prepare(
        "INSERT INTO users (user_id, username, country, age, subscription, favorite_genres, signup_date, devices) "
        "VALUES (?,?,?,?,?,?,?,?)"),
        [(u["user_id"], u["username"], u["country"], u["age"], u["subscription"], set(u["favorite_genres"]),
          date.fromisoformat(u["signup_date"]), u["devices"]) for u in users])
    load(s, "users_by_sub_country", wl.ins_usc,
         [(u["subscription"], u["country"], u["user_id"], u["username"]) for u in users])

    artists = read_jsonl("artists.jsonl")
    load(s, "artists", s.prepare(
        "INSERT INTO artists (artist_id, name, country, genres, members, social) VALUES (?,?,?,?,?,?)"),
        [(a["artist_id"], a["name"], a["country"], set(a["genres"]), a["members"], a["social"]) for a in artists])

    tracks = read_jsonl("tracks.jsonl")
    load(s, "tracks", s.prepare(
        "INSERT INTO tracks (track_id, title, artist_id, artist_name, album, duration_sec, genre, bpm, explicit) "
        "VALUES (?,?,?,?,?,?,?,?,?)"),
        [(t["track_id"], t["title"], t["artist_id"], t["artist_name"], Album(**t["album"]), t["duration_sec"],
          t["genre"], t["bpm"], t["explicit"]) for t in tracks])
    load(s, "tracks_by_artist", s.prepare(
        "INSERT INTO tracks_by_artist (artist_id, album_year, album_title, track_id, title, duration_sec, genre) "
        "VALUES (?,?,?,?,?,?,?)"),
        [(t["artist_id"], t["album"]["year"], t["album"]["title"], t["track_id"], t["title"], t["duration_sec"],
          t["genre"]) for t in tracks])

    print("Chargement des playlists")
    playlists = read_jsonl("playlists.jsonl")
    load(s, "playlists", wl.ins_playlist,
         [(UUID(p["playlist_id"]), p["owner_id"], p["name"], p["is_public"], p["tracks"], ts(p["created_at"]),
           0, p["followers"]) for p in playlists])
    load(s, "playlists_by_user", wl.ins_playlist_by_user,
         [(p["owner_id"], ts(p["created_at"]), UUID(p["playlist_id"]), p["name"], p["is_public"]) for p in playlists])
    load(s, "playlist_stats (compteur)", s.prepare(
        "UPDATE playlist_stats SET followers = followers + ? WHERE playlist_id = ?"),
        [(p["followers"], UUID(p["playlist_id"])) for p in playlists if p["followers"] > 0])

    print("Chargement des écoutes via T2 (insertions + compteurs pré-agrégés)")
    catalog = Catalog.from_rows(tracks, users)
    raw = read_jsonl("listens.jsonl")
    for i in range(0, len(raw), args.chunk):
        chunk = raw[i:i + args.chunk]
        events = [catalog.enrich({**e, "listened_at": ts(e["listened_at"]), "listen_id": UUID(e["listen_id"])})
                  for e in chunk]
        t0 = time.perf_counter()
        n_statements = wl.t2_bulk_listens(events)
        dt = time.perf_counter() - t0
        print(f"  écoutes {i + 1:>7}-{i + len(chunk):<7} {n_statements:>8} requêtes {dt:6.1f} s "
              f"({len(chunk) / dt:,.0f} écoutes/s)")

    with open(os.path.join(DATA_DIR, "meta.json"), encoding="utf-8") as f:
        meta = json.load(f)
    entries = {"dataset_end": meta["end_date"], "dataset_start": meta["start_date"],
               "loaded_at": datetime.now().isoformat(timespec="seconds")}
    entries.update({f"count_{k}": str(v) for k, v in meta["counts"].items()})
    load(s, "meta", s.prepare("INSERT INTO meta (key, value) VALUES (?, ?)"), list(entries.items()))

    print(f"\nChargement terminé en {time.perf_counter() - t_start:.0f} s. Vérification :")
    for table in ["users", "artists", "tracks", "playlists", "listens_by_user"]:
        n = s.execute(f"SELECT COUNT(*) FROM {table}", execution_profile="SLOW").one()[0]
        print(f"  {table:<16} {n:>8}")


if __name__ == "__main__":
    main()
