"""Génère un jeu de données réaliste et reproductible (fichiers JSON Lines).

Usage (dans le conteneur app) :
    python generate_data.py                      # volumes du sujet, graine 42
    python generate_data.py --listens 200000     # plus d'écoutes

Produit dans DATA_DIR : users.jsonl, artists.jsonl, tracks.jsonl,
playlists.jsonl, listens.jsonl et meta.json.

Les distributions ne sont pas uniformes, sinon les traitements Spark
n'auraient rien à montrer :
  - popularité des titres en loi de puissance (longue traîne -> A23) ;
  - 55 % des écoutes dans les genres préférés de l'utilisateur (-> ALS, S30) ;
  - écoutes groupées en sessions avec pauses (-> S28) ;
  - quelques nouveautés dont l'audience monte chaque semaine (-> S26) ;
  - premium plus actifs que gratuits (-> S29) ;
  - profil horaire réaliste (-> A22), probabilité de saut selon la source (-> A19).
"""
import argparse
import bisect
import json
import os
import random
import time
from datetime import date, datetime, timedelta, timezone

from cassandra.util import uuid_from_time
from faker import Faker

DATA_DIR = os.getenv("DATA_DIR", "/data")

# genre : (poids de popularité, bpm min, bpm max, probabilité explicite)
GENRES = {
    "pop": (14, 95, 130, 0.10), "hip-hop": (12, 80, 100, 0.55), "rap": (11, 80, 105, 0.60),
    "rock": (8, 100, 150, 0.08), "electro": (7, 120, 135, 0.05), "r&b": (6, 65, 100, 0.25),
    "rai": (6, 90, 125, 0.03), "house": (5, 118, 128, 0.04), "afrobeat": (5, 95, 120, 0.15),
    "chaabi": (4, 100, 140, 0.01), "latin": (4, 90, 130, 0.12), "reggae": (3, 60, 90, 0.10),
    "jazz": (3, 60, 160, 0.01), "classique": (3, 50, 140, 0.00), "metal": (3, 120, 190, 0.20),
    "k-pop": (3, 100, 140, 0.05), "gnawa": (2, 70, 110, 0.01), "blues": (2, 60, 120, 0.02),
    "folk": (2, 80, 120, 0.02), "soul": (2, 65, 110, 0.05),
}
USER_COUNTRIES = {"MA": 30, "FR": 18, "US": 8, "ES": 6, "CA": 5, "GB": 5, "DE": 5, "BE": 4,
                  "DZ": 4, "IT": 3, "SN": 3, "TN": 3, "EG": 2, "NG": 2, "BR": 2}
ARTIST_COUNTRIES = {"US": 20, "FR": 18, "MA": 14, "GB": 10, "DZ": 5, "NG": 5, "KR": 4, "ES": 4,
                    "DE": 4, "CA": 4, "BE": 3, "SN": 3, "BR": 3, "JM": 2, "IT": 1}
SUBSCRIPTIONS = {"free": 55, "premium": 30, "family": 15}
ACTIVITY_FACTOR = {"free": 0.8, "premium": 1.6, "family": 1.2}
DEVICES = {"mobile": 50, "desktop": 18, "web": 10, "tablet": 8, "smart_speaker": 6, "tv": 4, "car": 4}
SOURCE_WEIGHTS = {"free": {"playlist": 35, "album": 10, "search": 15, "radio": 40},
                  "premium": {"playlist": 40, "album": 25, "search": 20, "radio": 15},
                  "family": {"playlist": 40, "album": 20, "search": 20, "radio": 20}}
SKIP_BY_SOURCE = {"radio": 0.32, "playlist": 0.20, "album": 0.12, "search": 0.08}
# profil horaire (UTC) : creux la nuit, trajets du matin, pic en soirée
HOUR_WEIGHTS = [2, 1, .6, .4, .4, .6, 1.5, 3.5, 4.5, 4, 3.5, 3.5,
                4, 4, 3.5, 3.5, 4, 5, 6, 7, 7.5, 7, 5.5, 3.5]
LABELS = ["Universal", "Sony Music", "Warner", "Believe", "Because Music", "Ninja Tune",
          "XL Recordings", "Def Jam", "Atlantic", "Columbia", "Virgin", "Indépendant", "Autoproduit"]
BAND_SUFFIX = ["Club", "Collective", "Brothers", "Orchestra", "Project", "Sound System", "Crew", "Quartet"]
STAGE_PREFIX = {"rap": ["Lil", "MC", "Big"], "hip-hop": ["Lil", "Young", "MC"], "electro": ["DJ"],
                "house": ["DJ"], "rai": ["Cheb", "Cheba"], "chaabi": ["Haj"], "gnawa": ["Maâlem"]}
SOCIAL_NETWORKS = ["instagram", "x", "youtube", "tiktok", "soundcloud"]
PLAYLIST_NAMES = ["{g} du moment", "Ma sélection {g}", "{w} vibes", "Road trip {g}", "Soirée {g}",
                  "Chill {w}", "Workout {g}", "Découvertes {g}", "Best of {g}", "{w} & {g}"]


class Picker:
    """Tirage pondéré en O(log n) (sommes cumulées + recherche dichotomique)."""

    def __init__(self, items, weights, rng):
        self.items, self.rng, self.cum, total = list(items), rng, [], 0.0
        for w in weights:
            total += w
            self.cum.append(total)
        self.total = total

    def pick(self):
        return self.items[bisect.bisect_right(self.cum, self.rng.random() * self.total)]


def iso(dt):
    return dt.isoformat(timespec="milliseconds")


def title_from(fake, rng, min_words=1, max_words=4):
    words = fake.words(nb=rng.randint(min_words, max_words))
    return " ".join(words).capitalize()


def gen_users(n, rng, fakes, end):
    country_p = Picker(USER_COUNTRIES, USER_COUNTRIES.values(), rng)
    sub_p = Picker(SUBSCRIPTIONS, SUBSCRIPTIONS.values(), rng)
    genre_p = Picker(GENRES, [g[0] for g in GENRES.values()], rng)
    device_p = Picker(DEVICES, DEVICES.values(), rng)
    users, seen = [], set()
    for uid in range(1, n + 1):
        name = rng.choice(fakes).user_name()
        if name in seen:
            name = f"{name}{uid}"
        seen.add(name)
        sub = sub_p.pick()
        favs = set()
        for _ in range(rng.choice([1, 2, 2, 3, 3, 4])):
            favs.add(genre_p.pick())
        devices = []
        for _ in range(rng.choice([1, 1, 2, 2, 3])):
            d = device_p.pick()
            if d not in devices:
                devices.append(d)
        signup = end - timedelta(days=int(rng.random() ** 1.6 * 365 * 6))
        users.append({
            "user_id": uid, "username": name, "country": country_p.pick(),
            "age": int(min(75, max(13, rng.gauss(29, 10)))), "subscription": sub,
            "favorite_genres": sorted(favs), "signup_date": signup.isoformat(), "devices": devices,
            "_activity": min(60.0, rng.paretovariate(1.6)) * ACTIVITY_FACTOR[sub],
        })
    return users


def gen_artists(n, rng, fakes):
    country_p = Picker(ARTIST_COUNTRIES, ARTIST_COUNTRIES.values(), rng)
    genre_p = Picker(GENRES, [g[0] for g in GENRES.values()], rng)
    artists = []
    for aid in range(1, n + 1):
        fake = rng.choice(fakes)
        genres = [genre_p.pick()]
        for _ in range(rng.choice([0, 0, 1, 1, 2])):
            g = genre_p.pick()
            if g not in genres:
                genres.append(g)
        if rng.random() < 0.35:  # groupe
            name = f"{fake.word().capitalize()} {rng.choice(BAND_SUFFIX)}"
            members = [fake.name() for _ in range(rng.randint(2, 5))]
        else:  # artiste solo, parfois avec un nom de scène
            first = fake.first_name()
            prefix = STAGE_PREFIX.get(genres[0])
            name = f"{rng.choice(prefix)} {first}" if prefix and rng.random() < 0.5 else f"{first} {fake.last_name()}"
            members = [name]
        slug = "".join(c for c in name.lower() if c.isalnum())[:20] or f"artist{aid}"
        social = {net: (f"youtube.com/@{slug}" if net == "youtube" else f"@{slug}")
                  for net in rng.sample(SOCIAL_NETWORKS, rng.randint(1, 4))}
        artists.append({
            "artist_id": aid, "name": name, "country": country_p.pick(), "genres": genres,
            "members": members, "social": social, "_popularity": rng.paretovariate(1.2),
        })
    return artists


def gen_tracks(n, artists, rng, fakes, end):
    # chaque artiste a au moins un titre ; le reste est réparti selon la popularité
    counts = [1] * len(artists)
    share = Picker(range(len(artists)), [a["_popularity"] ** 0.5 + 0.5 for a in artists], rng)
    for _ in range(n - len(artists)):
        counts[share.pick()] += 1
    tracks, tid = [], 1
    for artist, count in zip(artists, counts):
        fake = rng.choice(fakes)
        n_albums = max(1, round(count / rng.randint(6, 12)))
        year = rng.randint(1985, end.year - 1)
        albums = []
        for _ in range(n_albums):
            albums.append({"title": title_from(fake, rng, 1, 3), "year": min(year, end.year),
                           "label": rng.choice(LABELS)})
            year += rng.randint(1, 4)
        for k in range(count):
            genre = artist["genres"][0] if rng.random() < 0.7 else rng.choice(artist["genres"])
            _, bpm_lo, bpm_hi, p_explicit = GENRES[genre]
            tracks.append({
                "track_id": tid, "title": title_from(fake, rng), "artist_id": artist["artist_id"],
                "artist_name": artist["name"], "album": dict(albums[min(k * n_albums // count, n_albums - 1)]),
                "duration_sec": int(min(600, max(90, rng.gauss(215, 50)))), "genre": genre,
                "bpm": rng.randint(bpm_lo, bpm_hi), "explicit": rng.random() < p_explicit,
                # popularité : artiste populaire x rang du titre chez l'artiste x bruit
                "_weight": artist["_popularity"] / (k + 1) ** 0.8 * rng.lognormvariate(0, 0.6),
            })
            tid += 1
    return tracks


def mark_rising_tracks(tracks, rng, end, n_rising=60):
    """Nouveautés : sorties récemment en single, leur audience monte (S26)."""
    rising = rng.sample(tracks, n_rising)
    for t in rising:
        t["album"] = {"title": f"{t['title']} (Single)", "year": end.year, "label": rng.choice(LABELS)}
        t["_release"] = end - timedelta(days=rng.randint(7, 42))
        t["_peak"] = rng.uniform(0.5, 3.0)
    return rising


def gen_playlists(n, users, tracks, genre_pickers, global_picker, rng, fakes, end):
    owner_p = Picker(range(len(users)), [u["_activity"] for u in users], rng)
    playlists = []
    for _ in range(n):
        owner = users[owner_p.pick()]
        fake = rng.choice(fakes)
        favs = owner["favorite_genres"]
        size = rng.randint(8, 60)
        chosen, seen = [], set()
        while len(chosen) < size:
            picker = genre_pickers[rng.choice(favs)] if rng.random() < 0.7 else global_picker
            tid = picker.pick()
            if tid not in seen:
                seen.add(tid)
                chosen.append(tid)
        public = rng.random() < 0.75
        # loi de Lomax : environ 10 % des playlists publiques dépassent 100 abonnés
        followers = min(9000, int((rng.paretovariate(1.1) - 1) * 15)) if public else 0
        signup = date.fromisoformat(owner["signup_date"])
        created = datetime.combine(signup, datetime.min.time()) + timedelta(
            seconds=rng.randint(0, max(1, int((end - signup).total_seconds()))))
        playlists.append({
            "playlist_id": str(fake.uuid4()), "owner_id": owner["user_id"],
            "name": rng.choice(PLAYLIST_NAMES).format(g=rng.choice(favs).capitalize(), w=fake.word().capitalize()),
            "is_public": public, "tracks": chosen, "followers": followers, "created_at": iso(created),
        })
    return playlists


def gen_listens(n, users, tracks, playlists, rising, genre_pickers, global_picker, rng, start, days):
    by_id = {t["track_id"]: t for t in tracks}
    album_tracks = {}
    for t in tracks:  # ordre des pistes de chaque album
        album_tracks.setdefault((t["artist_id"], t["album"]["title"]), []).append(t["track_id"])
    user_playlists = {}
    for p in playlists:
        user_playlists.setdefault(p["owner_id"], []).append(p)
    public = [p for p in playlists if p["is_public"]]
    public_p = Picker(range(len(public)), [p["followers"] + 1 for p in public], rng)

    # popularité des nouveautés, jour par jour (montée sur 3 semaines)
    trend_pickers = []
    for d in range(days):
        day = start + timedelta(days=d)
        weights = [t["_peak"] * min(1.0, (day - t["_release"]).days / 21) if day >= t["_release"] else 0
                   for t in rising]
        trend_pickers.append(Picker([t["track_id"] for t in rising], weights, rng) if sum(weights) else None)

    day_p = Picker(range(days), [(1 + 0.35 * d / max(1, days - 1)) *
                                 (1.15 if (start + timedelta(days=d)).weekday() >= 5 else 1.0)
                                 for d in range(days)], rng)
    hour_p = Picker(range(24), HOUR_WEIGHTS, rng)
    user_p = Picker(range(len(users)), [u["_activity"] for u in users], rng)
    source_p = {s: Picker(w, w.values(), rng) for s, w in SOURCE_WEIGHTS.items()}
    end_dt = datetime.combine(start + timedelta(days=days), datetime.min.time())

    def pick_for(user, d):
        r = rng.random()
        if r < 0.06 and trend_pickers[d] is not None:
            return trend_pickers[d].pick()
        if r < 0.61:
            return genre_pickers[rng.choice(user["favorite_genres"])].pick()
        return global_picker.pick()

    listens = []
    while len(listens) < n:
        user = users[user_p.pick()]
        signup = date.fromisoformat(user["signup_date"])
        d = day_p.pick()
        if start + timedelta(days=d) < signup:
            first = (signup - start).days
            if first >= days:
                continue
            d = rng.randint(first, days - 1)
        t = (datetime.combine(start + timedelta(days=d), datetime.min.time())
             + timedelta(hours=hour_p.pick(), minutes=rng.randrange(60), seconds=rng.randrange(60)))
        sub = user["subscription"]
        source = source_p[sub].pick()
        device = user["devices"][0] if rng.random() < 0.6 else rng.choice(user["devices"])
        length = 1 + min(39, int(rng.expovariate(1 / (7 if sub != "free" else 5))))

        if source == "album":
            first = by_id[pick_for(user, d)]
            seq = album_tracks[(first["artist_id"], first["album"]["title"])]
            i = seq.index(first["track_id"])
            seq = seq[i:i + length]
        elif source == "playlist":
            own = user_playlists.get(user["user_id"])
            pl = rng.choice(own) if own and rng.random() < 0.5 else public[public_p.pick()]
            off = rng.randrange(len(pl["tracks"]))
            seq = pl["tracks"][off:off + length]
        elif source == "radio":
            seed = by_id[pick_for(user, d)]
            seq = [seed["track_id"]] + [genre_pickers[seed["genre"]].pick() for _ in range(length - 1)]
        else:  # search : peu de titres, choisis volontairement
            seq = [pick_for(user, d) for _ in range(min(length, rng.randint(1, 4)))]

        for tid in seq:
            if t >= end_dt or len(listens) >= n:
                break
            track = by_id[tid]
            p_skip = SKIP_BY_SOURCE[source] + (-0.07 if track["genre"] in user["favorite_genres"] else 0.07)
            p_skip += 0.04 if sub == "free" else 0.0
            skipped = rng.random() < max(0.02, min(0.9, p_skip))
            duration_ms = track["duration_sec"] * 1000
            ms = (rng.randint(3000, max(4000, min(45000, duration_ms // 2))) if skipped
                  else int(duration_ms * rng.uniform(0.93, 1.0)))
            listens.append({
                "listen_id": str(uuid_from_time(t.replace(tzinfo=timezone.utc), node=rng.getrandbits(48),
                                                clock_seq=rng.getrandbits(14))),
                "user_id": user["user_id"], "track_id": tid, "listened_at": iso(t),
                "ms_played": ms, "device": device, "source": source, "skipped": skipped,
            })
            t += timedelta(milliseconds=ms + rng.randint(0, 6000))
    listens.sort(key=lambda x: x["listened_at"])
    return listens


def write_jsonl(name, rows):
    path = os.path.join(DATA_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps({k: v for k, v in r.items() if not k.startswith("_")}, ensure_ascii=False) + "\n")
    print(f"  {name:<16} {len(rows):>8} lignes")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--users", type=int, default=10_000)
    ap.add_argument("--artists", type=int, default=2_000)
    ap.add_argument("--tracks", type=int, default=20_000)
    ap.add_argument("--playlists", type=int, default=5_000)
    ap.add_argument("--listens", type=int, default=120_000)
    ap.add_argument("--days", type=int, default=90, help="profondeur de l'historique d'écoutes")
    ap.add_argument("--end", default=None, help="dernier jour (AAAA-MM-JJ), par défaut hier (UTC)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    t0 = time.time()
    rng = random.Random(args.seed)
    Faker.seed(args.seed)
    fakes = [Faker("fr_FR"), Faker("en_US")]
    for i, f in enumerate(fakes):
        f.seed_instance(args.seed + i)
    end = date.fromisoformat(args.end) if args.end else datetime.now(timezone.utc).date() - timedelta(days=1)
    start = end - timedelta(days=args.days - 1)
    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"Génération (graine {args.seed}, écoutes du {start} au {end})")
    users = gen_users(args.users, rng, fakes, end)
    artists = gen_artists(args.artists, rng, fakes)
    tracks = gen_tracks(args.tracks, artists, rng, fakes, end)
    rising = mark_rising_tracks(tracks, rng, end)
    genre_pickers = {}
    for g in GENRES:
        pool = [t for t in tracks if t["genre"] == g] or tracks
        genre_pickers[g] = Picker([t["track_id"] for t in pool], [t["_weight"] for t in pool], rng)
    global_picker = Picker([t["track_id"] for t in tracks], [t["_weight"] for t in tracks], rng)
    playlists = gen_playlists(args.playlists, users, tracks, genre_pickers, global_picker, rng, fakes, end)
    listens = gen_listens(args.listens, users, tracks, playlists, rising, genre_pickers,
                          global_picker, rng, start, args.days)

    for name, rows in [("users.jsonl", users), ("artists.jsonl", artists), ("tracks.jsonl", tracks),
                       ("playlists.jsonl", playlists), ("listens.jsonl", listens)]:
        write_jsonl(name, rows)
    meta = {"seed": args.seed, "start_date": start.isoformat(), "end_date": end.isoformat(),
            "days": args.days, "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "counts": {"users": len(users), "artists": len(artists), "tracks": len(tracks),
                       "playlists": len(playlists), "listens": len(listens)}}
    with open(os.path.join(DATA_DIR, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    total = sum(meta["counts"].values())
    print(f"Total : {total:,} éléments en {time.time() - t0:.1f} s -> {DATA_DIR}")


if __name__ == "__main__":
    main()
