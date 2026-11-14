"""Les 25 requêtes natives du workload : T1 à T15 (transactionnelles), A16 à A25 (analytiques).

Chaque méthode exécute le CQL de la requête et renvoie des objets Python simples.
Le paramètre `profile` choisit le niveau de cohérence : "ONE", "QUORUM" ou "ALL"
(None = profil par défaut, QUORUM).
"""
import heapq
from collections import defaultdict, namedtuple
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent
from cassandra.query import BatchStatement, BatchType

# Un objet dont les attributs portent les noms des champs suffit pour écrire un UDT
Album = namedtuple("Album", "title year label")


def _ep(profile):
    return profile or EXEC_PROFILE_DEFAULT


def week_start(d):
    """Lundi de la semaine ISO contenant d (même convention que date_trunc('week') de Spark)."""
    return d - timedelta(days=d.weekday())


def month_key(d):
    return d.strftime("%Y-%m")


def to_date(v):
    """Le pilote renvoie les colonnes date en cassandra.util.Date."""
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return v.date()


class Catalog:
    """Métadonnées que l'application garde en mémoire (titre en cours de lecture, pays de l'utilisateur).
    Elles servent à enrichir une écoute avant T1 / T2, sans lecture supplémentaire."""

    def __init__(self, tracks, user_country):
        self.tracks = tracks              # track_id -> dict
        self.user_country = user_country  # user_id -> pays

    @classmethod
    def from_rows(cls, tracks, users):
        return cls({t["track_id"]: {"title": t["title"], "artist_id": t["artist_id"],
                                    "artist_name": t["artist_name"], "genre": t["genre"],
                                    "album_title": t["album"]["title"], "album_year": t["album"]["year"],
                                    "duration_sec": t["duration_sec"]} for t in tracks},
                   {u["user_id"]: u["country"] for u in users})

    @classmethod
    def from_cassandra(cls, session):
        tracks = [{"track_id": r.track_id, "title": r.title, "artist_id": r.artist_id,
                   "artist_name": r.artist_name, "genre": r.genre, "duration_sec": r.duration_sec,
                   "album": {"title": r.album.title, "year": r.album.year}}
                  for r in session.execute("SELECT track_id, title, artist_id, artist_name, genre, "
                                           "duration_sec, album FROM tracks", execution_profile="SLOW")]
        users = [{"user_id": r.user_id, "country": r.country}
                 for r in session.execute("SELECT user_id, country FROM users", execution_profile="SLOW")]
        return cls.from_rows(tracks, users)

    def enrich(self, event):
        e = dict(event)
        e.update(self.tracks[e["track_id"]])
        e["country"] = self.user_country[e["user_id"]]
        return e


class Workload:
    def __init__(self, session):
        self.s = session
        p = session.prepare
        self._end = None

        # ---- T1 / T2 : une écoute = 1 insertion + 8 incréments de compteurs
        self.ins_listen = p(
            "INSERT INTO listens_by_user (user_id, listened_at, listen_id, track_id, title, artist_id, "
            "artist_name, genre, ms_played, device, source, skipped) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
        self.inc = {
            "track": p("UPDATE track_listen_counts SET listens = listens + ? WHERE track_id = ?"),
            "week": p("UPDATE weekly_track_counts SET listens = listens + ? WHERE week_start = ? AND track_id = ?"),
            "country_artist": p("UPDATE country_artist_counts SET listens = listens + ? "
                                "WHERE country = ? AND artist_id = ?"),
            "album": p("UPDATE yearly_album_counts SET listens = listens + ? "
                       "WHERE album_year = ? AND artist_id = ? AND album_title = ?"),
            "day": p("UPDATE daily_listen_counts SET listens = listens + ? WHERE month = ? AND day = ?"),
            "hour": p("UPDATE hourly_listen_counts SET listens = listens + ? WHERE month = ? AND hour = ?"),
        }
        self.inc_genre = p("UPDATE genre_stats SET plays = plays + ?, skips = skips + ? WHERE genre = ?")
        self.inc_device = p("UPDATE device_stats SET plays = plays + ?, total_ms = total_ms + ? WHERE device = ?")

        # ---- T3 à T15
        self.sel_user = p("SELECT * FROM users WHERE user_id = ?")
        self.sel_history = p("SELECT listened_at, track_id, title, artist_name, ms_played, device, source, skipped "
                             "FROM listens_by_user WHERE user_id = ? LIMIT 50")
        self.sel_track = p("SELECT * FROM tracks WHERE track_id = ?")
        self.sel_artist = p("SELECT * FROM artists WHERE artist_id = ?")
        self.ins_playlist = p("INSERT INTO playlists (playlist_id, owner_id, name, is_public, tracks, created_at, "
                              "version, followers_snapshot) VALUES (?,?,?,?,?,?,?,?)")
        self.ins_playlist_by_user = p("INSERT INTO playlists_by_user (owner_id, created_at, playlist_id, name, "
                                      "is_public) VALUES (?,?,?,?,?)")
        self.sel_playlist_tracks = p("SELECT tracks, version FROM playlists WHERE playlist_id = ?")
        self.upd_playlist_tracks = p("UPDATE playlists SET tracks = ?, version = ? WHERE playlist_id = ? IF version = ?")
        self.ins_follower = p("INSERT INTO playlist_followers (playlist_id, user_id, followed_at) "
                              "VALUES (?,?,?) IF NOT EXISTS")
        self.inc_followers = p("UPDATE playlist_stats SET followers = followers + 1 WHERE playlist_id = ?")
        self.sel_user_sub = p("SELECT subscription, country, username FROM users WHERE user_id = ?")
        self.upd_user_sub = p("UPDATE users SET subscription = ? WHERE user_id = ?")
        self.del_usc = p("DELETE FROM users_by_sub_country WHERE subscription = ? AND country = ? AND user_id = ?")
        self.ins_usc = p("INSERT INTO users_by_sub_country (subscription, country, user_id, username) VALUES (?,?,?,?)")
        self.add_genre = p("UPDATE users SET favorite_genres = favorite_genres + ? WHERE user_id = ?")
        self.sel_user_playlists = p("SELECT playlist_id, name, is_public, created_at FROM playlists_by_user "
                                    "WHERE owner_id = ?")
        self.del_history = p("DELETE FROM listens_by_user WHERE user_id = ?")
        self.sel_artist_tracks = p("SELECT album_year, album_title, track_id, title, duration_sec, genre "
                                   "FROM tracks_by_artist WHERE artist_id = ?")
        self.sel_track_count = p("SELECT listens FROM track_listen_counts WHERE track_id = ?")

        # ---- A16 à A25
        self.sel_daily = p("SELECT day, listens FROM daily_listen_counts WHERE month IN ? AND day >= ? AND day <= ?")
        self.sel_week = p("SELECT track_id, listens FROM weekly_track_counts WHERE week_start = ?")
        self.sel_tracks_in = p("SELECT track_id, title, artist_name FROM tracks WHERE track_id IN ?")
        self.sel_country_artists = p("SELECT artist_id, listens FROM country_artist_counts WHERE country = ?")
        self.sel_artists_in = p("SELECT artist_id, name FROM artists WHERE artist_id IN ?")
        self.sel_genre_stats = p("SELECT genre, plays, skips FROM genre_stats")
        self.sel_device_stats = p("SELECT device, plays, total_ms FROM device_stats")
        self.sel_sub_country = p("SELECT subscription, country, COUNT(*) AS users FROM users_by_sub_country "
                                 "GROUP BY subscription, country")
        self.sel_hourly = p("SELECT month, hour, listens FROM hourly_listen_counts")
        self.sel_all_track_ids = p("SELECT track_id FROM tracks")
        self.sel_listened_ids = p("SELECT track_id, listens FROM track_listen_counts")
        self.sel_popular_playlists = p("SELECT playlist_id, name, owner_id, followers_snapshot FROM playlists "
                                       "WHERE is_public = true AND followers_snapshot > ?")
        self.sel_year_albums = p("SELECT artist_id, album_title, listens FROM yearly_album_counts WHERE album_year = ?")
        self.sel_followers = p("SELECT playlist_id, followers FROM playlist_stats")
        self.upd_snapshot = p("UPDATE playlists SET followers_snapshot = ? WHERE playlist_id = ?")
        self.sel_meta = p("SELECT value FROM meta WHERE key = ?")

    # ------------------------------------------------------------------ outils
    def dataset_end(self):
        """Dernier jour du jeu de données (les analyses sont relatives à cette date)."""
        if self._end is None:
            row = self.s.execute(self.sel_meta, ("dataset_end",)).one()
            self._end = date.fromisoformat(row.value) if row else datetime.now(timezone.utc).date()
        return self._end

    def last_complete_week(self):
        end = self.dataset_end()
        return week_start(end) if end.weekday() == 6 else week_start(end) - timedelta(days=7)

    def _listen_params(self, e):
        return (e["user_id"], e["listened_at"], e["listen_id"], e["track_id"], e["title"], e["artist_id"],
                e["artist_name"], e["genre"], e["ms_played"], e["device"], e["source"], e["skipped"])

    # ------------------------------------------------------- transactionnelles
    def t1_record_listen(self, e, profile=None):
        """T1 : 9 écritures indépendantes envoyées en parallèle (asynchrones).
        Pas de batch : les partitions sont toutes différentes, un batch surchargerait le coordinateur."""
        d = e["listened_at"].date()
        statements = [
            (self.ins_listen, self._listen_params(e)),
            (self.inc["track"], (1, e["track_id"])),
            (self.inc["week"], (1, week_start(d), e["track_id"])),
            (self.inc["country_artist"], (1, e["country"], e["artist_id"])),
            (self.inc["album"], (1, e["album_year"], e["artist_id"], e["album_title"])),
            (self.inc["day"], (1, month_key(d), d)),
            (self.inc["hour"], (1, month_key(d), e["listened_at"].hour)),
            (self.inc_genre, (1, int(e["skipped"]), e["genre"])),
            (self.inc_device, (1, e["ms_played"], e["device"])),
        ]
        futures = [self.s.execute_async(st, params, execution_profile=_ep(profile)) for st, params in statements]
        for f in futures:
            f.result()

    def _counter_statements(self, events, sign=1):
        """Agrège côté client les incréments de compteurs d'un lot d'écoutes :
        sur le jeu généré, 20 000 écoutes donnent 28 542 incréments au lieu de 160 000."""
        agg = defaultdict(int)
        genre = defaultdict(lambda: [0, 0])
        device = defaultdict(lambda: [0, 0])
        for e in events:
            d = e["listened_at"].date()
            agg[("track", (e["track_id"],))] += 1
            agg[("week", (week_start(d), e["track_id"]))] += 1
            agg[("country_artist", (e["country"], e["artist_id"]))] += 1
            agg[("album", (e["album_year"], e["artist_id"], e["album_title"]))] += 1
            agg[("day", (month_key(d), d))] += 1
            agg[("hour", (month_key(d), e["listened_at"].hour))] += 1
            genre[e["genre"]][0] += 1
            genre[e["genre"]][1] += int(e["skipped"])
            device[e["device"]][0] += 1
            device[e["device"]][1] += e["ms_played"]
        statements = [(self.inc[kind], (sign * n,) + key) for (kind, key), n in agg.items()]
        statements += [(self.inc_genre, (sign * p, sign * s, g)) for g, (p, s) in genre.items()]
        statements += [(self.inc_device, (sign * p, sign * ms, d)) for d, (p, ms) in device.items()]
        return statements

    def _run_concurrent(self, statements, concurrency=200, profile=None):
        for _ in execute_concurrent(self.s, statements, concurrency=concurrency, raise_on_first_error=True,
                                    results_generator=True, execution_profile=_ep(profile)):
            pass

    def t2_bulk_listens(self, events, concurrency=200, profile=None):
        """T2 : import en lot = insertions des écoutes + compteurs pré-agrégés, en parallèle."""
        statements = [(self.ins_listen, self._listen_params(e)) for e in events]
        statements += self._counter_statements(events)
        self._run_concurrent(statements, concurrency, profile)
        return len(statements)

    def rollback_counters(self, events):
        """Annule l'effet d'écoutes de test sur les compteurs (décrément des mêmes quantités)."""
        self._run_concurrent(self._counter_statements(events, sign=-1))

    def t3_user_profile(self, user_id, profile=None):
        return self.s.execute(self.sel_user, (user_id,), execution_profile=_ep(profile)).one()

    def t4_last_listens(self, user_id, profile=None):
        return list(self.s.execute(self.sel_history, (user_id,), execution_profile=_ep(profile)))

    def t5_track_sheet(self, track_id):
        """Titre + album (UDT embarqué) + nom d'artiste en 1 lecture ; fiche complète de l'artiste en 2e lecture."""
        track = self.s.execute(self.sel_track, (track_id,)).one()
        artist = self.s.execute(self.sel_artist, (track.artist_id,)).one() if track else None
        return track, artist

    def t6_create_playlist(self, owner_id, name, is_public=True, tracks=None):
        """T6 : batch LOGGED = les deux tables dénormalisées sont écrites ensemble ou pas du tout."""
        pid, now = uuid4(), datetime.now(timezone.utc).replace(microsecond=0)
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(self.ins_playlist, (pid, owner_id, name, is_public, tracks or [], now, 0, 0))
        batch.add(self.ins_playlist_by_user, (owner_id, now, pid, name, is_public))
        self.s.execute(batch)
        return pid

    def t7_insert_track_at(self, playlist_id, track_id, position, max_attempts=10):
        """T7 : une liste CQL ne sait pas insérer au milieu -> lecture, insertion en mémoire,
        réécriture conditionnelle (LWT) sur la version : une écriture concurrente fait échouer
        la condition et on recommence, aucune mise à jour n'est perdue."""
        for attempt in range(1, max_attempts + 1):
            row = self.s.execute(self.sel_playlist_tracks, (playlist_id,)).one()
            tracks = list(row.tracks or [])
            pos = max(0, min(position, len(tracks)))
            tracks.insert(pos, track_id)
            if self.s.execute(self.upd_playlist_tracks, (tracks, row.version + 1, playlist_id, row.version)).was_applied:
                return pos, attempt
        raise RuntimeError("T7 : conflit persistant")

    def t8_remove_track(self, playlist_id, track_id, max_attempts=10):
        """T8 : même protocole que T7 (on ne mélange pas écritures LWT et non-LWT sur une ligne).
        Variante native sans lecture : UPDATE playlists SET tracks = tracks - [?] WHERE playlist_id = ?"""
        for _ in range(max_attempts):
            row = self.s.execute(self.sel_playlist_tracks, (playlist_id,)).one()
            tracks = list(row.tracks or [])
            if track_id not in tracks:
                return False
            tracks.remove(track_id)
            if self.s.execute(self.upd_playlist_tracks, (tracks, row.version + 1, playlist_id, row.version)).was_applied:
                return True
        raise RuntimeError("T8 : conflit persistant")

    def t9_follow_playlist(self, playlist_id, user_id):
        """T9 : IF NOT EXISTS rend l'abonnement idempotent ; le compteur n'est incrémenté qu'une fois."""
        applied = self.s.execute(self.ins_follower, (playlist_id, user_id, datetime.now(timezone.utc))).was_applied
        if applied:
            self.s.execute(self.inc_followers, (playlist_id,))
        return applied

    def t10_change_subscription(self, user_id, new_subscription):
        """T10 : lecture de l'ancien abonnement puis batch LOGGED sur users et users_by_sub_country."""
        row = self.s.execute(self.sel_user_sub, (user_id,)).one()
        if row.subscription == new_subscription:
            return False
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(self.upd_user_sub, (new_subscription, user_id))
        batch.add(self.del_usc, (row.subscription, row.country, user_id))
        batch.add(self.ins_usc, (new_subscription, row.country, user_id, row.username))
        self.s.execute(batch)
        return True

    def t11_add_favorite_genre(self, user_id, genre):
        """T11 : ajout à un set, sans lecture préalable et idempotent."""
        self.s.execute(self.add_genre, ({genre}, user_id))

    def t12_user_playlists(self, user_id):
        return list(self.s.execute(self.sel_user_playlists, (user_id,)))

    def t13_erase_history(self, user_id):
        """T13 : une seule tombstone de partition. Les compteurs (agrégats anonymes) sont conservés."""
        self.s.execute(self.del_history, (user_id,))

    def t14_artist_tracks(self, artist_id):
        """T14 : déjà trié par année décroissante grâce au clustering."""
        return list(self.s.execute(self.sel_artist_tracks, (artist_id,)))

    def t15_track_total_listens(self, track_id):
        row = self.s.execute(self.sel_track_count, (track_id,)).one()
        return row.listens if row else 0

    # -------------------------------------------------------------- analytiques
    def a16_daily_listens(self, days=30, end=None):
        end = end or self.dataset_end()
        start = end - timedelta(days=days - 1)
        months = sorted({month_key(start + timedelta(days=i)) for i in range(days)})
        counts = {to_date(r.day): r.listens for r in self.s.execute(self.sel_daily, (months, start, end))}
        return [(start + timedelta(days=i), counts.get(start + timedelta(days=i), 0)) for i in range(days)]

    def _names(self, statement, ids, attr):
        if not ids:
            return {}
        return {r[0]: getattr(r, attr) for r in self.s.execute(statement, (list(ids),))}

    def a17_top_tracks_week(self, week=None, k=10):
        """A17 : un compteur ne peut pas être une colonne de clustering, donc Cassandra ne trie pas
        par nombre d'écoutes : on lit la partition de la semaine et on garde le top k côté client."""
        week = week or self.last_complete_week()
        # list() d'abord : un ResultSet du pilote revient au début de sa page si on le réitère,
        # ce que fait heapq.nlargest ; sans list(), des lignes seraient comptées deux fois.
        top = heapq.nlargest(k, list(self.s.execute(self.sel_week, (week,))), key=lambda r: r.listens)
        info = {r.track_id: r for r in self.s.execute(self.sel_tracks_in, ([r.track_id for r in top],))} if top else {}
        return week, [(r.track_id, info[r.track_id].title, info[r.track_id].artist_name, r.listens) for r in top]

    def a18_top_artists_by_country(self, country, k=10):
        top = heapq.nlargest(k, list(self.s.execute(self.sel_country_artists, (country,))), key=lambda r: r.listens)
        names = self._names(self.sel_artists_in, [r.artist_id for r in top], "name")
        return [(r.artist_id, names.get(r.artist_id), r.listens) for r in top]

    def a19_skip_rate_by_genre(self):
        rows = [(r.genre, r.plays, r.skips, r.skips / r.plays) for r in self.s.execute(self.sel_genre_stats) if r.plays]
        return sorted(rows, key=lambda x: x[3], reverse=True)

    def a20_avg_duration_by_device(self):
        rows = [(r.device, r.plays, r.total_ms / r.plays / 1000) for r in self.s.execute(self.sel_device_stats) if r.plays]
        return sorted(rows, key=lambda x: x[2], reverse=True)

    def a21_users_by_subscription_country(self):
        """A21 : GROUP BY natif, possible car (subscription, country) est la clé de partition."""
        return [(r.subscription, r.country, r.users) for r in self.s.execute(self.sel_sub_country,
                                                                             execution_profile="SLOW")]

    def a22_listens_by_hour(self):
        totals = [0] * 24
        for r in self.s.execute(self.sel_hourly):
            totals[r.hour] += r.listens
        return list(enumerate(totals))

    def a23_never_listened(self):
        """A23 : pas d'anti-jointure en CQL -> différence de deux ensembles côté client."""
        all_ids = {r.track_id for r in self.s.execute(self.sel_all_track_ids, execution_profile="SLOW")}
        listened = {r.track_id for r in self.s.execute(self.sel_listened_ids, execution_profile="SLOW")
                    if r.listens > 0}  # un compteur remis à 0 laisse une ligne
        never = sorted(all_ids - listened)
        return len(never), never[:20]

    def a24_popular_public_playlists(self, min_followers=100):
        """A24 : intersection de deux index SAI, sans ALLOW FILTERING."""
        rows = list(self.s.execute(self.sel_popular_playlists, (min_followers,)))
        return sorted(rows, key=lambda r: r.followers_snapshot, reverse=True)

    def a25_albums_of_year(self, year, k=20):
        top = heapq.nlargest(k, list(self.s.execute(self.sel_year_albums, (year,))), key=lambda r: r.listens)
        names = self._names(self.sel_artists_in, {r.artist_id for r in top}, "name")
        return [(r.album_title, names.get(r.artist_id), r.listens) for r in top]

    # --------------------------------------------------------------- maintenance
    def sync_followers_snapshot(self, concurrency=100):
        """Recopie les compteurs d'abonnés (temps réel) dans la colonne indexée utilisée par A24."""
        statements = [(self.upd_snapshot, (r.followers, r.playlist_id))
                      for r in self.s.execute(self.sel_followers, execution_profile="SLOW")]
        for _ in execute_concurrent(self.s, statements, concurrency=concurrency, results_generator=True):
            pass
        return len(statements)
