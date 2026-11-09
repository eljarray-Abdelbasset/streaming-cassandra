-- =====================================================================
-- Sujet 06 - Streaming musical - Schéma Cassandra 5.0
-- Une table par motif d'accès (query-driven design).
-- Le commentaire au-dessus de chaque table indique les requêtes servies.
-- =====================================================================

CREATE KEYSPACE IF NOT EXISTS music
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}
  AND durable_writes = true;

USE music;

-- ---------------------------------------------------------------------
-- Type défini par l'utilisateur (UDT) : l'album est toujours lu avec le
-- titre (T5), on l'embarque donc dans la ligne du titre. frozen = valeur
-- sérialisée d'un bloc (obligatoire pour un UDT dans une clé ou une
-- collection, recommandé ici car l'album ne change jamais champ par champ).
-- ---------------------------------------------------------------------
CREATE TYPE IF NOT EXISTS album_info (
  title text,
  year  int,
  label text
);

-- =====================================================================
-- 1. Référentiels
-- =====================================================================

-- T3 profil, T10 changer d'abonnement, T11 ajouter un genre préféré
CREATE TABLE IF NOT EXISTS users (
  user_id         int PRIMARY KEY,
  username        text,
  country         text,
  age             int,
  subscription    text,            -- free | premium | family
  favorite_genres set<text>,       -- ensemble : pas de doublon, ajout sans lecture (T11)
  signup_date     date,
  devices         list<text>       -- liste ordonnée (appareil principal en tête)
);

-- A21 utilisateurs par abonnement et par pays : GROUP BY sur la clé de partition
CREATE TABLE IF NOT EXISTS users_by_sub_country (
  subscription text,
  country      text,
  user_id      int,
  username     text,
  PRIMARY KEY ((subscription, country), user_id)
);

-- T5 (fiche complète de l'artiste), noms d'artistes pour A18 / A25
CREATE TABLE IF NOT EXISTS artists (
  artist_id int PRIMARY KEY,
  name      text,
  country   text,
  genres    set<text>,
  members   list<text>,
  social    map<text, text>       -- réseau -> identifiant (clé-valeur)
);

-- T5 fiche d'un titre (album embarqué en UDT, nom d'artiste dénormalisé)
CREATE TABLE IF NOT EXISTS tracks (
  track_id     int PRIMARY KEY,
  title        text,
  artist_id    int,
  artist_name  text,
  album        frozen<album_info>,
  duration_sec int,
  genre        text,
  bpm          int,
  explicit     boolean
);

-- T14 titres d'un artiste triés par année : 1 partition par artiste,
-- tri assuré par le clustering (aucun tri côté client).
CREATE TABLE IF NOT EXISTS tracks_by_artist (
  artist_id    int,
  album_year   int,
  album_title  text,
  track_id     int,
  title        text,
  duration_sec int,
  genre        text,
  PRIMARY KEY ((artist_id), album_year, album_title, track_id)
) WITH CLUSTERING ORDER BY (album_year DESC, album_title ASC, track_id ASC);

-- =====================================================================
-- 2. Écoutes (événements)
-- =====================================================================

-- T1 enregistrer, T4 50 dernières écoutes, T13 effacer l'historique.
-- Partition = utilisateur, clustering = date décroissante : T4 lit le début
-- de la partition, T13 pose une seule tombstone de partition.
-- Source de lecture des traitements Spark (S26 à S30).
CREATE TABLE IF NOT EXISTS listens_by_user (
  user_id     int,
  listened_at timestamp,
  listen_id   timeuuid,           -- départage deux écoutes à la même milliseconde
  track_id    int,
  title       text,               -- dénormalisé pour l'affichage (T4)
  artist_id   int,
  artist_name text,
  genre       text,
  ms_played   int,
  device      text,
  source      text,               -- playlist | album | search | radio
  skipped     boolean,
  PRIMARY KEY ((user_id), listened_at, listen_id)
) WITH CLUSTERING ORDER BY (listened_at DESC, listen_id DESC);

-- =====================================================================
-- 3. Compteurs alimentés à chaque écoute (analytiques natives)
-- Une table counter ne contient que la clé primaire et des compteurs.
-- =====================================================================

-- T15 nombre total d'écoutes d'un titre ; A23 (titres présents = écoutés)
CREATE TABLE IF NOT EXISTS track_listen_counts (
  track_id int PRIMARY KEY,
  listens  counter
);

-- A17 top 10 des titres de la semaine (semaine = lundi de la semaine ISO)
CREATE TABLE IF NOT EXISTS weekly_track_counts (
  week_start date,
  track_id   int,
  listens    counter,
  PRIMARY KEY ((week_start), track_id)
);

-- A18 top 10 des artistes par pays (pays de l'auditeur)
CREATE TABLE IF NOT EXISTS country_artist_counts (
  country   text,
  artist_id int,
  listens   counter,
  PRIMARY KEY ((country), artist_id)
);

-- A25 albums d'une année triés par nombre d'écoutes
CREATE TABLE IF NOT EXISTS yearly_album_counts (
  album_year  int,
  artist_id   int,
  album_title text,
  listens     counter,
  PRIMARY KEY ((album_year), artist_id, album_title)
);

-- A16 écoutes par jour sur 30 jours (partition mensuelle : 1 à 2 partitions lues)
CREATE TABLE IF NOT EXISTS daily_listen_counts (
  month   text,                    -- 'AAAA-MM'
  day     date,
  listens counter,
  PRIMARY KEY ((month), day)
);

-- A22 écoutes par heure de la journée (UTC)
CREATE TABLE IF NOT EXISTS hourly_listen_counts (
  month   text,
  hour    int,
  listens counter,
  PRIMARY KEY ((month), hour)
);

-- A19 taux de titres sautés par genre = skips / plays
CREATE TABLE IF NOT EXISTS genre_stats (
  genre text PRIMARY KEY,
  plays counter,
  skips counter
);

-- A20 durée moyenne d'écoute par appareil = total_ms / plays
CREATE TABLE IF NOT EXISTS device_stats (
  device   text PRIMARY KEY,
  plays    counter,
  total_ms counter
);

-- =====================================================================
-- 4. Playlists
-- =====================================================================

-- T6 créer, T7 insérer à une position, T8 retirer, A24 (via index SAI).
-- tracks : liste ordonnée d'identifiants ; version : verrou optimiste (LWT).
-- followers_snapshot : copie périodique du compteur, indexable (A24).
CREATE TABLE IF NOT EXISTS playlists (
  playlist_id        uuid PRIMARY KEY,
  owner_id           int,
  name               text,
  is_public          boolean,
  tracks             list<int>,
  created_at         timestamp,
  version            int,
  followers_snapshot int
);

-- T12 playlists d'un utilisateur (les plus récentes d'abord)
CREATE TABLE IF NOT EXISTS playlists_by_user (
  owner_id    int,
  created_at  timestamp,
  playlist_id uuid,
  name        text,
  is_public   boolean,
  PRIMARY KEY ((owner_id), created_at, playlist_id)
) WITH CLUSTERING ORDER BY (created_at DESC, playlist_id ASC);

-- T9 s'abonner : la ligne rend l'abonnement idempotent (INSERT ... IF NOT EXISTS)
CREATE TABLE IF NOT EXISTS playlist_followers (
  playlist_id uuid,
  user_id     int,
  followed_at timestamp,
  PRIMARY KEY ((playlist_id), user_id)
);

-- T9 compteur d'abonnés (source de vérité temps réel)
CREATE TABLE IF NOT EXISTS playlist_stats (
  playlist_id uuid PRIMARY KEY,
  followers   counter
);

-- =====================================================================
-- 5. Index SAI (Storage-Attached Index, Cassandra 5.0)
-- =====================================================================

-- A24 : playlists publiques de plus de 100 abonnés (intersection de 2 index)
CREATE INDEX IF NOT EXISTS playlists_public_idx    ON playlists (is_public)          USING 'sai';
CREATE INDEX IF NOT EXISTS playlists_followers_idx ON playlists (followers_snapshot) USING 'sai';

-- Expérience d'indexation sur T14 : index SAI vs table dédiée vs ALLOW FILTERING
CREATE INDEX IF NOT EXISTS tracks_artist_idx       ON tracks (artist_id)             USING 'sai';

-- =====================================================================
-- 6. Résultats calculés par Spark, servis par Cassandra au tableau de bord
-- =====================================================================

CREATE TABLE IF NOT EXISTS meta (
  key   text PRIMARY KEY,
  value text
);

-- S26 tendances de la semaine
CREATE TABLE IF NOT EXISTS trending_tracks (
  week_start        date,
  rank              int,
  track_id          int,
  title             text,
  artist_name       text,
  listens_week      int,
  listens_prev_week int,
  growth            int,
  growth_pct        double,
  PRIMARY KEY ((week_start), rank)
);

-- S27 recommandations ALS (top 10 par utilisateur)
CREATE TABLE IF NOT EXISTS recommendations_by_user (
  user_id     int,
  rank        int,
  track_id    int,
  title       text,
  artist_name text,
  score       double,
  PRIMARY KEY ((user_id), rank)
);

-- S30 artistes similaires
CREATE TABLE IF NOT EXISTS similar_artists (
  artist_id           int,
  rank                int,
  similar_artist_id   int,
  similar_artist_name text,
  common_listeners    int,
  jaccard             double,
  PRIMARY KEY ((artist_id), rank)
);

-- S28, S29 et autres indicateurs agrégés
CREATE TABLE IF NOT EXISTS spark_kpis (
  job         text,
  metric      text,
  dimension   text,
  value       double,
  computed_at timestamp,
  PRIMARY KEY ((job), metric, dimension)
);
