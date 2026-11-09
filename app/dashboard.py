"""Tableau de bord Streamlit du sujet 06 : requêtes natives en direct, résultats Spark et mesures.

Lancé automatiquement par docker compose : http://localhost:8501
Les requêtes natives passent par queries.py, les mêmes fonctions que les benchmarks.
"""
import glob
import os
from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st

from db import RESULTS_DIR, connect
from queries import Workload

st.set_page_config(page_title="Streaming musical : Cassandra et Spark", page_icon="🎧", layout="wide")
SESSION_BUCKETS = ["0-5 min", "5-15 min", "15-30 min", "30-60 min", "1-2 h", "plus de 2 h"]


@st.cache_resource
def get_workload():
    return Workload(connect())


wl = get_workload()


def fmt(n):
    return f"{n:,.0f}".replace(",", "\u202f")


# ------------------------------------------------------------------ données (mises en cache 60 s)
@st.cache_data(ttl=60)
def cql(query, params=()):
    """Exécute une requête CQL et renvoie un DataFrame (les dates Cassandra deviennent du texte)."""
    df = pd.DataFrame([r._asdict() for r in wl.s.execute(query, params)])
    for col in df.columns:
        if df[col].map(lambda v: type(v).__name__ == "Date").any():
            df[col] = df[col].astype(str)
    return df


@st.cache_data(ttl=60)
def meta():
    return {r.key: r.value for r in wl.s.execute("SELECT key, value FROM meta")}


@st.cache_data(ttl=60)
def daily():
    return pd.DataFrame(wl.a16_daily_listens(), columns=["Jour", "Écoutes"])


@st.cache_data(ttl=60)
def top_week():
    week, rows = wl.a17_top_tracks_week()
    return str(week), pd.DataFrame(rows, columns=["track_id", "Titre", "Artiste", "Écoutes"])


@st.cache_data(ttl=60)
def top_artists(country):
    return pd.DataFrame(wl.a18_top_artists_by_country(country), columns=["artist_id", "Artiste", "Écoutes"])


@st.cache_data(ttl=60)
def skip_rates():
    return pd.DataFrame(wl.a19_skip_rate_by_genre(), columns=["Genre", "Écoutes", "Sautées", "Taux"])


@st.cache_data(ttl=60)
def devices():
    return pd.DataFrame(wl.a20_avg_duration_by_device(), columns=["Appareil", "Écoutes", "Durée moyenne (s)"])


@st.cache_data(ttl=300)
def subs_countries():
    return pd.DataFrame(wl.a21_users_by_subscription_country(), columns=["Abonnement", "Pays", "Utilisateurs"])


@st.cache_data(ttl=60)
def hourly():
    return pd.DataFrame(wl.a22_listens_by_hour(), columns=["Heure (UTC)", "Écoutes"])


@st.cache_data(ttl=300)
def never_listened():
    return wl.a23_never_listened()


@st.cache_data(ttl=60)
def popular_playlists():
    return pd.DataFrame([(r.name, r.owner_id, r.followers_snapshot) for r in wl.a24_popular_public_playlists()],
                        columns=["Playlist", "Propriétaire", "Abonnés"])


@st.cache_data(ttl=60)
def albums(year):
    return pd.DataFrame(wl.a25_albums_of_year(year), columns=["Album", "Artiste", "Écoutes"])


def kpis(job):
    return cql("SELECT metric, dimension, value FROM spark_kpis WHERE job = %s", (job,))


def read_csv(name):
    path = os.path.join(RESULTS_DIR, name)
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()


# ------------------------------------------------------------------ page
info = meta()
if not info:
    st.title("Streaming musical")
    st.warning("Aucune donnée chargée. Lancez « python generate_data.py » puis « python load_data.py --reset » "
               "dans le conteneur app.")
    st.stop()

st.title(f"Ce qu'écoutent {fmt(int(info.get('count_users', 0)))} abonnés")
st.caption(f"Écoutes du {info.get('dataset_start')} au {info.get('dataset_end')}, servies par Cassandra 5.0 "
           "(3 nœuds, RF 3). Tendances, sessions et recommandations calculées par Spark 3.5.")
tabs = st.tabs(["Vue d'ensemble", "Classements", "Comportements", "Utilisateur", "Calculs Spark", "Performances"])

with tabs[0]:
    skips = skip_rates()
    total = int(skips["Écoutes"].sum()) if not skips.empty else 0
    cols = st.columns(4)
    cols[0].metric("Écoutes enregistrées", fmt(total))
    cols[1].metric("Titres au catalogue", fmt(int(info.get("count_tracks", 0))))
    cols[2].metric("Playlists", fmt(int(info.get("count_playlists", 0))))
    cols[3].metric("Titres sautés", f"{skips['Sautées'].sum() / total:.1%}" if total else "-")
    left, right = st.columns(2)
    with left:
        st.subheader("Écoutes par jour sur 30 jours (A16)")
        st.plotly_chart(px.line(daily(), x="Jour", y="Écoutes", markers=True))
    with right:
        st.subheader("Écoutes par heure de la journée (A22)")
        st.plotly_chart(px.bar(hourly(), x="Heure (UTC)", y="Écoutes"))

with tabs[1]:
    week, top = top_week()
    st.subheader(f"Top 10 des titres, semaine du {week} (A17)")
    if top.empty:
        st.info("Aucune écoute pendant cette semaine.")
    else:
        st.plotly_chart(px.bar(top.sort_values("Écoutes"), x="Écoutes", y="Titre", orientation="h",
                               hover_data=["Artiste"]))
    left, right = st.columns(2)
    with left:
        countries = sorted(subs_countries()["Pays"].unique())
        country = st.selectbox("Pays des auditeurs", countries, index=countries.index("MA") if "MA" in countries else 0)
        st.subheader(f"Top 10 des artistes écoutés depuis {country} (A18)")
        st.plotly_chart(px.bar(top_artists(country).sort_values("Écoutes"), x="Écoutes", y="Artiste",
                               orientation="h"))
    with right:
        end_year = int(info.get("dataset_end", "2026")[:4])
        year = st.selectbox("Année de sortie des albums", list(range(end_year, 1984, -1)))
        st.subheader(f"Albums sortis en {year}, par nombre d'écoutes (A25)")
        df = albums(year)
        if df.empty:
            st.info("Aucun album de cette année n'a été écouté.")
        else:
            st.dataframe(df, hide_index=True, height=400)

with tabs[2]:
    left, right = st.columns(2)
    with left:
        st.subheader("Taux de titres sautés par genre (A19)")
        st.plotly_chart(px.bar(skip_rates(), x="Genre", y="Taux", hover_data=["Écoutes"]).update_yaxes(tickformat=".0%"))
    with right:
        st.subheader("Durée moyenne d'écoute par appareil (A20)")
        st.plotly_chart(px.bar(devices(), x="Appareil", y="Durée moyenne (s)", hover_data=["Écoutes"]))
    st.subheader("Utilisateurs par abonnement et par pays (A21, GROUP BY natif)")
    pivot = subs_countries().pivot_table(index="Abonnement", columns="Pays", values="Utilisateurs", fill_value=0)
    st.plotly_chart(px.imshow(pivot, text_auto=True, aspect="auto", color_continuous_scale="Purples"))
    left, right = st.columns(2)
    with left:
        n, sample = never_listened()
        st.subheader("Titres jamais écoutés (A23)")
        st.metric("Titres sans aucune écoute", fmt(n))
        st.caption("Premiers identifiants : " + ", ".join(map(str, sample[:10])))
    with right:
        df = popular_playlists()
        st.subheader("Playlists publiques de plus de 100 abonnés (A24)")
        st.caption(f"{len(df)} playlists, trouvées par l'intersection de deux index SAI.")
        st.dataframe(df.head(50), hide_index=True, height=260)

with tabs[3]:
    uid = int(st.number_input("Identifiant de l'utilisateur", min_value=1, max_value=10_000, value=9))
    user = wl.t3_user_profile(uid)
    if user is None:
        st.info("Aucun utilisateur ne porte cet identifiant.")
    else:
        cols = st.columns(4)
        cols[0].metric("Pseudo", user.username)
        cols[1].metric("Pays", user.country)
        cols[2].metric("Âge", user.age)
        cols[3].metric("Abonnement", user.subscription)
        st.write(f"Genres préférés : {', '.join(sorted(user.favorite_genres or []))}. "
                 f"Appareils : {', '.join(user.devices or [])}.")
        left, right = st.columns([3, 2])
        with left:
            st.subheader("50 dernières écoutes (T4)")
            hist = pd.DataFrame([(r.listened_at, r.title, r.artist_name, r.device, r.source, r.skipped,
                                  round(r.ms_played / 1000)) for r in wl.t4_last_listens(uid)],
                                columns=["Date (UTC)", "Titre", "Artiste", "Appareil", "Source", "Sauté", "Écouté (s)"])
            if hist.empty:
                st.info("Aucune écoute pour cet utilisateur.")
            else:
                st.dataframe(hist, hide_index=True, height=380)
        with right:
            st.subheader("Recommandations ALS (S27)")
            recs = cql("SELECT rank, title, artist_name, score FROM recommendations_by_user WHERE user_id = %s", (uid,))
            if recs.empty:
                st.info("Lancez « spark-submit s27_als.py » pour calculer les recommandations.")
            else:
                st.dataframe(recs.rename(columns={"rank": "Rang", "title": "Titre", "artist_name": "Artiste",
                                                  "score": "Score"}), hide_index=True)
            st.subheader("Playlists (T12)")
            pls = pd.DataFrame([(r.name, "publique" if r.is_public else "privée", r.created_at.date())
                                for r in wl.t12_user_playlists(uid)], columns=["Nom", "Visibilité", "Créée le"])
            if pls.empty:
                st.info("Cet utilisateur n'a créé aucune playlist.")
            else:
                st.dataframe(pls, hide_index=True)

with tabs[4]:
    st.subheader("Titres en plus forte progression d'une semaine à l'autre (S26, fonction lag)")
    weeks = cql("SELECT DISTINCT week_start FROM trending_tracks")
    if weeks.empty:
        st.info("Lancez « spark-submit s26_trends.py » pour calculer les tendances.")
    else:
        week = max(weeks["week_start"])
        tr = cql("SELECT rank, title, listens_prev_week, listens_week FROM trending_tracks WHERE week_start = %s",
                 (date.fromisoformat(week),)).sort_values("rank").head(10)
        long = tr.melt(id_vars=["rank", "title"], value_vars=["listens_prev_week", "listens_week"],
                       var_name="Semaine", value_name="Écoutes")
        long["Semaine"] = long["Semaine"].map({"listens_prev_week": "Semaine précédente",
                                               "listens_week": f"Semaine du {week}"})
        st.plotly_chart(px.bar(long, x="Écoutes", y="title", color="Semaine", barmode="group", orientation="h",
                               labels={"title": ""}).update_yaxes(autorange="reversed"))

    left, right = st.columns(2)
    with left:
        st.subheader("Durée des sessions d'écoute (S28)")
        s28 = kpis("S28")
        if s28.empty:
            st.info("Lancez « spark-submit s28_sessions.py » pour reconstituer les sessions.")
        else:
            k = {(m, d): v for m, d, v in s28.itertuples(index=False)}
            st.caption(f"{fmt(k.get(('sessions', 'total'), 0))} sessions, durée médiane "
                       f"{k.get(('duree_mediane_min', 'total'), 0):.1f} min, "
                       f"{k.get(('titres_par_session', 'total'), 0):.1f} titres par session.")
            hist = s28[s28["metric"] == "histogramme_duree"].copy()
            hist["dimension"] = pd.Categorical(hist["dimension"], SESSION_BUCKETS, ordered=True)
            st.plotly_chart(px.bar(hist.sort_values("dimension"), x="dimension", y="value",
                                   labels={"dimension": "Durée de la session", "value": "Sessions"}))
    with right:
        st.subheader("Gratuit, premium et famille (S29)")
        s29 = kpis("S29")
        if s29.empty:
            st.info("Lancez « spark-submit s29_free_vs_premium.py » pour comparer les abonnements.")
        else:
            names = {"listens_per_user": "Écoutes par utilisateur actif", "activation_rate": "Part d'utilisateurs actifs",
                     "skip_rate": "Taux de saut", "hours_per_user": "Heures par utilisateur actif"}
            wide = s29.pivot_table(index="dimension", columns="metric", values="value")
            long = wide[list(names)].rename(columns=names).reset_index().melt(id_vars="dimension")
            fig = px.bar(long, x="dimension", y="value", facet_col="metric", facet_col_wrap=2,
                         labels={"dimension": "", "value": ""}, height=420)
            fig.update_yaxes(matches=None, showticklabels=True)
            fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
            st.plotly_chart(fig)

    left, right = st.columns(2)
    with left:
        st.subheader("Artistes similaires (S30, auditeurs communs)")
        ranking = top_artists("MA")
        default_artist = int(ranking["artist_id"].iloc[0]) if not ranking.empty else 1
        aid = int(st.number_input("Identifiant de l'artiste", min_value=1, max_value=100_000, value=default_artist))
        artist = wl.s.execute(wl.sel_artist, (aid,)).one()
        sim = cql("SELECT rank, similar_artist_name, common_listeners, jaccard FROM similar_artists "
                  "WHERE artist_id = %s", (aid,))
        if sim.empty:
            st.info("Lancez « spark-submit s30_similar_artists.py », ou choisissez un artiste plus écouté.")
        else:
            st.caption(f"Voisins de {artist.name if artist else aid}, classés par indice de Jaccard.")
            st.plotly_chart(px.bar(sim.sort_values("rank", ascending=False), x="jaccard", y="similar_artist_name",
                                   orientation="h", hover_data=["common_listeners"],
                                   labels={"jaccard": "Indice de Jaccard", "similar_artist_name": ""}))
    with right:
        st.subheader("ALS face à la popularité (S27, top 10)")
        s27 = kpis("S27")
        ev = s27[s27["metric"].str.endswith("@10")] if not s27.empty else s27
        if ev.empty:
            st.info("Lancez « spark-submit s27_als.py » pour évaluer les recommandations.")
        else:
            st.plotly_chart(px.bar(ev, x="metric", y="value", color="dimension", barmode="group",
                                   labels={"metric": "", "value": "Score", "dimension": "Méthode"}))

with tabs[5]:
    native = read_csv("bench_native.csv")
    spark = read_csv("bench_spark.csv")
    st.subheader("Temps médian des 25 requêtes natives")
    if native.empty:
        st.info("Lancez « python bench_workload.py » pour mesurer les requêtes natives.")
    else:
        st.plotly_chart(px.bar(native, x="query", y="p50_ms", color="category", log_y=True,
                               hover_data=["label", "p95_ms", "runs"],
                               labels={"query": "", "p50_ms": "Médiane (ms, échelle log)", "category": "Type"}))
    left, right = st.columns(2)
    with left:
        st.subheader("Même requête : Cassandra native ou Spark")
        vs = spark[spark["job"] == "native_vs_spark"].copy() if not spark.empty else spark
        if native.empty or vs.empty:
            st.info("Lancez « python bench_workload.py » puis « spark-submit native_vs_spark.py ».")
        else:
            vs[["query", "mode"]] = vs["step"].str.split(" ", n=1, expand=True)
            vs = vs.groupby(["query", "mode"], as_index=False)["seconds"].last()
            vs["ms"], vs["Exécution"] = vs["seconds"] * 1000, "Spark, " + vs["mode"]
            nat = native[native["query"].isin(vs["query"])][["query", "p50_ms"]].rename(columns={"p50_ms": "ms"})
            nat["Exécution"] = "Cassandra native"
            st.plotly_chart(px.bar(pd.concat([nat, vs[["query", "ms", "Exécution"]]]), x="query", y="ms",
                                   color="Exécution", barmode="group", log_y=True,
                                   labels={"query": "", "ms": "Temps (ms, échelle log)"}))
    with right:
        st.subheader("Durée totale des traitements Spark")
        jobs = spark[spark["step"] == "total"].groupby("job", as_index=False)["seconds"].last() \
            if not spark.empty else spark
        if jobs.empty:
            st.info("Lancez les traitements Spark (s26 à s30) pour les chronométrer.")
        else:
            st.plotly_chart(px.bar(jobs, x="job", y="seconds", labels={"job": "", "seconds": "Durée (s)"}))
    st.subheader("Niveau de cohérence : latence médiane et échecs")
    files = sorted(glob.glob(os.path.join(RESULTS_DIR, "consistency_*.csv")))
    if not files:
        st.info("Lancez « python bench_consistency.py --label 3_noeuds », puis la même commande avec un nœud arrêté.")
    else:
        cons = pd.concat([pd.read_csv(f) for f in files])
        st.plotly_chart(px.bar(cons, x="level", y="p50_ms", color="label", barmode="group", facet_col="operation",
                               category_orders={"level": ["ONE", "QUORUM", "ALL"]},
                               labels={"level": "", "p50_ms": "Médiane (ms)", "label": "Cluster"}))
        failed = cons[cons["errors"] > 0]
        if not failed.empty:
            st.dataframe(failed[["label", "level", "operation", "ok", "errors", "error_types"]], hide_index=True)
    st.subheader("Effet de l'indexation (T14 et A24)")
    idx = read_csv("indexing.csv")
    if idx.empty:
        st.info("Lancez « python bench_indexing.py » (hors démonstration : il supprime puis recrée les index).")
    else:
        st.plotly_chart(px.bar(idx, x="p50_ms", y="variant", color="query", orientation="h", log_x=True,
                               labels={"p50_ms": "Médiane (ms, échelle log)", "variant": ""}))
