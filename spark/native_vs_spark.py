"""Comparaison natif / Spark : A16, A17, A19 et A23 recalculées par Spark à partir des écoutes brutes.

Usage : spark-submit native_vs_spark.py
Chaque requête est mesurée 3 fois (médiane retenue) :
  froid : relecture de Cassandra à chaque exécution (ce que paie un job ponctuel) ;
  chaud : écoutes et titres déjà en mémoire Spark (coût du calcul seul).
Les temps natifs correspondants sont dans bench_native.csv (bench_workload.py).
"""
import statistics
import time
from datetime import timedelta

from pyspark.sql import functions as F

from common import get_spark, log_timing, read_table

RUNS = 3
spark = get_spark("natif-vs-spark")


def load():
    return (read_table(spark, "listens_by_user").select("listened_at", "track_id", "genre", "skipped"),
            read_table(spark, "tracks").select("track_id"))


end = load()[0].agg(F.max(F.to_date("listened_at"))).first()[0]
week = end - timedelta(days=end.weekday() + (0 if end.weekday() == 6 else 7))
day = F.to_date("listened_at")


def a16(listens, tracks):   # écoutes par jour sur 30 jours
    return listens.filter(day > F.lit(end - timedelta(days=30))).groupBy(day.alias("day")).count() \
        .orderBy("day").collect()


def a17(listens, tracks):   # top 10 des titres de la semaine
    return (listens.filter(day.between(F.lit(week), F.lit(week + timedelta(days=6))))
            .groupBy("track_id").count().orderBy(F.desc("count")).limit(10).collect())


def a19(listens, tracks):   # taux de titres sautés par genre
    return listens.groupBy("genre").agg(F.avg(F.col("skipped").cast("int")).alias("skip_rate")) \
        .orderBy(F.desc("skip_rate")).collect()


def a23(listens, tracks):   # titres jamais écoutés : anti-jointure
    return tracks.join(listens.select("track_id").distinct(), "track_id", "left_anti").count()


def bench(mode, frames):
    for name, fn in [("A16", a16), ("A17", a17), ("A19", a19), ("A23", a23)]:
        times = []
        for _ in range(RUNS):
            listens, tracks = frames()
            t0 = time.perf_counter()
            result = fn(listens, tracks)
            times.append(time.perf_counter() - t0)
        log_timing("native_vs_spark", f"{name} {mode}", statistics.median(times),
                   result if isinstance(result, int) else len(result))


bench("froid", load)
cached = tuple(df.cache() for df in load())
for df in cached:
    df.count()
bench("chaud", lambda: cached)
spark.stop()
