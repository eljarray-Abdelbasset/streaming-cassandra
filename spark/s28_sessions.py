"""S28 - Sessions d'écoute : une pause de plus de 30 minutes entre deux écoutes ouvre une nouvelle session.

Usage : spark-submit s28_sessions.py
Sortie : indicateurs dans spark_kpis (job = S28).
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import Timer, get_spark, read_table, write_kpis

JOB, GAP_S = "S28", 30 * 60
BUCKETS = [(0, 5, "0-5 min"), (5, 15, "5-15 min"), (15, 30, "15-30 min"), (30, 60, "30-60 min"),
           (60, 120, "1-2 h"), (120, 10 ** 9, "plus de 2 h")]
spark = get_spark("S28-sessions")

with Timer(JOB, "total") as timer:
    l = (read_table(spark, "listens_by_user").select("user_id", "listened_at", "ms_played", "device")
         .withColumn("start_s", F.col("listened_at").cast("double"))
         .withColumn("end_s", F.col("start_s") + F.col("ms_played") / 1000.0))
    w = Window.partitionBy("user_id").orderBy("start_s")
    l = (l.withColumn("prev_end", F.lag("end_s").over(w))
         .withColumn("new_session", (F.col("prev_end").isNull() |
                                     (F.col("start_s") - F.col("prev_end") > GAP_S)).cast("int"))
         # somme cumulée des débuts de session = numéro de session de chaque écoute
         .withColumn("session_no", F.sum("new_session").over(w.rowsBetween(Window.unboundedPreceding, 0))))
    sessions = (l.groupBy("user_id", "session_no")
                .agg(F.min("start_s").alias("start_s"), F.max("end_s").alias("end_s"),
                     F.count("*").alias("tracks"), F.min_by("device", "start_s").alias("device"))
                .withColumn("duration_min", (F.col("end_s") - F.col("start_s")) / 60.0))
    users = read_table(spark, "users").select("user_id", "subscription")
    sessions = sessions.join(F.broadcast(users), "user_id", "left").cache()

    g = sessions.agg(F.count("*").alias("n"), F.avg("duration_min").alias("avg"),
                     F.expr("percentile_approx(duration_min, 0.5)").alias("median"),
                     F.avg("tracks").alias("tracks"), F.countDistinct("user_id").alias("users")).first()
    kpis = [("sessions", "total", g["n"]), ("duree_moyenne_min", "total", g["avg"]),
            ("duree_mediane_min", "total", g["median"]), ("titres_par_session", "total", g["tracks"]),
            ("sessions_par_utilisateur", "total", g["n"] / g["users"])]
    for r in sessions.groupBy("device").agg(F.count("*").alias("n"), F.avg("duration_min").alias("avg")).collect():
        kpis += [("sessions_par_appareil", r["device"], r["n"]),
                 ("duree_moyenne_min_par_appareil", r["device"], r["avg"])]
    for r in sessions.groupBy("subscription").agg(F.avg("duration_min").alias("avg"),
                                                  F.avg("tracks").alias("t")).collect():
        kpis += [("duree_moyenne_min_par_abonnement", r["subscription"], r["avg"]),
                 ("titres_par_session_par_abonnement", r["subscription"], r["t"])]
    bucket = None
    for lo, hi, label in BUCKETS:
        cond = (F.col("duration_min") >= lo) & (F.col("duration_min") < hi)
        bucket = F.when(cond, label) if bucket is None else bucket.when(cond, label)
    for r in sessions.groupBy(bucket.alias("bucket")).count().collect():
        kpis.append(("histogramme_duree", r["bucket"], r["count"]))
    timer.rows = g["n"]
    write_kpis(spark, JOB, kpis)

print(f"\n{g['n']} sessions pour {g['users']} utilisateurs ; durée moyenne {g['avg']:.1f} min, "
      f"médiane {g['median']:.1f} min, {g['tracks']:.1f} titres par session.")
for m, d, v in kpis:
    print(f"  {m:<36} {d:<14} {v:10.2f}")
spark.stop()
