"""S29 - Activité comparée des comptes gratuits, premium et famille (jointure utilisateurs x écoutes).

Usage : spark-submit s29_free_vs_premium.py
Sortie : indicateurs dans spark_kpis (job = S29). Le plan physique affiché montre la jointure par diffusion.
"""
from pyspark.sql import functions as F

from common import Timer, get_spark, read_table, write_kpis

JOB = "S29"
SOURCES = ["playlist", "album", "search", "radio"]
spark = get_spark("S29-gratuit-vs-premium")

with Timer(JOB, "total") as timer:
    users = read_table(spark, "users").select("user_id", "subscription")
    listens = read_table(spark, "listens_by_user").select("user_id", "track_id", "ms_played", "skipped", "source")
    # 10 000 utilisateurs : assez petit pour être diffusé à chaque exécuteur (broadcast hash join).
    # Les écoutes, la grande table, ne sont donc pas redistribuées (pas de shuffle).
    joined = listens.join(F.broadcast(users), "user_id")
    print("Plan physique de la jointure :")
    joined.explain()
    per_user = joined.groupBy("user_id", "subscription").agg(
        F.count("*").alias("listens"), F.sum("ms_played").alias("ms"),
        F.sum(F.col("skipped").cast("int")).alias("skips"), F.countDistinct("track_id").alias("distinct_tracks"))
    by_sub = per_user.groupBy("subscription").agg(
        F.count("*").alias("active_users"), F.avg("listens").alias("listens_per_user"),
        (F.avg("ms") / 3_600_000).alias("hours_per_user"), (F.sum("skips") / F.sum("listens")).alias("skip_rate"),
        F.avg("distinct_tracks").alias("distinct_tracks_per_user"))
    result = (by_sub.join(users.groupBy("subscription").agg(F.count("*").alias("users")), "subscription")
              .withColumn("activation_rate", F.col("active_users") / F.col("users"))
              .orderBy("subscription").cache())
    sources = (joined.groupBy("subscription").pivot("source", SOURCES).count()
               .join(joined.groupBy("subscription").count().withColumnRenamed("count", "total"), "subscription"))
    rows, src = result.collect(), sources.collect()
    kpis = [(col, r["subscription"], r[col]) for r in rows
            for col in ["users", "active_users", "activation_rate", "listens_per_user", "hours_per_user",
                        "skip_rate", "distinct_tracks_per_user"]]
    kpis += [(f"part_source_{s}", r["subscription"], (r[s] or 0) / r["total"]) for r in src for s in SOURCES]
    timer.rows = len(rows)
    write_kpis(spark, JOB, kpis)

result.select("subscription", "users", "active_users", F.round("activation_rate", 3).alias("activation"),
              F.round("listens_per_user", 1).alias("ecoutes/util"), F.round("hours_per_user", 2).alias("heures/util"),
              F.round("skip_rate", 3).alias("taux_saut"),
              F.round("distinct_tracks_per_user", 1).alias("titres_distincts")).show()
spark.stop()
