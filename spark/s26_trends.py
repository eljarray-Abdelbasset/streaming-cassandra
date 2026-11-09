"""S26 - Tendances : titres dont les écoutes progressent le plus d'une semaine à l'autre (fonction lag).

Usage : spark-submit s26_trends.py
Sortie : table trending_tracks (top 20 de la dernière semaine complète).
"""
from datetime import timedelta

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import Timer, get_spark, read_table, write_table

JOB = "S26"
spark = get_spark("S26-tendances")

with Timer(JOB, "total") as timer:
    listens = read_table(spark, "listens_by_user").select("track_id", "listened_at").cache()
    last_day = listens.agg(F.max(F.to_date("listened_at"))).first()[0]
    # dernière semaine complète (du lundi au dimanche) présente dans les données
    last_week = last_day - timedelta(days=last_day.weekday() + (0 if last_day.weekday() == 6 else 7))

    weekly = (listens.withColumn("week_start", F.to_date(F.date_trunc("week", "listened_at")))
              .groupBy("track_id", "week_start").agg(F.count("*").alias("listens")))

    w = Window.partitionBy("track_id").orderBy("week_start")
    trend = (weekly
             .withColumn("prev_week", F.lag("week_start").over(w))
             .withColumn("prev_listens", F.lag("listens").over(w))
             # lag renvoie la semaine précédente PRÉSENTE : si elle n'est pas contiguë,
             # le titre n'avait aucune écoute la semaine d'avant
             .withColumn("listens_prev_week",
                         F.when(F.datediff("week_start", "prev_week") == 7, F.col("prev_listens")).otherwise(0))
             .withColumn("growth", F.col("listens") - F.col("listens_prev_week"))
             .withColumn("growth_pct", F.round(F.col("growth") * 100.0 /
                                               F.greatest(F.col("listens_prev_week"), F.lit(1)), 1))
             .filter((F.col("week_start") == F.lit(last_week)) & (F.col("listens") >= 5)))

    order = [F.desc("growth"), F.desc("listens"), F.asc("track_id")]
    tracks = read_table(spark, "tracks").select("track_id", "title", "artist_name")
    top = (trend.orderBy(*order).limit(20).join(tracks, "track_id")
           .withColumn("rank", F.row_number().over(Window.orderBy(*order)))
           .select("week_start", F.col("rank").cast("int").alias("rank"), "track_id", "title", "artist_name",
                   F.col("listens").cast("int").alias("listens_week"),
                   F.col("listens_prev_week").cast("int").alias("listens_prev_week"),
                   F.col("growth").cast("int").alias("growth"), "growth_pct")
           .cache())
    timer.rows = top.count()
    write_table(top, "trending_tracks")

print(f"\nTendances de la semaine du {last_week} (écoutes de la semaine vs semaine précédente) :")
top.orderBy("rank").select("rank", "title", "artist_name", "listens_prev_week", "listens_week", "growth",
                           "growth_pct").show(10, truncate=28)
spark.stop()
