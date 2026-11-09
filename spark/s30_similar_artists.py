"""S30 - Artistes similaires : nombre d'auditeurs communs entre paires d'artistes, normalisé par Jaccard.

Usage : spark-submit s30_similar_artists.py [--min-common 3]
Sortie : table similar_artists (10 voisins par artiste, triés par indice de Jaccard).
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import Timer, get_spark, read_table, write_table

ap = argparse.ArgumentParser()
ap.add_argument("--min-common", type=int, default=3, help="auditeurs communs minimum pour garder une paire")
ap.add_argument("--max-artists-per-user", type=int, default=300, help="garde-fou contre l'explosion quadratique")
args = ap.parse_args()
JOB = "S30"
spark = get_spark("S30-artistes-similaires")

with Timer(JOB, "total") as timer:
    ua = (read_table(spark, "listens_by_user").filter(~F.col("skipped"))
          .select("user_id", "artist_id").distinct())
    # un auditeur de n artistes produit n(n-1)/2 paires : on écarte les comptes extrêmes
    keep = ua.groupBy("user_id").count().filter(F.col("count") <= args.max_artists_per_user).select("user_id")
    ua = ua.join(keep, "user_id").cache()
    a, b = ua.alias("a"), ua.alias("b")
    pairs = (a.join(b, (F.col("a.user_id") == F.col("b.user_id")) & (F.col("a.artist_id") < F.col("b.artist_id")))
             .groupBy(F.col("a.artist_id").alias("artist_a"), F.col("b.artist_id").alias("artist_b"))
             .agg(F.count("*").alias("common"))
             .filter(F.col("common") >= args.min_common))
    listeners = ua.groupBy("artist_id").agg(F.count("*").alias("n"))
    pairs = (pairs.join(listeners.select(F.col("artist_id").alias("artist_a"), F.col("n").alias("n_a")), "artist_a")
             .join(listeners.select(F.col("artist_id").alias("artist_b"), F.col("n").alias("n_b")), "artist_b")
             .withColumn("jaccard", F.col("common") / (F.col("n_a") + F.col("n_b") - F.col("common")))
             .cache())
    both = (pairs.select(F.col("artist_a").alias("artist_id"), F.col("artist_b").alias("similar_artist_id"),
                         "common", "jaccard")
            .unionByName(pairs.select(F.col("artist_b").alias("artist_id"), F.col("artist_a").alias("similar_artist_id"),
                                      "common", "jaccard")))
    names = read_table(spark, "artists").select("artist_id", "name")
    w = Window.partitionBy("artist_id").orderBy(F.desc("jaccard"), F.desc("common"), "similar_artist_id")
    top = (both.withColumn("rank", F.row_number().over(w)).filter("rank <= 10")
           .join(names.select(F.col("artist_id").alias("similar_artist_id"),
                              F.col("name").alias("similar_artist_name")), "similar_artist_id")
           .select("artist_id", F.col("rank").cast("int").alias("rank"), "similar_artist_id", "similar_artist_name",
                   F.col("common").cast("int").alias("common_listeners"), "jaccard"))
    timer.rows = top.count()
    write_table(top, "similar_artists")

na = names.select(F.col("artist_id").alias("artist_a"), F.col("name").alias("artiste_a"))
nb = names.select(F.col("artist_id").alias("artist_b"), F.col("name").alias("artiste_b"))
print(f"\n{pairs.count()} paires d'artistes avec au moins {args.min_common} auditeurs communs. Top 15 :")
(pairs.orderBy(F.desc("common")).limit(15).join(na, "artist_a").join(nb, "artist_b")
 .select("artiste_a", "artiste_b", "common", "n_a", "n_b", F.round("jaccard", 3).alias("jaccard"))
 .orderBy(F.desc("common")).show(15, truncate=24))
spark.stop()
