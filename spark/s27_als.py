"""S27 - Recommandation par filtrage collaboratif ALS (MLlib, retour implicite) : top 10 par utilisateur.

Usage : spark-submit s27_als.py [--user 42] [--no-eval]
Réglage par défaut (rang 8, reg 0.1, alpha 1) = meilleur de 4 essais sur le jeu généré.
1. Évaluation hors ligne : entraînement sur les écoutes antérieures aux 7 derniers jours, test sur les
   titres découverts pendant ces 7 jours ; comparaison avec la base « titres les plus populaires ».
2. Modèle final sur toutes les écoutes -> top 10 de chaque utilisateur dans recommendations_by_user.
"""
import argparse
from datetime import timedelta

from pyspark.ml.evaluation import RankingEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import Timer, get_spark, read_table, write_kpis, write_table

ap = argparse.ArgumentParser()
ap.add_argument("--user", type=int, help="utilisateur dont on affiche le top 10")
ap.add_argument("--no-eval", action="store_true", help="sauter l'évaluation hors ligne")
ap.add_argument("--eval-only", action="store_true", help="évaluer seulement (réglage des paramètres)")
ap.add_argument("--rank", type=int, default=8)
ap.add_argument("--reg", type=float, default=0.1)
ap.add_argument("--alpha", type=float, default=1.0)
ap.add_argument("--iter", type=int, default=10)
args = ap.parse_args()

JOB = "S27"
spark = get_spark("S27-ALS")


def ratings(df):
    # Retour implicite : écoute complète = +1, titre sauté = -0,5.
    # ALS implicite lit r > 0 comme « aime », r <= 0 comme « n'aime pas », |r| comme une confiance.
    return (df.groupBy("user_id", "track_id")
            .agg(F.sum(F.when(F.col("skipped"), -0.5).otherwise(1.0)).cast("float").alias("rating")))


def fit(train):
    return ALS(userCol="user_id", itemCol="track_id", ratingCol="rating", implicitPrefs=True,
               rank=args.rank, maxIter=args.iter, regParam=args.reg, alpha=args.alpha,
               coldStartStrategy="drop", seed=42).fit(train)


def top_k(recs, seen, k=10):
    """Retire des recommandations les titres déjà écoutés, puis garde les k premiers."""
    flat = (recs.select("user_id", F.posexplode("recommendations").alias("pos", "rec"))
            .select("user_id", "pos", F.col("rec.track_id").alias("track_id"), F.col("rec.rating").alias("score"))
            .join(seen, ["user_id", "track_id"], "left_anti"))
    return (flat.withColumn("rank", F.row_number().over(Window.partitionBy("user_id").orderBy("pos")))
            .filter(F.col("rank") <= k))


def as_arrays(df):
    """Format attendu par RankingEvaluator : un tableau de doubles par utilisateur, dans l'ordre du rang."""
    return (df.groupBy("user_id").agg(F.sort_array(F.collect_list(F.struct("rank", "track_id"))).alias("s"))
            .select("user_id", F.col("s.track_id").cast("array<double>").alias("prediction")))


with Timer(JOB, "total"):
    listens = read_table(spark, "listens_by_user").select("user_id", "track_id", "skipped", "listened_at").cache()

    if not args.no_eval:
        with Timer(JOB, "evaluation"):
            cut = listens.agg(F.max("listened_at")).first()[0] - timedelta(days=7)
            train_l = listens.filter(F.col("listened_at") < F.lit(cut))
            test_l = listens.filter(F.col("listened_at") >= F.lit(cut))
            train = ratings(train_l).cache()
            seen = train.select("user_id", "track_id")
            truth = (test_l.filter(~F.col("skipped")).select("user_id", "track_id").distinct()
                     .join(seen, ["user_id", "track_id"], "left_anti")
                     .groupBy("user_id").agg(F.collect_set(F.col("track_id").cast("double")).alias("label")))
            users_eval = truth.select("user_id").join(seen.select("user_id").distinct(), "user_id").cache()
            model = fit(train)
            pred_als = as_arrays(top_k(model.recommendForUserSubset(users_eval, 100), seen))
            popular = (train_l.groupBy("track_id").count()
                       .withColumn("pos", F.row_number().over(Window.orderBy(F.desc("count"), "track_id")))
                       .filter("pos <= 200").select("track_id", "pos"))
            pred_pop = as_arrays(users_eval.crossJoin(popular).join(seen, ["user_id", "track_id"], "left_anti")
                                 .withColumn("rank", F.row_number().over(Window.partitionBy("user_id").orderBy("pos")))
                                 .filter("rank <= 10"))
            kpis = [("utilisateurs_evalues", "test", users_eval.count())]
            print(f"\nÉvaluation hors ligne sur {kpis[0][2]} utilisateurs (titres découverts la dernière semaine) :")
            for name, pred in [("ALS", pred_als), ("popularite", pred_pop)]:
                data = pred.join(truth, "user_id").cache()
                for metric in ["precisionAtK", "recallAtK", "ndcgAtK"]:
                    value = RankingEvaluator(predictionCol="prediction", labelCol="label",
                                             metricName=metric, k=10).evaluate(data)
                    kpis.append((f"{metric}@10", name, value))
                    print(f"  {name:<11} {metric}@10 = {value:.4f}")
            write_kpis(spark, JOB, kpis)
    if args.eval_only:
        spark.stop()
        raise SystemExit(0)

    with Timer(JOB, "entrainement_complet"):
        full = ratings(listens).cache()
        model = fit(full)
    with Timer(JOB, "recommandations") as rec_timer:
        tracks = read_table(spark, "tracks").select("track_id", "title", "artist_name", "genre")
        recs = (top_k(model.recommendForAllUsers(60), full.select("user_id", "track_id"))
                .join(tracks, "track_id").cache())
        out = recs.select("user_id", F.col("rank").cast("int").alias("rank"), "track_id", "title", "artist_name",
                          F.col("score").cast("double").alias("score"))
        rec_timer.rows = out.count()
        write_table(out, "recommendations_by_user")

user = args.user or (listens.groupBy("user_id").count().filter("count BETWEEN 20 AND 60")
                     .orderBy("user_id").first()["user_id"])
fav = read_table(spark, "users").filter(F.col("user_id") == user).select("favorite_genres").first()
print(f"\nTop 10 pour l'utilisateur {user} (genres préférés : {sorted(fav[0]) if fav else '?'}) :")
recs.filter(F.col("user_id") == user).orderBy("rank").select("rank", "title", "artist_name", "genre", "score") \
    .show(10, truncate=28)
spark.stop()
