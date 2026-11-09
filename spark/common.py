"""Outils communs aux traitements Spark : session, lecture / écriture Cassandra, chronométrage."""
import csv
import os
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession

KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "music")
RESULTS_DIR = os.getenv("RESULTS_DIR", "/opt/app/results")


def get_spark(app_name):
    spark = (SparkSession.builder.appName(app_name)
             .config("spark.cassandra.connection.host",
                     os.getenv("CASSANDRA_HOSTS", "cassandra1,cassandra2,cassandra3"))
             .config("spark.cassandra.connection.localDC", os.getenv("CASSANDRA_DC", "dc1"))
             .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "16"))
             .config("spark.cassandra.input.split.sizeInMB", "16")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_table(spark, table):
    """Lecture distribuée : le connecteur découpe l'anneau en plages de jetons, une tâche Spark par plage."""
    return spark.read.format("org.apache.spark.sql.cassandra").options(keyspace=KEYSPACE, table=table).load()


def write_table(df, table):
    """Écriture = upsert (mode append) : les colonnes du DataFrame doivent porter les noms des colonnes CQL."""
    df.write.format("org.apache.spark.sql.cassandra").options(keyspace=KEYSPACE, table=table) \
        .mode("append").save()


def log_timing(job, step, seconds, rows=""):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, "bench_spark.csv")
    new_file = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(["job", "step", "seconds", "rows", "measured_at"])
        writer.writerow([job, step, round(seconds, 3), rows,
                         datetime.now(timezone.utc).isoformat(timespec="seconds")])
    print(f"[{job}] {step} : {seconds:.2f} s {rows}")


class Timer:
    """with Timer("S26", "total") as t: ... ; t.rows = n  -> une ligne dans bench_spark.csv"""

    def __init__(self, job, step):
        self.job, self.step, self.rows = job, step, ""

    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            log_timing(self.job, self.step, time.perf_counter() - self.t0, self.rows)


def write_kpis(spark, job, rows):
    """rows : liste de (indicateur, dimension, valeur) -> table spark_kpis, lue par le tableau de bord."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    df = spark.createDataFrame([(job, m, str(d), float(v), now) for m, d, v in rows],
                               "job string, metric string, dimension string, value double, computed_at timestamp")
    write_table(df, "spark_kpis")
