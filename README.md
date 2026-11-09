# Sujet 06 : streaming musical avec Cassandra et Spark

Projet de départ complet : cluster Cassandra 5.0.9 à 3 nœuds (RF = 3), Spark 3.5.9
(1 master, 2 workers) avec le connecteur Cassandra 3.5.1, application Python (générateur,
chargeur, 25 requêtes natives, benchmarks) et tableau de bord Streamlit. Chaque composant a été
testé contre Cassandra 5.0.9 et Spark 3.5.9 ; la pile Docker complète est à valider sur votre
machine (étape 1). Le guide pas à pas qui accompagne ce dépôt explique et justifie chaque choix.

## Prérequis

Docker Desktop (sous Windows : moteur WSL 2) avec au moins 8 Go de mémoire et 4 processeurs
alloués (Settings > Resources). Si Docker n'a que 8 Go, passez `MAX_HEAP_SIZE` à `512M`
dans `docker-compose.yml`.

## 1. Démarrer l'infrastructure (3 à 5 minutes)

```bash
docker compose up -d --build
docker compose logs cassandra-init                # doit finir par « Schema applique »
docker compose exec cassandra1 nodetool status    # 3 lignes commençant par UN
```

Interfaces : Spark http://localhost:8080 (2 workers ALIVE) et tableau de bord http://localhost:8501.

## 2. Générer et charger les données (3 à 5 minutes)

```bash
docker compose exec app python generate_data.py
docker compose exec app python load_data.py --reset
docker compose exec cassandra1 nodetool refreshsizeestimates
docker compose exec cassandra2 nodetool refreshsizeestimates
docker compose exec cassandra3 nodetool refreshsizeestimates
```

## 3. Mesurer les requêtes natives

```bash
docker compose exec app python bench_workload.py                      # T1 à T15, A16 à A25
docker compose exec app python bench_indexing.py                      # table dédiée / SAI / ALLOW FILTERING
docker compose exec app python bench_consistency.py --label 3_noeuds  # ONE / QUORUM / ALL
docker compose stop cassandra3
docker compose exec app python bench_consistency.py --label 2_noeuds
docker compose start cassandra3                                       # attendre UN dans nodetool status
```

## 4. Lancer les traitements Spark

```bash
docker compose exec spark-master spark-submit s26_trends.py
docker compose exec spark-master spark-submit s27_als.py --user 42
docker compose exec spark-master spark-submit s28_sessions.py
docker compose exec spark-master spark-submit s29_free_vs_premium.py
docker compose exec spark-master spark-submit s30_similar_artists.py
docker compose exec spark-master spark-submit native_vs_spark.py
```

Les mesures arrivent dans `results/` (CSV) et les résultats Spark dans Cassandra : le tableau de
bord les affiche dès le rafraîchissement suivant.

## Arborescence

```
docker-compose.yml        8 services : 3 nœuds Cassandra, init du schéma, Spark (1+2), app
cassandra/schema.cql      keyspace, UDT, 23 tables, 3 index SAI
app/                      image Python 3.12
  db.py                   connexion et profils de cohérence ONE / QUORUM / ALL
  generate_data.py        jeu de données reproductible (graine 42) -> data/*.jsonl
  load_data.py            chargement concurrent, écoutes via T2
  queries.py              les 25 requêtes natives T1 à T15 et A16 à A25
  bench_workload.py       temps des 25 requêtes -> results/bench_native.csv
  bench_consistency.py    cohérence, 3 puis 2 nœuds -> results/consistency_*.csv
  bench_indexing.py       indexation -> results/indexing.csv
  dashboard.py            tableau de bord Streamlit (17 graphiques)
spark/                    image Spark 3.5.9 + connecteur 3.5.1
  s26_trends.py ... s30_similar_artists.py, native_vs_spark.py
data/  results/           fichiers générés (montés dans les conteneurs)
```

## Arrêter

```bash
docker compose down        # conserve les données (volumes)
docker compose down -v     # efface aussi les données Cassandra
```
