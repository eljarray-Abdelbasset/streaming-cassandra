"""Connexion Cassandra partagée par tous les scripts Python.

Les profils d'exécution permettent de choisir le niveau de cohérence
requête par requête : session.execute(stmt, params, execution_profile="ALL").
"""
import os

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

HOSTS = [h.strip() for h in os.getenv("CASSANDRA_HOSTS", "cassandra1,cassandra2,cassandra3").split(",")]
LOCAL_DC = os.getenv("CASSANDRA_DC", "dc1")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "music")
DATA_DIR = os.getenv("DATA_DIR", "/data")
RESULTS_DIR = os.getenv("RESULTS_DIR", "/results")

LEVELS = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
}


def _profile(level, timeout=60):
    # TokenAware : le pilote envoie la requête directement à un réplica
    # de la partition (le coordinateur évite un saut réseau).
    return ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)),
        consistency_level=level,
        request_timeout=timeout,
    )


def connect(keyspace=KEYSPACE, default_level="QUORUM"):
    """Retourne une session. Profil par défaut : QUORUM (lecture et écriture),
    donc R + W = 2 + 2 > RF = 3 : cohérence forte."""
    profiles = {EXEC_PROFILE_DEFAULT: _profile(LEVELS[default_level])}
    for name, level in LEVELS.items():
        profiles[name] = _profile(level)
    profiles["SLOW"] = _profile(LEVELS[default_level], timeout=300)  # comptages, scans
    cluster = Cluster(HOSTS, execution_profiles=profiles, connect_timeout=30)
    return cluster.connect(keyspace)
