#!/usr/bin/env python3
"""
kafka_config.py — Shared Kafka / Azure Event Hubs connection configuration.

Supports two modes controlled by KAFKA_USE_EVENT_HUBS env var:
  • true  → Azure Event Hubs (SASL/SSL, production)
  • false → local plain Kafka broker (dev via docker-compose)

Usage:
    from kafka_config import get_producer_config, get_consumer_config, TOPIC_DICTIONARIES, TOPIC_CASES
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Topic / group names (mirror constants.py for standalone use)
# ---------------------------------------------------------------------------
TOPIC_DICTIONARIES = os.getenv("KAFKA_TOPIC_DICTIONARIES", "bronze-dictionaries")
TOPIC_CASES        = os.getenv("KAFKA_TOPIC_CASES",        "bronze-cases")
CONSUMER_GROUP     = os.getenv("KAFKA_CONSUMER_GROUP",     "bronze-consumer")

# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _use_event_hubs() -> bool:
    """Returns True when connecting to Azure Event Hubs (SASL/SSL mode)."""
    return os.getenv("KAFKA_USE_EVENT_HUBS", "true").lower() == "true"


def get_producer_config() -> dict:
    """
    Returns a confluent_kafka.Producer config dict.

    Azure Event Hubs requires SASL_SSL with a connection string encoded as:
        username = "$ConnectionString"
        password = <full Event Hubs connection string>
    """
    if _use_event_hubs():
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        conn_str  = os.getenv("KAFKA_CONNECTION_STRING")
        if not bootstrap or not conn_str:
            raise EnvironmentError(
                "KAFKA_BOOTSTRAP_SERVERS and KAFKA_CONNECTION_STRING must be set "
                "when KAFKA_USE_EVENT_HUBS=true."
            )
        return {
            "bootstrap.servers":       bootstrap,
            "security.protocol":       "SASL_SSL",
            "sasl.mechanism":          "PLAIN",
            "sasl.username":           "$ConnectionString",
            "sasl.password":           conn_str,
            # Delivery reliability
            "acks":                    "all",
            "retries":                 5,
            "retry.backoff.ms":        1000,
            # Batching / throughput
            "linger.ms":               50,
            "batch.size":              65536,
        }
    else:
        # Local dev — plain broker from docker-compose
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        return {
            "bootstrap.servers": bootstrap,
            "acks":              "all",
            "retries":           5,
        }


def get_consumer_config() -> dict:
    """
    Returns a confluent_kafka.Consumer config dict for the bronze-consumer group.
    """
    base = {
        "group.id":           CONSUMER_GROUP,
        "auto.offset.reset":  "earliest",
        # Manual commit — we commit only after successful ADLS write
        "enable.auto.commit": False,
    }

    if _use_event_hubs():
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        conn_str  = os.getenv("KAFKA_CONNECTION_STRING")
        if not bootstrap or not conn_str:
            raise EnvironmentError(
                "KAFKA_BOOTSTRAP_SERVERS and KAFKA_CONNECTION_STRING must be set "
                "when KAFKA_USE_EVENT_HUBS=true."
            )
        base.update({
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism":    "PLAIN",
            "sasl.username":     "$ConnectionString",
            "sasl.password":     conn_str,
        })
    else:
        base["bootstrap.servers"] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    return base
