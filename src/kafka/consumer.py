#!/usr/bin/env python3
"""
consumer.py — Kafka Consumer for the Bronze layer.

Subscribes to the bronze-dictionaries and bronze-cases topics, deserialises
each message, and writes the raw JSON payload to the ADLS bronze container
at the same directory structure as the original ingest_sudop.py.

Designed to run as a persistent (always-on) Azure Container App replica.
Commits offsets only after a confirmed ADLS write (at-least-once guarantee).
Handles SIGTERM gracefully for clean ACA shutdown.
"""

import json
import logging
import os
import signal
import sys
from datetime import datetime

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError, KafkaException

# Resolve paths so the script works whether called from src/kafka/ or project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import constants as C
from kafka_config import get_consumer_config, TOPIC_DICTIONARIES, TOPIC_CASES
from common import setup_logging, get_adls_client
from bronze.ingest_sudop import save_json_to_adls

load_dotenv()

# ---------------------------------------------------------------------------
# Graceful shutdown flag
# ---------------------------------------------------------------------------

_running = True


def _handle_sigterm(signum, frame):  # noqa: ANN001
    global _running
    logging.info("[Consumer] SIGTERM received — shutting down gracefully.")
    _running = False


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT,  _handle_sigterm)

# ---------------------------------------------------------------------------
# ADLS path resolver
# ---------------------------------------------------------------------------

def _resolve_adls_path(payload: dict) -> tuple[str, str]:
    """
    Given a message payload, returns (container, file_path) for ADLS.

    Message types:
      dictionary → bronze/dictionaries/slownik_<name>_<timestamp>.json
      case       → bronze/cases/przypadki_pomocy_gmina-siedziby-kod_<kod>_<timestamp>.json
    """
    msg_type  = payload.get("type")
    timestamp = payload.get("timestamp", datetime.utcnow().isoformat())
    # Normalise ISO timestamp to filename-safe format
    ts_safe   = timestamp.replace(":", "").replace("-", "").replace("T", "_").split(".")[0]

    if msg_type == "dictionary":
        name      = payload["name"]
        file_path = f"{C.BRONZE_DICTIONARIES_DIR}/slownik_{name}_{ts_safe}.json"
    elif msg_type == "case":
        gmina_kod = payload["gmina_kod"]
        file_path = (
            f"{C.BRONZE_CASES_DIR}/"
            f"przypadki_pomocy_gmina-siedziby-kod_{gmina_kod}_{ts_safe}.json"
        )
    else:
        raise ValueError(f"Unknown message type: '{msg_type}'")

    return C.BRONZE_CONTAINER, file_path


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def main():
    setup_logging()
    logging.info("=== SUDOP Kafka Consumer Starting ===")

    # ADLS client
    try:
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        adls_client = get_adls_client(connection_string)
    except ValueError as e:
        logging.critical(f"ADLS configuration error: {e}")
        sys.exit(1)

    # Kafka consumer
    try:
        config   = get_consumer_config()
        consumer = Consumer(config)
        consumer.subscribe([TOPIC_DICTIONARIES, TOPIC_CASES])
        logging.info(
            f"[Consumer] Subscribed to topics: "
            f"{TOPIC_DICTIONARIES}, {TOPIC_CASES}"
        )
    except (EnvironmentError, KafkaException) as e:
        logging.critical(f"Failed to create Kafka consumer: {e}")
        sys.exit(1)

    messages_processed = 0

    try:
        while _running:
            msg = consumer.poll(timeout=2.0)

            if msg is None:
                # No message within timeout window — keep polling
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition — not an error, just wait for more
                    logging.debug(
                        f"[Consumer] EOF reached: "
                        f"{msg.topic()} [{msg.partition()}] @ {msg.offset()}"
                    )
                else:
                    logging.error(f"[Consumer] Kafka error: {msg.error()}")
                continue

            # --------------- Process message ---------------
            topic  = msg.topic()
            key    = msg.key().decode("utf-8") if msg.key() else "unknown"
            offset = msg.offset()

            logging.info(
                f"[Consumer] Received message | topic={topic} "
                f"key={key} offset={offset}"
            )

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logging.error(
                    f"[Consumer] Failed to deserialise message "
                    f"(key={key}, offset={offset}): {e} — skipping."
                )
                # Commit anyway to avoid infinite retry on a poison message
                consumer.commit(message=msg)
                continue

            try:
                container, file_path = _resolve_adls_path(payload)
                data_to_write = payload.get("data", payload)
                save_json_to_adls(adls_client, container, file_path, data_to_write)
            except (ValueError, Exception) as e:
                logging.error(
                    f"[Consumer] Failed to write message to ADLS "
                    f"(key={key}, offset={offset}): {e} — will NOT commit offset."
                )
                # Do NOT commit — message will be redelivered after consumer restart
                continue

            # Commit offset only after successful ADLS write
            consumer.commit(message=msg)
            messages_processed += 1
            logging.info(
                f"[Consumer] ✓ Committed offset {offset} | "
                f"wrote → {container}/{file_path}"
            )

    except KafkaException as e:
        logging.critical(f"[Consumer] Fatal Kafka exception: {e}")
        sys.exit(1)
    finally:
        consumer.close()
        logging.info(
            f"=== SUDOP Kafka Consumer Stopped === "
            f"(total messages processed: {messages_processed})"
        )


if __name__ == "__main__":
    main()
