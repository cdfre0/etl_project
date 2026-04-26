#!/usr/bin/env python3
"""
producer.py — Kafka Producer for the Bronze layer.

Fetches data from the SUDOP API and publishes each result as a self-describing
JSON message to the appropriate Kafka / Event Hubs topic.

Designed to run as a short-lived process (Azure Container App Job) triggered
by Azure Data Factory via Web Activity.

Topics:
  bronze-dictionaries  – one message per dictionary (5 total)
  bronze-cases         – one message per municipality result
"""

import json
import logging
import os
import sys
import time
from datetime import datetime

import requests
from dotenv import load_dotenv
from confluent_kafka import Producer, KafkaException

# Resolve paths so the script works whether called from src/kafka/ or the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import constants as C
from kafka_config import get_producer_config, TOPIC_DICTIONARIES, TOPIC_CASES
from common import setup_logging, get_adls_client, list_adls_files, read_json_from_adls

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

RATE_LIMIT_DELAY = C.RATE_LIMIT_DELAY_SECONDS
RETRY_BACKOFF    = C.RETRY_BACKOFF_SECONDS
MAX_RETRIES      = C.MAX_RETRIES
POLL_INTERVAL    = C.POLL_INTERVAL_SECONDS
MAX_POLLS        = C.MAX_POLLS

# ---------------------------------------------------------------------------
# API helpers (pure fetch — no ADLS writes)
# ---------------------------------------------------------------------------

def _make_request(url: str, allow_redirects: bool = True):
    """Makes a GET request to the SUDOP API with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, allow_redirects=allow_redirects, timeout=30)
            if resp.status_code == 429:
                logging.warning(f"Rate limit (429). Backing off {RETRY_BACKOFF}s.")
                time.sleep(RETRY_BACKOFF)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt + 1 < MAX_RETRIES:
                time.sleep(1)
    return None


def fetch_dictionaries() -> list[tuple[str, list | dict]]:
    """
    Fetches all SUDOP dictionaries from the API.

    Returns:
        List of (dictionary_name, data) tuples.
    """
    results = []
    for name in C.SUDOP_DICTIONARIES:
        url = f"{C.SUDOP_BASE_URL}/slownik/{name.replace('_', '-')}"
        logging.info(f"[Producer] Fetching dictionary: {url}")
        resp = _make_request(url)
        if resp:
            try:
                results.append((name, resp.json()))
            except json.JSONDecodeError:
                logging.error(f"Failed to decode JSON for dictionary '{name}'.")
        time.sleep(RATE_LIMIT_DELAY)
    return results


def fetch_case_for_municipality(gmina_kod: str, gmina_name: str) -> dict | None:
    """
    Fetches aid cases for a single municipality using the SUDOP queue API.

    Returns:
        The result data dict, or None if unavailable/timeout.
    """
    api_url   = f"{C.SUDOP_BASE_URL}/api"
    query_url = f"{api_url}/przypadki-pomocy?gmina-siedziby-kod={gmina_kod}"
    logging.info(f"[Producer] Fetching cases for {gmina_name} ({gmina_kod})")

    resp = _make_request(query_url)
    if not resp:
        return None

    # Resolve queue ID
    queue_id = None
    if "kolejka" in resp.url:
        queue_id = resp.url.split("/")[-1]
    else:
        try:
            queue_id = resp.json().get("id-kolejka")
        except json.JSONDecodeError:
            logging.warning(f"No JSON in initial response for {gmina_kod}.")

    if not queue_id:
        logging.info(f"No queue ID for {gmina_kod} — likely no data.")
        return None

    # Poll until data is ready
    queue_url = f"{api_url}/kolejka/{queue_id}"
    for i in range(MAX_POLLS):
        time.sleep(POLL_INTERVAL)
        logging.info(f"Polling {i + 1}/{MAX_POLLS} for queue {queue_id}")
        poll_resp = _make_request(queue_url)
        if not poll_resp:
            continue
        try:
            data = poll_resp.json()
            if data.get("wyniki"):
                return data
        except json.JSONDecodeError:
            logging.warning(f"Non-JSON poll response for queue {queue_id}.")

    logging.warning(f"Polling timed out for queue {queue_id} ({gmina_kod}).")
    return None


# ---------------------------------------------------------------------------
# Kafka publish helpers
# ---------------------------------------------------------------------------

_delivery_errors: list[str] = []


def _on_delivery(err, msg):
    """Confluent-Kafka delivery callback — logs success or records failure."""
    if err:
        error_text = (
            f"Delivery failed for topic={msg.topic()} "
            f"key={msg.key()}: {err}"
        )
        logging.error(error_text)
        _delivery_errors.append(error_text)
    else:
        logging.debug(
            f"Delivered → {msg.topic()} "
            f"[partition {msg.partition()}] offset {msg.offset()}"
        )


def publish(producer: Producer, topic: str, key: str, payload: dict) -> None:
    """Serialises payload to JSON and produces a message to the given topic."""
    try:
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            on_delivery=_on_delivery,
        )
        # Poll to trigger delivery callbacks and avoid internal queue overflow
        producer.poll(0)
    except KafkaException as e:
        logging.error(f"KafkaException producing to {topic}: {e}")
        raise
    except BufferError:
        logging.warning("Producer queue full — flushing before retrying.")
        producer.flush()
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            on_delivery=_on_delivery,
        )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_dictionaries_stage(producer: Producer) -> list | None:
    """
    Stage 1: Fetch all dictionaries and publish to bronze-dictionaries topic.
    Returns the gmina_siedziby data (needed by Stage 2), or None on failure.
    """
    logging.info("=== STAGE 1: Dictionary Ingestion ===")
    gminy_data = None

    dicts = fetch_dictionaries()
    for name, data in dicts:
        payload = {
            "type":      "dictionary",
            "name":      name,
            "timestamp": datetime.utcnow().isoformat(),
            "data":      data,
        }
        publish(producer, TOPIC_DICTIONARIES, key=name, payload=payload)
        logging.info(f"[Producer] Published dictionary '{name}' → {TOPIC_DICTIONARIES}")

        if name == "gmina_siedziby":
            gminy_data = data

    producer.flush()
    logging.info(f"[Producer] Stage 1 complete. {len(dicts)} dictionaries published.")
    return gminy_data


def run_cases_stage(producer: Producer, gminy_data: list) -> int:
    """
    Stage 2: Fetch cases per municipality and publish to bronze-cases topic.
    Returns the count of successfully published messages.
    """
    logging.info("=== STAGE 2: Case Ingestion by Municipality ===")
    published = 0
    total = len(gminy_data)

    for i, gmina in enumerate(gminy_data):
        gmina_kod  = gmina.get("number", "").strip()
        gmina_name = gmina.get("name", "")

        if not gmina_kod or gmina_name in ("NZ", "BRAK DANYCH"):
            continue

        logging.info(f"Municipality {i + 1}/{total}: {gmina_name} ({gmina_kod})")
        data = fetch_case_for_municipality(gmina_kod, gmina_name)

        if data:
            payload = {
                "type":         "case",
                "gmina_kod":    gmina_kod,
                "gmina_name":   gmina_name,
                "search_param": "gmina-siedziby-kod",
                "timestamp":    datetime.utcnow().isoformat(),
                "data":         data,
            }
            publish(producer, TOPIC_CASES, key=gmina_kod, payload=payload)
            published += 1
            logging.info(f"[Producer] Published cases for {gmina_name} → {TOPIC_CASES}")

        time.sleep(RATE_LIMIT_DELAY)

    producer.flush()
    logging.info(f"[Producer] Stage 2 complete. {published} municipality results published.")
    return published


def main():
    setup_logging()
    logging.info("=== SUDOP Kafka Producer Starting ===")

    try:
        config   = get_producer_config()
        producer = Producer(config)
        logging.info("[Producer] Connected to Kafka broker.")
    except (EnvironmentError, KafkaException) as e:
        logging.critical(f"Failed to create Kafka producer: {e}")
        sys.exit(1)

    # Stage 1 — Dictionaries
    gminy_data = run_dictionaries_stage(producer)

    if not gminy_data:
        logging.error(
            "gmina_siedziby dictionary not available — cannot run case ingestion. "
            "Attempting to fall back to ADLS for municipality list..."
        )
        # Fallback: read from ADLS if a previous run stored it
        try:
            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            adls_client = get_adls_client(connection_string)
            dict_files  = list_adls_files(adls_client, C.BRONZE_CONTAINER, C.BRONZE_DICTIONARIES_DIR)
            gmina_files = sorted([f for f in dict_files if "gmina_siedziby" in f], reverse=True)
            if gmina_files:
                gminy_data = read_json_from_adls(adls_client, C.BRONZE_CONTAINER, gmina_files[0])
        except Exception as fallback_err:
            logging.error(f"ADLS fallback failed: {fallback_err}")

    # Stage 2 — Cases
    if gminy_data:
        run_cases_stage(producer, gminy_data)
    else:
        logging.error("No municipality data available. Skipping case ingestion.")

    # Report delivery errors
    if _delivery_errors:
        logging.error(f"{len(_delivery_errors)} delivery error(s) occurred:")
        for err in _delivery_errors:
            logging.error(f"  {err}")
        sys.exit(1)

    logging.info("=== SUDOP Kafka Producer Finished Successfully ===")


if __name__ == "__main__":
    main()
