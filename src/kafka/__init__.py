"""
Kafka package for the SUDOP ETL pipeline.

Contains:
  kafka_config.py  – shared connection config for Azure Event Hubs / Kafka
  producer.py      – fetches from SUDOP API and publishes messages to Kafka topics
  consumer.py      – consumes messages and writes raw JSON to ADLS bronze container
"""
