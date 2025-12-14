# Databricks notebook source
# Import libraries
from faker import Faker
from confluent_kafka import Consumer
import json
import random
from datetime import datetime, timedelta
import time
from pyspark.sql.functions import *
from pyspark.sql.types import *
import warnings
warnings.filterwarnings('ignore')

%run ./kafka_config.ipynb

fake = Faker('en_IN')

spark.sql("USE CATALOG food_delivery")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

# ---------------------------------------------------------
#  BRONZE CONSUMER — Confluent Python Client → Delta
# ---------------------------------------------------------

from confluent_kafka import Consumer
import time

# 1. Use correct catalog + schema
spark.sql("USE CATALOG food_delivery")
spark.sql("USE SCHEMA bronze")
print("Using catalog food_delivery / schema bronze")

# 2. Confluent config (Python client style)
BASE_CONF = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,  # e.g. "pkc-xxxx.ap-south-1.aws.confluent.cloud:9092"
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": KAFKA_API_KEY,
    "sasl.password": KAFKA_API_SECRET,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

# Adjust topic names if needed to match Confluent exactly
TOPICS = {
    "orders": "orders",
    "delivery_status": "delivery_status",
    "restaurant_updates": "restaurant-updates",
    "customer_actions": "customer-actions",
}


def consume_to_bronze(topic_name: str, table_name: str, max_empty_polls: int = 10):
    """
    Read ALL available messages from a Kafka topic using Confluent Consumer
    and write them into food_delivery.bronze.<table_name> as a single 'value' column.
    """
    print(f"\nConsuming → topic '{topic_name}' → bronze.{table_name}")

    # Fresh group.id each run so we always read from beginning
    conf = BASE_CONF.copy()
    conf["group.id"] = f"bronze_{table_name}_{int(time.time())}"

    consumer = Consumer(conf)
    consumer.subscribe([topic_name])

    rows = []
    empty_polls = 0

    while empty_polls < max_empty_polls:
        msg = consumer.poll(1.0)

        if msg is None:
            empty_polls += 1
            continue

        if msg.error():
            print("⚠ Kafka error:", msg.error())
            empty_polls += 1
            continue

        # Got a real message
        empty_polls = 0
        rows.append((msg.value().decode("utf-8"),))

    consumer.close()

    if not rows:
        print(f"⚠ No records read from topic '{topic_name}'. "
              f"Check topic name or whether producer ran.")
        return

    print(f"Total messages read from '{topic_name}': {len(rows)}")

    # Create a single-column DF: value (raw JSON string)
    df = spark.createDataFrame(rows, ["value"])

    # Overwrite bronze table each time we run this
    df.write.format("delta").mode("overwrite").saveAsTable(f"bronze.{table_name}")

    print(f"Saved {len(rows)} records to food_delivery.bronze.{table_name}")


# 3. Run for all topics
consume_to_bronze(TOPICS["orders"], "raw_orders")
consume_to_bronze(TOPICS["delivery_status"], "raw_delivery_status")
consume_to_bronze(TOPICS["restaurant_updates"], "raw_restaurant_updates")
consume_to_bronze(TOPICS["customer_actions"], "raw_customer_actions")

print("\n🎯 Bronze ingestion completed.")


# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS bronze.raw_orders;
# DROP TABLE IF EXISTS bronze.raw_delivery_status;
# DROP TABLE IF EXISTS bronze.raw_restaurant_updates;
# DROP TABLE IF EXISTS bronze.raw_customer_actions;

# COMMAND ----------

