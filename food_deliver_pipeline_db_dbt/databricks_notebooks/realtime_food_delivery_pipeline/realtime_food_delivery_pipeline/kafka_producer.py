# Databricks notebook source
# Import libraries
from faker import Faker
import json
import random
from datetime import datetime, timedelta
import time
from pyspark.sql.functions import *
import warnings
import builtins
import random
from confluent_kafka import Producer
warnings.filterwarnings('ignore')

%run ./kafka_config.ipynb

fake = Faker('en_IN')
producer = Producer(conf)


# COMMAND ----------

# Constants
INDIAN_CITIES = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Pune", "Kolkata", "Ahmedabad", "Jaipur"]
CUISINES = ["North Indian", "South Indian", "Chinese", "Italian", "Mexican", "Fast Food", "Biryani", "Street Food", "Continental"]


FOOD_ITEMS = [
    {"name": "Butter Chicken", "price": 280, "cuisine": "North Indian"},
    {"name": "Paneer Tikka", "price": 240, "cuisine": "North Indian"},
    {"name": "Chicken Biryani", "price": 320, "cuisine": "Biryani"},
    {"name": "Veg Biryani", "price": 250, "cuisine": "Biryani"},
    {"name": "Masala Dosa", "price": 120, "cuisine": "South Indian"},
    {"name": "Idli Sambar", "price": 80, "cuisine": "South Indian"},
    {"name": "Pizza Margherita", "price": 350, "cuisine": "Italian"},
    {"name": "Pasta Alfredo", "price": 320, "cuisine": "Italian"},
    {"name": "Hakka Noodles", "price": 180, "cuisine": "Chinese"},
    {"name": "Fried Rice", "price": 160, "cuisine": "Chinese"},
    {"name": "Dal Makhani", "price": 200, "cuisine": "North Indian"},
    {"name": "Burger", "price": 120, "cuisine": "Fast Food"}
]

DELIVERY_STATUSES = ["PLACED", "CONFIRMED", "PREPARING", "READY_FOR_PICKUP", "PICKED_UP", "OUT_FOR_DELIVERY", "DELIVERED"]

PIN_CODES = ["400001", "560001", "600001", "500001", "700001"]

def generate_order():
    """Generate a random order."""

    order_id = f'ORD{random.randint(100000, 999999)}'
    city = random.choice(INDIAN_CITIES)
    cuisine = random.choice(CUISINES)
    zipcode = random.choice(PIN_CODES)

    cuisine_items = [item for item in FOOD_ITEMS if item["cuisine"] == cuisine]

    if not cuisine_items:
        cuisine_items = FOOD_ITEMS


    num_items = random.randint(1, 6)

    # selected_items = random.sample(cuisine_items, min(num_items, len(cuisine_items)))
    selected_items = random.sample(
    cuisine_items,
    builtins.min(num_items, len(cuisine_items)))

    items_list = []
    subtotal = 0

    for i, item in enumerate(selected_items):
        quantity = random.randint(1, 4)
        item_total = quantity * item['price']

        items_list.append({
            "item_id": f"ITEM{i+1}",
            "name": item["name"],
            "quantity": quantity,
            "price": item["price"],
            "total": item_total
        })

        subtotal += item_total

    # delivery_fee = round(random.uniform(20, 50), 2)
    delivery_fee = builtins.round(random.uniform(20, 50), 2)
    
    taxes = builtins.round(subtotal * 0.05, 2)

    round_total_amount = builtins.round(subtotal + delivery_fee + taxes, 2)

    return {
        "order_id": order_id,
        "customer_id": f"CUST{random.randint(1000, 9999)}",
        "restaurant_id": f"REST{random.randint(100, 500)}",
        "restaurant_name": fake.company() + " Restaurant",
        "cuisine_type": cuisine,
        "city": city,
        "timestamp": datetime.now().isoformat(),
        "items": items_list,
        "subtotal": subtotal,
        "delivery_fee": delivery_fee,
        "taxes": taxes,
        "total_amount": round_total_amount,
        "delivery_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "zip": zipcode
        },
        "payment_method": random.choice(["CREDIT_CARD", "DEBIT_CARD", "UPI", "WALLET", "COD"])
    }


def generate_delivery_status(order_id):
    """Generate delivery status"""
    return {
        "order_id": order_id,
        "delivery_partner_id": f"DP{random.randint(1000, 5000)}",
        "delivery_partner_name": fake.name(),
        "status": random.choice(DELIVERY_STATUSES),
        "timestamp": datetime.now().isoformat(),
        "location": {
            "latitude": builtins.round(random.uniform(12.9, 28.7), 6),
            "longitude": builtins.round(random.uniform(72.8, 88.4), 6)
        },
        "estimated_delivery_minutes": random.randint(15, 45)
    }


def generate_restaurant_status():
    """Generate restaurant update"""

    return {
        "restaurant_id": f"REST{random.randint(100, 500)}",
        "restaurant_name": fake.company() + " Restaurant",
        "timestamp": datetime.now().isoformat(),
        "update_type": random.choice(["MENU_UPDATE", "AVAILABILITY_CHANGE", "TIMING_CHANGE"]),
        "is_accepting_orders": random.choice([True, True, True, False]),
        "average_prep_time_minutes": random.randint(15, 45),
        "rating": builtins.round(random.uniform(3.5, 5.0), 1),
        "total_reviews": random.randint(50, 5000)
    }


def generate_customer_action():
    "Generate customer actions"
    action_type = random.choice(["RATING", "REVIEW", "COMPLAINT", "CANCELLATION"])
    action = {
        "order_id": f"ORD{random.randint(100000, 999999)}",
        "customer_id": f"CUST{random.randint(1000, 9999)}",
        "timestamp": datetime.now().isoformat(),
        "action_type": action_type
    }

    if action_type in ["RATING", "REVIEW"]:
        action["food_rating"] = random.randint(1, 5)
        action["delivery_rating"] = random.randint(1, 5)
    
    elif action_type == "COMPLAINT":
        action['complaint_category'] = random.choice(["LATE_DELIVERY", "WRONG_ORDER", "COLD_FOOD"])
    
    elif action_type == "CANCELLATION":
        action['cancellation_reason'] = random.choice(["LONG_WAIT", "CHANGED_MIND", "PAYMENT_ISSUE"])

    return action

print("Data generators ready!")


# def send_to_kafka(data_list, topic_name):
#     """Send data to Kafka topic"""
#     df = spark.createDataFrame([(json.dumps(data),) for data in data_list], ["value"])
    
#     df.selectExpr("CAST(value AS STRING)") \
#         .write \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", KAFKA_CONFIG["kafka.bootstrap.servers"]) \
#         .option("kafka.security.protocol", KAFKA_CONFIG["kafka.security.protocol"]) \
#         .option("kafka.sasl.mechanism", KAFKA_CONFIG["kafka.sasl.mechanism"]) \
#         .option("kafka.sasl.jaas.config", KAFKA_CONFIG["kafka.sasl.jaas.config"]) \
#         .option("topic", topic_name) \
#         .save()
    
#     return len(data_list)

def send_to_kafka(data_list, topic_name, key_field=None):
    """Send list of dicts to Kafka topic using Python producer"""
    sent = 0
    for record in data_list:
        value_str = json.dumps(record)
        key_bytes = None
        if key_field and key_field in record:
            key_bytes = str(record[key_field]).encode("utf-8")

        producer.produce(
            topic=topic_name,
            key=key_bytes,
            value=value_str.encode("utf-8")
        )
        sent += 1
    
    producer.flush()
    return sent
    


print("Generating Orders")
orders = [generate_order() for _ in range(50)]
count = send_to_kafka(orders, TOPICS['orders'])
print(f"Sent {count} orders")

# Generate 100 delivery status updates
print("\nGenerating delivery statuses...")
statuses = [generate_delivery_status(f"ORD{random.randint(100000, 999999)}") for _ in range(100)]
count = send_to_kafka(statuses, TOPICS["delivery_status"])
print(f"Sent {count} delivery statuses")

# Generate 20 restaurant updates
print("\nGenerating restaurant updates...")
updates = [generate_restaurant_status() for _ in range(20)]
count = send_to_kafka(updates, TOPICS["restaurant_updates"])
print(f"Sent {count} restaurant updates")

# Generate 30 customer actions
print("\nGenerating customer actions...")
actions = [generate_customer_action() for _ in range(30)]
count = send_to_kafka(actions, TOPICS["customer_actions"])
print(f"Sent {count} customer actions")

print("\nAll data sent successfully!")