# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Use Silver schema
spark.sql("USE CATALOG food_delivery")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

print("Using catalog: food_delivery / schema: silver")

# =========================================================
# 1. Define Schemas for each JSON payload
# =========================================================

order_item_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("total", DoubleType(), True),
])

delivery_address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
])

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("restaurant_name", StringType(), True),
    StructField("cuisine_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("items", ArrayType(order_item_schema), True),
    StructField("subtotal", DoubleType(), True),
    StructField("delivery_fee", DoubleType(), True),
    StructField("taxes", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("delivery_address", delivery_address_schema, True),
    StructField("payment_method", StringType(), True),
])

delivery_status_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("delivery_partner_id", StringType(), True),
    StructField("delivery_partner_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]), True),
    StructField("estimated_delivery_minutes", IntegerType(), True),
])

restaurant_updates_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("restaurant_name", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("update_type", StringType(), True),
    StructField("is_accepting_orders", BooleanType(), True),
    StructField("average_prep_time_minutes", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("total_reviews", IntegerType(), True),
])

customer_actions_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("food_rating", IntegerType(), True),
    StructField("delivery_rating", IntegerType(), True),
    StructField("complaint_category", StringType(), True),
    StructField("cancellation_reason", StringType(), True),
])

# COMMAND ----------

# =========================================================
# 2. ORDERS → silver.orders + silver.order_items
# =========================================================

bronze_orders = spark.table("food_delivery.bronze.raw_orders")

orders_parsed = (
    bronze_orders
    .select(from_json(col("value"), orders_schema).alias("data"))
    .select("data.*")
    .withColumn("order_timestamp", to_timestamp("timestamp"))
    .withColumn("order_date", to_date("order_timestamp"))
    .drop("timestamp")
)

# main orders table
silver_orders = (
    orders_parsed
    .withColumn("delivery_street", col("delivery_address.street"))
    .withColumn("delivery_city", col("delivery_address.city"))
    .withColumn("delivery_zip", col("delivery_address.zip"))
    .drop("delivery_address")
)

silver_orders.write.format("delta").mode("overwrite").saveAsTable("silver.orders")

print(f"silver.orders created with {silver_orders.count()} rows")

# COMMAND ----------

# order_items table (exploded items)
order_items = (
    orders_parsed
    .select(
        "order_id",
        explode("items").alias("item"),
        "order_timestamp",
        "order_date",
        "restaurant_id",
        "restaurant_name",
        "cuisine_type",
        "city"
    )
    .select(
        "order_id",
        "order_timestamp",
        "order_date",
        "restaurant_id",
        "restaurant_name",
        "cuisine_type",
        "city",
        col("item.item_id").alias("item_id"),
        col("item.name").alias("item_name"),
        col("item.quantity").alias("quantity"),
        col("item.price").alias("price"),
        col("item.total").alias("item_total"),
    )
)

order_items.write.format("delta").mode("overwrite").saveAsTable("silver.order_items")

print(f"silver.order_items created with {order_items.count()} rows")

# COMMAND ----------

# =========================================================
# 3. DELIVERY STATUS → silver.delivery_status
# =========================================================

bronze_delivery = spark.table("food_delivery.bronze.raw_delivery_status")

delivery_parsed = (
    bronze_delivery
    .select(from_json(col("value"), delivery_status_schema).alias("data"))
    .select("data.*")
    .withColumn("status_timestamp", to_timestamp("timestamp"))
    .drop("timestamp")
    .withColumn("latitude", col("location.latitude"))
    .withColumn("longitude", col("location.longitude"))
    .drop("location")
)

delivery_parsed.write.format("delta").mode("overwrite").saveAsTable("silver.delivery_status")

print(f"silver.delivery_status created with {delivery_parsed.count()} rows")

# COMMAND ----------

# =========================================================
# 4. RESTAURANT UPDATES → silver.restaurant_updates
# =========================================================

bronze_rest = spark.table("food_delivery.bronze.raw_restaurant_updates")

rest_parsed = (
    bronze_rest
    .select(from_json(col("value"), restaurant_updates_schema).alias("data"))
    .select("data.*")
    .withColumn("update_timestamp", to_timestamp("timestamp"))
    .drop("timestamp")
)

rest_parsed.write.format("delta").mode("overwrite").saveAsTable("silver.restaurant_updates")

print(f"silver.restaurant_updates created with {rest_parsed.count()} rows")

# COMMAND ----------

# =========================================================
# 5. CUSTOMER ACTIONS → silver.customer_actions
# =========================================================

bronze_actions = spark.table("food_delivery.bronze.raw_customer_actions")

actions_parsed = (
    bronze_actions
    .select(from_json(col("value"), customer_actions_schema).alias("data"))
    .select("data.*")
    .withColumn("action_timestamp", to_timestamp("timestamp"))
    .drop("timestamp")
)

actions_parsed.write.format("delta").mode("overwrite").saveAsTable("silver.customer_actions")

print(f"silver.customer_actions created with {actions_parsed.count()} rows")

print("Silver layer build complete.")
