# Databricks notebook source
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import DeltaTable

class transformations:

    def dedup(self, df:DataFrame, dedup_cols:List, cdc:str):

        df = df.withColumn("dedup_key", concat(*dedup_cols))
        df = df.withColumn("dedup_id", row_number().over(Window.partitionBy("dedup_key").orderBy(desc(cdc))))
        df = df.filter(col("dedup_id") == 1)
        df = df.drop("dedup_key", "dedup_id")

        return df
    

    def process_timestamp(self, df:DataFrame):

        df = df.withColumn("process_timestamp", current_timestamp())

        return df
    
    def upsert(self, df:DataFrame, keyCols:List, table, cdc):

        keyCols = [col.strip().rstrip(',') for col in keyCols]
        
        merge_conditions = " AND ".join([f"src.{i} = tgt.{i}" for i in keyCols])

        dlt_obj = DeltaTable.forName(spark,f"pyspark_dbt.silver.{table}")
        dlt_obj.alias("tgt").merge(
        df.alias("src"),
        merge_conditions
    ).whenMatchedUpdateAll(condition=f"src.{cdc} >= tgt.{cdc}").whenNotMatchedInsertAll()\
        .execute()

        return 1


# COMMAND ----------

# MAGIC %md
# MAGIC # Customer
# MAGIC

# COMMAND ----------

df = spark.read.table("pyspark_dbt.bronze.customers")

# COMMAND ----------

df_cust = df.withColumn("full_name", concat("first_name", lit(" "), "last_name"))

# COMMAND ----------

df_cust = df_cust.drop("first_name", "last_name")

# COMMAND ----------

df_cust = df_cust.withColumn("cleaned_phone_number", regexp_replace(col("phone_number"), r"[^0-9]", ""))

# COMMAND ----------

df_cust = df_cust.drop("phone_number")

# COMMAND ----------

cust_obj = transformations()
# display(cust_df_trns)

# COMMAND ----------

df_cust = cust_obj.dedup(df_cust, ["customer_id"], "last_updated_timestamp")


# COMMAND ----------

df_cust = cust_obj.process_timestamp(df_cust)

# COMMAND ----------

display(df_cust)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark_dbt.silver.customers"):
    df_cust.write.format("delta").mode("append").saveAsTable("pyspark_dbt.silver.customers")
else:
    cust_obj.upsert(df_cust, ["customer_id"], "customers", "last_updated_timestamp")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(*)
# MAGIC from pyspark_dbt.silver.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ##DRIVERS

# COMMAND ----------

df_driver = spark.read.table("pyspark_dbt.bronze.drivers")
display(df_driver)

# COMMAND ----------

df_driver = df_driver.withColumn("full_name", concat("first_name", lit(" "), "last_name"))
df_driver = df_driver.drop("first_name", "last_name")
df_driver = df_driver.withColumn("cleaned_phone_number", regexp_replace(col("phone_number"), r"[^0-9]", ""))
df_driver = df_driver.drop("phone_number")

# COMMAND ----------

display(df_driver)

# COMMAND ----------

driver_obj = transformations()

# COMMAND ----------

df_driver = driver_obj.dedup(df_driver, ["driver_id"], "last_updated_timestamp")

# COMMAND ----------

df_driver = driver_obj.process_timestamp(df_driver)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark_dbt.silver.drivers"):
    df_driver.write.format("delta").mode("append").saveAsTable("pyspark_dbt.silver.drivers")
else:
    driver_obj.upsert(df_driver, ["driver_id"], "drivers", "last_updated_timestamp")


# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*)
# MAGIC from pyspark_dbt.silver.drivers

# COMMAND ----------

# MAGIC %md
# MAGIC ##Locations

# COMMAND ----------

df_locations = spark.read.table("pyspark_dbt.bronze.locations")

# COMMAND ----------

display(df_locations)

# COMMAND ----------

loc_obj = transformations()

# COMMAND ----------

df_locations = loc_obj.dedup(df_locations, ["location_id"], "last_updated_timestamp")
df_locations = loc_obj.process_timestamp(df_locations)

if not spark.catalog.tableExists("pyspark_dbt.silver.locations"):
    df_locations.write.format("delta").mode("append").saveAsTable('pyspark_dbt.silver.locations')
else:
    loc_obj.upsert(df_locations, ["location_id"], "locations", 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Payments

# COMMAND ----------

df_pay = spark.read.table('pyspark_dbt.bronze.payments')
display(df_pay)

# COMMAND ----------

df_pay = df_pay.withColumn(
    "online_payment_status",
    when(
        ((col("payment_method") == "Card") & (col("payment_status") == "Success")),"online-sucess"
        ).
    when(
        ((col("payment_method") == "Card") & (col("payment_status") == "Failed")),"online-failed"
        ).
    when(
        ((col("payment_method") == "Card") & (col("payment_status") == "Pending")),"online-pending"
        ).
    otherwise("offline"))

display(df_pay)

# COMMAND ----------

pay_obj = transformations()

df_pay = pay_obj.dedup(df_pay, ["payment_id"], "last_updated_timestamp")
df_pay = pay_obj.process_timestamp(df_pay)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark_dbt.silver.payments"):
    df_pay.write.format("delta").mode("append").saveAsTable("pyspark_dbt.silver.payments")
else:
    pay_obj.upsert(df_pay, ["payment_id"], "payments", "last_updated_timestamp")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*)
# MAGIC from pyspark_dbt.silver.payments

# COMMAND ----------

# MAGIC %md
# MAGIC ##Vehicles

# COMMAND ----------

df_vehicles = spark.read.table("pyspark_dbt.bronze.vehicles")
display(df_vehicles)


# COMMAND ----------

vehicles_obj = transformations()

df_vehicles = vehicles_obj.dedup(df_vehicles, ["vehicle_id"], "last_updated_timestamp")
df_vehicles = vehicles_obj.process_timestamp(df_vehicles)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark_dbt.silver.vehicles"):
    df_vehicles.write.format("delta").mode("append").saveAsTable("pyspark_dbt.silver.vehicles")
else:
    vehicles_obj.upsert(df_vehicles, ["vehicle_id"], "vehicles", "last_updated_timestamp")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*)
# MAGIC from pyspark_dbt.silver.vehicles

# COMMAND ----------

