# Databricks notebook source
entities = ['customers', 'trips', 'payments', 'locations', 'drivers', 'vehicles']

# COMMAND ----------

for entity in entities:

    df_batch = spark.read.format("csv")\
            .option("header", True)\
            .option("inferSchema", True)\
            .load(f"/Volumes/pyspark_dbt/source/source_data/{entity}")

    entity_schema = df_batch.schema

    df = spark.readStream.format("csv")\
            .option("header", True)\
            .schema(entity_schema)\
            .load(f"/Volumes/pyspark_dbt/source/source_data/{entity}")

    df.writeStream.format("delta")\
        .option("checkpointLocation", f"/Volumes/pyspark_dbt/bronze/checkpoint/{entity}")\
        .trigger(once=True)\
        .toTable(f"pyspark_dbt.bronze.{entity}")   
        



    

# COMMAND ----------

