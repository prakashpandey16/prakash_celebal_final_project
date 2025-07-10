# Databricks notebook source
# MAGIC %pip install faker schedule
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from faker import Faker
import pytz
from datetime import datetime

# Set Timezone to India
spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")

# Delta Table Path (DBFS)
delta_path = "/dbfs/tmp/delta/user_data"


# COMMAND ----------

fake = Faker()

def generate_fake_data(n=1000):
    data = [(fake.name(), fake.address(), fake.email(), datetime.now(pytz.timezone("Asia/Kolkata"))) for _ in range(n)]
    df = spark.createDataFrame(data, ["name", "address", "email", "ingestion_time"])
    return df


# COMMAND ----------

def create_or_append_delta_table(df):
    if DeltaTable.isDeltaTable(spark, delta_path):
        df.write.format("delta").mode("append").save(delta_path)
        print("✅ Appended to existing Delta table.")
    else:
        df.write.format("delta").save(delta_path)
        print("✅ New Delta table created.")


# COMMAND ----------

def show_latest_data(limit=20):
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_tbl = DeltaTable.forPath(spark, delta_path)
        df = delta_tbl.toDF().orderBy("ingestion_time", ascending=False)
        df.show(limit, truncate=False)
    else:
        print("⚠️ Delta Table not found.")


# COMMAND ----------

# Run pipeline with 1000 rows
df_1000 = generate_fake_data(1000)
create_or_append_delta_table(df_1000)
show_latest_data()


# COMMAND ----------

import time

# Simulate appending 200 new rows every 5 minutes, 3 times
for i in range(3):
    print(f"\n⏱️ Batch {i+1}")
    df_new = generate_fake_data(200)
    create_or_append_delta_table(df_new)
    show_latest_data()
    time.sleep(300)  # 300 sec = 5 minutes


# COMMAND ----------

df_all = spark.read.format("delta").load(delta_path)
print(f"📊 Total rows in Delta Table: {df_all.count()}")


# COMMAND ----------

df_all.orderBy("ingestion_time", ascending=False).limit(100) \
    .toPandas().to_csv("/tmp/user_data_latest.csv", index=False)


# COMMAND ----------

# Save top 100 rows to CSV using Spark
df_all.orderBy("ingestion_time", ascending=False).limit(100) \
    .write.mode("overwrite").option("header", "true") \
    .csv("dbfs:/tmp/user_data_latest")


# COMMAND ----------

# MAGIC %fs ls /tmp/user_data_latest
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/tmp/user_data_latest")
for f in files:
    if f.name.endswith(".csv"):
        filename = f.name
        break

print("✅ Download your file here:")
print(f"https://community.cloud.databricks.com/files/tmp/user_data_latest/{filename}")
