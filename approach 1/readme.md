# ✅ Delta Table Automation Project — End-to-End Guide

## 🔧 Tools Used
- Databricks Community Edition
- Local Python (for sending email)

---

## 📦 Project Features
- Generate 1000 fake rows using Faker
- Append to Delta table using Delta Lake API
- Export latest 100 rows as CSV
- Download CSV manually from DBFS
- Send email from your local machine with CSV as attachment

---

## 🔷 STEP 1: Generate & Append 1000 Fake Rows to Delta Table

```python
from pyspark.sql.functions import current_timestamp
from faker import Faker
import pandas as pd
from delta.tables import DeltaTable

fake = Faker()
Faker.seed(42)

def generate_fake_data(n):
    return pd.DataFrame([{
        "name": fake.name(),
        "address": fake.address().replace('\n', ', '),
        "email": fake.email(),
        "ingestion_time": pd.Timestamp.now()
    } for _ in range(n)])

pdf = generate_fake_data(1000)
df = spark.createDataFrame(pdf)

delta_path = "dbfs:/tmp/user_delta_table"

if DeltaTable.isDeltaTable(spark, delta_path):
    delta_table = DeltaTable.forPath(spark, delta_path)
    delta_table.alias("old").merge(
        df.alias("new"), "old.email = new.email"
    ).whenNotMatchedInsertAll().execute()
else:
    df.write.format("delta").mode("overwrite").save(delta_path)

print("✅ Delta table updated with 1000 rows.")

---
## 🔷 STEP 2: Read Full Delta Table & Show Latest 100 Rows

```python
# Load full Delta table
df_all = spark.read.format("delta").load(delta_path)

# Show latest 100 rows
from pyspark.sql.functions import col
df_all.orderBy(col("ingestion_time").desc()).show(100, truncate=False)

print(f"✅ Total rows in Delta Table: {df_all.count()}")

