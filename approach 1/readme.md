# ✅ Delta Table Automation Project — End-to-End

## 🔧 Tools
- Databricks Community Edition
- Local Python Script (for sending email)

## 📦 Features

1. **Data Generation**
   - Generate `1000` fake rows using libraries such as `Faker` or `random`.
   - Ensure schema matches target Delta table.

2. **Delta Table Append**
   - Use **Delta Lake API** to append generated data to existing Delta table.
   - Table versioning enabled for tracking changes.

3. **Data Export**
   - Query the **latest 100 rows** from the Delta table.
   - Export result as a **CSV file**.

4. **CSV Download**
   - Manually download the exported CSV from **DBFS (Databricks File System)**.
   - Use Databricks CLI or notebook commands.

5. **Email Automation**
   - Send email from **local system**.
   - Attach the downloaded CSV file.
   - Use `smtplib`, `email.mime`, or any preferred mail automation package.

# 🔷 STEP 1: Generate & Append 1000 Fake Rows to Delta Table

---

### 🧾 Python Code

```python
from pyspark.sql.functions import current_timestamp
from faker import Faker
import pandas as pd
from delta.tables import DeltaTable

# Initialize Faker
fake = Faker()
Faker.seed(42)

# 1. Generate 1000 fake rows
def generate_fake_data(n):
    return pd.DataFrame([{
        "name": fake.name(),
        "address": fake.address().replace('\n', ', '),
        "email": fake.email(),
        "ingestion_time": pd.Timestamp.now()
    } for _ in range(n)])

pdf = generate_fake_data(1000)
df = spark.createDataFrame(pdf)

# 2. Define Delta table path
delta_path = "dbfs:/tmp/user_delta_table"

# 3. Append or Create Delta table
if DeltaTable.isDeltaTable(spark, delta_path):
    delta_table = DeltaTable.forPath(spark, delta_path)
    delta_table.alias("old").merge(
        df.alias("new"), "old.email = new.email"
    ).whenNotMatchedInsertAll().execute()
else:
    df.write.format("delta").mode("overwrite").save(delta_path)

print("✅ Delta table updated with 1000 rows.")
--- 
# 🔷 STEP 2: Read Full Delta Table & Show Latest 100 Rows


### 🧾 Python Code

```python
# Load full Delta table
df_all = spark.read.format("delta").load(delta_path)

# Show latest 100 rows
from pyspark.sql.functions import col
df_all.orderBy(col("ingestion_time").desc()).show(100, truncate=False)

print(f"✅ Total rows in Delta Table: {df_all.count()}")

