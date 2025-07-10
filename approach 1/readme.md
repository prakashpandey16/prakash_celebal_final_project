# ✅ Delta Table Automation Project — End-to-End

🔧 **Tools**: Databricks Community Edition + Local Python (for Email)  
📦 **Features**:
- Generate 1000 fake rows  
- Append to Delta table using Delta Lake API  
- Export latest data (100 rows) as CSV  
- Manually download CSV from DBFS  
- Send email from your local system with CSV as attachment  

---

## 🔷 STEP 1: Generate & Append 1000 Fake Rows to Delta Table  
📍 Run this in **Databricks CE Notebook**

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
🔷 STEP 2: Read Full Delta Table & Show Latest 100 Rows
🧾 Python Code
python
Copy
Edit
# Load full Delta table
df_all = spark.read.format("delta").load(delta_path)

# Show latest 100 rows
from pyspark.sql.functions import col
df_all.orderBy(col("ingestion_time").desc()).show(100, truncate=False)

print(f"✅ Total rows in Delta Table: {df_all.count()}")
🔷 STEP 3: Export Top 100 Rows as CSV (Single File)
🧾 Python Code
python
Copy
Edit
# Save latest 100 rows to single CSV part file
df_all.orderBy("ingestion_time", ascending=False).limit(100) \
    .coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("dbfs:/tmp/user_data_latest")

print("✅ Exported top 100 rows to CSV.")
🔷 STEP 4: ✅ Manually Download the CSV File
Go to Databricks sidebar → Data → DBFS

Navigate to:

bash
Copy
Edit
/tmp/user_data_latest/
Find the file that starts with:

Copy
Edit
part-00000-
Right-click → Download

Rename it locally as:

Copy
Edit
user_data_latest.csv
✅ Now you have the CSV on your local system.
