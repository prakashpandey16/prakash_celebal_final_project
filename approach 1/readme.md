
# 📘 Databricks Delta Table Automation Project (Enhanced)

## ✅ Objective

This project simulates a mini-ETL pipeline using **Databricks Community Edition**, **Delta Lake**, and a **local Python script** to:

- 🔄 Generate and ingest fake user data
- 🧾 Track versioned Delta table changes
- 📤 Export the latest data to CSV on DBFS
- 🕒 Schedule batch ingestion (every N minutes)
- 📨 Email the latest data as CSV and HTML preview (from local machine)

---

## 🗂️ Components and Enhancements

| Component         | Description                                                                 |
|------------------|-----------------------------------------------------------------------------|
| Fake Data Gen     | ➕ Add `ingestion_time` (timezone-aware)                                    |
| Delta Table       | ✅ Append data to Delta table (`user_delta_table`)                          |
| Export to CSV     | 🔁 Export top 100 rows to `/dbfs/tmp` for download                         |
| Versioning        | 🔍 Track table versions via Delta Table API                                 |
| Email Notification| 📧 Email latest CSV + HTML preview using Gmail SMTP                         |
| Local Execution   | 🖥️ Send email using a Python script from your system                       |
| Scheduler         | ⏱️ Option to trigger this pipeline every N minutes using notebook jobs      |

---

# 🧰 Data Ingestion Pipeline in 6 Steps

## ✅ Step 1: Spark Session & Setup

```python
from pyspark.sql.functions import lit
from delta.tables import DeltaTable
from faker import Faker
import pandas as pd
import pytz

delta_path = "dbfs:/tmp/user_delta_table"
csv_export_path = "dbfs:/tmp/user_data_latest"
rows_per_batch = 100
timezone = "Asia/Kolkata"

fake = Faker()
Faker.seed(42)
```

## ✅ Step 2: Generate Fake User Data

```python
def generate_fake_data(n):
    now = pd.Timestamp.now(tz=pytz.timezone(timezone))
    return pd.DataFrame([{
        "name": fake.name(),
        "address": fake.address().replace("\n", ", "),
        "email": fake.email(),
        "ingestion_time": now
    } for _ in range(n)])
```

## ✅ Step 3: Append to Delta Table (with version control)

```python
def create_or_append_delta_table(pdf):
    df = spark.createDataFrame(pdf)
    if DeltaTable.isDeltaTable(spark, delta_path):
        df.write.format("delta").mode("append").save(delta_path)
    else:
        df.write.format("delta").mode("overwrite").save(delta_path)
```

## ✅ Step 4: Export Latest Records to CSV

```python
def export_latest_rows(n=100):
    df_all = spark.read.format("delta").load(delta_path)
    df_latest = df_all.orderBy("ingestion_time", ascending=False).limit(n)

    dbutils.fs.rm(csv_export_path, recurse=True)
    df_latest.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_export_path)

    return df_latest
```

## ✅ Step 5: Show Download Link for CSV

```python
def show_download_link():
    files = dbutils.fs.ls(csv_export_path)
    for f in files:
        if f.name.endswith(".csv"):
            print(f"📥 Download: https://community.cloud.databricks.com/files/tmp/user_data_latest/{f.name}")
```

## ✅ Step 6: Track Delta Table Versions

```python
def get_delta_table_versions():
    dt = DeltaTable.forPath(spark, delta_path)
    return dt.history().select("version", "timestamp", "operation").orderBy("version", ascending=False)

get_delta_table_versions().show()
```

## 🚀 Run the Full Ingestion + Export Pipeline

```python
pdf = generate_fake_data(rows_per_batch)
create_or_append_delta_table(pdf)
export_latest_rows()
show_download_link()
```

---

## 📨 Email Script (Run on Local Machine)

```python
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

sender_email = "prakashmailbox0016@gmail.com"
receiver_email = "prakashpandeysearch192@gmail.com"
app_password = "***************"
csv_file = "user_data_latest.csv"

df = pd.read_csv(csv_file)
html_table = df.head(10).to_html(index=False)

subject = "📊 Delta Table Export (Latest Data)"
body_text = "Hi,\n\nPlease find attached the latest exported data from the Delta table.\n\nRegards,\nPrakash Pandey"

msg = MIMEMultipart("alternative")
msg["From"] = sender_email
msg["To"] = receiver_email
msg["Subject"] = subject
msg.attach(MIMEText(body_text, "plain"))
msg.attach(MIMEText(f"<p>{body_text}</p>{html_table}", "html"))

with open(csv_file, "rb") as f:
    part = MIMEApplication(f.read(), Name=csv_file)
    part['Content-Disposition'] = f'attachment; filename="{csv_file}"'
    msg.attach(part)

server = smtplib.SMTP("smtp.gmail.com", 587)
server.starttls()
server.login(sender_email, app_password)
server.send_message(msg)
server.quit()

print("✅ Email sent with attachment and HTML preview.")
```

---

## 🔐 Gmail App Password Setup

1. Go to: https://myaccount.google.com/apppasswords
2. Enable 2-Step Verification
3. Generate App Password for "Mail"
4. Use this password in the local script

---

## 👨‍💻 Author

**Prakash Pandey**  
🎓 BCA Student | 🚀 Aspiring Data Engineer  
🔗 [LinkedIn](https://www.linkedin.com/in/prakash-pandey-1234)

