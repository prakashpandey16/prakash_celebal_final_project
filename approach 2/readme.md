
# ✅ Azure Delta Lake Ingestion Project

This project demonstrates how to automate data ingestion into **Delta Lake on Azure Data Lake Storage Gen2**, using **PySpark**, and send email summaries with the latest ingested data.

---

## 📌 Features

- 🧾 Generate fake user data (Name, Address, Email)
- 🧠 Store in Delta format using `delta-spark`
- 🔁 Append data every 5 minutes
- 📧 Send an email summary using Gmail SMTP
- 💾 Run from your **local Python environment**

---

## ✅ Step-by-Step Project Setup

### 🔹 STEP 1: Azure Storage Setup

1. **Login to Azure Portal**  
   [https://portal.azure.com](https://portal.azure.com)

2. **Create a Storage Account**
   - Use your *Azure for Students* subscription
   - Select **Enable Hierarchical Namespace (for ADLS Gen2)**

3. **Create a Container**
   - Name: `delta-data`
   - Access level: `Private`

4. **Get Access Key**
   - Go to: `Storage Account → Security + networking → Access Keys`
   - Copy **Key1**  
   - You'll use this in `config.py`

---

### 🔹 STEP 2: Local Environment Setup

1. **Install Java JDK (Required by Spark)**  
   - **Windows:**  
     ```bash
     choco install openjdk11
     ```
   - **Ubuntu:**  
     ```bash
     sudo apt install openjdk-11-jdk
     ```

2. **Install Python Packages**
   ```bash
   pip install pyspark==3.5.0 delta-spark==3.1.0 faker schedule pytz
   ```

---

### 🔹 STEP 3: Configure Your Project

Create a project folder, e.g., `delta_ingestion_project/`, and add the following files:

---

#### 1️⃣ `config.py`

```python
# config.py
AZURE_STORAGE_ACCOUNT_NAME = "prakashmailbox0016"
AZURE_CONTAINER_NAME = "delta-data"
AZURE_STORAGE_KEY = "********"

EMAIL_SENDER = "prakashmailbox0016@gmail.com"
EMAIL_PASSWORD = "************"
EMAIL_RECEIVER = "prakashpandeysearch192@gmail.com"
```

---

#### 2️⃣ `email_utils.py`

```python
# email_utils.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email(subject, body, sender, password, receiver):
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = receiver
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender, password)
        server.send_message(msg)
```

---

#### 3️⃣ `delta_pipeline.py`

```python
# delta_pipeline.py
import os, schedule, time
from datetime import datetime
from faker import Faker
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from email_utils import send_email
from config import *

fake = Faker()

builder = (
    SparkSession.builder.appName("DeltaIngestion")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:3.1.0")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.AzureLogStore")
    .config("spark.hadoop.fs.azure.account.key." + AZURE_STORAGE_ACCOUNT_NAME + ".dfs.core.windows.net", AZURE_STORAGE_KEY)
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def generate_data(n=10):
    return [
        (fake.name(), fake.address().replace("\n", ", "), fake.email(), datetime.now())
        for _ in range(n)
    ]

def append_to_delta():
    print("Appending data...")
    data = generate_data(10)
    df = spark.createDataFrame(data, ["name", "address", "email", "timestamp"])
    
    delta_path = f"abfss://{AZURE_CONTAINER_NAME}@{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/user_data"

    df.write.format("delta").mode("append").save(delta_path)

    latest_data = df.orderBy(df.timestamp.desc()).limit(10).toPandas()

    html_table = latest_data.to_html(index=False)
    send_email(
        subject="✅ Delta Table Ingestion Summary",
        body=f"<h2>Latest Ingested Records</h2>{html_table}",
        sender=EMAIL_SENDER,
        password=EMAIL_PASSWORD,
        receiver=EMAIL_RECEIVER,
    )
    print("Ingestion complete & email sent.")

# Schedule every 5 minutes
schedule.every(5).minutes.do(append_to_delta)

if __name__ == "__main__":
    print("Pipeline started... Running every 5 mins.")
    append_to_delta()  # initial run
    while True:
        schedule.run_pending()
        time.sleep(1)
```

---

### 🔹 STEP 4: Run Your Pipeline

In your terminal, run:

```bash
python delta_pipeline.py
```

✅ This will:

- Create or append data to Delta table
- Track ingestion with timestamps
- Send email summary (latest 10 records)
- Run automatically every 5 minutes
---

## 📦 Dependencies

- PySpark
- delta-spark
- Faker
- schedule
- pytz

---


## 📧 Email Configuration Note

- Use [Gmail App Password](https://myaccount.google.com/apppasswords) instead of your main Gmail password.
- Make sure 2-Step Verification is enabled on your Google account.

---

## 🙌 Credits

Project built by [Prakash Pandey](linkedin.com/in/prakash-pandey-2827522b1/)

