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
