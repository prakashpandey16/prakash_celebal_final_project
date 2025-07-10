# ⚙️ Delta Table Automation Pipeline – Solution Overview

## 📌 Problem Statement

Design and implement an automated data pipeline that:

- 🧪 Generates fake user data (`Name`, `Address`, `Email`)
- 🗃️ Appends data incrementally to a **Delta Lake** table
- 🧾 Maintains **version history** using Delta Table API
- 🌐 Uses **timezone-aware timestamps**
- 🔁 Runs automatically at regular intervals (e.g., every 5 minutes)
- ✉️ Sends **HTML email notifications** with summaries of newly appended data

---

## 🧠 Solution Strategy

We explored and implemented two approaches to solve the problem:

---

### ✅ Approach 1: **Databricks Community Edition (Initial Prototype)**

**Tech Stack:**  
Databricks CE | PySpark | Delta Lake | DBFS | Faker | Schedule | Local Email Script

**Highlights:**
- Great for quick prototyping and Delta Lake learning
- Native support for Delta Tables
- Email notifications sent via external/local Python script
- Lacked built-in scheduling and outbound email support

> **Challenge Faced:**  
While using Databricks CE, we faced cluster creation and runtime issues that temporarily blocked development. With guidance and persistence, we resolved it and were able to proceed smoothly.

---

### ✅ Approach 2: **Azure Portal + Azure Databricks (Final Implementation)**

**Tech Stack:**  
Azure Databricks | PySpark | Delta Lake | Azure Data Lake | Faker | Schedule | Pandas | External SMTP

**Highlights:**
- Scalable, cloud-native environment for Delta Lake pipelines
- Configurable timezone support for ingestion
- Integrated with external scripts for HTML email alerts
- Job automation possible using `schedule`, **Azure Databricks Jobs**, or **Azure Automation**

> **Outcome:**  
Successfully implemented an end-to-end automated ingestion and alerting pipeline using Azure's robust infrastructure.

---

## 🙏 Special Thanks

A special thanks to **Jash Tewani** for his timely support and guidance during the cluster issue in Azure Databricks. 

---

## 👤 Author

**Prakash Pandey**  
🔗 [LinkedIn](https://www.linkedin.com/in/prakash-pandey-2827522b1/)

---

