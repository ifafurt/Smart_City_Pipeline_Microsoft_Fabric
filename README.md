# Smart City Pipeline (Microsoft Fabric)

https://werhere-it-academy.gitbook.io/werhere-it-academy-handbook/data-engineering/azure-data-engineering-module/week-5-project-ii

🏙️ Smart City Energy Monitoring System

End-to-End Azure Fabric Data Engineering Project

📌 Project Overview

This project is an end-to-end data engineering pipeline built on Microsoft Fabric to collect, process, analyze, and visualize Smart City energy data.

The system ingests data from multiple external APIs, processes it through Bronze → Silver → Gold layers using PySpark, and delivers insights through a Power BI Semantic Model and Dashboard.

All steps are fully automated using Fabric Pipelines with scheduled execution.

🏗️ Architecture Overview

Technologies Used

Microsoft Fabric (Lakehouse, Notebooks, Pipelines)

PySpark

Power BI

Pipeline Layers:
<img width="2096" height="491" alt="son cizimxxxx" src="https://github.com/user-attachments/assets/6a56168e-e25c-4512-8c45-e2226faec35e" />

📂 Notebook Structure

This project is organized into 5 main notebooks:

1️⃣ nb_config

Purpose: Centralized configuration

Lakehouse paths (Bronze / Silver / Gold)

Dataset configurations

Environment-independent setup

2️⃣ nb_logging

Purpose: Operational logging

Creates a Delta-based logging table

Logs each pipeline step with:

Status (STARTED / SUCCESS / FAILED)

Notebook name

Layer

Step name

Timestamp

This ensures observability and traceability for all data operations.

3️⃣ nb_functions

Purpose: Reusable transformation toolbox
Contains core generic functions such as:

clean_column_names

explode_and_flatten

generate_surrogate_key

select_columns_safe

save_to_lakehouse

These functions enable config-driven, reusable, and scalable transformations.

4️⃣ nb_generic_bronze_to_silver

Purpose: Bronze → Silver processing

Key features:

Reads raw JSON data from Bronze layer

Cleans column names

Explodes & flattens nested JSON structures

Generates surrogate keys

Writes structured Delta tables to Silver layer

Logs every step using nb_logging

Handled datasets:

Weather

Air Quality

Energy Prices

5️⃣ nb_silver_to_gold

Purpose: Silver → Gold analytical processing

Key logic:

Uses Energy data as the main fact table

Left joins Weather and Air Quality data

Applies window functions:

Rolling 3-hour average energy price

Business logic:

If current price < last 3-hour average

And wind speed is high

Label the row as “OPPORTUNITY”

Saves final analytical dataset to Gold layer as Delta table

<img width="1919" height="1079" alt="PNG - gold_smart_city_analysis" src="https://github.com/user-attachments/assets/54152ca5-5360-4c08-b6e3-a46bc3057de7" />

⚙️ Automation & Scheduling

All notebooks are orchestrated using Microsoft Fabric Pipelines

Pipeline flow:
<img width="1919" height="1079" alt="PNG - Master" src="https://github.com/user-attachments/assets/ed15e3a7-cef8-4bad-ac2f-65f06f26bdb7" />

<img width="1919" height="1079" alt="image" src="https://github.com/user-attachments/assets/aa788425-bc2e-4e74-8cc0-e12510e9ab82" />

Pipeline is scheduled for automatic execution

Fully hands-free operation after deployment

🎯 Key Data Engineering Concepts Demonstrated

✔ Medallion Architecture (Bronze / Silver / Gold) ✔ Delta Lake & Schema Evolution ✔ Config-driven transformations ✔ Window functions for time-series analysis ✔ Logging & monitoring ✔ Pipeline orchestration ✔ Semantic modeling & BI integration

🚀 Outcome

This project demonstrates a production-style data engineering workflow using modern cloud-native tools and best practices.
