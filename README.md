# Smart City Pipeline (Microsoft Fabric)

https://werhere-it-academy.gitbook.io/werhere-it-academy-handbook/data-engineering/azure-data-engineering-module/week-5-project-ii

☢️ Week 5 (Project-II)
Data Processing and Transformation
Introduction
Until now, we have taken data from APIs and loaded it into the Bronze layer (Raw Data) of our Lakehouse.
However, this data is in a complex JSON format, which is hard to read and not ready for analysis.

This is where the real work of a Data Engineer begins.
In this module, we will use the power of Apache Spark to shape, clean, and transform raw data into valuable information.

After completing this module, you will learn:

How to develop code in Notebooks

How to write modular code (Config / Log / Function separation)

How to clean raw data (Bronze → Silver) and analyze it (Gold)

4.1 Apache Spark and the Notebook World
We moved the data using Data Factory.
Now, we will use Spark Notebooks to process the data.

Spark is a very fast engine for Big Data processing.
We will control this engine using Python (PySpark).

Learning Resources
Text

Microsoft Learn – What are Fabric Notebooks?

PySpark Cheat Sheet – Download this to your computer

Video

PySpark for Beginners
(The first 15–20 minutes are enough to understand the DataFrame concept)

Exercise: First Notebook
Create a new Notebook in Microsoft Fabric and test the environment by running these simple Python tasks:

Print "Hello Data Engineering" on the screen

Define two variables (example: x = 5, y = 10) and add them

Read one JSON dataset from the Bronze layer using PySpark and display the first 10 rows

4.2 From Raw to Clean: Bronze → Silver (Normalization)
Data in the Bronze layer is like nested boxes (Nested JSON).
To analyze this data, we need to open these boxes and spread the data into a flat table (rows and columns).

This process is called:

Flatten

Explode

Learning Resources
Text

PySpark Explode Function – How to turn lists into rows

Flatten Nested JSON in PySpark – How to flatten nested structures

Video

Handling Nested Data in Spark

Exercise: Transformation on Paper
Read the JSON file received from Energy Zero and apply normalization using PySpark.

When you display the data on the screen, it should look like a CSV table, with all data clearly separated into rows and columns.

4.3 From Normalization to Analysis: Silver → Gold (Business Logic)
Now we can store our data as clean tables in the Silver layer.
However, energy prices are in one table and weather data is in another table.

The company asks this question:

“What was the price when the wind was blowing?”

To answer this question, you must:

Join the tables

Use Window Functions to calculate past values
(example: average of the last 3 hours)

Learning Resources
Text

PySpark Join Types – What are Left, Inner, and Outer Join?

Window Functions in PySpark – How to calculate rolling averages

Video

Visualizing SQL Joins (Very useful to understand the logic)

Exercise: Understanding the Logic
Draw two tables in draw.io:

Table A (Time, Price)

12:00 → 100 TL

13:00 → 120 TL

Table B (Time, Temperature)

12:00 → 25°C

13:00 → 26°C

If you join these two tables using Time, how will the third table (Gold) look?

The goal is to understand the logic of joining tables.

Section 3: The Brain of the Project (Notebook Coding)
You have now learned how to think like an engineer.
In this section, you are expected to create 5 Notebooks based on the architecture below.

While writing code:

Avoid hard-coded values

Be dynamic and modular

1. Notebook: nb_config (Configuration)
This notebook is the brain of the project.
All constant values must be stored here.

Configuration Management
What is it?

Imagine you build a large house.
If you want to cut the electricity, do you remove every bulb one by one, or do you turn off the main fuse?

In software, the configuration file is the main fuse box of your project.

File paths, database names, and constant values should not be written directly into the code (hard-coding).
They should be stored in one central file.

Hard-Coding vs Soft-Coding
❌ Hard-Coding (Bad Practice)


Copy
df = spark.read.load("Files/Bronze/Energy")
Problem:
If you change the folder name to Bronze_V2, you must update this line in 50 notebooks.

✅ Soft-Coding / Config (Engineering Practice)


Copy
df = spark.read.load(PATH_BRONZE)
Solution:
If the folder name changes, you only update PATH_BRONZE in nb_config.
All notebooks are fixed at the same time.

This is called Single Source of Truth.

How Does It Work? (%RUN Magic Command)
In environments like Microsoft Fabric and Databricks, we use Magic Commands to run one notebook inside another.

In our project, when the nb_generic_bronze_to_silver notebook runs, you will see this command in the first line:


This command means:

“Go and find the nb_config notebook, run all the code inside it, and load all variables (such as PATH_BRONZE) into memory.”

Because of this, we do not need to write any file paths in the main notebook.
All paths come directly from nb_config.

Learning Resources
Text

The DRY Principle (Don’t Repeat Yourself) – The most basic rule of software development

Hard Coding vs Soft Coding – Why we should not write constant values inside the code

💡 Tip

While creating the nb_config notebook, note that we will need the following variables:

ROOT_PATH (Main Lakehouse path – ABFSS)

PATH_BRONZE

PATH_SILVER

PATH_GOLD

These variables will be the GPS coordinates of your project.

Expected Output
A variable that stores the Lakehouse file path (ABFSS path)

Variables that store Bronze, Silver, and Gold folder paths

When this notebook runs, it should only print:

“Config Loaded”

It should not perform any data processing.

2. Notebook: nb_logging (Logging)
While the code is running, we should track what happens in the background using professional logging, not print().

2.1 Logging – The Black Box of the Code
What is Logging?
When an airplane crashes, what is the first thing experts look for?
The Black Box.

The black box records everything: what the pilot did, when the engine stopped, and what happened every second.

In software, logging is the black box of your code.
While your code runs, it quietly writes notes like:

“At 14:00, data reading started”

“At 14:02, connection lost”

“At 14:05, process finished”

Why Don’t We Use print()?
Many students use print() to debug code.
However, in large projects, print() is not enough:

Persistence
Print output disappears when the screen is closed.
Logs are saved to files and can be checked even years later.

Importance Levels
print() shows everything the same way.
Logging separates messages as Info, Warning, or Error.

Format
Logs automatically include date and time information.

Learning Resources
Text

Real Python – Logging in Python (considered the best reference)

Python Docs – Logging HOWTO (official documentation)

Video

Python Logging Basics

Logging Logic: Traffic Lights (Levels)
Each log message has an urgency level.
This level shows the health of the code at that moment.

Level
Meaning
Description
Traffic Light
DEBUG

Debugging

Very detailed information. Used only while developing. Example: “Variable X is now 5”

Microscope

INFO

Information

Everything is working fine. Example: “System started”, “Data read”, “Process finished”

Green Light

WARNING

Warning

The process works, but there is a problem. Example: “Disk is getting full”

Yellow Light

ERROR

Error

A process failed, but the program did not crash. Example: “File not found”

Red Light

Practice and Exercises
Try these exercises in a Fabric Notebook to see the power of logging.

Exercise: First Log Record
Let’s create a simple logging setup using Python’s built-in logging library.

Task:
Run the given code and pay attention to the date and time format in the output.


Question
What is the difference between print output and logging output?
What extra information exists in log output?

Expected
A structure that initializes the Python logging library

Log format must be:


Copy
[Date] - [Module Name] - [Message]
3. Notebook: nb_functions (Toolbox)
Turn repeated tasks into functions.

CRITICAL
CRITICAL – System crashed, there is a fire!
The program stopped working. Sirens are on.

Functions and Modularity – The Toolbox of the Code
What Is It?
Imagine you are a carpenter.
Every time you need to hammer a nail, do you melt metal and create a new hammer,
or do you take a ready hammer from your toolbox?

In software, functions are your toolbox.

If you need to do the same job (for example: fixing date formats) in many places,
you do not copy and paste the code.
You create a function and call it whenever you need it.

The Golden Rule of Engineering: D.R.Y.
One of the most important principles in software engineering is DRY
(Don’t Repeat Yourself).

❌ Bad Practice (Copy–Paste)
You write 10 lines of code to clean column names for Energy data.
You copy the same code for Weather data and Air Quality data.

Problem:
If you find a bug, you must fix it in 3 different places.

✅ Good Practice (Modular)
You write one function called clean_columns().
You call this function for all datasets.

Solution:
You fix the bug only once, and all datasets are fixed automatically.

What Are Our “Tools” in This Project?
In this project, inside the nb_functions notebook, we will create the following main functions.
It is important to understand their logic now.

1. CLEAN_COLUMN_NAMES (Column Name Cleaner)
Column names from data sources are usually messy, for example:

First Name

Wind Speed (km/h)

Price.EUR

This function converts them into snake_case format:

first_name

wind_speed_kmh

price_eur

2. EXPLODE_AND_FLATTEN (Flattener)
JSON data is like nested boxes.

This function:

Explodes nested lists

Flattens nested structures

Converts data into a flat table

3. SAVE_TO_LAKEHOUSE (Smart Saver)
To avoid thinking every time about:

File format

Overwrite or not

We use this function.

You only give:

the DataFrame (df)

the table name

The function handles everything else.

4. GENERATE_SURROGATE_KEY (Unique ID Generator)
In databases, every row should have an ID.

Sometimes, there is no ID in the data.
Instead, combinations like Date + City make the row unique.

This function:

Takes selected columns (example: Date and City)

Creates a unique fingerprint using MD5 Hash

Example output: a3f9...

Why is this needed?
Matching text columns is slow in large tables.
Matching hash values is much faster.

5. CREATE_TIME_SERIES_FRAME (Time Series Creator)
All datasets in this project have date and time information in common.

Because our analysis is time-based, this function creates a time series frame
so that data from different tables can be matched correctly for the same time period.

6. SELECT_COLUMNS_SAFE (Safe Selector)
Sometimes we select a column like Wind_Direction, but that column does not exist in the data.

Normally, the code crashes with an error.

This function says:

“Stay calm. If the column exists, select it.
If not, ignore it.”

This prevents the code from crashing.

7. JOIN_DATAFRAMES (Joiner)
This function is used to join different tables
(example: Energy and Weather)
using a common key (example: Time).

Learning Resources

Text

W3Schools – Python Functions: Basics of defining functions

Modular Programming Definition – Why we should split code into parts

Video

Python Functions Explained

Snake Case vs Camel Case

Expected Functions
clean_column_names
A function that removes spaces from column names and converts them to lowercase.

explode_and_flatten
A function that converts nested JSON data into a flat table.

save_to_lakehouse
A function that saves the given DataFrame to the specified path in Delta format.

4. Notebook: nb_generic_bronze_to_silver (Transformation Engine)
This is the most critical part of the project.
This notebook must work in a Metadata-Driven (Configuration-Based) way.

Metadata-Driven ETL – The “Capsule” Logic
What Is It?
A classic programmer thinks like this:

“I will write one code for Energy data and another code for Weather data.”

A Data Engineer thinks like this:

“I will write one engine that works based on the rules I give it.”

We call this approach Metadata-Driven ETL, or in our project, the Capsule Logic.

What Is a Capsule?
A Capsule is the definition of the job, not the code itself.

Think about a washing machine.
The machine (engine) is always the same.

Cotton, 40°C, 800 RPM → one result

Synthetic, 30°C → different result

These settings are the Capsule.

How Does It Work?
While writing code in the nb_generic_bronze_to_silver notebook, you will design two sections.

Section 1: Capsule Definitions (The Config)
In this section, you define the rules inside a Python Dictionary.

Energy Capsule

Source: Bronze/Energy

Target: Silver_Energy

Steps: Explode(Prices) → Flatten → Rename

Weather Capsule

Source: Bronze/Weather

Target: Silver_Weather

Steps: Zip(Hourly) → Explode → Flatten

Section 2: The Engine
This section contains one single for loop.

The loop:

Goes through each capsule

Uses functions from nb_functions

Executes the transformation steps

Advantage:
If you add a new dataset (example: Water Quality) tomorrow,
you do not change the engine code.
You only add a new capsule.

Learning Resources
Text

Metadata Driven ETL Explained – Why we define rules instead of writing separate code

Python Dictionaries – Structure used to define capsules

Video

Automation with Python Loops

Expected Logic
Define a dictionary at the top of the notebook
This dictionary must include:

Source paths (Energy, Weather)

Target table names

Create a loop that reads the dictionary and:

Reads data from the Bronze layer

Cleans data using functions in nb_functions

Saves data to the Silver layer

💡 Tip:
Do not write separate code for Energy and Weather.
One single code must process both.

5. Notebook: nb_process_silver_to_gold (Analysis)
In the final step, prepare the data for Business Intelligence reporting.

Business Intelligence and Analysis – Silver to Gold
What Is It?
In the Silver layer, data is clean and tables are ready.
But we still do not have the answer to this question:

“When should I use electricity?”

The Gold layer is like the cooking and presentation stage of a meal.

We take ingredients from Silver (Energy, Weather),
mix them (Join),
cook them (Window Functions),
and serve them as a Gold table.

Techniques to Be Used
Inside the nb_process_silver_to_gold notebook, we will use these techniques.

1. Join
The Energy table has a record at 2024-01-01 12:00.
The Weather table also has a record at the same time.

We must combine these two rows into one row.

Method to use:

Left Join
(Keep Energy as the main table, add Weather data to it)

2. Window Functions
When analyzing data, we should not look only at now,
but also at the recent past.

Question:
Is the price cheap right now?

Answer:
To know this, we must check the average of the last 3 hours.
If the current price is lower than the average, it is cheap.

In Spark, this is called a Rolling Window.

Learning Resources
Text

Spark SQL Joins – Join types and logic

Expected Logic
Read Energy and Weather tables from the Silver layer

Join them using timestamp

Analysis:

Calculate the average price of the last 3 hours

If the current price is lower than the average and wind speed is high,
label the row as “FIRSAT”

Save the result to the Gold layer

POWER BI INTEGRATION
1. Direct Lake Connection and Data Modeling
OneLake Integration
Power BI must connect to the Gold layer tables created in Microsoft Fabric
(Lakehouse / Warehouse Gold) using Direct Lake mode,
without copying the data.

Time Series Relationship
Energy, Weather, and Air Quality tables in the Gold layer,
which are joined using time series,
must be modeled by connecting them to a central Calendar table
using a common Timestamp column.

Data Preparation
Gold tables that are ready for reporting are provided to reporting tools
during the Serve stage.

2. Definition of Critical Metrics (DAX)
Opportunity Analysis
Create DAX measures that count or show the current status of “FIRSAT” labels
in the Gold layer.
These labels represent moments when:

the price is below the 3-hour average

wind speed is high

Cost and Efficiency
Add metrics that calculate the percentage (%) difference
between the current energy price and the 3-hour rolling average.

Environmental Impact Scoring
Create a metric called “Clean Energy Score”
by combining:

air quality values (PM10, NO₂, CO)

wind speed

3. Decision Support Visualizations (Dashboard Design)
“Cheapest and Greenest Moment” Indicator
To support the company’s main goal of finding the best time to use energy,
use a large Card or Gauge visual
that shows the current status as “FIRSAT” or “NORMAL”.

Correlation Charts
Use Combo Charts to show trend analysis over time (hourly),
displaying how energy prices decrease
as wind speed and solar radiation increase.

Air Quality Monitoring
Add heatmaps or line charts to track city air pollution levels
(carbon monoxide and particulate data)
and show the relationship between energy usage and environmental impact.

Final Result
Congratulations 🎉
When you complete these steps, you will have an end-to-end Big Data Pipeline
that works from Bronze → Silver → Gold.

When these three steps are completed:

Raw data starts from Bronze

Is processed in Silver

Is analyzed in Gold

The result is a professional Decision Support System
that allows managers to make strategic decisions based on data.
