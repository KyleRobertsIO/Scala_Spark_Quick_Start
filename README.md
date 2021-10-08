# Scala Spark Quick Start On Databricks

The purpose of this page to help new people easily get started using Apache Spark inside the of the Databricks environment. The aim is to have you the read not have to spend hours trying to figure out how to get things up and running.

---

### Open A CSV File To Data Frame
This examples will return a result in the Databricks notebook for the column 
headers and thier types. The string `/FileStore/tables/daily_change_in_cases_by_phu.csv` is a path in the DBFS (Databricks File System) to a specific data file to be processed by Spark.

The `dataFrame` variable will hold the whole dataset in memory for processing.

    val dataFrame = spark.read.format("csv")
        .option("header", "true")
        .load("/FileStore/tables/daily_change_in_cases_by_phu.csv")

## Displaying Your Data Frame

Using the code below we can print out our data frame state into the notebook UI.

    display(dataFrame)

## Selecting Certain Fields From Data Frame

Maybe you have a very wide table for your dataset and you don't really need to see everything inside of it. In Spark we write Scala based code that will 
perform SQL style operations on the data frame.

    val depExpenseDF = dataFrame.select("id", "timestamp", "department_name", "department_expenses")

## Removing Null or NaN Values From Columns

You are likely to have datasets where you have null or NaN values in the data.
This type of process falls under the topic of data cleaning and it's important to having a mature health dataset for analytics.

    // "0" is the value to replace the null/NaN value in a column
    // "salary" is the column name to be affected by the cleaning
    val cleanedDF = dataFrame.na.fill("0", Seq("salary"))