# Scala Spark Quick Start On Databricks

The purpose of this page to help new people easily get started using Apache Spark inside the of the Databricks environment. The aim is to have you the read not have to spend hours trying to figure out how to get things up and running.

---

### Open A CSV File To Data Frame
This examples will return a result in the Databricks notebook for the column 
headers and thier types.

    val wholeDF = spark.read.format("csv")
        .option("header", "true")
        .load("/FileStore/tables/daily_change_in_cases_by_phu.csv")