# **Intro To Spark Scala**

## **Open A CSV File To Data Frame**
In this example we will return a result in the Databricks notebook for the column headers and their types. The string `/FileStore/tables/daily_change_in_cases_by_phu.csv` is a path in the DBFS (Databricks File System) to a specific data file to be processed by Spark.

The `dataFrame` variable will hold the whole dataset in memory for processing.

    val dataFrame = spark.read.format("csv")
        .option("header", "true")
        .load("/FileStore/tables/daily_change_in_cases_by_phu.csv")

<br>

## **Displaying Your Data Frame**

Using the code below we can print out our data frame state into the notebook UI.

    display(dataFrame)

<br>

## **Selecting Certain Fields From Data Frame**

Maybe you have a very wide table for your dataset and you don't really need to see everything inside of it. In Spark we write Scala based code that will 
perform SQL style operations on the data frame.

    val depExpenseDF = dataFrame.select("id", "timestamp", "department_name", "department_expenses")

<br>

## **Removing Null or NaN Values From Columns**

You are likely to have datasets where you have null or NaN values in the data.
This type of process falls under the topic of data cleaning and it's important to having a mature health dataset for analytics.

    // "0" is the value to replace the null/NaN value in a column
    // "salary" is the column name to be affected by the cleaning
    val cleanedDF = dataFrame.na.fill("0", Seq("salary"))

<br>

## **Casting A Column Type**

In this example we can take a column that maybe has a wrong datatype assignment and convert it to the proper one that we need for analysis. You can see all the different Spark types [here](https://spark.apache.org/docs/1.6.1/api/java/org/apache/spark/sql/types/package-summary.html).

    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.types.FloatType

    dataFrame.withColumn("salary", col("salary").cast(FloatType))

<br>

## **Using Aggregate Functions In Queries**
To use `Aggregate Functions` inside of Scala Spark you first need to import them into you notebook. Belows import will bring in all Spark SQL functions.
You can find a list of built-in aggregate functions for Spark [here](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html).

    import org.apache.spark.sql.functions._

<br>

***Dataset Example***

| outbreak_group  | number_ongoing_outbreaks |
|-----------------|--------------------------|
| Congregate Care | 73                       |
| Workpalace      | 43                       |
| Workpalace      | 65                       |
| Other/Unknown   | 12                       |

<br>

Below is an example of averaging a columns values.

    // Create data frame of the average result of a row.
    val avgOutbreaksDF = df.select(avg("number_ongoing_outbreaks").alias("avg_outbreaks"))

    // Collect only the value from row[0], column[0]
    val avgOutbreaks = avgOutbreaksDF.collect()(0)(0)

    // Print the result to the notebook console
    println("\nAverage Outbreaks [" + avgOutbreaks + "]\n")

<br>

Below is an example of a group by average of types.

    val groupAvgDF = dataframe.groupBy("outbreak_group")
        .agg(
            avg("number_ongoing_outbreaks").alias("avg_outbreak")
        )
    display(groupAvgDF)

<br>

***Output Example***

| outbreak_group    | avg_outbreak       |
|-------------------|--------------------|
| Other/Unknown     | 6.7129770992366415 |
| Workplace         | 30.566350710900473 |
| Congregate Living | 13.913430935709739 |
