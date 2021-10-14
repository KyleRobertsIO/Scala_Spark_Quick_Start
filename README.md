# Scala Spark Quick Start On Databricks

The purpose of this page to help new people easily get started using Apache Spark inside the of the Databricks environment. The aim is to have you the reader not have to spend hours trying to figure out how to get things up and running.

This document was written during October 2021 for:
* Databrick Runtime V8.4
* Scala V2.12
* Spark V3.1.2

---

### **Open A CSV File To Data Frame**
This examples will return a result in the Databricks notebook for the column 
headers and thier types. The string `/FileStore/tables/daily_change_in_cases_by_phu.csv` is a path in the DBFS (Databricks File System) to a specific data file to be processed by Spark.

The `dataFrame` variable will hold the whole dataset in memory for processing.

    val dataFrame = spark.read.format("csv")
        .option("header", "true")
        .load("/FileStore/tables/daily_change_in_cases_by_phu.csv")

### Displaying Your Data Frame

Using the code below we can print out our data frame state into the notebook UI.

    display(dataFrame)

### **Selecting Certain Fields From Data Frame**

Maybe you have a very wide table for your dataset and you don't really need to see everything inside of it. In Spark we write Scala based code that will 
perform SQL style operations on the data frame.

    val depExpenseDF = dataFrame.select("id", "timestamp", "department_name", "department_expenses")


### **Removing Null or NaN Values From Columns**

You are likely to have datasets where you have null or NaN values in the data.
This type of process falls under the topic of data cleaning and it's important to having a mature health dataset for analytics.

    // "0" is the value to replace the null/NaN value in a column
    // "salary" is the column name to be affected by the cleaning
    val cleanedDF = dataFrame.na.fill("0", Seq("salary"))

### **Casting A Column Type**

In this example we can take a column that maybe has a wrong datatype assignment and convert it to the proper one that we need for analysis. You can see all the different Spark types [here](https://spark.apache.org/docs/1.6.1/api/java/org/apache/spark/sql/types/package-summary.html).

    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.types.FloatType

    dataFrame.withColumn("salary", col("salary").cast(FloatType))

### **Using Aggregate Functions In Queries**
To use `Aggregate Functions` inside of Scala Spark you first need to import them into you notebook. Belows import will bring in all Spark SQL functions.
You can find a list of built-in aggregate functions for Spark [here](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html).

    import org.apache.spark.sql.functions._

---

\
***Dataset Example***

| outbreak_group  | number_ongoing_outbreaks |
|-----------------|--------------------------|
| Congregate Care | 73                       |
| Workpalace      | 43                       |
| Workpalace      | 65                       |
| Other/Unknown   | 12                       |

\
Below is an example of averaging a columns values.

    // Create data frame of the average result of a row.
    val avgOutbreaksDF = df.select(avg("number_ongoing_outbreaks").alias("avg_outbreaks"))

    // Collect only the value from row[0], column[0]
    val avgOutbreaks = avgOutbreaksDF.collect()(0)(0)

    // Print the result to the notebook console
    println("\nAverage Outbreaks [" + avgOutbreaks + "]\n")

\
Below is an example of a group by average of types.

    val groupAvgDF = dataframe.groupBy("outbreak_group")
        .agg(
            avg("number_ongoing_outbreaks").alias("avg_outbreak")
        )
    display(groupAvgDF)

\
***Output Example***

| outbreak_group    | avg_outbreak       |
|-------------------|--------------------|
| Other/Unknown     | 6.7129770992366415 |
| Workplace         | 30.566350710900473 |
| Congregate Living | 13.913430935709739 |

---

## Using Delta Lake

### **Making A Delta Lake Table**

Making a Delta Lake table is simple and not much different from a csv, json or parquet table.

    dataFrame.write
        .format("delta")
        .save("/mnt/delta/my_dataset_name")

    spark.sql(
        "CREATE TABLE <table_name> USING DELTA LOCATION '/mnt/delta/my_dataset_name'"
    )

While having a Delta table is nice we would like to speed up query times when using Spark. One of the common ways to speed up queries is by partitioning the dataset into seperate files based on a column and its values.

Using the same type of code above we just add a .partitionBy() method.

    dataFrame.write
        .format("delta")
        .partitionBy("column_name)")
        .save("/mnt/delta/my_dataset_name")

Creating the Delta table will now have seperate files that are divided by your distinct column values.

### **Reading Your File System**

If you want to see what is the state of your files in a directory you can just use your notebook with this command.

    display(
        dbutils.fs.ls("/mnt/delta/my_dataset_name")
    )

### **Deleting A Delta Table**

If you need to remove your Delta table from the behind the seens Hive instance or the databricks filesystem, just using the commands below.

    // Delete the table.
    spark.sql("DROP TABLE <table_name>")
    
    // Delete the saved data.
    dbutils.fs.rm("/mnt/delta/my_dataset_name)", true)

### **View Partitioned Delta Table**

If you have your dataset partitioned, you can can easily aggregate it together
to view the whole dataset or query against it.

    val dataFrame = spark.read
        .format("delta")
        .load("/mnt/delta/my_dataset_name")

---

## **Upsert Column Data In Delta Table**
Inorder to update the state of the Delta table data and append a new version for time travelling
you need do some mapping.

    import io.delta.tables._

    val deltaTable = DeltaTable.forPath(spark, "/mnt/delta/my_dataset_name")

    // Load whole dataset
    val dataFrame = spark.read
        .format("delta")
        .load("/mnt/delta/my_dataset_name")

    // Select required fields and update column data
    // Here we will remove nulls from column by replacing them
    val updatedDF = dataFrame.select("Id", "Employee_Type")
        .na.fill("Unknown", Seq("Employee_Type"))

    deltaTable.as("dataset") // Name the table alias
        .merge(
            updatedDF.as("updates"), // Name the data frame alias
            "dataset.Id = updates.Id" // Define the unique key to match rows
        )
        .whenMatched
        .updateExpr( // Update row fields
            // Map table column to data frame column
            Map("Employee_Type" -> "updates.Employee_Type")
        )
        .execute()

Running this command you can will update the version of the Delta table. You can run this following command to see the MERGE action in the table history.

    display(
        deltaTable.history()
    )
