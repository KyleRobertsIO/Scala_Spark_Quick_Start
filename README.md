# Scala Spark Quick Start On Databricks

The purpose of this page to help new people easily get started using Apache Spark inside the of the Databricks environment. The aim is to have you the reader not have to spend hours trying to figure out how to get things up and running.

This document was written during October 2021 for:
* Databrick Runtime V8.4
* Scala V2.12
* Spark V3.1.2

---

### **Getting Free Databricks Account**
You want to head over to [databricks.com/try-databricks.com](https://databricks.com/try-databricks) and create an account (No credit card required upfront). Once created you want to select the `Community` edition of Databricks which is free for learning. Once you've select that you at anytime access the community edition from the link below.

**Login Portal For Community Databricks Accounts:**

[community.cloud.databricks.com](https://community.cloud.databricks.com/)

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
        .partitionBy("column_name")
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

### **Upsert Column Data In Delta Table**
In order to update the state of the Delta table data and append a new version for time traveling
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

### **Evolving Delta Schema**
Delta tables have the great functionality to merge schema changes automatically. You can can read more about this functionality [here](https://docs.delta.io/0.6.0/delta-update.html#automatic-schema-evolution).

    // Enable automatic schema evolution
    spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

    val deltaTable = DeltaTable.forPath(spark, "/mnt/delta/my_dataset_name")

    deltaTable.alias("target")
        .merge(
            modifiedDataFrame.alias("src"),
            "target.id_column = src.id_column"
        )
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()

---
## **Delta Merge Streaming Demo**

In this demonstration we build some quick data tables for a Bronze and Silver tier of storage. Then we will stream using Spark streaming data from Bronze into the Silver tier adding new columns of data to bring more context to data. 

1. [Initialize Bronze Table](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2050445406720207/2894996849343305/4305005950935427/latest.html)

2. [Initialize Silver Table](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2050445406720207/3495167117540935/4305005950935427/latest.html)

3. [Streaming From Bronze To Silver](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2050445406720207/2894996849343313/4305005950935427/latest.html)