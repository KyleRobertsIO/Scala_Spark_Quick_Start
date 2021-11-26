# **Stream From Delta To JDBC**

## **Initialize JDBC Connection Properties**
In this sample code you can initialize the connection properties needed to be used to access SQL Server.


    import java.util.Properties

    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val jdbcHostname = "<sql-server-host-name>"
    val jdbcPort = 1433
    val jdbcDatabase = "<database-name>"

    val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

    val connectionProperties = new Properties()

    val jdbcUsername = "username-string"
    val jdbcPassword = "password-string"

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")
    connectionProperties.setProperty("Driver", driverClass)

<br>

## **Detect New Columns From Delta Table**
This code sample can be used to detect a new column being added in a Delta table. If there is a new column a JDBC connection will be made to ALTER the defined table state.

    import org.apache.spark.sql.types.StructField
    import java.sql.DriverManager
    import java.sql.Connection

    def AddNewJdbcColumnFromDelta(field: StructField) {
        val sqlStatement = s"ALTER TABLE <database>.<tableName> ADD ${field.name} NVARCHAR(max) NULL"
        
        val jdbcUrlDirect = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};user=${jdbcUsername};password=${jdbcPassword}"

        val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)

        val statement = connection.createStatement();
        val result = statement.execute(sqlStatement)
        connection.close()
    }

    // Collecition the fields that don't exist yet in JDBC connection
    for (field <- readStreamDF.schema) {
        if (!jdbcFields.contains(field)) {
            AddNewJdbcColumnFromDelta(field)
        }
    }