// COMMAND ----------

// MAGIC %md
// MAGIC #### Data description
// MAGIC
// MAGIC The dataset contains time series data from a period of 13 months (from the beginning of May 2023 to the end of May 2024). Each row contains the average of the measured values for a single minute. The following columns are included in the data:
// MAGIC
// MAGIC | column name        | column type   | description |
// MAGIC | ------------------ | ------------- | ----------- |
// MAGIC | time               | long          | The UNIX timestamp in second precision |
// MAGIC | temperature        | double        | The temperature measured by the weather station on top of Sähkötalo (`°C`) |
// MAGIC | humidity           | double        | The humidity measured by the weather station on top of Sähkötalo (`%`) |
// MAGIC | wind_speed         | double        | The wind speed measured by the weather station on top of Sähkötalo (`m/s`) |
// MAGIC | power_tenants      | double        | The total combined electricity power used by the tenants on Kampusareena (`W`) |
// MAGIC | power_maintenance  | double        | The total combined electricity power used by the building maintenance systems on Kampusareena (`W`) |
// MAGIC | power_solar_panels | double        | The total electricity power produced by the solar panels on Kampusareena (`W`) |
// MAGIC | electricity_price  | double        | The market price for electricity in Finland (`€/MWh`) |
// MAGIC

// COMMAND ----------

//Data preparation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// Path to parquet dataset stored in Azure Data Lake !!! Link doesn't work anymore !!!
val MlDataPath = "abfss://shared@tunics320f2024gen2.dfs.core.windows.net/assignment/energy/procem_13m.parquet/procem.parquet"

// Load parquet dataset
val MLRawDataDF: DataFrame =  spark.read.parquet(MlDataPath)

//Drop missing vals
val MLCleanedDataDF = MLRawDataDF.na.drop()

//Engineered features from UNIX timestamps
val MLdataWithTimeDF = MLCleanedDataDF.withColumn("month", month(from_unixtime(col("time"))))
  .withColumn("hour", hour(from_unixtime(col("time"))))

MLdataWithTimeDF

// COMMAND ----------
