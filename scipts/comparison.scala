// COMMAND ----------

// MAGIC %md
// MAGIC We compare Naive-Bayes models performance to Random Forest classifiers performance on predicting the day of week from: temperature, humidity and windspeed.

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, round}

//Adding weekday column to our dataset
val MLdataWithDayDF = MLCleanedDataDF.withColumn("WeekDay", date_format(from_unixtime(col("time")), "EEEE"))

//Fixes a bug
val filteredMLdataWithDayDF = MLdataWithDayDF.filter(
  col("temperature") >= 0 && 
  col("humidity") >= 0 && 
  col("wind_speed") >= 0 &&
  !col("temperature").isNaN && 
  !col("humidity").isNaN && 
  !col("wind_speed").isNaN &&
  !col("temperature").isNull && 
  !col("humidity").isNull && 
  !col("wind_speed").isNull
)

//Schema initalization
val MLschema = StructType(Array(
  StructField("classifier", StringType, true),
  StructField("input", StringType, true),
  StructField("label", StringType, true),
  StructField("correct", DoubleType, true),
  StructField("within_one", DoubleType, true),
  StructField("within_two", DoubleType, true),
  StructField("avgProbability", DoubleType, true)
))

//Naive-Bayes results
val NBresults = Seq(
  Naive_Bayes(filteredMLdataWithDayDF,  Array("temperature", "humidity", "wind_speed"), "WeekDay")
)

//Naive-Bayes results to rows
val NBrows = NBresults.map { case (classifier, input, label, correct, within_one, within_two, avgProbability) =>
  Row(classifier, input, label, correct, within_one, within_two, avgProbability)
}

//Naive-Bayes results to dataframe
val NBresultsDF = spark.createDataFrame(spark.sparkContext.parallelize(NBrows), MLschema)

//Naive-Bayes query used in comparison
val NBFinalDF = NBresultsDF.select(
  col("classifier"),
  col("input"),
  col("label"),
  round(col("correct"), 2).alias("correct"),
  round(col("within_one"), 2).alias("within_one"),
  round(col("within_two"), 2).alias("within_two"),
  round(col("avgProbability"), 4).alias("avg_prob")
)

//Results for Random Forest model
val RFresults = Seq(
  Random_forest(filteredMLdataWithDayDF,  Array("temperature", "humidity", "wind_speed"), "WeekDay")
)

//RF results to rows
val RFrows = RFresults.map { case (classifier, input, label, correct, within_one, within_two, avgProbability) =>
  Row(classifier, input, label, correct, within_one, within_two, avgProbability)
}

//Rows to dataframe
val RFresultsDF = spark.createDataFrame(spark.sparkContext.parallelize(RFrows), MLschema)

//Query from dataframe for end result
val RFfinalDF = RFresultsDF.select(
  col("classifier"),
  col("input"),
  col("label"),
  round(col("correct"), 2).alias("correct"),
  round(col("within_one"), 2).alias("within_one"),
  round(col("within_two"), 2).alias("within_two"),
  round(col("avgProbability"), 4).alias("avg_prob")
)

//Comparing RandomForest and NaiveBayes
val LastOneDF = NBFinalDF.union(RFfinalDF)

display(LastOneDF)
