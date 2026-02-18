//Script for evaluating and comparing performance of the models

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

//Schema for adding results from model to a DataFrame
val MLschema = StructType(Array(
  StructField("classifier", StringType, true),
  StructField("input", StringType, true),
  StructField("label", StringType, true),
  StructField("correct", DoubleType, true),
  StructField("within_one", DoubleType, true),
  StructField("within_two", DoubleType, true),
  StructField("avgProbability", DoubleType, true)
))

//RANDOM FOREST MODEL

//Model calculating results for different measurements from data
val RFresults = Seq(
  Random_forest(MLdataWithTimeDF, Array("temperature", "humidity", "wind_speed", "power_tenants", "power_maintenance", "power_solar_panels", "electricity_price"), "month"), //All Features -> Month
  Random_forest(MLdataWithTimeDF, Array("temperature", "humidity", "wind_speed"), "month"), //Weather features -> Month
  Random_forest(MLdataWithTimeDF, Array("power_tenants", "power_maintenance", "power_solar_panels"), "month"), //Energy features -> Month
  Random_forest(MLdataWithTimeDF, Array("temperature", "humidity", "wind_speed", "power_tenants", "power_maintenance", "power_solar_panels", "electricity_price"), "hour"), //All Features -> Hour
  Random_forest(MLdataWithTimeDF, Array("power_tenants", "power_maintenance", "power_solar_panels"), "hour"), //Weather features -> Hour
  Random_forest(MLdataWithTimeDF, Array("temperature", "humidity", "wind_speed"), "hour") //Energy feature -> Hour
)

//Moving results from RF model to rows to be added to dataframe
val rows = RFresults.map { case (classifier, input, label, correct, within_one, within_two, avgProbability) =>
  Row(classifier, input, label, correct, within_one, within_two, avgProbability)
}

//Models results to dataframe
val MLresultsDF = spark.createDataFrame(spark.sparkContext.parallelize(rows), MLschema)

//Query for getting results
val MLFinalDF = MLresultsDF.select(
  col("classifier"),
  col("input"),
  col("label"),
  round(col("correct"), 2).alias("correct"),
  round(col("within_one"), 2).alias("within_one"),
  round(col("within_two"), 2).alias("within_two"),
  round(col("avgProbability"), 4).alias("avg_prob")
)

display(MLFinalDF)

//NAIVE BAYES MODEL

//Model calculating results for different measurements from data
val NBresults = Seq(
  Naive_Bayes(MLdataWithTimeDF, Array("temperature", "humidity", "wind_speed", "power_tenants", "power_maintenance", "power_solar_panels", "electricity_price"), "month"), //All Features -> Month
  Naive_Bayes(MLdataWithTimeDF, Array("temperature", "humidity", "wind_speed"), "month"), //Weather features -> Month
  Naive_Bayes(MLdataWithTimeDF, Array("power_tenants", "power_maintenance", "power_solar_panels"), "month"), //Energy features -> Month
  Naive_Bayes(MLdataWithTimeDF, Array("temperature", "humidity", "wind_speed", "power_tenants", "power_maintenance", "power_solar_panels", "electricity_price"), "hour"), //All Features -> Hour
  Naive_Bayes(MLdataWithTimeDF, Array("power_tenants", "power_maintenance", "power_solar_panels"), "hour"), //Weather features -> Hour
  Naive_Bayes(MLdataWithTimeDF, Array("temperature", "humidity", "wind_speed"), "hour") //Energy feature -> Hour
)

//Naive Bayes Models results to dataframe
val MLresultsNB = spark.createDataFrame(spark.sparkContext.parallelize(rows), MLschema)

//Query for getting results
val MLFinalNB = MLresultsNB.select(
  col("classifier"),
  col("input"),
  col("label"),
  round(col("correct"), 2).alias("correct"),
  round(col("within_one"), 2).alias("within_one"),
  round(col("within_two"), 2).alias("within_two"),
  round(col("avgProbability"), 4).alias("avg_prob")
)

display(MLFinalNB)


