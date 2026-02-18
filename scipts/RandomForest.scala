// RF MODEL CREATION 

//Model 
def Random_forest(data: DataFrame, features: Array[String], labels: String): (String, String, String, Double, Double, Double, Double) = {

  //Split data into training (80%) and test (20%) sets for evaluation
  val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2), seed = 1)

  //Combine feature columns into a single feature vector
  val featureAssembler = new VectorAssembler()
    .setInputCols(features)       // Specify which columns to combine
    .setOutputCol("features")     // Name of the new column containing assembled features

  //Encode the target label as numeric values
  val labelIndexer = new StringIndexer()
    .setInputCol(labels)          // Original label column
    .setOutputCol("indexedLabel") // Column used by Spark ML models
    .fit(data)                    // Fit the indexer to the full dataset

  //Initialize Random Forest Classifier
  val randomForest = new RandomForestClassifier()
    .setFeaturesCol("features")       // Feature column
    .setLabelCol("indexedLabel")      // Label column
    .setNumTrees(8)                   // Number of trees in the forest

  //Combine feature assembler, label indexer, and Random Forest into a pipeline
  val pipeline = new Pipeline().setStages(Array(featureAssembler, labelIndexer, randomForest))

  //Train the model on training data
  println(s"Training a 'RandomForest' model to predict $labels based on inputs: ${features.mkString(" ")}")
  val model = pipeline.fit(trainingData) //Model creation through pipeline

  //Make predictions on the test dataset
  val predictions = model.transform(testData)

  //Evaluate accuracy using built-in Spark evaluator
  val sparkEvaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("indexedLabel")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")
  
  val accuracy = sparkEvaluator.evaluate(predictions)
  println(s"Accuracy: $accuracy")

  //Custom accuracy calculation 
  val correctPredictions = predictions.filter(col("indexedLabel") === col("prediction")).count()
  val totalPredictions = predictions.count()
  val customAccuracy = correctPredictions.toDouble / totalPredictions
  println(s"Custom Accuracy: $customAccuracy")

  //Handle cyclic nature of target variable (months 1–12 or hours 0–23)
  val cyclicRange = if (labels == "month") 12 else 24

  //Calculate percentage of predictions within 1 unit of the true value
  val oneAway = predictions.filter(row => {
    val label = row.getAs[Double]("indexedLabel")
    val pred = row.getAs[Double]("prediction")
    Math.abs(label - pred) <= 1 || Math.abs(label - pred) == cyclicRange - 1
  }).count().toDouble / totalPredictions

  //Calculate percentage of predictions within 2 units of the true value
  val twoAway = predictions.filter(row => {
    val label = row.getAs[Double]("indexedLabel")
    val pred = row.getAs[Double]("prediction")
    Math.abs(label - pred) <= 2 || Math.abs(label - pred) >= cyclicRange - 2
  }).count().toDouble / totalPredictions
  
  println(s"Percentage within 1 unit: $oneAway")
  println(s"Percentage within 2 units: $twoAway")

  //Extract the probability assigned to the correct label
  // Spark outputs a probability vector for each class; we take the probability for the correct class
  val extractProbability = udf((probability: Vector, label: Double) => {
    val labelIndex = label.toInt
    probability(labelIndex)
  })

  //Add a column with the probability of the correct prediction
  val predictionsWithProb = predictions.withColumn(
    "correctProbability",
    extractProbability(col("probability"), col("indexedLabel"))
  )

  //Calculate average probability for correct predictions
  val avgProbability = predictionsWithProb.agg(avg("correctProbability")).first().getDouble(0)
  println(s"Average Probability for Correct Value: $avgProbability")

  //Return model type, input features, label, and evaluation metrics
  (
    "RandomForest",             // Model type
    features.mkString(" "),     // Input features used
    labels,                     // Label predicted
    customAccuracy * 100,       // Overall accuracy (%)
    oneAway * 100,              // % predictions within 1 unit
    twoAway * 100,              // % predictions within 2 units
    avgProbability              // Avg probability of correct predictions
  )
}

// COMMAND ----------
