# 🌦️ Prediction Modeling Using Weather and Energy Data
- Machine learning project exploring how weather conditions, power consumption, and electricity price can explain the time of day and month of the year.
- Project is extended from Tampere University courses voluntary work: COMP.CS.320 Data-Intensive Programming
- Aim of the project was to learn Machine learning methods for predictive purposes via Scala with Sparks Machine Learning libraries.
---

## 📌 Project Overview

This project investigates whether weather measurements, electricity consumption, and electricity price contain enough embedded temporal information to predict:

- 📅 **Month of the year (1–12)**
- 🕒 **Hour of the day (0–23)**

Two machine learning models were implemented and compared:

- 🌲 Random Forest Classifier  
- 📊 Naive Bayes Classifier  

The goal was not traditional forecasting, but to analyze how strongly different data domains encode seasonal and daily structure.

---

## 📂 Dataset Description

The dataset consists of **13 months of minute-level time series data** (May 2023 – May 2024).

Each row contains averaged measurements for one minute.

Time series data collected in the ProCem research (https://www.senecc.fi/projects/procem-2) project is used as the training and test data.

### Features

| Feature | Description |
|----------|-------------|
| `temperature` | Outdoor temperature (°C) |
| `humidity` | Relative humidity (%) |
| `wind_speed` | Wind speed (m/s) |
| `power_tenants` | Electricity usage by tenants (W) |
| `power_maintenance` | Building maintenance electricity usage (W) |
| `power_solar_panels` | Solar power production (W) |
| `electricity_price` | Finnish electricity market price (€/MWh) |
| `time` | UNIX timestamp in second precision |

### Engineered Features Generated from UNIX timestamps
- `month`
- `hour`
- `weekday`

---

## 🧠 Modeling Tasks

Six classification cases were tested:

### Predicting Month (12 classes)

1. Using weather features only  
2. Using power features only  
3. Using all features combined  

### Predicting Hour (24 classes)

4. Using weather features only  
5. Using power features only  
6. Using all features combined

### Predicting day of the week
7. Naive Bayes classifier was trained and evaluated against Random Forest model using weather featrues to predict day of the week.

---

## ⚙️ Methodology

The modeling workflow was structured as a pipeline to ensure consistent and reproducible processing for both Random Forest and Naive Bayes models.

### Data Preparation

The raw dataset was first cleaned by removing missing or invalid values. UNIX timestamps were converted into month, hour, and weekday features, providing temporal targets for prediction. Input features were vectorized using Spark ML’s VectorAssembler, and target labels were encoded with StringIndexer. The data was then split into 80% training and 20% testing sets, forming the foundation for model training and evaluation.

### Model Pipeline

Both Random Forest and Naive Bayes models were trained using a Spark ML Pipeline, which integrates all preprocessing and modeling steps into a single workflow. The pipeline included:

1. Feature assembly – combining selected input features into a single feature vector.

2. Label encoding – converting categorical target variables into numeric indices.

3. Model training – fitting either a Random Forest or Naive Bayes classifier on the training set.

Using a pipeline ensured that all transformations applied to the training data were consistently applied to the test data, enabling robust evaluation and direct comparison between models.

### Evaluation Metrics

Models were assessed using standard accuracy along with custom cyclic metrics to account for temporal wrap-around:

Within ±1 unit (e.g., hour 23 vs 0)

Within ±2 units

The average predicted probability for correct class was also computed to quantify model confidence. These metrics provide a comprehensive view of model performance across temporal prediction tasks.
---
## 📊 Key Results and Insights

- Random Forest outperformed Naive Bayes for predicting the month and hour of the day due to its ability to model nonlinear relationships and feature interactions.

- Naive Bayes performed slightly better for predicting the day of the week, likely because day-of-week patterns are more regular and less dependent on complex feature interactions.

---

## 🛠️ Technologies Used

- Apache Spark (MLlib)
- Scala
- Databricks
- RandomForestClassifier
- NaiveBayes
- MulticlassClassificationEvaluator
  
---

## 👤 Author: Olli Könönen

Master’s student in Data Science  
Interested in energy systems, forecasting, and applied machine learning.

## Usage of AI
- AI was used for parts of the project
- Usage: BugFixes, Scala and spark syntax, theory explanation
- Model: GPT-5 mini
