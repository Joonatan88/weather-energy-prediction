# 🌦️ Prediction Modeling Using Weather and Energy Data
- Machine learning project exploring how weather conditions, power consumption, and electricity price can explain the time of day and month of the year.
- Project is extended from Tampere University courses voluntary work: COMP.CS.320 Data-Intensive Programming
- Aim of the project was to learn Scala with Sparks Machine Learning libraries.
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

### 🔹 Task 1 – Random Forest Experiments

Six classification cases were tested:

### Predicting Month (12 classes)

1. Using weather features only  
2. Using power features only  
3. Using all features combined  

### Predicting Hour (24 classes)

4. Using weather features only  
5. Using power features only  
6. Using all features combined  

---

### 🔹 Task 2 – Model Comparison

Naive Bayes classifier was implemented and compared against Random Forest using weather features.

The objective was to evaluate:

- Linear probabilistic assumptions (Naive Bayes)
- Nonlinear ensemble learning (Random Forest)

---

## ⚙️ Methodology

### Data Preparation
- Removed missing values
- Extracted month and hour from UNIX timestamp
- Feature vectorization using Spark ML `VectorAssembler`
- Label encoding using `StringIndexer`
- 80/20 train-test split

### Evaluation Metrics
- Accuracy
- Custom cyclic tolerance metrics:
  - Within ±1 unit (e.g., hour 23 vs 0)
  - Within ±2 units
- Average predicted probability of correct class

Cyclic tolerance was included because temporal classes (month/hour) wrap around.

---

## 📊 Key Insights

- Power consumption features strongly encoded **hour-of-day patterns**.
- Weather variables carried stronger **seasonal (monthly) signals**.
- Combining weather, power, and price improved performance.
- Random Forest consistently outperformed Naive Bayes due to nonlinear feature interactions.

---

## 🛠️ Technologies Used

- Apache Spark (MLlib)
- Scala
- Databricks
- RandomForestClassifier
- NaiveBayes
- MulticlassClassificationEvaluator
  
---

## 👤 Olli Könönen

Master’s student in Data Science  
Interested in energy systems, forecasting, and applied machine learning.
