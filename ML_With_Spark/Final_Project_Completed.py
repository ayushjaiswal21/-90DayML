# coding: utf-8

# # Airfoil Self-Noise Prediction using PySpark - Complete Version
# 
# This project uses the NASA Airfoil Self-Noise dataset to predict the sound pressure level based on several input features using Apache Spark.

# Install required packages ohk
import subprocess
import sys

def install_packages():
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark==3.1.2", "-q"])
    subprocess.check_call([sys.executable, "-m", "pip", "install", "findspark", "-q"])

# Uncomment the line below if packages are not installed
# install_packages()

import warnings
warnings.filterwarnings('ignore')

import findspark

# Try to find Spark automatically, if not found, use PySpark's built-in Spark
try:
    findspark.init()
    print("Spark found and initialized successfully")
except ValueError as e:
    print(f"Spark not found locally: {e}")
    print("Using PySpark's built-in Spark session...")
    # Continue without findspark.init() - PySpark will handle it

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import os

# Create SparkSession
spark = SparkSession.builder \
    .appName("AirfoilNoisePrediction") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

# Create tmp directory if it doesn't exist
os.makedirs("/tmp", exist_ok=True)

print("=" * 60)
print("AIRFOIL SELF-NOISE PREDICTION PROJECT")
print("=" * 60)

# Define schema manually
schema = StructType([
    StructField("Frequency", DoubleType(), True),
    StructField("AngleOfAttack", DoubleType(), True),
    StructField("ChordLength", DoubleType(), True),
    StructField("FreeStreamVelocity", DoubleType(), True),
    StructField("SuctionSideDisplacement", DoubleType(), True),
    StructField("SoundLevel", DoubleType(), True)
])

# Load the data
file_path = r"C:\Users\pc\Desktop\90DayML\ML_With_Spark\NASA_airfoil_noise_raw.csv"
df = spark.read.csv(file_path, schema=schema, header=False)

print("\n1. INITIAL DATA ANALYSIS")
print("-" * 30)

# Question 1: How many rows are present in the original dataset?
original_row_count = df.count()
print(f"Original dataset rows: {original_row_count}")

# Show sample data
print("\nSample data:")
df.show(5)
df.printSchema()

print("\n2. DATA CLEANING")
print("-" * 30)

# Question 2: Remove duplicates
df_no_duplicates = df.dropDuplicates()
no_duplicates_count = df_no_duplicates.count()
print(f"Rows after removing duplicates: {no_duplicates_count}")

# Question 3: Remove null values
df_clean = df_no_duplicates.na.drop()
clean_count = df_clean.count()
print(f"Rows after removing nulls: {clean_count}")

# Question 4: Column renaming (SoundLevel is already the target name)
# If we needed to rename from Sound_pressure_level to SoundLevel:
# df_clean = df_clean.withColumnRenamed("Sound_pressure_level", "SoundLevel")
print(f"Target column name: SoundLevel")

# Question 5: Save cleaned data as parquet
parquet_filename = "cleaned_airfoil_data.parquet"
df_clean.write.mode("overwrite").parquet(parquet_filename)
print(f"Parquet file created: {parquet_filename}")

# Question 6: Load parquet data and count rows
df_parquet = spark.read.parquet(parquet_filename)
parquet_row_count = df_parquet.count()
print(f"Rows in cleaned parquet dataset: {parquet_row_count}")

print("\n3. FEATURE ENGINEERING")
print("-" * 30)

# Select features and prepare for ML pipeline
feature_cols = [
    "Frequency", "AngleOfAttack", "ChordLength",
    "FreeStreamVelocity", "SuctionSideDisplacement"
]

# Question 7, 8, 9: Create ML Pipeline stages
# Stage 1: VectorAssembler
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
print("Pipeline Stage 1: VectorAssembler")

# Stage 2: LinearRegression
lr = LinearRegression(featuresCol="features", labelCol="SoundLevel")
print("Pipeline Stage 2: LinearRegression")

# Create the pipeline
pipeline = Pipeline(stages=[assembler, lr])
print("Pipeline Stage 3: Pipeline Model")

print("\n4. MODEL TRAINING")
print("-" * 30)

# Split data
train_data, test_data = df_parquet.randomSplit([0.8, 0.2], seed=42)
print(f"Training data rows: {train_data.count()}")
print(f"Test data rows: {test_data.count()}")

# Train the pipeline
pipeline_model = pipeline.fit(train_data)
print("Pipeline model trained successfully")

# Question 15: Number of stages in pipeline
num_stages = len(pipeline_model.stages)
print(f"Number of stages in pipeline model: {num_stages}")

# Get the trained linear regression model
lr_model = pipeline_model.stages[1]

# Question 10: Label column
print(f"Label column in LinearRegression: {lr.getLabelCol()}")

print("\n5. MODEL EVALUATION")
print("-" * 30)

# Make predictions
predictions = pipeline_model.transform(test_data)

# Question 11: Mean Squared Error
mse_evaluator = RegressionEvaluator(
    labelCol="SoundLevel",
    predictionCol="prediction",
    metricName="mse"
)
mse = mse_evaluator.evaluate(predictions)
print(f"Mean Squared Error: {mse}")

# Question 12: Mean Absolute Error
mae_evaluator = RegressionEvaluator(
    labelCol="SoundLevel",
    predictionCol="prediction",
    metricName="mae"
)
mae = mae_evaluator.evaluate(predictions)
print(f"Mean Absolute Error: {mae}")

# Question 13: R Squared
r2_evaluator = RegressionEvaluator(
    labelCol="SoundLevel",
    predictionCol="prediction",
    metricName="r2"
)
r2 = r2_evaluator.evaluate(predictions)
print(f"R Squared: {r2}")

# Also calculate RMSE for reference
rmse_evaluator = RegressionEvaluator(
    labelCol="SoundLevel",
    predictionCol="prediction",
    metricName="rmse"
)
rmse = rmse_evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse}")

print("\n6. MODEL COEFFICIENTS")
print("-" * 30)

# Question 14: Intercept
intercept = lr_model.intercept
print(f"Model Intercept: {intercept}")

# Questions 16-20: Individual coefficients
coefficients = lr_model.coefficients
print(f"Model Coefficients: {coefficients}")

# Map coefficients to feature names
coeff_dict = dict(zip(feature_cols, coefficients))

print(f"Frequency coefficient: {coeff_dict['Frequency']}")
print(f"AngleOfAttack coefficient: {coeff_dict['AngleOfAttack']}")
print(f"ChordLength coefficient: {coeff_dict['ChordLength']}")
print(f"FreeStreamVelocity coefficient: {coeff_dict['FreeStreamVelocity']}")
print(f"SuctionSideDisplacement coefficient: {coeff_dict['SuctionSideDisplacement']}")

print("\n7. SAMPLE PREDICTIONS")
print("-" * 30)
predictions.select("SoundLevel", "prediction", "features").show(10)

print("\n8. SUMMARY OF ANSWERS")
print("-" * 30)
print(f"Q1 - Original dataset rows: {original_row_count}")
print(f"Q2 - Rows after removing duplicates: {no_duplicates_count}")
print(f"Q3 - Rows after removing nulls: {clean_count}")
print(f"Q4 - New column name: SoundLevel")
print(f"Q5 - Parquet filename: {parquet_filename}")
print(f"Q6 - Parquet dataset rows: {parquet_row_count}")
print(f"Q7 - First pipeline stage: VectorAssembler")
print(f"Q8 - Second pipeline stage: LinearRegression")
print(f"Q9 - Third pipeline stage: Pipeline Model")
print(f"Q10 - Label column: SoundLevel")
print(f"Q11 - Mean Squared Error: {mse}")
print(f"Q12 - Mean Absolute Error: {mae}")
print(f"Q13 - R Squared: {r2}")
print(f"Q14 - Intercept: {intercept}")
print(f"Q15 - Number of pipeline stages: {num_stages}")
print(f"Q16 - Frequency coefficient: {coeff_dict['Frequency']}")
print(f"Q17 - AngleOfAttack coefficient: {coeff_dict['AngleOfAttack']}")
print(f"Q18 - ChordLength coefficient: {coeff_dict['ChordLength']}")
print(f"Q19 - FreeStreamVelocity coefficient: {coeff_dict['FreeStreamVelocity']}")
print(f"Q20 - SuctionSideDisplacement coefficient: {coeff_dict['SuctionSideDisplacement']}")

# Stop SparkSession
spark.stop()
print("\nProject completed successfully!")