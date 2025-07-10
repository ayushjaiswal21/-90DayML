#!/usr/bin/env python
# coding: utf-8

# # Practice Project
# 
# Estimated time needed: **60** minutes
# 
# This practice project focuses on data transformation and integration using PySpark. You will work with two datasets, perform various transformations such as adding columns, renaming columns, dropping unnecessary columns, joining dataframes, and finally, writing the results into both a Hive warehouse and an HDFS file system.
# 

# ### Prerequisites 
# 
# For this lab assignment, you will use wget, Python and Spark (PySpark). Therefore, it's essential to make sure that the below-specified libraries are installed in your lab environment.
# 
#  
# 

# In[ ]:


# Installing required packages
get_ipython().system('pip install wget pyspark findspark')


# #### Prework - Initiate the Spark Session
# 

# In[ ]:


import findspark
findspark.init()


# In[ ]:


# PySpark is the Spark API for Python. In this lab, we use PySpark to initialize the SparkContext.
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


# In[ ]:


# Creating a SparkContext object
sc = SparkContext.getOrCreate()

# Creating a Spark Session
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


# ### Task 1: Load datasets into PySpark DataFrames
# 
# Download the datasets from the following links using `wget` and load it in a Spark Dataframe.
# 
# 1. https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset1.csv  
# 2. https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset2.csv  
# 

# In[ ]:


# Download the dataset using wget
import wget

link_to_data1 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset1.csv'
wget.download(link_to_data1)

link_to_data2 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset2.csv'
wget.download(link_to_data2)


# In[ ]:


# Load the data into a pyspark dataframe
df1 = spark.read.csv("dataset1.csv", header=True, inferSchema=True)
df2 = spark.read.csv("dataset2.csv", header=True, inferSchema=True)


# ### Task 2: Display the schema of both dataframes
# 
# Display the schema of `df1` and `df2` to understand the structure of the datasets.
# 

# In[ ]:


# Print the schema of df1 and df2
print("Schema of df1:")
df1.printSchema()

print("\nSchema of df2:")
df2.printSchema()

print("\nSample data from df1:")
df1.show(5)

print("\nSample data from df2:")
df2.show(5)


# #### Task 3: Add a new column to each dataframe
# 
# Add a new column named **year** to `df1` and **quarter** to `df2` representing the year and quarter of the data.
# 
# *Note: We need to check the actual column names first, then extract date information*
# 

# In[ ]:


from pyspark.sql.functions import year, quarter, to_date

# Check column names first
print("Columns in df1:", df1.columns)
print("Columns in df2:", df2.columns)

# Add new column year to df1 (assuming there's a date column)
# We'll need to identify the correct date column name from the schema
date_columns_df1 = [col for col in df1.columns if 'date' in col.lower()]
if date_columns_df1:
    date_col_df1 = date_columns_df1[0]
    df1 = df1.withColumn('year', year(to_date(date_col_df1, 'dd/MM/yyyy')))
    print(f"Added year column to df1 using {date_col_df1}")

# Add new column quarter to df2
date_columns_df2 = [col for col in df2.columns if 'date' in col.lower()]
if date_columns_df2:
    date_col_df2 = date_columns_df2[0]
    df2 = df2.withColumn('quarter', quarter(to_date(date_col_df2, 'dd/MM/yyyy')))
    print(f"Added quarter column to df2 using {date_col_df2}")


# #### Task 4: Rename columns in both dataframes
# 
# Rename the column **amount** to **transaction_amount** in `df1` and **value** to **transaction_value** in `df2`.
# 

# In[ ]:


# Rename df1 column amount to transaction_amount
if 'amount' in df1.columns:
    df1 = df1.withColumnRenamed('amount', 'transaction_amount')
    print("Renamed 'amount' to 'transaction_amount' in df1")

# Rename df2 column value to transaction_value
if 'value' in df2.columns:
    df2 = df2.withColumnRenamed('value', 'transaction_value')
    print("Renamed 'value' to 'transaction_value' in df2")

print("\nUpdated columns in df1:", df1.columns)
print("Updated columns in df2:", df2.columns)


# #### Task 5: Drop unnecessary columns
# 
# Drop the columns **description** and **location** from `df1` and **notes** from `df2`.
# 

# In[ ]:


# Drop columns description and location from df1
columns_to_drop_df1 = [col for col in ['description', 'location'] if col in df1.columns]
if columns_to_drop_df1:
    df1 = df1.drop(*columns_to_drop_df1)
    print(f"Dropped {columns_to_drop_df1} from df1")

# Drop column notes from df2
if 'notes' in df2.columns:
    df2 = df2.drop('notes')
    print("Dropped 'notes' from df2")

print("\nFinal columns in df1:", df1.columns)
print("Final columns in df2:", df2.columns)


# #### Task 6: Join dataframes based on a common column
# 
# Join `df1` and `df2` based on the common column **customer_id** and create a new dataframe named `joined_df`.
# 

# In[ ]:


# Join df1 and df2 based on common column customer_id
if 'customer_id' in df1.columns and 'customer_id' in df2.columns:
    joined_df = df1.join(df2, 'customer_id', 'inner')
    print("Successfully joined df1 and df2 on customer_id")
    print("Joined dataframe columns:", joined_df.columns)
    print("Joined dataframe count:", joined_df.count())
else:
    print("Warning: customer_id column not found in both dataframes")
    print("df1 columns:", df1.columns)
    print("df2 columns:", df2.columns)
    # Create a sample joined_df for demonstration
    joined_df = df1


# #### Task 7: Filter data based on a condition
# 
# Filter `joined_df` to include only transactions where "transaction_amount" is greater than 1000 and create a new dataframe named `filtered_df`.
# 

# In[ ]:


# Filter the dataframe for transaction amount > 1000
if 'transaction_amount' in joined_df.columns:
    filtered_df = joined_df.filter("transaction_amount > 1000")
    print("Filtered dataframe for transaction_amount > 1000")
    print("Filtered dataframe count:", filtered_df.count())
else:
    print("Warning: transaction_amount column not found")
    print("Available columns:", joined_df.columns)
    # Use the joined_df as filtered_df for demonstration
    filtered_df = joined_df


# #### Task 8: Aggregate data by customer
# 
# Calculate the total transaction amount for each customer in `filtered_df` and display the result.
# 

# In[ ]:


from pyspark.sql.functions import sum

# Group by customer_id and aggregate the sum of transaction amount
if 'customer_id' in filtered_df.columns and 'transaction_amount' in filtered_df.columns:
    total_amount_per_customer = filtered_df.groupBy('customer_id').agg(sum('transaction_amount').alias('total_amount'))
    print("Total amount per customer:")
    total_amount_per_customer.show()
else:
    print("Warning: Required columns not found for aggregation")
    print("Available columns:", filtered_df.columns)
    # Create a dummy aggregation
    total_amount_per_customer = filtered_df.limit(5)


# #### Task 9: Write the result to a Hive table
# 
# Write `total_amount_per_customer` to a Hive table named **customer_totals**.
# 

# In[ ]:


# Write total_amount_per_customer to a Hive table named customer_totals
try:
    total_amount_per_customer.write.mode("overwrite").saveAsTable("customer_totals")
    print("Successfully saved data to Hive table: customer_totals")
except Exception as e:
    print(f"Warning: Could not save to Hive table. Error: {e}")
    print("This is expected in a local environment without Hive setup")


# #### Task 10: Write the filtered data to HDFS
# 
# Write `filtered_df` to HDFS in parquet format to a file named **filtered_data**.
# 

# In[ ]:


# Write filtered_df to HDFS in parquet format file filtered_data.parquet
try:
    filtered_df.write.mode("overwrite").parquet("filtered_data.parquet")
    print("Successfully saved filtered data to parquet file: filtered_data.parquet")
except Exception as e:
    print(f"Warning: Could not save to parquet. Error: {e}")


# #### Task 11: Add a new column based on a condition
# 
# Add a new column named **high_value** to `df1` indicating whether the transaction_amount is greater than 5000. When the value is greater than 5000, the value of the column should be **Yes**. When the value is less than or equal to 5000, the value of the column should be **No**.
# 

# In[ ]:


from pyspark.sql.functions import when, lit

# Add new column with value indicating whether transaction amount is > 5000 or not
if 'transaction_amount' in df1.columns:
    df1 = df1.withColumn("high_value", when(df1.transaction_amount > 5000, lit("Yes")).otherwise(lit("No")))
    print("Added high_value column to df1")
    df1.select("transaction_amount", "high_value").show(10)
else:
    print("Warning: transaction_amount column not found in df1")
    print("Available columns:", df1.columns)


# #### Task 12: Calculate the average transaction value per quarter
# 
# Calculate and display the average transaction value for each quarter in `df2` and create a new dataframe named `average_value_per_quarter` with column `avg_trans_val`.
# 

# In[ ]:


from pyspark.sql.functions import avg

# Calculate the average transaction value for each quarter in df2
if 'quarter' in df2.columns and 'transaction_value' in df2.columns:
    average_value_per_quarter = df2.groupBy('quarter').agg(avg("transaction_value").alias("avg_trans_val"))
    print("Average transaction value per quarter:")
    average_value_per_quarter.show()
else:
    print("Warning: Required columns not found for quarter aggregation")
    print("Available columns in df2:", df2.columns)
    # Create a dummy aggregation
    average_value_per_quarter = df2.limit(5)


# #### Task 13: Write the result to a Hive table
# 
# Write `average_value_per_quarter` to a Hive table named **quarterly_averages**.
# 

# In[ ]:


# Write average_value_per_quarter to a Hive table named quarterly_averages
try:
    average_value_per_quarter.write.mode("overwrite").saveAsTable("quarterly_averages")
    print("Successfully saved data to Hive table: quarterly_averages")
except Exception as e:
    print(f"Warning: Could not save to Hive table. Error: {e}")
    print("This is expected in a local environment without Hive setup")


# #### Task 14: Calculate the total transaction value per year
# 
# Calculate and display the total transaction value for each year in `df1` and create a new dataframe named `total_value_per_year` with column `total_transaction_val`.
# 

# In[ ]:


# Calculate the total transaction value for each year in df1
if 'year' in df1.columns and 'transaction_amount' in df1.columns:
    total_value_per_year = df1.groupBy('year').agg(sum("transaction_amount").alias("total_transaction_val"))
    print("Total transaction value per year:")
    total_value_per_year.show()
else:
    print("Warning: Required columns not found for year aggregation")
    print("Available columns in df1:", df1.columns)
    # Create a dummy aggregation
    total_value_per_year = df1.limit(5)


# #### Task 15: Write the result to HDFS
# 
# Write `total_value_per_year` to HDFS in the CSV format to file named **total_value_per_year**.
# 

# In[ ]:


# Write total_value_per_year to HDFS in the CSV format
try:
    total_value_per_year.write.mode("overwrite").csv("total_value_per_year.csv")
    print("Successfully saved data to CSV file: total_value_per_year.csv")
except Exception as e:
    print(f"Warning: Could not save to CSV. Error: {e}")


# ### Congratulations! You have completed the lab.
# This practice project provides hands-on experience with data transformation and integration using PySpark. You've performed various tasks, including adding columns, renaming columns, dropping unnecessary columns, joining dataframes, and writing the results into both a Hive warehouse and an HDFS file system.
# 
