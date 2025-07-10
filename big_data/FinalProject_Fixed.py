#!/usr/bin/env python
# coding: utf-8

# # Final Project: Data Analysis using Spark
# 
# This final project focuses on data analysis using Spark SQL. You will create a DataFrame by loading data from a CSV file and apply transformations and actions using Spark SQL.
# 
# **Tasks Overview:**
# 1. Generate DataFrame from CSV data
# 2. Define a schema for the data
# 3. Display schema of DataFrame
# 4. Create a temporary view
# 5. Execute an SQL query
# 6. Calculate Average Salary by Department
# 7. Filter and Display IT Department Employees
# 8. Add 10% Bonus to Salaries
# 9. Find Maximum Salary by Age
# 10. Self-Join on Employee Data
# 11. Calculate Average Employee Age
# 12. Calculate Total Salary by Department
# 13. Sort Data by Age and Salary
# 14. Count Employees in Each Department
# 15. Filter Employees with the letter 'o' in the Name
# 

# In[ ]:


# Installing required packages
get_ipython().system('pip install pyspark findspark wget')


# In[ ]:


import findspark
findspark.init()


# In[ ]:


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, sum, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


# In[ ]:


# Creating a SparkContext object
sc = SparkContext.getOrCreate()

# Creating a SparkSession
spark = SparkSession \
    .builder \
    .appName("Final Project - Employee Data Analysis") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("Spark session created successfully")


# In[ ]:


# Download the CSV data
import wget
print("Downloading employees.csv...")
wget.download("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/data/employees.csv")
print("\nEmployees data downloaded successfully")


# #### Task 1: Generate a Spark DataFrame from the CSV data
# 

# In[ ]:


# Read data from the employees CSV file and import it into a DataFrame variable named "employees_df"
employees_df = spark.read.csv("employees.csv", header=True, inferSchema=True)
print("Successfully loaded employees.csv into DataFrame")
print(f"Number of rows: {employees_df.count()}")
print(f"Number of columns: {len(employees_df.columns)}")


# #### Task 2: Define a schema for the data
# 

# In[ ]:


# Define a Schema for the input data and read the file using the user-defined Schema
print("Current DataFrame schema:")
employees_df.printSchema()
print("\nSample data:")
employees_df.show(5)

# Define a schema based on the expected structure
schema = StructType([
    StructField("Emp_No", IntegerType(), True),
    StructField("Emp_Name", StringType(), True),
    StructField("Salary", DoubleType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Department", StringType(), True)
])

# Read the CSV file with the defined schema
employees_df = spark.read.csv("employees.csv", header=True, schema=schema)
print("\nDataFrame loaded with defined schema:")
employees_df.printSchema()


# #### Task 3: Display schema of DataFrame
# 

# In[ ]:


# Display all columns of the DataFrame, along with their respective data types
print("Schema of employees_df DataFrame:")
employees_df.printSchema()

print("\nColumn names and data types:")
for field in employees_df.schema.fields:
    print(f"Column: {field.name}, Data Type: {field.dataType}, Nullable: {field.nullable}")

print("\nSample data:")
employees_df.show(10)


# #### Task 4: Create a temporary view
# 

# In[ ]:


# Create a temporary view named "employees" for the DataFrame
employees_df.createOrReplaceTempView("employees")
print("Successfully created temporary view 'employees'")

# Verify the view is created by running a simple query
result = spark.sql("SELECT COUNT(*) as total_employees FROM employees")
result.show()
print("Temporary view is working correctly!")


# #### Task 5: Execute an SQL query
# 

# In[ ]:


# SQL query to fetch solely the records from the View where the age exceeds 30
query = "SELECT * FROM employees WHERE Age > 30"
result_df = spark.sql(query)

print("Employees with age > 30:")
result_df.show()

print(f"\nTotal number of employees with age > 30: {result_df.count()}")


# #### Task 6: Calculate Average Salary by Department
# 

# In[ ]:


# SQL query to calculate the average salary of employees grouped by department
query = "SELECT Department, AVG(Salary) as Average_Salary FROM employees GROUP BY Department ORDER BY Average_Salary DESC"
avg_salary_by_dept = spark.sql(query)

print("Average salary by department:")
avg_salary_by_dept.show()


# #### Task 7: Filter and Display IT Department Employees
# 

# In[ ]:


# Apply a filter to select records where the department is 'IT'
it_employees = employees_df.filter(employees_df.Department == 'IT')

print("IT Department employees:")
it_employees.show()

print(f"\nTotal number of IT employees: {it_employees.count()}")


# #### Task 8: Add 10% Bonus to Salaries
# 

# In[ ]:


# Add a new column "SalaryAfterBonus" with 10% bonus added to the original salary
employees_with_bonus = employees_df.withColumn("SalaryAfterBonus", col("Salary") * 1.10)

print("Employees with salary after 10% bonus:")
employees_with_bonus.select("Emp_No", "Emp_Name", "Salary", "SalaryAfterBonus").show()

# Update the main dataframe
employees_df = employees_with_bonus
print("\nDataFrame updated with bonus column")


# #### Task 9: Find Maximum Salary by Age
# 

# In[ ]:


# Group data by age and calculate the maximum salary for each age group
max_salary_by_age = employees_df.groupBy("Age").agg(max("Salary").alias("Max_Salary")).orderBy("Age")

print("Maximum salary by age:")
max_salary_by_age.show()


# #### Task 10: Self-Join on Employee Data
# 

# In[ ]:


# Join the DataFrame with itself based on the "Emp_No" column
# Create aliases for the dataframes to distinguish columns
employees_alias1 = employees_df.alias("emp1")
employees_alias2 = employees_df.alias("emp2")

# Self-join on Emp_No (this will create a cartesian product of matching Emp_No)
self_joined = employees_alias1.join(employees_alias2, 
                                   employees_alias1.Emp_No == employees_alias2.Emp_No, 
                                   "inner")

print("Self-joined DataFrame on Emp_No:")
# Select specific columns to make the output more readable
self_joined.select("emp1.Emp_No", "emp1.Emp_Name", "emp1.Salary", 
                  "emp2.Emp_No", "emp2.Emp_Name", "emp2.Salary").show(10)

print(f"\nTotal records after self-join: {self_joined.count()}")


# #### Task 11: Calculate Average Employee Age
# 

# In[ ]:


# Calculate the average age of employees
avg_age = employees_df.agg(avg("Age").alias("Average_Age"))

print("Average age of employees:")
avg_age.show()

# Alternative: collect the value and print it
avg_age_value = avg_age.collect()[0]["Average_Age"]
print(f"\nThe average age of employees is: {avg_age_value:.2f} years")


# #### Task 12: Calculate Total Salary by Department
# 

# In[ ]:


# Calculate the total salary for each department using GroupBy and Aggregate functions
total_salary_by_dept = employees_df.groupBy("Department").agg(sum("Salary").alias("Total_Salary")).orderBy("Total_Salary", ascending=False)

print("Total salary by department:")
total_salary_by_dept.show()


# #### Task 13: Sort Data by Age and Salary
# 

# In[ ]:


# Sort the DataFrame by age in ascending order and then by salary in descending order
sorted_employees = employees_df.orderBy("Age", col("Salary").desc())

print("Employees sorted by age (ascending) and salary (descending):")
sorted_employees.select("Emp_No", "Emp_Name", "Age", "Salary", "Department").show()


# #### Task 14: Count Employees in Each Department
# 

# In[ ]:


# Calculate the number of employees in each department
employee_count_by_dept = employees_df.groupBy("Department").agg(count("Emp_No").alias("Employee_Count")).orderBy("Employee_Count", ascending=False)

print("Number of employees in each department:")
employee_count_by_dept.show()


# #### Task 15: Filter Employees with the letter 'o' in the Name
# 

# In[ ]:


# Apply a filter to select records where the employee's name contains the letter 'o'
employees_with_o = employees_df.filter(col("Emp_Name").contains("o"))

print("Employees with letter 'o' in their name:")
employees_with_o.select("Emp_No", "Emp_Name", "Salary", "Age", "Department").show()

print(f"\nTotal number of employees with 'o' in their name: {employees_with_o.count()}")


# ## Summary
# 
# Congratulations! You have completed the final project.
# 
# You have successfully:
# - ✅ Generated a DataFrame from CSV data
# - ✅ Defined and applied a schema
# - ✅ Created temporary views for SQL operations
# - ✅ Executed various SQL queries and DataFrame operations
# - ✅ Performed data transformations and aggregations
# - ✅ Applied filters and sorting operations
# - ✅ Calculated statistics and insights from the data
# 
# You now have hands-on experience with Spark SQL and DataFrame operations for data analysis!
# 

# In[ ]:


# Stop the Spark session
spark.stop()
print("Spark session stopped successfully")

