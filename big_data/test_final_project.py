#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Final Project: Data Analysis using Spark
"""

import subprocess
import sys

def install_packages():
    """Install required packages"""
    packages = ['wget', 'pyspark', 'findspark']
    for package in packages:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"Successfully installed {package}")
        except subprocess.CalledProcessError:
            print(f"Warning: Could not install {package}")

def main():
    """Main function to run the PySpark data analysis project"""
    
    # Install packages
    print("Installing required packages...")
    install_packages()
    
    try:
        import findspark
        findspark.init()
        print("Findspark initialized successfully")
    except ImportError:
        print("Warning: Could not import findspark")
    
    try:
        from pyspark import SparkContext, SparkConf
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, avg, max, sum, count
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
        print("PySpark imported successfully")
    except ImportError as e:
        print(f"Error importing PySpark: {e}")
        return
    
    # Creating Spark Session
    try:
        sc = SparkContext.getOrCreate()
        spark = SparkSession \
            .builder \
            .appName("Python Spark DataFrames basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
        print("SparkSession created successfully")
    except Exception as e:
        print(f"Error creating Spark context/session: {e}")
        return
    
    # Download CSV data
    print("\n=== Downloading CSV data ===")
    try:
        import wget
        wget.download("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/data/employees.csv")
        print("employees.csv downloaded successfully")
    except Exception as e:
        print(f"Error downloading data: {e}")
        return
    
    # Task 1: Generate DataFrame from CSV data
    print("\n=== Task 1: Generate DataFrame ===")
    try:
        employees_df = spark.read.csv("employees.csv", header=True, inferSchema=True)
        print("Successfully loaded employees.csv into DataFrame")
        print(f"Number of rows: {employees_df.count()}")
        print(f"Number of columns: {len(employees_df.columns)}")
    except Exception as e:
        print(f"Error in Task 1: {e}")
        return
    
    # Task 2: Define schema
    print("\n=== Task 2: Define schema ===")
    try:
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
    except Exception as e:
        print(f"Error in Task 2: {e}")
    
    # Task 3: Display schema
    print("\n=== Task 3: Display schema ===")
    try:
        employees_df.printSchema()
        employees_df.show(10)
    except Exception as e:
        print(f"Error in Task 3: {e}")
    
    # Task 4: Create temporary view
    print("\n=== Task 4: Create temporary view ===")
    try:
        employees_df.createOrReplaceTempView("employees")
        print("Created temporary view 'employees'")
        
        # Verify the view
        result = spark.sql("SELECT COUNT(*) as total_employees FROM employees")
        result.show()
    except Exception as e:
        print(f"Error in Task 4: {e}")
    
    # Task 5: Execute SQL query
    print("\n=== Task 5: Execute SQL query ===")
    try:
        result = spark.sql("SELECT * FROM employees WHERE Age > 30")
        print("Employees with age > 30:")
        result.show()
        print(f"Total employees with age > 30: {result.count()}")
    except Exception as e:
        print(f"Error in Task 5: {e}")
    
    # Task 6: Calculate Average Salary by Department
    print("\n=== Task 6: Average Salary by Department ===")
    try:
        avg_salary = spark.sql("SELECT Department, AVG(Salary) as Average_Salary FROM employees GROUP BY Department ORDER BY Average_Salary DESC")
        avg_salary.show()
    except Exception as e:
        print(f"Error in Task 6: {e}")
    
    # Task 7: Filter IT Department Employees
    print("\n=== Task 7: Filter IT Department ===")
    try:
        it_employees = employees_df.filter(employees_df.Department == 'IT')
        print("IT Department employees:")
        it_employees.show()
        print(f"Total IT employees: {it_employees.count()}")
    except Exception as e:
        print(f"Error in Task 7: {e}")
    
    # Task 8: Add 10% Bonus to Salaries
    print("\n=== Task 8: Add 10% Bonus ===")
    try:
        employees_with_bonus = employees_df.withColumn("SalaryAfterBonus", col("Salary") * 1.10)
        print("Employees with salary after 10% bonus:")
        employees_with_bonus.select("Emp_Name", "Salary", "SalaryAfterBonus").show()
        employees_df = employees_with_bonus  # Update the main dataframe
    except Exception as e:
        print(f"Error in Task 8: {e}")
    
    # Task 9: Find Maximum Salary by Age
    print("\n=== Task 9: Maximum Salary by Age ===")
    try:
        max_salary_by_age = employees_df.groupBy("Age").agg(max("Salary").alias("Max_Salary")).orderBy("Age")
        max_salary_by_age.show()
    except Exception as e:
        print(f"Error in Task 9: {e}")
    
    # Task 10: Self-Join
    print("\n=== Task 10: Self-Join ===")
    try:
        employees_alias1 = employees_df.alias("emp1")
        employees_alias2 = employees_df.alias("emp2")
        
        self_joined = employees_alias1.join(employees_alias2, 
                                           employees_alias1.Emp_No == employees_alias2.Emp_No, 
                                           "inner")
        print("Self-joined DataFrame on Emp_No:")
        self_joined.select("emp1.Emp_No", "emp1.Emp_Name", "emp1.Salary", 
                          "emp2.Emp_No", "emp2.Emp_Name", "emp2.Salary").show(10)
        print(f"Total records after self-join: {self_joined.count()}")
    except Exception as e:
        print(f"Error in Task 10: {e}")
    
    # Task 11: Calculate Average Employee Age
    print("\n=== Task 11: Average Employee Age ===")
    try:
        avg_age = employees_df.agg(avg("Age").alias("Average_Age"))
        print("Average age of employees:")
        avg_age.show()
        
        avg_age_value = avg_age.collect()[0]["Average_Age"]
        print(f"The average age of employees is: {avg_age_value:.2f} years")
    except Exception as e:
        print(f"Error in Task 11: {e}")
    
    # Task 12: Calculate Total Salary by Department
    print("\n=== Task 12: Total Salary by Department ===")
    try:
        total_salary_by_dept = employees_df.groupBy("Department").agg(sum("Salary").alias("Total_Salary")).orderBy("Total_Salary", ascending=False)
        print("Total salary by department:")
        total_salary_by_dept.show()
    except Exception as e:
        print(f"Error in Task 12: {e}")
    
    # Task 13: Sort Data by Age and Salary
    print("\n=== Task 13: Sort by Age and Salary ===")
    try:
        sorted_employees = employees_df.orderBy("Age", col("Salary").desc())
        print("Employees sorted by age (ascending) and salary (descending):")
        sorted_employees.select("Emp_No", "Emp_Name", "Age", "Salary", "Department").show()
    except Exception as e:
        print(f"Error in Task 13: {e}")
    
    # Task 14: Count Employees in Each Department
    print("\n=== Task 14: Count Employees by Department ===")
    try:
        employee_count_by_dept = employees_df.groupBy("Department").agg(count("Emp_No").alias("Employee_Count")).orderBy("Employee_Count", ascending=False)
        print("Number of employees in each department:")
        employee_count_by_dept.show()
    except Exception as e:
        print(f"Error in Task 14: {e}")
    
    # Task 15: Filter Employees with 'o' in Name
    print("\n=== Task 15: Filter Employees with 'o' in Name ===")
    try:
        employees_with_o = employees_df.filter(col("Emp_Name").contains("o"))
        print("Employees with letter 'o' in their name:")
        employees_with_o.select("Emp_No", "Emp_Name", "Salary", "Age", "Department").show()
        print(f"Total employees with 'o' in their name: {employees_with_o.count()}")
    except Exception as e:
        print(f"Error in Task 15: {e}")
    
    print("\n=== All tasks completed! ===")
    
    # Stop Spark session
    try:
        spark.stop()
        print("Spark session stopped successfully")
    except Exception as e:
        print(f"Error stopping Spark session: {e}")

if __name__ == "__main__":
    main()
