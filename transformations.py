# Transformation 1: Selecting specific columns (name, age)
# Transformation 2: Filtering rows based on a condition Age >30
# Transformation 3: Adding a new column (Salary+10000)
# Transformation 4: Grouping (group by) and aggregating data, based on age calculate average salary
# Transformation 5: Sorting by a column (order by age)
# Transformation 6: Adding a new column with a conditional expression( if age >30 "Yes" else "No")
# Transformation 7: Dropping a column (drop salary)
# Transformation 8: Renaming columns (name to full name)
# Transformation 9: Union of two DataFrames

import os
# Set the PYSPARK_PYTHON environment variable
python_executable = r"C:\Users\Ingrid Rodriguez\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_PYTHON"] = python_executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("transformations").getOrCreate()

# Read the CSV file into a DataFrame
csv_path = "C:/Users/Ingrid Rodriguez/Downloads/sample_data.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Transformation 1: Selecting specific columns (name, age)
selected_columns = df.select("Name", "Age")
selected_columns.show()

# Transformation 2: Filtering rows based on a condition Age > 30
filtered_df = df.filter(df["Age"] > 30)
filtered_df.show()

# Transformation 3: Adding a new column (Salary+10000)
df_with_new_column = df.withColumn("SalaryPlus10K", col("Salary") + 10000)
df_with_new_column.show()

# Transformation 4: Grouping and aggregating data, based on age calculate average salary
average_salary_by_age = df.groupBy("Age").agg(avg("Salary").alias("AverageSalary"))
average_salary_by_age.show()

# Transformation 5: Sorting by a column (order by age)
sorted_df = df.orderBy("Age")
sorted_df.show()

# Transformation 6: Adding a new column with a conditional expression (if age > 30 "Yes" else "No")
df_with_conditional_column = df.withColumn("IsOver30", when(df["Age"] > 30, "Yes").otherwise("No"))
df_with_conditional_column.show()

# Transformation 7: Dropping a column (drop salary)
df_without_salary = df.drop("Salary")
df_without_salary.show()

# Transformation 8: Renaming columns (name to full name)
df_with_renamed_column = df.withColumnRenamed("Name", "FullName")
df_with_renamed_column.show()

# Transformation 9: Union of two DataFrames using the provided DataFrame df2
data = [Row(Name="AMike", Age=40, Salary=80000)]
df2 = spark.createDataFrame(data)

unioned_df = df.union(df2)
unioned_df.orderBy("Name").show()

# Stop the SparkSession
spark.stop()
