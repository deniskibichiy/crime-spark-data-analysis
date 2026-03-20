
import os
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schema import crime_schema 
import pyspark.sql.functions as F

def main():
    spark = SparkSession.builder \
        .appName("ChicagoCrimeAnalysis") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark Session Started...")

    file_path = "data/chicago_crimes.csv"
    
    df = spark.read.csv(file_path, header=True, schema=crime_schema)
    
    print(f"Data loaded. Initial row count: {df.count()}")

    df_clean = df.dropna()
    
    df_clean = df_clean.withColumn("Date", F.to_timestamp("Date", "MM/dd/yyyy hh:mm:ss a"))

    df_clean.show(5)
    print("Data cleaned and Date format converted.")

if __name__ == "__main__":
    main()