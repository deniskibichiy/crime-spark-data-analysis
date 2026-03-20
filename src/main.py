
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
    
    df_clean = df.withColumn("Date", F.to_timestamp("Date", "MM/dd/yyyy hh:mm:ss a"))

    df_clean = df_clean.dropna()

    df_recent = df_clean.filter(F.col("Year") >= 2016)

    excluded_crimes = [
        'NON-CRIMINAL (SUBJECT SPECIFIED)', 
        'OTHER OFFENSE', 
        'STALKING', 
        'NON - CRIMINAL', 
        'ARSON'
    ]
    df_filtered = df_recent.filter(~F.col("Primary Type").isin(excluded_crimes))

    df_final = df_filtered.withColumn("Primary Type", 
        F.when(F.col("Primary Type").isin("SEX OFFENSE", "PROSTITUTION"), "SEX OFFENSE/PROSTITUTION")
        .otherwise(F.col("Primary Type"))
    )

    print("\n--- Year-wise Crime Trend (2016-2026) ---")
    yearly_trend = df_final.groupBy("Year").count().orderBy("Year")
    yearly_trend.show()

    hourly_crime = df_final.withColumn("Hour", F.hour("Date")) \
        .groupBy("Hour").count().orderBy(F.desc("count"))
    
    peak_hour = hourly_crime.first()
    print(f"RESULT: The peak hour for crime is {peak_hour['Hour']}:00 with {peak_hour['count']} incidents.")

    print("Generating Bar Chart...")
    top_10_pdf = df_final.groupBy("Primary Type").count().orderBy(F.desc("count")).limit(10).toPandas()

    plt.figure(figsize=(12, 6))
    plt.bar(top_10_pdf["Primary Type"], top_10_pdf["count"], color='crimson')
    plt.xticks(rotation=45, ha='right')
    plt.title("Top 10 Crimes in Chicago (Last 10 Years)")
    plt.ylabel("Number of Incidents")
    plt.tight_layout()

    os.makedirs("output", exist_ok=True)
    plt.savefig("output/top_10_crimes_chart.png")
    print("SUCCESS: Chart saved to output/top_10_crimes_chart.png")

    spark.stop()

if __name__ == "__main__":
    main()