from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round as spark_round

def create_spark_session(app_name="Binge_Watching_Analysis"):
    """
    Initialize and return a SparkSession.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_csv_data(spark, file_path):
    """
    Load movie ratings data from a CSV file into a Spark DataFrame.
    """
    return spark.read.option("header", True).option("inferSchema", True).csv(file_path)

def analyze_binge_watching(df):
    """
    Calculate the percentage of binge-watchers within each age group.
    """
    binge_watchers = df.filter(col("IsBingeWatched") == True)
    binge_counts = binge_watchers.groupBy("AgeGroup").agg(count("UserID").alias("Binge_Watchers"))
    total_users = df.groupBy("AgeGroup").agg(count("UserID").alias("Total_Users"))

    binge_stats = binge_counts.join(total_users, "AgeGroup")\
        .withColumn("Percentage", spark_round((col("Binge_Watchers") / col("Total_Users")) * 100, 2))
    
    return binge_stats

def save_results(df, output_path):
    """
    Save the processed DataFrame as a CSV file.
    """
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)

def main():
    """
    Execute the binge-watching analysis workflow.
    """
    spark = create_spark_session()
    input_file = "input/movie_ratings_data.csv"  # Adjusted input path
    df = read_csv_data(spark, input_file)

    # Perform binge-watching analysis
    binge_watch_stats = analyze_binge_watching(df)
    save_results(binge_watch_stats, "Outputs/binge_watching_patterns.csv")

    spark.stop()

if __name__ == "__main__":
    main()
