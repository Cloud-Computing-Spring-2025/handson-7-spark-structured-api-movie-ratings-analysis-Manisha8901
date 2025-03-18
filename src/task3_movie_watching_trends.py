from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def create_spark_session(app_name="Movie_Watching_Trend_Analysis"):
    """
    Initialize and return a SparkSession.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_csv_data(spark, file_path):
    """
    Load movie ratings data from a CSV file into a Spark DataFrame.
    """
    return spark.read.option("header", True).option("inferSchema", True).csv(file_path)

def analyze_watching_trends(df):
    """
    Examine movie-watching patterns over the years and determine peak activity years.
    """
    trends_df = df.groupBy("WatchedYear").agg(count("MovieID").alias("Movies_Watched"))
    return trends_df.orderBy("WatchedYear")

def save_results(df, output_path):
    """
    Save the processed trend analysis results as a CSV file.
    """
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)

def main():
    """
    Execute the movie-watching trend analysis workflow.
    """
    spark = create_spark_session()
    input_file = "input/movie_ratings_data.csv"  # Adjusted input path
    df = read_csv_data(spark, input_file)

    # Perform trend analysis
    watching_trends = analyze_watching_trends(df)
    save_results(watching_trends, "Outputs/movie_watching_trends.csv")

    spark.stop()

if __name__ == "__main__":
    main()
