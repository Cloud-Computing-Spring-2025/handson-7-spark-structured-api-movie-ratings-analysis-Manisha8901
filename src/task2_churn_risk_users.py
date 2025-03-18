from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit

def create_spark_session(app_name="Churn_Risk_Analysis"):
    """
    Initialize and return a SparkSession.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_csv_data(spark, file_path):
    """
    Load movie ratings data from a CSV file into a Spark DataFrame.
    """
    return spark.read.option("header", True).option("inferSchema", True).csv(file_path)

def detect_churn_risk_users(df):
    """
    Identify users who have canceled their subscription and have watch time below 100 minutes.
    """
    churn_risk_df = df.filter((col("SubscriptionStatus") == "Canceled") & (col("WatchTime") < 100))
    churn_user_count = churn_risk_df.agg(count("UserID").alias("Total_Users"))

    # Adding a descriptive label column
    churn_user_count = churn_user_count.withColumn("Churn_Risk_Users", 
                                                   lit("Users with low watch time & canceled subscriptions"))

    return churn_user_count.select("Churn_Risk_Users", "Total_Users")

def save_results(df, output_path):
    """
    Save the DataFrame results as a CSV file.
    """
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)

def main():
    """
    Execute the churn risk user identification workflow.
    """
    spark = create_spark_session()
    input_file = "input/movie_ratings_data.csv"  # Adjusted file path
    df = read_csv_data(spark, input_file)

    # Analyze churn risk users
    churn_risk_stats = detect_churn_risk_users(df)
    save_results(churn_risk_stats, "Outputs/churn_risk_users.csv")

    spark.stop()

if __name__ == "__main__":
    main()
