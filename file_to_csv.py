from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, desc

# Initialize Spark session
spark = SparkSession.builder \
    .appName("BDA_MiniProject") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.access.key", "acsess-key") \
    .config("spark.hadoop.fs.s3a.secret.key", "secret-key") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")>    .getOrCreate()

# Read data from S3
df = spark.read.json("s3a://bda-miniproject-hchauhan/Digital_Music.jsonl")
df_transformed_simple = spark.read.csv("transformed_data", header=True)

# 1. Rating Distribution by Year
yearly_ratings = df_transformed_simple.groupBy("year") \
    .agg(avg("rating").alias("avg_rating"), count("*").alias("review_count")) \
    .orderBy("year")
yearly_ratings.write.csv('yearly_ratings', header=True, mode='overwrite')
print("Successfully saved yearly_ratings.csv")

# 2. Helpful Vote Analysis by Rating
helpful_votes_analysis = df_transformed_simple.groupBy("rating") \
    .agg(avg("helpful_vote").alias("avg_helpful_votes"), count("*").alias("total_reviews")) \
    .orderBy("rating")
helpful_votes_analysis.write.csv('helpful_votes_analysis', header=True, mode='overwrite')
print("Successfully saved helpful_votes_analysis.csv")

# 3. Verified Purchase Impact by Year
verified_impact = df_transformed_simple.groupBy("verified_purchase", "year") \
    .agg(avg("rating").alias("avg_rating"), count("*").alias("review_count")) \
    .orderBy("year", "verified_purchase")
verified_impact.write.csv('verified_impact', header=True, mode='overwrite')
print("Successfully saved verified_impact.csv")

# 4. Monthly Review Volume
monthly_volume = df_transformed_simple.groupBy("year", "month") \
    .agg(count("*").alias("review_count")) \
    .orderBy("year", "month")
monthly_volume.write.csv('monthly_volume', header=True, mode='overwrite')
print("Successfully saved monthly_volume.csv")

# 5. Top Reviewers Analysis
top_reviewers = df_transformed_simple.groupBy("user_id") \
    .agg(count("*").alias("review_count"),
         avg("rating").alias("avg_rating"),
         avg("helpful_vote").alias("avg_helpful_votes")) \
    .orderBy(desc("review_count")) \
    .limit(10)
top_reviewers.write.csv('top_reviewers', header=True, mode='overwrite')
print("Successfully saved top_reviewers.csv")
