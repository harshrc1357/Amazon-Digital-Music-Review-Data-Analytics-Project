from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, year, month, avg, count, desc

spark = SparkSession.builder \
    .appName("BDA_MiniProject") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.access.key", "access-key") \
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
print("\n1. Rating Distribution by Year:")
yearly_ratings.show()

# 2. Helpful Vote Analysis by Rating
helpful_votes_analysis = df_transformed_simple.groupBy("rating") \
    .agg(avg("helpful_vote").alias("avg_helpful_votes"), count("*").alias("total_reviews")) \
    .orderBy("rating")
print("\n2. Helpful Vote Analysis by Rating:")
helpful_votes_analysis.show()

# 3. Verified Purchase Impact
verified_impact = df_transformed_simple.groupBy("verified_purchase", "year") \
    .agg(avg("rating").alias("avg_rating"), count("*").alias("review_count")) \
    .orderBy("year", "verified_purchase")
print("\n3. Verified Purchase Impact by Year:")
verified_impact.show()

# 4. Monthly Review Volume
monthly_volume = df_transformed_simple.groupBy("year", "month") \
    .agg(count("*").alias("review_count")) \
    .orderBy("year", "month")
print("\n4. Monthly Review Volume:")
monthly_volume.show()

# 5. Top Reviewers Analysis
top_reviewers = df_transformed_simple.groupBy("user_id") \
    .agg(count("*").alias("review_count"),
         avg("rating").alias("avg_rating"),
         avg("helpful_vote").alias("avg_helpful_votes")) \
    .orderBy(desc("review_count")) \
    .limit(10)
print("\n5. Top Reviewers Analysis:")
top_reviewers.show()