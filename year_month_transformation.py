from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, year, month

# Initialize Spark Session with S3 configurations
spark = SparkSession.builder \
    .appName("BDA_MiniProject") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.access.key", "access-key") \
    .config("spark.hadoop.fs.s3a.secret.key", "secret-key") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")>    .getOrCreate()

# Read data from S3
s3_path = "s3a://bda-miniproject-hchauhan/Digital_Music.jsonl"
df = spark.read.json(s3_path)

# Transform timestamp to year and month
df_transformed = df \
    .withColumn("review_date", from_unixtime(df.timestamp/1000)) \
    .withColumn("year", year("review_date")) \
    .withColumn("month", month("review_date"))

# Select columns excluding 'images'
df_transformed_simple = df_transformed.select(
    'asin', 'helpful_vote', 'parent_asin', 'rating', 'text',
    'timestamp', 'title', 'user_id', 'verified_purchase',
    'review_date', 'year', 'month'
)

# Show transformed data
print("Transformed Data Sample:")
df_transformed_simple.show(5)

# Write to CSV
df_transformed_simple.write.option("header", "true").csv("transformed_data", mode="overwrite")