from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, year, month

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("BDA_MiniProject") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.access.key", "access-key") \
    .config("spark.hadoop.fs.s3a.secret.key", "secret-key") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# Load the transformed data
df = spark.read.csv("s3a://bda-miniproject-hchauhan/Processed_Data/transformed_data.csv", header=True)

# Create temporary view for SQL queries
df.createOrReplaceTempView("digital_music")

# 1. Top-Rated Products Analysis
query1 = """
SELECT asin, title,
       AVG(CAST(rating AS DOUBLE)) as avg_rating,
       COUNT(*) as review_count
FROM digital_music
GROUP BY asin, title
HAVING COUNT(*) > 10
ORDER BY avg_rating DESC
LIMIT 5
"""

# 2. Monthly Review Growth Analysis
query2 = """
SELECT year, month,
       COUNT(*) as review_count,
       LAG(COUNT(*)) OVER (PARTITION BY year ORDER BY month) as prev_month_count
FROM digital_music
GROUP BY year, month
ORDER BY year, month
"""

# 3. Verified vs Non-Verified Purchase Impact
query3 = """
SELECT verified_purchase,
       AVG(CAST(rating AS DOUBLE)) as avg_rating,
       COUNT(*) as total_reviews,
       AVG(CAST(helpful_vote AS DOUBLE)) as avg_helpful_votes
FROM digital_music
GROUP BY verified_purchase
"""

# 4. Most Helpful Reviews Analysis
query4 = """
SELECT user_id, rating, title, helpful_vote
FROM digital_music
WHERE CAST(helpful_vote AS INT) > 0
ORDER BY CAST(helpful_vote AS INT) DESC
LIMIT 10
"""

# 5. Yearly Rating Distribution
query5 = """
SELECT year,
       COUNT(*) as total_reviews,
       AVG(CAST(rating AS DOUBLE)) as avg_rating,
       COUNT(CASE WHEN CAST(rating AS DOUBLE) >= 4 THEN 1 END) as positive_reviews
FROM digital_music
GROUP BY year
ORDER BY year
"""

# Execute and display results
print("\nExecuting SQL Queries for Digital Music Analysis:")

queries = [query1, query2, query3, query4, query5]
titles = [
    "Top-Rated Products Analysis",
    "Monthly Review Growth Analysis",
    "Verified vs Non-Verified Purchase Impact",
    "Most Helpful Reviews Analysis",
    "Yearly Rating Distribution"
]

for i, (query, title) in enumerate(zip(queries, titles), 1):
    print(f"\n{i}. {title}:")
    spark.sql(query).show(truncate=False)

