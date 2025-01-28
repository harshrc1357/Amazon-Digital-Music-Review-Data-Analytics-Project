from pyspark.sql import SparkSession

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

# Basic Dataset Information
print("Dataset Overview:")
print(f"Number of rows: {df.count()}")
print(f"Number of columns: {len(df.columns)}")

# Display schema
print("\nDataset Schema:")
df.printSchema()

# Show sample data
print("\nSample Data:")
df.show(5)





