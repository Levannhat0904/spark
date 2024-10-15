from pyspark import SparkContext
from pyspark.sql import SparkSession

# Tạo SparkContext và SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-1.amazonaws.com") \
    .getOrCreate()

# Đọc dữ liệu từ S3
df = spark.read.json("s3a://nhattest1/employees.json")

# Hiển thị dữ liệu
df.show()
