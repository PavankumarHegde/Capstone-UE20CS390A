import re
from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField, TimestampType


def clean_text(text):
    # Remove mentions, hashtags, urls and non-alphanumeric characters
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#\w+', '', text)
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'[^a-zA-Z\d\s]', '', text)
    # Convert to lowercase
    text = text.lower()
    return text


# UDF for cleaning text
clean_text_udf = udf(lambda x: clean_text(x) if x else '', StringType())

# Schema for social media data
social_media_schema = StructType([
    StructField("date", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("text", StringType(), True),
    StructField("sentiment", StringType(), True)
])

# Schema for stock data
stock_schema = StructType([
    StructField("date", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", StringType(), True)
])

# Schema for news data
news_schema = StructType([
    StructField("date", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    StructField("sentiment", StringType(), True)
])

def process_social_media_data(data):
    # Clean text column
    data = data.withColumn("text", clean_text_udf(data.text))
    # Parse date column
    data = data.withColumn("date", udf(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"), TimestampType())(data.date))
    # Add sentiment column if not present
    if "sentiment" not in data.columns:
        data = data.withColumn("sentiment", udf(lambda x: "", StringType())(data.text))
    return data.select("date", "source", "text", "sentiment").na.drop()

def process_stock_data(data):
    # Parse date column
    data = data.withColumn("date", udf(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"), TimestampType())(data.date))
    return data.select("date", "source", "symbol", "price").na.drop()

def process_news_data(data):
    # Clean title and description columns
    data = data.withColumn("title", clean_text_udf(data.title))
    data = data.withColumn("description", clean_text_udf(data.description))
    # Parse date column
    data = data.withColumn("date", udf(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"), TimestampType())(data.date))
    # Add sentiment column if not present
    if "sentiment" not in data.columns:
        data = data.withColumn("sentiment", udf(lambda x: "", StringType())(data.title))
    return data.select("date", "source", "title", "description", "url", "sentiment").na.drop()

