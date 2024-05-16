from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pickle

# Create a SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .config("spark.executor.memory", "70g")
         .config("spark.driver.memory", "50g")
         .config("spark.driver.maxResultSize", "50g")
         .appName("LoadParquetFile").getOrCreate()
         )
item_df = spark.read.parquet("item_df.parquet")

with open("top_popular_each_genre_precompute.pickle", 'rb') as file:
    cache = pickle.load(file)
    print(cache)

with open("top_popular_each_source_title_precompute.pickle", 'rb') as file:
    cache = pickle.load(file)
    print(cache)