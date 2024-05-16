import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pickle


def top_trending_each_genre_precompute(user_df, item_df):
    """

    :param user_df:
    :param item_df:
    :return:
    """
    watch_df = (user_df.select(F.explode("watched_item_list").alias("item_id"))
                .groupBy("item_id")
                .agg(F.count("*").alias("watch"))
                )
    exposure_df = (user_df
                   .select(F.explode("exposed_item_list").alias("item_id"))
                   .groupBy("item_id")
                   .agg(F.count("*").alias("exposure"))
                   )

    item_property = (
        exposure_df.join(watch_df, "item_id", "left_outer")
        .na.fill(0, "watch")
        .join(item_df, "item_id", "inner")
        .withColumn("watch_per_exposure", F.col("watch") / F.col("exposure"))
        .withColumn("watch_cum", F.sum(F.col("watch")).over(Window.partitionBy().orderBy(F.desc(F.col("watch")))))
        .withColumn("watch_sum", F.sum(F.col("watch")).over(Window.partitionBy().orderBy()))
        .withColumn("watch_sum_percent", F.col("watch_cum") / F.col("watch_sum"))
        .cache()
    )
    window_spec = Window.partitionBy("genre").orderBy(F.col("watch_per_exposure").desc())
    item_frequency_and_genre = (
        item_property.filter(F.col("watch_sum_percent") < 0.8) # Remove content with small watch
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") < 10)
        .filter(F.col("genre").isNotNull())
    )
    rows = item_frequency_and_genre.collect()
    item_id_top_genre = dict()
    for r in rows:
        genre = r["genre"]
        item_id = r["item_id"]
        item_id_top_genre[item_id] = genre
    return item_id_top_genre


def top_popular_each_genre_precompute(user_df, item_df):
    """

    :param user_df:
    :param item_df:
    :return:
    """
    item_id_df = user_df.select(F.explode("watched_item_list").alias("item_id"))
    grouped_df = item_id_df.groupBy("item_id").agg(F.count("*").alias("count")).orderBy(F.desc("count"))
    window_spec = Window.partitionBy("genre").orderBy(F.col("count").desc())
    item_frequency_and_genre = (
        grouped_df.join(item_df, "item_id", "inner")
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") < 10)
        .filter(F.col("genre").isNotNull())
    )
    rows = item_frequency_and_genre.collect()
    item_id_top_genre = dict()
    for r in rows:
        genre = r["genre"]
        item_id = r["item_id"]
        item_id_top_genre[item_id] = genre
    return item_id_top_genre


def top_popular_each_source_title_precompute(user_df, item_df):
    co_occurrence = (
        user_df.select(F.col("user_id"), F.col("watched_item_list").alias("watched_item_list_source"))
        .withColumn("watched_item_list_source", F.explode("watched_item_list_source"))
        .join(user_df.select(F.col("user_id"), F.col("watched_item_list").alias("item_id")), "user_id")
        .withColumn("item_id", F.explode("item_id"))
        .filter(F.col("watched_item_list_source") != F.col("item_id"))
        .groupBy("watched_item_list_source", "item_id")
        .count()
    )
    window_spec = Window.partitionBy("watched_item_list_source").orderBy(F.col("count").desc())
    top_item_each_source = (
        co_occurrence.join(item_df, "item_id", "inner")
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") < 10)
        .groupBy("watched_item_list_source")
        .agg(
            F.collect_list("item_id").alias("item_id_list")
        )
    )
    precomputed_result = dict()
    top_item_each_source_dict = dict()
    for r in top_item_each_source.collect():
        source = r["watched_item_list_source"]
        item_id_list = r["item_id_list"]
        top_item_each_source_dict[source] = item_id_list

    item_name_dict = dict()
    for r in item_df.collect():
        item_id = r["item_id"]
        name = r["name"]
        item_name_dict[item_id] = name
    precomputed_result["item_name"] = item_name_dict

    user_history_dict = dict()
    for r in user_df.collect():
        user_id = r["user_id"]
        history = r["watched_item_list"]
        user_history_dict[user_id] = history
    precomputed_result["user_history"] = user_history_dict
    precomputed_result["top_item_each_source_dict"] = top_item_each_source_dict
    return precomputed_result


if __name__ == '__main__':
    from pyspark.sql import SparkSession
    # Create a SparkSession
    spark = (SparkSession
             .builder
             .master("local[*]")
             .config("spark.executor.memory", "70g")
             .config("spark.driver.memory", "50g")
             .config("spark.driver.maxResultSize", "50g")
             .appName("LoadParquetFile").getOrCreate()
             )

    user_df = spark.read.parquet("user_df.parquet")
    item_df = spark.read.parquet("item_df.parquet")
    precompute_func_list = [top_popular_each_genre_precompute,
                            top_trending_each_genre_precompute,
                            top_popular_each_source_title_precompute]
    for precompute_func in precompute_func_list:
        precomputed_res = precompute_func(user_df=user_df, item_df=item_df)
        serialized_res = pickle.dumps(precomputed_res)
        pickle_name = f"{precompute_func.__name__}.pickle"
        with open(pickle_name, "wb") as file:
            file.write(serialized_res)

