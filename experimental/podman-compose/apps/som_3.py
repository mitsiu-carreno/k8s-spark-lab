import numpy as np
import random
from pyspark.sql import SparkSession
from pyspark.ml.feature import (
    VectorAssembler,
    NGram,
)
from sklearn.metrics.pairwise import euclidean_distances
from pyspark.sql.functions import (
    array,
    flatten,
    split,
    col,
    concat,
    explode_outer,
    transform,
    regexp_replace,
)

spark = SparkSession.builder.appName("SOM").getOrCreate()

df = (
    spark.read.parquet("s3a://logs/output/extract/known/")
    .select(
        "remote_usr",
        "req_method_onehot",
        "clean_path",
        "clean_query_list",
        "status",
        "body_bytes_sent",
        "user_agent",
        "fabstime",
        "day_of_week",
        "domain_index",
        "domain",
    )
    .filter("domain = 'ieec.mx'")
    .limit(10)
)

print(f"{df.count()}, {len(df.columns)}")

df.show(100)

df.printSchema()

df = df.withColumn("path_words", split(regexp_replace(col("clean_path"), "^/", ""), "/"))
#df = df.withColumn("query_words", 
#    transform(col("clean_query_list"), lambda x: split(x, "=")))

ngram = NGram(n=2, inputCol="path_words", outputCol="path_ngrams")

df = ngram.transform(df)

df = df.withColumn("url_features", 
    concat(
        col("path_ngrams"),
        #flatten(col("query_words"))
        col("clean_query_list")
    )
)

from pyspark.ml.feature import HashingTF

hashingTF = HashingTF(inputCol="url_features", outputCol="hash_url_features", numFeatures=1000)

df = hashingTF.transform(df)


from pyspark.ml.feature import VectorAssembler

vector_assembler = VectorAssembler(
    inputCols=[
        #"remote_usr",
        "req_method_onehot",
        "status",
        "body_bytes_sent",
        #"user_agent",
        "hash_url_features",
        "fabstime",
        "day_of_week",
    ],
    outputCol="features"
)

df_final = vector_assembler.transform(df)

df_final.show(truncate=False)

#rdd_data = df_final.select("features").rdd.map(lambda row: np.array(row['features']))
rdd_data = df_final.select("features").rdd.map(lambda row: row["features"].toArray()).collect()

#data = np.array(rdd_data.collect())
data = np.array(rdd_data)

print(data.shape)

print(data)
