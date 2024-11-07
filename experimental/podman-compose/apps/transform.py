from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode_outer,
    when,
    size,
)

spark = SparkSession.builder.appName("log_transform").getOrCreate()
# Cauton dont read /known/** because domain column is lost
df_known = spark.read.parquet("s3a://logs/output/known/")

#df_known.show(truncate=False)
#print(f"{df_known.count()}, {len(df_known.columns)}")

#df_known.printSchema()

# explode_outer doesn't discard empty arrays
df_exploded = df_known.withColumn("query_param", explode_outer(col("clean_query_list")))

row = df_exploded.take(1)
print(row)

#print(f"{df_exploded.count()}, {len(df_exploded.columns)}")

#rows = df_exploded.limit(10020).collect()[10000:10020]
#df_subset = spark.createDataFrame(rows, df_exploded.schema)
#df_subset.show(truncate=False)


#df_exploded.filter((df_exploded["domain"] != "canvas.ieec.mx") & (df_exploded["query_param"].isNotNull())).groupBy("domain").count().show(truncate=False)


"""
val input = spark.sqlContext.createDataFrame(Seq(("POST", "/api/v1/courses/11083/quizzes/331373/submissions/94734/events", 204, 0, "", 0.016666666666666666), ("POST", "/api/v1/courses/11083/quizzes/331373/submissions/94734/events", 204, 0, "asdf=asdf", 0.22),("GET", "/api/v3/home", 200, 300, "asdf=asdf", 12.0))).toDF("req_method", "clean_path", "status", "body_bytes_sent", "query_param", "fabstime")

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.feature.{VectorAssembler, CountVectorizer}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.Pipeline

# ADD DAY OF WEEK

val req_method_indexer = new StringIndexer().setInputCol("req_method").setOutputCol("req_method_index")

val encoder = new OneHotEncoder().setInputCol("req_method_index").setOutputCol("req_method_vec")

val assembler = new VectorAssembler().setInputCols(Array("status","body_bytes_sent", "req_method_vec", "fabstime")).setOutputCol("features")

val kmeans = new KMeans().setK(2).setFeaturesCol("features").setPredictionCol("prediction")

val pipeline = new Pipeline().setStages(Array(req_method_indexer, encoder, assembler, kmeans))

val kMeansPredictionModel = pipeline.fit(input)

val predictionResult = kMeansPredictionModel.transform(input)
"""


"""
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

// Initialize Spark session
val spark = SparkSession.builder()
  .appName("CountVectorizer Example")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// Sample data: documents (text)
val data = Seq(
  "Hi I heard about Spark",
  "I wish Java could use case classes",
  "Logistic regression models are neat",
  "I love Spark"
)

// Create a DataFrame
val df = data.toDF("text")

// Tokenize the text into words using a simple split
val tokenizer = udf((text: String) => text.split(" ").toSeq)
val tokenizedDf = df.withColumn("words", tokenizer(col("text")))

// Show the tokenized data
tokenizedDf.show(truncate = false)

// Create and fit the CountVectorizer
val countVectorizer = new CountVectorizer()
  .setInputCol("words")
  .setOutputCol("features")

val model = countVectorizer.fit(tokenizedDf)

// Transform the data to get the word count vectors
val result = model.transform(tokenizedDf)

// Show the results
result.select("text", "features").show(truncate = false)

"""
