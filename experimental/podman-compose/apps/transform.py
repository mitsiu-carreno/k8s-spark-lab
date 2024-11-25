from datetime import datetime
from pyspark.ml.feature import NGram
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode_outer,
    when,
    size,
    dayofweek,
    split,
    concat,
    transform,
)

spark = SparkSession.builder.appName("log_transform").getOrCreate()
# Cauton dont read /known/** because domain column is lost
df_known = spark.read.parquet("s3a://logs/output/extract/known/").select("fabstime", "day_of_week", "req_method_onehot", "body_bytes_sent", "hash_url_features", "domain_index")
#df_known = spark.read.parquet("s3a://logs/output/test/")


###Playground
"""
from pyspark.sql.functions import explode, col, length, countDistinct, min, max, avg

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

df_exploded = df_exploded.withColumn("path_words", split(col("clean_path"), "/"))



# Count unique words
df_exploded = df_exploded.withColumn("word", explode(col("path_words")))
unique_words_count = df_exploded.select("word").distinct().count()
print(f"Unique words count: {unique_words_count}")

# 2. Get min, max, and average characters per word
df_with_word_length = df_exploded.withColumn("word_length", length(col("word")))

# Min, Max, and Average word lengths
min_length = df_with_word_length.select(min("word_length")).collect()[0][0]
max_length = df_with_word_length.select(max("word_length")).collect()[0][0]
avg_length = df_with_word_length.select(avg("word_length")).collect()[0][0]

print(f"Minimum word length: {min_length}")
print(f"Maximum word length: {max_length}")
print(f"Average word length: {avg_length}")

# Calculating the standard deviation of word lengths
stddev_length = df_with_word_length.agg(F.stddev("word_length")).collect()[0][0]
print(f"Standard Deviation of word length: {stddev_length}")

# Calculating the length of each array (number of words)
df_with_array_length = df_exploded.withColumn("array_length", F.size("path_words"))

# Calculating min, max, and average length of the arrays
min_array_length = df_with_array_length.agg(F.min("array_length")).collect()[0][0]
max_array_length = df_with_array_length.agg(F.max("array_length")).collect()[0][0]
avg_array_length = df_with_array_length.agg(F.avg("array_length")).collect()[0][0]

# Calculating standard deviation of array lengths
stddev_array_length = df_with_array_length.agg(F.stddev("array_length")).collect()[0][0]

print(f"Min array length: {min_array_length}")
print(f"Max array length: {max_array_length}")
print(f"Average array length: {avg_array_length}")
print(f"Standard Deviation of array length: {stddev_array_length}")

spark.stop()
###End-playground
"""



"""
from pyspark.sql.functions import levenshtein, col

df_self_joined = df_exploded.alias("df1").join(
    df_exploded.alias("df2"),
    on=col("df1.domain") == col("df2.domain"),
    how="inner"
).filter(col("df1.clean_path") != col("df2.clean_path"))


df_with_levenshtein = df_self_joined.withColumn(
    "levenshtein_distance", levenshtein(col("df1.clean_path"), col("df2.clean_path"))
)

df_with_levenshtein.select("df1.clean_path", "df2.clean_path", "levenshtein_distance").show(truncate=False)
"""


from pyspark.ml.feature import VectorAssembler

# Create the feature vector
vector_assembler = VectorAssembler(
    inputCols=[
        #"levenshtein_distance", 
        "fabstime", 
        "day_of_week", 
        "req_method_onehot", 
        "body_bytes_sent",
        "hash_url_features"
    ],
    outputCol="features"
)

df_known.printSchema()
row = df_known.take(1)
print("-"*100)
print(row)


df_final = vector_assembler.transform(df_known)
#df_final.select("clean_path", "features", "domain_onehot").show(truncate=False)


from pyspark.ml.classification import LogisticRegression

# Train a logistic regression model
lr = LogisticRegression(featuresCol="features", labelCol="domain_index", family="multinomial", maxIter=100)

# Split data into training and testing sets
train_data, test_data = df_final.randomSplit([0.8, 0.2], seed=123)

# Train the model
lr_model = lr.fit(train_data)

lr_model.save("s3a://logs/models/" + datetime.now().strftime("%y-%m-%d-%H_%M") +"/domain_classifier")

"""
from pyspark.ml.classification import LogisticRegressionModel

# Load the saved model
loaded_lr_model = LogisticRegressionModel.load("s3a://logs/models/domain_classifier")
"""

# Make predictions on the test set
predictions = lr_model.transform(test_data)

# Evaluate the model
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(labelCol="domain_index", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"Test Accuracy: {accuracy}")


# Evaluate the model
evaluator = MulticlassClassificationEvaluator(labelCol="domain_index", predictionCol="prediction")
accuracy = evaluator.evaluate(predictions)
print(f"Accuracy: {accuracy}")

# Precision, recall, F1-score, etc.
evaluator_precision = MulticlassClassificationEvaluator(labelCol="domain_index", predictionCol="prediction", metricName="weightedPrecision")
precision = evaluator_precision.evaluate(predictions)
print(f"Weighted Precision: {precision}")
# weightedPrecision: Precision considering class imbalance.

# Create the evaluator with weightedRecall
evaluator_recall = MulticlassClassificationEvaluator(
    labelCol="domain_index", 
    predictionCol="prediction", 
    metricName="weightedRecall"
)

# Evaluate the recall
recall = evaluator_recall.evaluate(predictions)
print(f"Weighted Recall: {recall}")
#weightedRecall: Recall considering class imbalance.

# Create the evaluator with f1 score
evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol="domain_index", 
    predictionCol="prediction", 
    metricName="f1"
)

# Evaluate the F1 score
f1_score = evaluator_f1.evaluate(predictions)
print(f"F1 Score: {f1_score}")
#f1: F1 score, which is the harmonic mean of precision and recall.

spark.stop()
df_known.printSchema()
print(f"Test Accuracy: {accuracy}")
print(f"Accuracy: {accuracy}")
print(f"Weighted Precision: {precision}")
print(f"Weighted Recall: {recall}")
print(f"F1 Score: {f1_score}")





#----------------------------------------------------------------------------------------

#NGRAMS AND levenshtein_distance
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, NGram
from pyspark.sql.functions import levenshtein

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Ngrams and Levenshtein") \
    .getOrCreate()

# Sample URLs
data = [
    Row(url="/api/v1/student/1234"),
    Row(url="/api/v1/student/5637"),
    Row(url="/apigator/1256?role=student"),
    Row(url="/api/v1/student/1256"),
    Row(url="/api/v2/student/1256")
]

# Create DataFrame
df = spark.createDataFrame(data)

# Show the DataFrame
df.show(truncate=False)

# Tokenizing the URL (splitting by "/")
tokenizer = Tokenizer(inputCol="url", outputCol="words")
df_words = tokenizer.transform(df)

# Generate 3-grams from words
ngram = NGram(n=3, inputCol="words", outputCol="word_ngrams")
df_ngrams = ngram.transform(df_words)

# Show word-level N-grams
df_ngrams.select("url", "word_ngrams").show(truncate=False)

# Tokenizing the URL at the character level
df_chars = df.withColumn("char_ngrams", F.split("url", ""))

# Generate 3-character N-grams
char_ngram = NGram(n=3, inputCol="char_ngrams", outputCol="char_ngrams_output")
df_char_ngrams = char_ngram.transform(df_chars)

# Show character-level N-grams
df_char_ngrams.select("url", "char_ngrams_output").show(truncate=False)

# Compute Levenshtein distance between pairs of URLs
url_pairs = df.alias("df1").crossJoin(df.alias("df2"))

# Calculate Levenshtein distance between the "url" columns
df_levenshtein = url_pairs.withColumn(
    "levenshtein_distance",
    levenshtein(col("df1.url"), col("df2.url"))
)

# Show Levenshtein distance results
df_levenshtein.select("df1.url", "df2.url", "levenshtein_distance").show(truncate=False)


"""


