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
#df_known = spark.read.parquet("s3a://logs/output/known/")
df_known = spark.read.parquet("s3a://logs/output/test/")


df_exploded = df_known.withColumn("day_of_week", dayofweek(col("fdate_time")))
# ADD MONTH
# ADD YEAR

df_exploded = df_exploded.withColumn("path_characters", split(col("clean_path"), ""))

ngram = NGram(n=3, inputCol="path_characters", outputCol="path_ngrams")
df_ngrams = ngram.transform(df_exploded)

df_ngrams = df_ngrams.withColumn("url_features", 
    concat(
        col("path_ngrams"), 
        transform(col("clean_query_list"), lambda x: split(x, "=")[0]))
)

df_ngrams.select("clean_path", "path_ngrams", "clean_query_list", "url_features").show(truncate=False)


print(f"{df_exploded.count()}, {len(df_exploded.columns)}")


df_exploded.show(truncate=False)
df_exploded.printSchema()





from pyspark.ml.feature import HashingTF

hashingTF = HashingTF(inputCol="url_features", outputCol="hash_url_features", numFeatures=1000)

df_ngrams = hashingTF.transform(df_ngrams)

df_ngrams.show(truncate=False)

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


from pyspark.ml.feature import StringIndexer, OneHotEncoder

indexer_http_method = StringIndexer(inputCol="req_method", outputCol="req_method_index")
encoder_http_method = OneHotEncoder(inputCol="req_method_index", outputCol="req_method_onehot")

# This will change to one domain at a time
indexer_domain = StringIndexer(inputCol="domain", outputCol="domain_index")
encoder_domain = OneHotEncoder(inputCol="domain_index", outputCol="domain_onehot")

# Apply the transformations
#df_encoded = indexer_day_of_week.fit(df_ngrams).transform(df_ngrams)
#df_encoded = encoder_day_of_week.fit(df_encoded).transform(df_encoded)
df_encoded = indexer_http_method.fit(df_ngrams).transform(df_ngrams)
df_encoded = encoder_http_method.fit(df_encoded).transform(df_encoded)
df_encoded = indexer_domain.fit(df_encoded).transform(df_encoded)
df_encoded = encoder_domain.fit(df_encoded).transform(df_encoded)

df_encoded.select("clean_path", "day_of_week", "req_method_onehot", "domain_onehot").show(truncate=False)

from pyspark.ml.feature import VectorAssembler

# Create the feature vector
vector_assembler = VectorAssembler(
    inputCols=[
        #"levenshtein_distance", 
        "fabstime", 
        "day_of_week", 
        "req_method_onehot", 
        "hash_url_features"
    ],
    outputCol="features"
)

df_encoded.printSchema()

df_final = vector_assembler.transform(df_encoded)
df_final.select("clean_path", "features", "domain_onehot").show(truncate=False)


from pyspark.ml.classification import LogisticRegression

# Train a logistic regression model
lr = LogisticRegression(featuresCol="features", labelCol="domain_index", family="multinomial")

# Split data into training and testing sets
train_data, test_data = df_final.randomSplit([0.8, 0.2], seed=123)

# Train the model
lr_model = lr.fit(train_data)

lr_model.save("s3a://logs/models/" + datetime.now().strftime("%y-%m-%d-%H:%M") +"/domain_classifier")

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


