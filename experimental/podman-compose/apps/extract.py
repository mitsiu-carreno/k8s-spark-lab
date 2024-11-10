from pyspark.sql import SparkSession
from pyspark.ml.feature import NGram
from pyspark.sql.functions import (
    regexp_extract,
    col,
    udf,
    to_timestamp,
    explode,
    isnan,
    when,
    trim,
    hour,
    minute,
    concat_ws,
    dayofweek,
    size,
    explode_outer,
    when,
    size,
    dayofweek,
    split,
    concat,
    transform,
)
from pyspark.sql.types import IntegerType, StringType, ArrayType, TimestampType
import urllib.parse
from datetime import datetime
import time

spark = SparkSession.builder.appName("log_extract").getOrCreate()

df_raw = spark.read.text("s3a://logs/input/**")

# df_raw.show()

log_columns = [
    "remote_addr",
    "remote_usr",
    "date_time",
    "req_method",
    "req_uri",
    "http_ver",
    "status",
    "body_bytes_sent",
    "http_referer",
    "user_agent",
    "gzip_ratio",
]

log_pattern = r"^((?:\d{1,3}\.){3}\d{1,3}|[0-9a-fA-F:]+) - (-|[^\s]+) \[([^\]]+)\] \"([A-Z]+) ([^ ]+) (HTTP/\d\.\d)\" (\d{3}) (\d+) \"([^\"]*)\" \"([^\"]*)\"(?:\s+(\d+))?"

df_parsed = df_raw.select(
    *[
        regexp_extract(col("value"), log_pattern, i).alias(log_columns[i - 1])
        for i in range(1, len(log_columns) + 1)
    ]
)

df_parsed = df_parsed.selectExpr(
    "remote_addr",
    "remote_usr",
    "date_time",
    "substring(date_time, 1, 11) as date",
    "substring(date_time, 13, 8) as time",
    "req_method",
    "req_uri",
    "http_ver",
    "status",
    "body_bytes_sent",
    "http_referer",
    "user_agent",
    "gzip_ratio",
)

df_parsed = df_parsed.withColumn("status", df_parsed["status"].cast(IntegerType()))
df_parsed = df_parsed.withColumn(
    "body_bytes_sent", df_parsed["body_bytes_sent"].cast(IntegerType())
)

# df_parsed.show(1, truncate=False)
# row = df_parsed.take(1)
# print(row)

df_parsed = df_parsed.drop("gzip_ratio")

#df_parsed.printSchema()

# print(f"{df_parsed.count()}, {len(df_parsed.columns)}")


# Change to null emtries and nan
def to_null(c):
    return when(~(col(c).isNull() | isnan(col(c)) | (trim(col(c)) == "")), col(c))


df_parsed = df_parsed.select([to_null(c).alias(c) for c in df_parsed.columns]).na.drop()

# print(f"{df_parsed.count()}, {len(df_parsed.columns)}")

# df_parsed.filter(df_parsed.status.isNull()).count()

# df_parsed.filter(df_parsed.body_bytes_sent.isNull()).count()


# User defined functions
def decode_uri(uri):
    return urllib.parse.unquote(uri) if uri is not None else None


def get_path(uri):
    return urllib.parse.urlparse(uri).path if uri is not None else None


def get_query_list(uri):
    if uri is None:
        return []
    query = urllib.parse.urlparse(uri).query
    return [f"{k}={v}" for k, v in urllib.parse.parse_qsl(query)]


def get_domain(referer):
    netloc = urllib.parse.urlparse(referer).netloc
    return netloc if netloc not in (None, "", "-") else "Unknown"
    #return urllib.parse.urlparse(referer).netloc if referer not in (None, "-") else "Unknown"


def parse_date(date_str):
    return (
        datetime.strptime(date_str, "%d/%b/%Y:%H:%M:%S +0000")
        if date_str is not None
        else None
    )


def to_unix_timestamp(dt):
    return time.mktime(dt.timetuple()) if dt is not None else None


# Register UDF's
decode_uri_udf = udf(decode_uri, StringType())
get_path_udf = udf(get_path, StringType())
get_query_list_udf = udf(get_query_list, ArrayType(StringType()))
get_domain_udf = udf(get_domain, StringType())
parse_date_udf = udf(parse_date, TimestampType())
to_unix_timestamp_udf = udf(to_unix_timestamp, StringType())

df = df_parsed
df = df.withColumn("dec_req_uri", decode_uri_udf(col("req_uri")))
df = df.withColumn("clean_path", get_path_udf(col("dec_req_uri")))
df = df.withColumn("clean_query_list", get_query_list_udf(col("dec_req_uri")))
df = df.withColumn("domain", get_domain_udf(col("http_referer")))
df = df.withColumn("fdate_time", parse_date_udf(col("date_time")))
df = df.withColumn("dateunixtimest", to_unix_timestamp_udf(col("fdate_time")))
df = df.withColumn(
    "fabstime", (hour(col("fdate_time")) + minute(col("fdate_time")) / 60.0)
)

#df.printSchema()

#row = df.take(1)
#print(row)


# Clusterizing
"""
from pyspark.ml.feature import VectorAssembler, CountVectorizer
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

count_vectorizer = CountVectorizer(inputCol="clean_query_list", outputCol="features_array")

test_df = df.withColumn("day_of_week", dayofweek(col("fdate_time")))
test_df = test_df.filter(size("clean_query_list") > 0)

#test_df.select("day_of_week").distinct().show(truncate=False)

assembler = VectorAssembler(
    inputCols=[
        "fabstime",
        "body_bytes_sent"
    ],
    outputCol="features"
)

test_df.printSchema()

kmeans = KMeans(k=25, seed=1)

pipeline = Pipeline(stages=[assembler, kmeans])

test_df = test_df.na.drop()

model = pipeline.fit(test_df)
"""

df = df.withColumn("day_of_week", dayofweek(col("fdate_time")))

df= df.withColumn("path_characters", split(col("clean_path"), ""))

ngram = NGram(n=9, inputCol="path_characters", outputCol="path_ngrams")
df = ngram.transform(df)

df= df.withColumn("url_features", 
    concat(
        col("path_ngrams"), 
        transform(col("clean_query_list"), lambda x: split(x, "=")[0]))
)

from pyspark.ml.feature import HashingTF

hashingTF = HashingTF(inputCol="url_features", outputCol="hash_url_features", numFeatures=16384)

df= hashingTF.transform(df)

from pyspark.ml.feature import StringIndexer, OneHotEncoder

indexer_http_method = StringIndexer(inputCol="req_method", outputCol="req_method_index")
encoder_http_method = OneHotEncoder(inputCol="req_method_index", outputCol="req_method_onehot")

#df.groupBy("domain").count().show(100, truncate=False)
#df.filter(df["domain"] == "").select("http_referer", "domain").show(truncate=False)

indexer_domain = StringIndexer(inputCol="domain", outputCol="domain_index")
encoder_domain = OneHotEncoder(inputCol="domain_index", outputCol="domain_onehot")

df = indexer_http_method.fit(df).transform(df)
df = encoder_http_method.fit(df).transform(df)
df = indexer_domain.fit(df).transform(df)
df = encoder_domain.fit(df).transform(df)

#df.select("clean_path", "day_of_week", "req_method_onehot", "domain_onehot").show(truncate=False)

"""
#Move to transform
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

#df.printSchema()

df= vector_assembler.transform(df)

"""

registered_domains = [
    "2021.designa.mx",
    "donacionescn.designa.mx",
    "rimaems.ieec.mx",
    "a286preprod.designa.mx",
    "einstitucional.designamx.com",
    "rimaems-pwa.ieec.mx",
    "acuerdo286.designa.mx",
    "encuestapec.com",
    "rimaemstest.ieec.mx",
    "alumnosstage.upa.edu.mx",
    "encuestas.causanatura.org",
    "servicios.ucags.edu.mx",
    "alumnos.ucags.edu.mx",
    "enrr.designa.mx",
    "stagealumnos.ucags.edu.mx",
    "alumnos.upa.edu.mx",
    "fnsm2023.designa.mx",
    "stagecuestionarios.ieec.mx",
    "api.upa.edu.mx",
    "fnsm.designa.mx",
    "stage.designa.mx",
    "auth.mastermanual.mx",
    "fnsm.feriapp.mx",
    "stage.encuestapec.com",
    "authstage.mastermanual.mx",
    "ieec.mx",
    "stagefinanzas.ucags.edu.mx",
    "avicola.designa.mx",
    "kardexupa.prosinergia.mx",
    "stage.mastermanual.mx",
    "cajasstage.upa.edu.mx",
    "mastermanual.mx",
    "stage.perezchica.mx",
    "cajas.upa.edu.mx",
    "nlaprende.ieec.mx",
    "stageservicios.ucags.edu.mx",
    "canvas.ieec.mx",
    "pai.ucags.edu.mx",
    "survey.ieec.mx",
    "cdp.solucti.mx",
    "perezchica.mx",
    "testjalisco.ieec.mx",
    "cms.ieec.mx",
    "phpmyadmin5.designa.mx",
    "upa.prosinergia.mx",
    "cuestionarios.ieec.mx",
    "prod.acuerdo286.designa.mx",
    "v2.ieec.mx",
    "dbmanager.designa.mx",
    "productores.designa.mx",
    "xmas.designa.mx",
    "default",
    "recrea.ieec.mx",
    "demo.edual.mx",
    "reportes.upa.edu.mx",
]

print(f"{df.count()}, {len(df.columns)}")
df.printSchema()

row = df.take(1)
print("-"*100)
print(row)

df_known_domains = df.filter(col("domain").isin(*registered_domains))
df_unknown_domains = df.filter(~col("domain").isin(*registered_domains))
#df_known_domains = df_known_domains.repartition(50)
df_known_domains.write.partitionBy("domain").parquet("s3a://logs/output/extract/known/")
df_unknown_domains.write.parquet("s3a://logs/output/extract/unknown")
