from pyspark.sql import SparkSession
from pyspark.sql.functions import \
    regexp_extract, \
    col, \
    udf, \
    to_timestamp, \
    explode, \
    isnan, \
    when, \
    trim, \
    hour, \
    minute, \
    concat_ws 
from pyspark.sql.types import \
    IntegerType, \
    StringType, \
    ArrayType, \
    TimestampType
import urllib.parse
from datetime import datetime
import time

spark = SparkSession.builder \
    .appName("log_transform") \
    .getOrCreate()

df_raw = spark.read.text("s3a://logs/input/*")

#df_raw.show()

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
    "gzip_ratio"
]

log_pattern = r"^((?:\d{1,3}\.){3}\d{1,3}|[0-9a-fA-F:]+) - (-|[^\s]+) \[([^\]]+)\] \"([A-Z]+) ([^ ]+) (HTTP/\d\.\d)\" (\d{3}) (\d+) \"([^\"]*)\" \"([^\"]*)\"(?:\s+(\d+))?"

df_parsed = df_raw.select(
    *[regexp_extract(col("value"), log_pattern, i).alias(log_columns[i - 1]) for i in range(1, len(log_columns)+1)]
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
    "gzip_ratio"
)

df_parsed = df_parsed.withColumn("status", df_parsed["status"].cast(IntegerType()))
df_parsed = df_parsed.withColumn("body_bytes_sent", df_parsed["body_bytes_sent"].cast(IntegerType()))

#df_parsed.show(1, truncate=False)
#row = df_parsed.take(1)
#print(row)

df_parsed = df_parsed.drop('gzip_ratio')

df_parsed.printSchema()

#print(f"{df_parsed.count()}, {len(df_parsed.columns)}")

# Delete all null rows
def to_null(c):
    return when(~(col(c).isNull() | isnan(col(c)) | (trim(col(c)) == "")), col(c))

df_parsed = df_parsed.select([to_null(c).alias(c) for c in df_parsed.columns]).na.drop()

#print(f"{df_parsed.count()}, {len(df_parsed.columns)}")

#df_parsed.filter(df_parsed.status.isNull()).count()

#df_parsed.filter(df_parsed.body_bytes_sent.isNull()).count()

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
    return urllib.parse.urlparse(referer).netloc if referer is not None else None

def parse_date(date_str):
    return datetime.strptime(date_str, "%d/%b/%Y:%H:%M:%S +0000") if date_str is not None else None

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
df = df.withColumn('dec_req_uri', decode_uri_udf(col('req_uri')))
df = df.withColumn('clean_path', get_path_udf(col('dec_req_uri')))
df = df.withColumn('clean_query_list', get_query_list_udf(col('dec_req_uri')))
df = df.withColumn('domain', get_domain_udf(col('http_referer')))
df = df.withColumn('fdate_time', parse_date_udf(col('date_time')))
df = df.withColumn('dateunixtimest', to_unix_timestamp_udf(col('fdate_time')))
df = df.withColumn('fabstime', ( hour(col('fdate_time')) + minute(col('fdate_time')) / 60.0))

df.printSchema()

row = df.take(1)
print(row)

