from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("Test1")
    # .set("spark.jars", "/opt/spark-extra-jars/*")
    sc = SparkContext(conf=conf)

    input_dir = "s3a://logs/input/*"
    input_files = sc.textFile(input_dir)

    words = input_files.flatMap(lambda line: line.split(" "))
    words_count = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    sorted = words_count.sortBy(lambda pair: pair[1], ascending=True)
    final = sorted.collect()

    for word, count in final:
        print("%s, %i" % (word, count))

    sorted.coalesce(1).saveAsTextFile("s3a://logs/output/word_count")

    sc.stop()
