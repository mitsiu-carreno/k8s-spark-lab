from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("WordCount_submit")
    sc = SparkContext(conf=conf)
    input_dir="s3a://demo/input/*" #2476
    input_files = sc.textFile(input_dir)

    words = input_files.flatMap(lambda line: line.split(" "))
    word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b: a+b)
    sorted  = word_counts.sortBy(lambda pair: pair[1], ascending=True)
    final = sorted.collect()
    for (word, count) in final:
        print("%s: %i" % (word, count))
    
    sorted.map(lambda x: f"{x[0], x[1]}").saveAsTextFile("s3a://demo/output/wordcount_submit")
    #word_counts.toDF().coalesce(1).write.text("s3a://demo/output/wordcount_submit")
    sc.stop()
