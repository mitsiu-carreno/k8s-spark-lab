from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # We define our app name
    conf = SparkConf().setAppName("WordCount_submit")
    # Initialize the spark context 
    sc = SparkContext(conf=conf)
    # Define the path to access the input data
    input_dir="s3a://demo/input/*" 
    # Initialize the list of input files
    input_files = sc.textFile(input_dir)

    # Per file, create a list of all words (divide everythime a space is found)
    words = input_files.flatMap(lambda line: line.split(" "))
    # Group duplicated words and count occurrences
    word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b: a+b)
    # Sort words by the number of occurrences
    sorted  = word_counts.sortBy(lambda pair: pair[1], ascending=True)
    # Join from all workers
    final = sorted.collect()
    # Iterate and print
    for (word, count) in final:
        print("%s: %i" % (word, count))
    
    # Output into a single file on the s3 bucket
    sorted.coalesce(1).saveAsTextFile("s3a://demo/output/wordcount_submit")
    #word_counts.toDF().coalesce(1).write.text("s3a://demo/output/wordcount_submit")
    sc.stop()
