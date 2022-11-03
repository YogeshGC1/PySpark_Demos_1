from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("Demo PySpark Application Started")

    spark=SparkSession \
          .builder \
          .appName("WordCount") \
          .master("local[*]") \
          .getOrCreate()

    sc=spark.sparkContext

    input_file_path="C:/Users/NEW/Desktop/project/source/WordCount.txt"
    text_file=sc.textFile(input_file_path)
    Counts=text_file.flatMap(lambda line: line.split(" ")) \
                    .map(lambda word: (word, 1)) \
                    .reduceByKey(lambda x, y:x+y)

    output=Counts.collect()
    for (word,Counts) in output:
        print("%s:%i"%(word,Counts))


# Working Source code of word count program in pyspark