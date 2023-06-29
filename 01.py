import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession
             .builder
             .appName("test")
             .getOrCreate())
    file = sys.argv[1]
    df = (spark.read.format('csv')
          .option("header", "true")
          .option("inferSchema", "true")
          .load(file))
    df.show()
    spark.stop()