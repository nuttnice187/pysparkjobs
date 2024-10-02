import sys

from pyspark.sql import DataFrame, SparkSession

from typing import List

def main(spark: SparkSession, *args: str) -> None:
    source = args[0]
    df = spark.read.json(source)
    df.show()
    
if __name__ == "__main__":
    spark: SparkSession= SparkSession.builder.master("local[*]").getOrCreate()
    args: List[str] = sys.argv[1:]
    main(spark, args)
