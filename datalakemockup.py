import sys
from pyspark.sql import SparkSession
from typing import List

def main(spark: SparkSession, *args: str):
    print(args)
    
if __name__ == "__main__":
    spark: SparkSession= SparkSession.builder.master("local[*]").getOrCreate()
    args: List[str] = sys.argv
    main(spark, args)
