import sys

from pyspark import SparkContext, SparkFiles

from pyspark.sql import DataFrameWriter, Row, SparkSession

from pyspark.sql.functions import monotonically_increasing_id

from typing import List

def main(spark: SparkSession, *args: str) -> None:
    URL, destination, mode = args[0]
    sc: SparkContext= spark.sparkContext
    sc.addFile(URL)
    file_name: str= SparkFiles.get('weight-height.csv')
    spark.read.csv(file_name, header=True, inferSchema=True) \
        .select(monotonically_increasing_id().alias('Id'), '*') \
        .write \
        .partitionBy('Id') \
        .json(destination, mode)
    
if __name__ == "__main__":
    spark: SparkSession= SparkSession.builder.master("local[*]").getOrCreate()
    args: List[str] = sys.argv[1:]
    main(spark, args)
