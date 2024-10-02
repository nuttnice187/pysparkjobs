import sys
from datetime import date, datetime
from pyspark.sql import Row, SparkSession
from typing import List

def main(spark: SparkSession, *args: str) -> None:
    df = spark.createDataFrame([
        Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
    ])
    df.show()
    print(args)
    
if __name__ == "__main__":
    spark: SparkSession= SparkSession.builder.master("local[*]").getOrCreate()
    args: List[str] = sys.argv
    main(spark, args)
