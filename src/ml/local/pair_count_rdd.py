"""Đếm cặp phổ biến bằng RDD trên Spark/YARN."""

import sys
from itertools import combinations
from operator import add

from pyspark.sql import SparkSession


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit("Cách dùng: pair_count_rdd.py <input> <output> <min_count>")
    input_path, output_path, min_count_raw = sys.argv[1:]
    min_count = int(min_count_raw)
    if min_count < 1:
        raise ValueError("min_count phải >= 1")
    spark = SparkSession.builder.appName("pair-count-rdd").getOrCreate()
    tx = spark.sparkContext.textFile(input_path).map(lambda line: tuple(int(value) for value in line.split())).cache()
    frequent = (
        tx.flatMap(lambda row: ((item, 1) for item in row))
        .reduceByKey(add)
        .filter(lambda pair: pair[1] >= min_count)
        .keys()
        .collect()
    )
    broadcast = spark.sparkContext.broadcast(frozenset(frequent))
    try:
        pairs = (
            tx.flatMap(
                lambda row: ((pair, 1) for pair in combinations([item for item in row if item in broadcast.value], 2))
            )
            .reduceByKey(add)
            .filter(lambda pair: pair[1] >= min_count)
        )
        pairs.map(lambda pair: f"{pair[0][0]},{pair[0][1]},{pair[1]}").saveAsTextFile(output_path)
    finally:
        tx.unpersist()
        broadcast.unpersist(blocking=True)
        spark.stop()


if __name__ == "__main__":
    main()
