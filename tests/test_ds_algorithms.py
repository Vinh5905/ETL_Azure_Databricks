"""Kiểm tra DApriori và DATID trên các giao dịch đồ chơi."""

import random
from collections import Counter
from itertools import combinations

import pytest

from ml.fim.apriori_gen import apriori_gen
from ml.fim.dapriori import dapriori
from ml.fim.datid import datid


def _brute_force(rows, min_count, max_k=None):
    counts = Counter()
    for row in rows:
        for size in range(1, min(len(row), max_k or len(row)) + 1):
            counts.update(combinations(row, size))
    return {itemset: count for itemset, count in counts.items() if count >= min_count}


def test_apriori_gen_prunes_candidates():
    level = [(1, 2), (1, 3), (1, 4), (2, 3), (2, 4)]
    assert apriori_gen(level) == [(1, 2, 3), (1, 2, 4)]


def test_apriori_gen_rejects_unsorted_itemsets():
    with pytest.raises(ValueError):
        apriori_gen([(2, 1), (2, 3)])


@pytest.mark.parametrize("algorithm", [dapriori, datid])
@pytest.mark.parametrize("max_k", [None, 3])
def test_spark_fim_algorithms_match_exhaustive(spark, algorithm, max_k):
    rows = []
    expected = {}
    for seed in range(6):
        rng = random.Random(seed)
        offset = seed * 10
        sample = [
            tuple(sorted(offset + item for item in rng.sample(range(1, 7), rng.randint(1, 4)))) for _ in range(15)
        ]
        rows.extend(sample)
        expected.update(_brute_force(sample, 3, max_k))
    tx = spark.sparkContext.parallelize(rows, 2)
    assert algorithm(tx, min_count=3, max_k=max_k) == expected
