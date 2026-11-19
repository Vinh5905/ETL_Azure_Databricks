"""Kiểm tra DApriori và DATID trên sáu bộ giao dịch ngẫu nhiên."""

import math
import random
from collections import Counter
from itertools import combinations

import pytest

from ml.fim.apriori_gen import apriori_gen
from ml.fim.dapriori import dapriori
from ml.fim.datid import datid


def brute(txs, min_count, max_k=None):
    counts = Counter(
        itemset
        for transaction in txs
        for size in range(1, min(max_k or len(transaction), len(transaction)) + 1)
        for itemset in combinations(transaction, size)
    )
    return {itemset: count for itemset, count in counts.items() if count >= min_count}


def test_apriori_gen_join_and_prune():
    assert apriori_gen([(1,), (2,), (3,)]) == [(1, 2), (1, 3), (2, 3)]
    assert apriori_gen([(1, 2), (1, 3), (2, 3), (2, 4)]) == [(1, 2, 3)]


@pytest.mark.parametrize("seed", range(6))
def test_all_algorithms_agree(spark, seed):
    rnd = random.Random(seed)
    n_items = rnd.choice([8, 15, 30])
    txs = [tuple(sorted(rnd.sample(range(n_items), rnd.randint(1, min(9, n_items))))) for _ in range(400)]
    min_support, max_k = rnd.choice([0.02, 0.05, 0.1]), rnd.choice([None, 3])
    min_count = math.ceil(min_support * len(txs))
    reference = brute(txs, min_count, max_k)
    rdd = spark.sparkContext.parallelize(txs, 4)
    assert dapriori(rdd, min_count, max_k) == reference
    assert datid(rdd, min_count, max_k) == reference


def test_dapriori_preserves_callers_cached_rdd(spark):
    rdd = spark.sparkContext.parallelize([(1, 2), (1, 2), (2, 3)], 2).cache()
    try:
        dapriori(rdd, min_count=2)
        assert rdd.is_cached
    finally:
        rdd.unpersist()
