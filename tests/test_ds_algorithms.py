"""Kiểm tra đúng đắn các thuật toán DS trên các giao dịch đồ chơi."""

from collections import Counter
from itertools import combinations

import pytest

from ml.fim.apriori_gen import apriori_gen
from ml.fim.dapriori import dapriori
from ml.fim.datid import datid
from ml.fim.fpgrowth import fpgrowth
from ml.fim.rules import gen_rules


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
@pytest.mark.parametrize("max_k", [None, 2, 3])
def test_spark_fim_algorithms_match_exhaustive(spark, algorithm, max_k):
    rows = [(1, 2, 3), (1, 2), (1, 3), (2, 3), (1, 2, 3), (4,)]
    tx = spark.sparkContext.parallelize(rows, 2)
    assert algorithm(tx, min_count=2, max_k=max_k) == _brute_force(rows, 2, max_k)


def test_rule_metrics_and_single_consequent_filter(spark):
    freq = {(1,): 4, (2,): 3, (3,): 2, (1, 2): 2, (1, 3): 2, (2, 3): 2, (1, 2, 3): 2}
    rules = gen_rules(spark, freq, n_tx=5, min_conf=0.5).collect()
    rule = next(row for row in rules if row.antecedent == [1] and row.consequent == [2])
    assert rule.support == pytest.approx(0.4)
    assert rule.confidence == pytest.approx(0.5)
    assert rule.lift == pytest.approx(5 / 6)
    single = gen_rules(spark, freq, n_tx=5, min_conf=0.5, single_consequent=True).collect()
    assert all(len(row.consequent) == 1 for row in single)


def test_fpgrowth_matches_exhaustive_counts(spark):
    rows = [(1, 2, 3), (1, 2), (1, 3), (2, 3), (1, 2, 3), (4,)]
    frame = spark.createDataFrame([(list(row),) for row in rows], ["items"])
    model = fpgrowth(frame, min_support=0.34)
    actual = {tuple(sorted(row.items)): row.freq for row in model.freqItemsets.collect()}
    assert actual == _brute_force(rows, min_count=3)
