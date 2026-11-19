"""Kiểm tra thuật toán FIM và luật kết hợp trên dữ liệu nhỏ."""

import random
from collections import Counter
from itertools import combinations
from math import ceil

import pytest
from pyspark.sql import functions as F

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


def _seed_rows(seed):
    rng = random.Random(seed)
    n_items = rng.choice([8, 15, 30])
    return [tuple(sorted(rng.sample(range(1, n_items + 1), rng.randint(1, min(9, n_items))))) for _ in range(400)]


def test_apriori_gen_prunes_candidates():
    level = [(1, 2), (1, 3), (1, 4), (2, 3), (2, 4)]
    assert apriori_gen(level) == [(1, 2, 3), (1, 2, 4)]


def test_apriori_gen_rejects_unsorted_itemsets():
    with pytest.raises(ValueError):
        apriori_gen([(2, 1), (2, 3)])


def test_rule_metrics_and_single_consequent_filter(spark):
    freq = {(1,): 4, (2,): 3, (3,): 2, (1, 2): 2, (1, 3): 2, (2, 3): 2, (1, 2, 3): 2}
    rules = gen_rules(spark, freq, n_tx=5, min_conf=0.5).collect()
    rule = next(row for row in rules if row.antecedent == [1] and row.consequent == [2])
    assert rule.support == pytest.approx(0.4)
    assert rule.confidence == pytest.approx(0.5)
    assert rule.lift == pytest.approx(5 / 6)
    single = gen_rules(spark, freq, n_tx=5, min_conf=0.5, single_consequent=True).collect()
    assert all(len(row.consequent) == 1 for row in single)
    assert gen_rules(spark, {(1,): 4}, n_tx=5, min_conf=0.5).count() == 0
    with pytest.raises(ValueError, match="Thiếu support"):
        gen_rules(spark, {(1,): 4, (1, 2): 2}, n_tx=5, min_conf=0.5)


@pytest.mark.parametrize("seed", range(6))
def test_four_way_fim_and_rule_comparison(spark, seed):
    rng = random.Random(seed)
    rows = _seed_rows(seed)
    min_support = rng.choice([0.02, 0.05, 0.1])
    max_k = rng.choice([None, 3])
    min_count = ceil(min_support * len(rows))
    expected_freq = _brute_force(rows, min_count, max_k=max_k)
    tx = spark.sparkContext.parallelize(rows, 2)
    assert dapriori(tx, min_count=min_count, max_k=max_k) == expected_freq
    assert datid(tx, min_count=min_count, max_k=max_k) == expected_freq

    frame = spark.createDataFrame([(list(row),) for row in rows], ["items"])
    min_conf = 0.1
    model = fpgrowth(frame, min_support=min_support, min_conf=min_conf, num_partitions=2)
    fp_freq = {tuple(sorted(row.items)): row.freq for row in model.freqItemsets.collect()}
    actual_freq = {itemset: count for itemset, count in fp_freq.items() if max_k is None or len(itemset) <= max_k}
    assert actual_freq == expected_freq

    generated = gen_rules(spark, fp_freq, n_tx=len(rows), min_conf=min_conf, single_consequent=True).collect()
    mllib = model.associationRules.where(F.size("consequent") == 1).collect()
    generated_metrics = {
        (tuple(sorted(row.antecedent)), tuple(sorted(row.consequent))): (row.confidence, row.lift) for row in generated
    }
    mllib_metrics = {
        (tuple(sorted(row.antecedent)), tuple(sorted(row.consequent))): (row.confidence, row.lift) for row in mllib
    }
    assert generated_metrics.keys() == mllib_metrics.keys()
    for key, (confidence, lift) in generated_metrics.items():
        assert confidence == pytest.approx(mllib_metrics[key][0], abs=1e-9)
        assert lift == pytest.approx(mllib_metrics[key][1], abs=1e-9)
