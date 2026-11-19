"""Kiểm tra thuật toán FIM và luật kết hợp trên dữ liệu nhỏ."""

import random
from collections import Counter
from itertools import combinations

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


def _six_seed_rows():
    rows = []
    for seed in range(6):
        rng = random.Random(seed)
        offset = seed * 10
        rows.extend(
            tuple(sorted(offset + item for item in rng.sample(range(1, 7), rng.randint(1, 4)))) for _ in range(15)
        )
    return rows


def test_apriori_gen_prunes_candidates():
    level = [(1, 2), (1, 3), (1, 4), (2, 3), (2, 4)]
    assert apriori_gen(level) == [(1, 2, 3), (1, 2, 4)]


def test_apriori_gen_rejects_unsorted_itemsets():
    with pytest.raises(ValueError):
        apriori_gen([(2, 1), (2, 3)])


@pytest.mark.parametrize("algorithm", [dapriori, datid])
@pytest.mark.parametrize("max_k", [None, 3])
def test_spark_fim_algorithms_match_exhaustive(spark, algorithm, max_k):
    rows = _six_seed_rows()
    tx = spark.sparkContext.parallelize(rows, 2)
    assert algorithm(tx, min_count=3, max_k=max_k) == _brute_force(rows, 3, max_k)


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


def test_fpgrowth_and_single_consequent_rules_match_mllib(spark):
    rows = _six_seed_rows()
    frame = spark.createDataFrame([(list(row),) for row in rows], ["items"])
    min_count = 3
    min_conf = 0.1
    model = fpgrowth(frame, min_support=min_count / len(rows), min_conf=min_conf, num_partitions=2)
    expected_freq = _brute_force(rows, min_count, max_k=3)
    actual_freq = {
        tuple(sorted(row.items)): row.freq for row in model.freqItemsets.where(F.size("items") <= 3).collect()
    }
    assert actual_freq == expected_freq

    freq = dapriori(spark.sparkContext.parallelize(rows, 2), min_count=min_count, max_k=3)
    generated = gen_rules(spark, freq, n_tx=len(rows), min_conf=min_conf, single_consequent=True).collect()
    mllib = model.associationRules.where(F.size("consequent") == 1).collect()
    generated_metrics = {
        (tuple(sorted(row.antecedent)), tuple(sorted(row.consequent))): (row.confidence, row.lift) for row in generated
    }
    mllib_metrics = {
        (tuple(sorted(row.antecedent)), tuple(sorted(row.consequent))): (row.confidence, row.lift) for row in mllib
    }
    extra = generated_metrics.keys() - mllib_metrics.keys()
    missing = mllib_metrics.keys() - generated_metrics.keys()
    print("generated", len(generated_metrics), "mllib", len(mllib_metrics))
    print("extra", [(key, generated_metrics[key]) for key in list(extra)[:10]])
    print("missing", [(key, mllib_metrics[key]) for key in list(missing)[:10]])
    assert generated_metrics.keys() == mllib_metrics.keys()
    for key, (confidence, lift) in generated_metrics.items():
        assert confidence == pytest.approx(mllib_metrics[key][0], abs=1e-9)
        assert lift == pytest.approx(mllib_metrics[key][1], abs=1e-9)
