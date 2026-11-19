"""So DApriori, DATID, FP-Growth với vét cạn và gen_rules với MLlib trên sáu bộ giao dịch ngẫu nhiên."""

import math
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
    assert apriori_gen([(1, 2), (1, 3), (1, 4), (2, 3), (2, 4)]) == [(1, 2, 3), (1, 2, 4)]


def test_apriori_gen_rejects_unsorted_itemsets():
    with pytest.raises(ValueError):
        apriori_gen([(2, 1), (2, 3)])


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

    # FP-Growth của MLlib là exhaustive: lọc size <= max_k phải trùng vét cạn.
    min_conf = 0.1
    frame = spark.createDataFrame([(list(t),) for t in txs], "items array<int>")
    model = fpgrowth(frame, min_support, min_conf, num_partitions=2)
    fp = {tuple(sorted(r["items"])): r["freq"] for r in model.freqItemsets.collect()}
    assert {s: n for s, n in fp.items() if max_k is None or len(s) <= max_k} == reference

    # Luật một vế phải của gen_rules phải trùng associationRules của MLlib, lệch < 1e-9.
    ours = {
        (tuple(sorted(r.antecedent)), tuple(r.consequent)): (r.confidence, r.lift)
        for r in gen_rules(spark, fp, len(txs), min_conf, single_consequent=True).collect()
    }
    mllib = {
        (tuple(sorted(r.antecedent)), tuple(r.consequent)): (r.confidence, r.lift)
        for r in model.associationRules.where(F.size("consequent") == 1).collect()
    }
    assert ours.keys() == mllib.keys()
    for key, (confidence, lift) in ours.items():
        assert confidence == pytest.approx(mllib[key][0], abs=1e-9)
        assert lift == pytest.approx(mllib[key][1], abs=1e-9)


def test_gen_rules_metrics_single_consequent_and_errors(spark):
    freq = {(1,): 4, (2,): 3, (3,): 2, (1, 2): 2, (1, 3): 2, (2, 3): 2, (1, 2, 3): 2}
    rules = gen_rules(spark, freq, n_tx=5, min_conf=0.5).collect()
    rule = next(r for r in rules if r.antecedent == [1] and r.consequent == [2])
    assert rule.support == pytest.approx(0.4)
    assert rule.confidence == pytest.approx(0.5)
    assert rule.lift == pytest.approx(5 / 6)
    assert any(len(r.consequent) == 2 for r in rules)
    single = gen_rules(spark, freq, n_tx=5, min_conf=0.5, single_consequent=True).collect()
    assert single and all(len(r.consequent) == 1 for r in single)
    # Không có tập >= 2 thì trả DataFrame rỗng đúng schema.
    empty = gen_rules(spark, {(1,): 4}, n_tx=5, min_conf=0.5)
    assert empty.count() == 0 and empty.columns == ["antecedent", "consequent", "support", "confidence", "lift"]
    # Thiếu tập con bậc k-1 (kể cả ở tập bậc 3) là lỗi dữ liệu.
    with pytest.raises(ValueError, match="Thiếu support"):
        gen_rules(spark, {(1,): 4, (1, 2): 2}, n_tx=5, min_conf=0.5)
    with pytest.raises(ValueError, match="Thiếu support"):
        gen_rules(spark, {k: v for k, v in freq.items() if k != (2, 3)}, n_tx=5, min_conf=0.5)


def test_dapriori_preserves_callers_cached_rdd(spark):
    rdd = spark.sparkContext.parallelize([(1, 2), (1, 2), (2, 3)], 2).cache()
    try:
        dapriori(rdd, min_count=2)
        assert rdd.is_cached
    finally:
        rdd.unpersist()
