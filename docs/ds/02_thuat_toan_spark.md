# 02 · Thuật toán trên Spark: DApriori, DATID, sinh luật, FP-Growth

**Đầu vào:**
- RDD hoặc DataFrame giao dịch từ `src/ml/data.py` ([01 §2](01_du_lieu_thuc_nghiem.md)).
- Ngưỡng `min_support` (tỉ lệ), `min_confidence`, `max_k`.

**Đầu ra:**
- `dict[tuple[int, ...], int]`: tập phổ biến → số lần xuất hiện.
- DataFrame luật `(antecedent, consequent, support, confidence, lift)`.

**Xong khi:**
- Test ở mục 6 đạt: DApriori = DATID = đếm vét cạn = FP-Growth MLlib trên dữ liệu đồ chơi.
- Chạy được một lần ở 10% dữ liệu thật cho cả 3 cấp item (mốc DS-1).

> Code trong file này **đã chạy thử** với PySpark 3.5 local (`local[2]`). Kết quả: 6 bộ dữ liệu ngẫu nhiên, mọi so khớp ở mục 6 đều đạt. Khi chuyển vào `src/ml/fim/`, giữ nguyên logic và tách thành các file như [00 §8](00_tong_quan_ds.md).

---

## 1. Khái niệm và quy ước số

| Khái niệm | Định nghĩa | Trong code |
|---|---|---|
| Support của tập X | Tỉ lệ giao dịch chứa X: Supp(X) = count(X) / N | `freq[X] / n_tx` |
| MinSupp → ngưỡng đếm | `min_count = ceil(min_support × N)` | Giống hệt cách `FPGrowth` của Spark làm tròn, nên hai bên so được từng tập |
| Confidence của X → Y | Supp(X ∪ Y) / Supp(X) | `f[s] / f[x]` |
| Lift của X → Y | Conf(X → Y) / Supp(Y) | `conf / (f[y] / n_tx)` |
| Tính chất downward closure | Mọi tập con của một tập phổ biến đều phổ biến | Dùng để tỉa ứng viên trong `apriori_gen` |
| Giao dịch | `tuple[int, ...]` tăng dần, không trùng | DE đảm bảo ([de/08 §3](../de/08_du_lieu_cho_ds.md)) |

## 2. DApriori (theo bài báo, mục 3.2)

![Cơ chế DApriori và DATID trên Spark](diagrams/02_dapriori_datid.png)

<sub>File gốc: [diagrams/ds_diagrams.drawio](diagrams/ds_diagrams.drawio), trang 2.</sub>

**Hai pha, đúng như bài báo:**
1. **Pha 1:** `flatMap` tách item → `map` thành `<item, 1>` → `reduceByKey` → lọc theo `min_count`. Kết quả là L₁.
2. **Pha 2, lặp theo k:**
   - Sinh ứng viên Cₖ₊₁ từ Lₖ trên driver (`apriori_gen`).
   - **Broadcast Cₖ₊₁ dưới dạng hash set.**
   - `flatMap` mỗi giao dịch ra các ứng viên nó chứa → `reduceByKey` → lọc.
   - Dừng khi không còn ứng viên, hoặc khi k chạm `max_k`.

**Khác bài báo một chi tiết cài đặt** (ghi vào báo cáo): trước khi sinh tổ hợp, mỗi giao dịch được **lọc chỉ giữ item có mặt trong ứng viên** (biến broadcast `bc_items`).
- Không lọc thì một giỏ 145 món sinh C(145,3) ≈ 500 nghìn bộ ba ở k = 3, chỉ để tra hash set.
- Việc lọc này **không** thay đổi dữ liệu đã cache. DApriori vẫn quét lại giao dịch gốc ở mỗi bậc. Đó chính là điểm phân biệt với DATID.

```python
# src/ml/fim/apriori_gen.py
from itertools import combinations


def apriori_gen(level: list[tuple[int, ...]]) -> list[tuple[int, ...]]:
    """Ứng viên bậc k+1 từ tập phổ biến bậc k (tuple tăng dần): join theo tiền tố k-1, rồi tỉa."""
    prev = sorted(level)
    if not prev:
        return []
    k, seen, out, i = len(prev[0]), set(prev), [], 0
    while i < len(prev):
        j = i
        while j < len(prev) and prev[j][:-1] == prev[i][:-1]:
            j += 1
        block = prev[i:j]                                  # cùng tiền tố k-1
        for a in range(len(block)):
            for b in range(a + 1, len(block)):
                cand = block[a] + (block[b][-1],)
                if all(sub in seen for sub in combinations(cand, k)):   # downward closure
                    out.append(cand)
        i = j
    return out
```

```python
# src/ml/fim/_count.py  (dùng chung cho DApriori và DATID)
from itertools import combinations
from operator import add

from pyspark import RDD


def count_items(tx: RDD, min_count: int) -> list[tuple[int, int]]:
    """Pha 1: <item, 1> → reduceByKey → lọc theo MinSupp."""
    return (tx.flatMap(lambda t: ((i, 1) for i in t)).reduceByKey(add)
              .filter(lambda kv: kv[1] >= min_count).collect())


def count_level(tx: RDD, cands: list[tuple[int, ...]], k: int, min_count: int) -> list[tuple[tuple[int, ...], int]]:
    """Một lượt MapReduce: flatMap các ứng viên có trong giao dịch → reduceByKey → lọc."""
    sc = tx.context
    bc_cands = sc.broadcast(frozenset(cands))                            # hash table ứng viên
    bc_items = sc.broadcast(frozenset(i for c in cands for i in c))

    def emit(t):
        items = [i for i in t if i in bc_items.value]                    # chỉ giữ item nằm trong ứng viên
        if len(items) >= k:
            cs = bc_cands.value
            for c in combinations(items, k):
                if c in cs:
                    yield c, 1

    out = tx.flatMap(emit).reduceByKey(add).filter(lambda kv: kv[1] >= min_count).collect()
    bc_cands.unpersist()
    bc_items.unpersist()
    return out
```

```python
# src/ml/fim/dapriori.py
from pyspark import RDD, StorageLevel

from ml.fim._count import count_items, count_level
from ml.fim.apriori_gen import apriori_gen


def dapriori(tx: RDD, min_count: int, max_k: int | None = None) -> dict[tuple[int, ...], int]:
    """DApriori: mỗi bậc k quét lại toàn bộ giao dịch gốc (đã cache)."""
    tx = tx.persist(StorageLevel.MEMORY_AND_DISK)
    freq = {(i,): c for i, c in count_items(tx, min_count)}              # pha 1
    level, k = sorted(freq), 1
    while level and (max_k is None or k < max_k):                        # pha 2
        cands = apriori_gen(level)
        if not cands:
            break
        k += 1
        lk = count_level(tx, cands, k, min_count)
        freq.update(lk)
        level = sorted(c for c, _ in lk)
    tx.unpersist()
    return freq
```

## 3. DATID (theo bài báo, mục 3.3)

**Khác DApriori ở đúng chỗ bài báo mô tả:** sau pha 1, **bỏ item không phổ biến khỏi mọi giao dịch** (biến broadcast `Global_FreqItemset`), rồi cache bản đã thu gọn. Pha 2 đếm trên bản thu gọn này.

**Thêm theo tinh thần Apriori-TID:** sau mỗi bậc, tiếp tục bỏ item không nằm trong Lₖ và bỏ giao dịch quá ngắn (ít hơn k+1 món). Dữ liệu cache **nhỏ dần qua từng bậc**. Bài báo giải thích lợi thế bộ nhớ của DATID chính bằng cơ chế này.

**Thứ tự theo support:** bài báo sắp item theo support giảm dần. DE đã đánh `idx` theo tần suất giảm dần ([00 §6.3](00_tong_quan_ds.md)), nên giữ thứ tự `idx` tăng dần **chính là** sắp theo support. Không cần broadcast thêm bảng thứ hạng.

```python
# src/ml/fim/datid.py
from pyspark import RDD, StorageLevel

from ml.fim._count import count_items, count_level
from ml.fim.apriori_gen import apriori_gen


def datid(tx: RDD, min_count: int, max_k: int | None = None) -> dict[tuple[int, ...], int]:
    """DATID: sau pha 1 bỏ item không phổ biến khỏi giao dịch; mỗi bậc tiếp tục thu gọn dữ liệu đã cache."""
    sc = tx.context
    l1 = count_items(tx, min_count)
    freq = {(i,): c for i, c in l1}
    bc_f1 = sc.broadcast(frozenset(i for i, _ in l1))                    # Global_FreqItemset
    cur = (tx.map(lambda t: tuple(i for i in t if i in bc_f1.value))     # idx tăng dần = support giảm dần
             .filter(lambda t: len(t) >= 2)
             .persist(StorageLevel.MEMORY_AND_DISK))
    level, k = sorted(freq), 1
    while level and (max_k is None or k < max_k):
        cands = apriori_gen(level)
        if not cands:
            break
        k += 1
        lk = count_level(cur, cands, k, min_count)
        freq.update(lk)
        level = sorted(c for c, _ in lk)
        if level and (max_k is None or k < max_k):                       # thu gọn cho bậc sau
            bc_lk = sc.broadcast(frozenset(i for c in level for i in c))
            nxt = (cur.map(lambda t: tuple(i for i in t if i in bc_lk.value))
                      .filter(lambda t, n=k + 1: len(t) >= n)
                      .persist(StorageLevel.MEMORY_AND_DISK))
            nxt.count()                                                  # vật chất hóa trước khi bỏ bản cũ
            cur.unpersist()
            cur = nxt
    cur.unpersist()
    return freq
```

## 4. Sinh luật kết hợp (theo bài báo, mục 4)

Bài báo cho `map` trên từng partition sinh mọi luật từ danh sách tập phổ biến kèm confidence, rồi `reduce` theo khóa luật để gom kết quả.

Cách làm ở đây: mỗi tập phổ biến S (|S| ≥ 2) sinh **mọi** cách tách S = X ∪ Y. Mỗi luật chỉ sinh ra **đúng một lần** từ đúng tập S của nó, nên bước `reduce` gom trùng của bài báo là không cần. Bảng `freq` được broadcast để tra support của X và Y.

```python
# src/ml/fim/rules.py
from itertools import combinations

from pyspark.sql import DataFrame, SparkSession

RULE_SCHEMA = "antecedent array<int>, consequent array<int>, support double, confidence double, lift double"


def gen_rules(spark: SparkSession, freq: dict[tuple[int, ...], int], n_tx: int, min_conf: float,
              single_consequent: bool = False) -> DataFrame:
    """Sinh luật X → Y từ mọi tập phổ biến (|X ∪ Y| ≥ 2). support, confidence, lift tính từ freq đã broadcast."""
    sc = spark.sparkContext
    bc = sc.broadcast(freq)
    itemsets = [s for s in freq if len(s) >= 2]

    def rules_of(s):
        f = bc.value
        for r in range(1, len(s)):
            for x in combinations(s, r):
                y = tuple(i for i in s if i not in x)
                if single_consequent and len(y) != 1:
                    continue
                conf = f[s] / f[x]
                if conf >= min_conf:
                    yield list(x), list(y), f[s] / n_tx, conf, conf / (f[y] / n_tx)

    if not itemsets:
        return spark.createDataFrame([], RULE_SCHEMA)
    rdd = sc.parallelize(itemsets, max(1, min(len(itemsets) // 2000, 64))).flatMap(rules_of)
    return spark.createDataFrame(rdd, RULE_SCHEMA)
```

**`single_consequent`** chỉ dùng khi so với MLlib: `associationRules` của Spark **chỉ sinh luật có vế phải 1 item**. Khi so số luật giữa các thuật toán (E1, E2):
- So **luật một vế phải** cho cả ba thuật toán.
- Báo cáo thêm **tổng số luật mọi dạng** chỉ cho DApriori/DATID, vì MLlib không có số này.

## 5. FP-Growth (Spark MLlib, Parallel FP-Growth)

```python
# src/ml/fim/fpgrowth.py
from pyspark.ml.fpm import FPGrowth, FPGrowthModel
from pyspark.sql import DataFrame


def fpgrowth(df: DataFrame, min_support: float, min_conf: float, num_partitions: int | None = None) -> FPGrowthModel:
    """df(items array<int>). model.freqItemsets(items, freq); model.associationRules(antecedent, consequent,
    confidence, lift, support). minCount = ceil(minSupport × N), giống min_count của DApriori."""
    fp = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_conf)
    if num_partitions:
        fp = fp.setNumPartitions(num_partitions)
    return fp.fit(df)
```

**Ba điểm phải ghi rõ khi so sánh:**
1. **FP-Growth không có `max_k`.** Ở cấp product (k ≤ 3), FP-Growth vẫn tìm cả tập dài hơn, nên thời gian của nó tính cả phần đó. Khi so **kết quả**, lọc `size(items) <= 3`. Khi so **thời gian**, ghi chú sự khác biệt này. Thực tế ở cấp product với MinSupp ≥ 0,001, tập dài hơn 3 rất ít.
2. **Tính exhaustive.** Bài báo nói PFP chỉ trả top-Q tập dài nhất. Test ở mục 6 cho thấy `FPGrowth` của Spark 3.5 trả về **mọi** tập ≥ minSupport, khớp đúng từng tập với đếm vét cạn. E1 lặp lại phép so này trên dữ liệu thật. Đây là một kết luận nên nêu trong báo cáo: nhận định của bài báo có thể đúng với bản PFP gốc dùng cho gợi ý truy vấn, nhưng không đúng với MLlib hiện tại.
3. **`numPartitions`** là tham số cấu hình riêng của FP-Growth, nằm trong lưới thực nghiệm (bản đầu của hình kiến trúc ghi "cấu hình: shuffle partitions, cache"). Mặc định bằng số partition của input.

## 6. Kiểm tra đúng đắn

### 6.1 Unit test trên dữ liệu đồ chơi (CI, máy local)

```python
# tests/ml/test_fim.py  (đã chạy thử: 6/6 bộ dữ liệu đạt)
import math
import random
from collections import Counter
from itertools import combinations

import pytest

from ml.fim.apriori_gen import apriori_gen
from ml.fim.dapriori import dapriori
from ml.fim.datid import datid
from ml.fim.fpgrowth import fpgrowth
from ml.fim.rules import gen_rules


def brute(txs, min_count, max_k=None):
    c = Counter(s for t in txs for k in range(1, (max_k or len(t)) + 1) for s in combinations(t, k))
    return {s: n for s, n in c.items() if n >= min_count}


def test_apriori_gen_join_and_prune():
    assert apriori_gen([(1,), (2,), (3,)]) == [(1, 2), (1, 3), (2, 3)]
    assert apriori_gen([(1, 2), (1, 3), (2, 3), (2, 4)]) == [(1, 2, 3)]      # (2,3,4) bị tỉa vì thiếu (3,4)


@pytest.mark.parametrize("seed", range(6))
def test_all_algorithms_agree(spark, seed):
    rnd = random.Random(seed)
    n_items = rnd.choice([8, 15, 30])
    txs = [tuple(sorted(rnd.sample(range(n_items), rnd.randint(1, min(9, n_items))))) for _ in range(400)]
    min_support, max_k = rnd.choice([0.02, 0.05, 0.1]), rnd.choice([None, 3])
    min_count = math.ceil(min_support * len(txs))
    ref = brute(txs, min_count, max_k)

    rdd = spark.sparkContext.parallelize(txs, 4)
    assert dapriori(rdd, min_count, max_k) == ref
    assert datid(rdd, min_count, max_k) == ref

    model = fpgrowth(spark.createDataFrame([(list(t),) for t in txs], "items array<int>"), min_support, 0.3)
    fp = {tuple(sorted(r["items"])): r["freq"] for r in model.freqItemsets.collect()}
    assert {s: n for s, n in fp.items() if max_k is None or len(s) <= max_k} == ref    # MLlib là exhaustive

    ours = {(tuple(r.antecedent), tuple(r.consequent)): round(r.lift, 9)
            for r in gen_rules(spark, fp, len(txs), 0.3, single_consequent=True).collect()}
    mllib = {(tuple(sorted(r.antecedent)), tuple(r.consequent)): round(r.lift, 9)
             for r in model.associationRules.collect()}
    assert ours == mllib
```

- Dùng fixture `spark` có sẵn trong `tests/conftest.py` (`local[1]`, tắt UI). `pythonpath = ["src"]` trong `pyproject.toml` nên import được `ml.fim.*`.
- `pyspark.ml` cần **numpy**. Phải thêm `numpy` vào nhóm dev của `pyproject.toml`; nếu không, CI báo `ModuleNotFoundError` ([devops/04](../devops/04_github_actions_cicd.md)).

### 6.2 Kiểm tra trên dữ liệu thật (thực nghiệm E1)

Ở 10% dữ liệu, với mỗi cấp item và một MinSupp trung bình:

| So sánh | Đạt khi |
|---|---|
| Tập phổ biến của DApriori và DATID | **Bằng nhau từng tập**, cùng `freq` |
| FP-Growth (lọc `size ≤ max_k`) và DApriori | Bằng nhau từng tập |
| Luật một vế phải: MLlib và `gen_rules(..., single_consequent=True)` | Cùng tập luật; confidence và lift lệch < 1e-9 |
| Support của item bậc 1 và `ds_data.item_index.freq_prior` (ở 100%) | Bằng nhau |

Kết quả so khớp ghi vào `ml.experiment_runs` (`experiment = 'E1'`, cột `check_passed`). Không đạt thì **dừng lưới** cho tới khi tìm ra nguyên nhân.

## 7. Lưu ý hiệu năng (đo ở mốc DS-1)

| Rủi ro | Dấu hiệu | Cách xử lý |
|---|---|---|
| PySpark RDD chậm vì mỗi bản ghi đi qua worker Python | DApriori ở 100% product mất > 30 phút | Chạy lưới product ở 25%, chỉ vài điểm ở 100%. Ghi rõ trong báo cáo: Python RDD chậm hơn Scala, đúng như khác biệt ngôn ngữ của các bài cài đặt Apriori |
| Broadcast ứng viên quá lớn | OOM ở driver hoặc worker khi n(C₂) vượt vài triệu | Nâng MinSupp tối thiểu ([01 §3](01_du_lieu_thuc_nghiem.md)). **Không** tăng RAM bằng máy to hơn, vì policy cố định `Standard_D4ds_v4` (4 vCPU, 16 GB) |
| Bùng nổ tổ hợp ở giỏ dài | Một task chạy rất lâu (skew) trong pha k = 3 | Đã có lọc `bc_items`; nếu vẫn chậm thì ghi số liệu này làm bằng chứng cho lập luận về giới hạn k |
| Exhaustive ở cấp department | Số tập phổ biến lên hàng chục nghìn khi MinSupp = 0,05 | Đây là **kết quả cần có** (tái hiện đường hàm mũ của bài báo). Timeout ghi `status = 'timeout'`, không coi là lỗi |

**Phương án dự phòng (chỉ dùng khi DS-1 cho thấy RDD không chạy nổi lưới):** cài thêm bản `dapriori_df` dùng DataFrame (`explode` + join với DataFrame ứng viên đã broadcast). Bản này chạy hoàn toàn trên JVM. Nó là biến thể thứ tư trong lưới, **không thay** bản RDD, vì bản RDD mới là bản bám bài báo.
