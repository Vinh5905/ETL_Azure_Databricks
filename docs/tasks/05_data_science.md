# Phân việc · Data Science (`AD-DS-xx`)

27 task. Quy ước, trạng thái và bảng tổng ở [README](README.md). Vai ghi theo P1–P4, người thật được gán trên GitHub Project.

| Mã | Việc | Vai | Cổng | Phụ thuộc | Issue |
|---|---|---|---|---|---|
| [AD-DS-01](#ad-ds-01) | Cài apriori_gen, DApriori và DATID trên RDD kèm test vét cạn | P4 | G0 | Chuẩn bị ngay | [#86](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/86) |
| [AD-DS-02](#ad-ds-02) | Cài sinh luật gen_rules và bọc FP-Growth MLlib, test so khớp với MLlib | P4 | G0 | Chuẩn bị ngay; Hoàn tất: AD-DS-01 | [#87](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/87) |
| [AD-DS-03](#ad-ds-03) | Viết hàm đọc dữ liệu thực nghiệm data.py và bộ đo measure.py có test local | P4 | G0 | Chuẩn bị ngay | [#88](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/88) |
| [AD-DS-04](#ad-ds-04) | Dựng cụm Hadoop HDFS + YARN + Spark on YARN bằng Docker ở local | P3 | G0 | Chuẩn bị ngay | [#89](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/89) |
| [AD-DS-05](#ad-ds-05) | Viết chương trình MapReduce Java ItemCount và PairCount kèm test MiniMRYarnCluster | P3 | G0 | Chuẩn bị ngay | [#90](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/90) |
| [AD-DS-06](#ad-ds-06) | Viết đếm cặp Spark on YARN bản DataFrame và RDD, đối chiếu với MapReduce | P3 | G0 | Bắt đầu: AD-DS-04; Hoàn tất: AD-DS-05 | [#91](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/91) |
| [AD-DS-07](#ad-ds-07) | Viết script tải dữ liệu export về máy và nạp lên HDFS local | P3 | G5 | Bắt đầu: AD-DS-04; Hoàn tất: AD-DE-21 | [#92](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/92) |
| [AD-DS-08](#ad-ds-08) | Viết bench.py chạy lưới E6 và script upload kết quả lên ds/inbox | P3 | G5 | Bắt đầu: AD-DS-06 | [#93](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/93) |
| [AD-DS-09](#ad-ds-09) | Định nghĩa lưới experiments.yml và viết plan_grid.py bung tổ hợp theo run_key | P4 | G5 | Chuẩn bị ngay | [#94](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/94) |
| [AD-DS-10](#ad-ds-10) | Viết run_experiment.py chạy một tổ hợp, ghi ml.experiment_runs và MLflow | P4 | G5 | Bắt đầu: AD-DS-01, AD-DS-02, AD-DS-03, AD-DS-09 | [#95](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/95) |
| [AD-DS-11](#ad-ds-11) | Khám phá dữ liệu ds_data v1 và chốt lưới MinSupport theo cấp item | P4 | G3 | Bắt đầu: AD-DE-21; Hoàn tất: AD-DS-09 | [#96](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/96) |
| [AD-DS-12](#ad-ds-12) | Khai báo job ds-benchmark trong DAB và viết ingest_local.sql | P4 | G5 | Chuẩn bị ngay; Hoàn tất: AD-DS-08, AD-DS-10, AD-OPS-30, AD-OPS-22 | [#97](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/97) |
| [AD-DS-13](#ad-ds-13) | Chạy spike DS-1 và thực nghiệm E1 kiểm đúng đắn trên dữ liệu thật | P4 | G5 | Bắt đầu: AD-DS-11, AD-DS-12 | [#98](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/98) |
| [AD-DS-14](#ad-ds-14) | Chạy toàn bộ lưới E2–E5 trên Databricks | P4 | G5 | Bắt đầu: AD-DS-13 | [#99](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/99) |
| [AD-DS-15](#ad-ds-15) | Chạy lưới E6 MapReduce và Spark on YARN ở local, nạp kết quả lên ml.experiment_runs | P3 | G5 | Bắt đầu: AD-DS-07, AD-DS-08; Hoàn tất: AD-DS-12 | [#100](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/100) |
| [AD-DS-16](#ad-ds-16) | Cài AprioriMR lặp k ≤ 3 trên MapReduce và DApriori trên YARN (mở rộng) | P3 | G5 | Bắt đầu: AD-DS-15 | [#101](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/101) |
| [AD-DS-17](#ad-ds-17) | Dựng demo pipeline HDFS + Spark dựng giỏ prior từ CSV gốc (tùy chọn) | P4 | G5 | Bắt đầu: AD-DS-04; Hoàn tất: AD-DE-17 | [#102](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/102) |
| [AD-DS-18](#ad-ds-18) | Viết đánh giá luật trên holdout evaluate_holdout.py và baseline phổ biến | P4 | G5 | Chuẩn bị ngay | [#103](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/103) |
| [AD-DS-19](#ad-ds-19) | Viết model pyfunc BasketRules gợi ý món từ giỏ hàng kèm test | P4 | G5 | Chuẩn bị ngay; Hoàn tất: AD-DS-18 | [#104](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/104) |
| [AD-DS-20](#ad-ds-20) | Chạy thực nghiệm E7 và đánh giá luật trên holdout val/test | P4 | G5 | Bắt đầu: AD-DS-13, AD-DS-18 | [#105](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/105) |
| [AD-DS-21](#ad-ds-21) | Chọn champion, đăng ký model basket_rules vào Unity Catalog và khai báo job ds-champion | P4 | G5 | Bắt đầu: AD-DS-19, AD-DS-20 | [#106](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/106) |
| [AD-DS-22](#ad-ds-22) | Viết view gold cho DS: association_rules, recommendations, experiment_metrics, holdout_metrics | P4 | G5 | Chuẩn bị ngay; Hoàn tất: AD-DS-14, AD-DS-15, AD-DS-21, AD-OPS-20 | [#107](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/107) |
| [AD-DS-23](#ad-ds-23) | Khai báo Model Serving endpoint instacart-basket-rules trong DAB và kiểm API | P4 | G5 | Bắt đầu: AD-DS-21, AD-OPS-18 | [#108](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/108) |
| [AD-DS-24](#ad-ds-24) | Viết Azure Function recommend gọi endpoint, có phương án B tự tra luật | P4 | G5 | Chuẩn bị ngay; Hoàn tất: AD-DS-21, AD-DS-23, AD-OPS-31 | [#109](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/109) |
| [AD-DS-25](#ad-ds-25) | Làm Power BI trang 6 luật kết hợp và trang 7 thực nghiệm thuật toán | P4 | G5 | Bắt đầu: AD-DS-22, AD-BI-11, AD-BI-09 | [#110](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/110) |
| [AD-DS-26](#ad-ds-26) | Phân tích kết quả thực nghiệm và viết phần DS của báo cáo, đối chiếu bài báo | P4 | G5 | Bắt đầu: AD-DS-14, AD-DS-15, AD-DS-20; Hoàn tất: AD-DS-21 | [#111](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/111) |
| [AD-DS-27](#ad-ds-27) | Đối chiếu checklist G5 và chốt bằng chứng phần DS | P4 | G5 | Bắt đầu: AD-DS-25, AD-DS-26; Hoàn tất: AD-DS-23, AD-DS-24 | [#112](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/112) |

### AD-DS-01

**Cài apriori_gen, DApriori và DATID trên RDD kèm test vét cạn**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G0**
- Issue: [#86](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/86)
- Tài liệu: `docs/ds/00_tong_quan_ds.md §2, §8`, `docs/ds/02_thuat_toan_spark.md §1–3, §6.1`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DS-02](05_data_science.md#ad-ds-02) (hoàn tất), [AD-DS-10](05_data_science.md#ad-ds-10) (bắt đầu)

Lõi thuật toán bám bài báo, viết bằng RDD. Làm và kiểm hoàn toàn ở máy local với dữ liệu đồ chơi, không cần Azure.

**Việc cần làm**

1. Tạo package src/ml/__init__.py, src/ml/fim/__init__.py, tests/ml/__init__.py
2. Thêm numpy vào nhóm dev của pyproject.toml (pyspark.ml cần), uv sync
3. Viết src/ml/fim/apriori_gen.py (join theo tiền tố k-1, tỉa downward closure)
4. Viết src/ml/fim/_count.py: count_items, count_level (broadcast bc_cands, bc_items)
5. Viết src/ml/fim/dapriori.py và src/ml/fim/datid.py (thu gọn giao dịch sau mỗi bậc)
6. Viết tests/ml/test_fim.py: test apriori_gen, hàm brute() và so DApriori = DATID = vét cạn trên 6 bộ ngẫu nhiên, max_k None và 3

**Đầu ra**

- src/ml/fim/apriori_gen.py, _count.py, dapriori.py, datid.py
- tests/ml/test_fim.py (phần apriori_gen, DApriori, DATID)
- pyproject.toml có numpy trong dev

**Xong khi**

- [ ] uv run pytest -q tests/ml pass: DApriori và DATID bằng đếm vét cạn từng tập trên 6 seed
- [ ] test_apriori_gen_join_and_prune pass (ứng viên (2,3,4) bị tỉa)
- [ ] uv run ruff check . và ruff format --check sạch

### AD-DS-02

**Cài sinh luật gen_rules và bọc FP-Growth MLlib, test so khớp với MLlib**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G0**
- Issue: [#87](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/87)
- Tài liệu: `docs/ds/02_thuat_toan_spark.md §4–6`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DS-01](05_data_science.md#ad-ds-01) [#86](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/86)
- **Mở khóa:** [AD-DS-10](05_data_science.md#ad-ds-10) (bắt đầu)

Sinh luật (support, confidence, lift) phân tán và baseline PFP của bài báo. Test 4 bên chỉ hoàn tất khi đã có DApriori/DATID.

**Việc cần làm**

1. Viết src/ml/fim/rules.py: gen_rules(spark, freq, n_tx, min_conf, single_consequent) với RULE_SCHEMA
2. Viết src/ml/fim/fpgrowth.py: bọc pyspark.ml.fpm.FPGrowth, tham số num_partitions
3. Bổ sung test_all_algorithms_agree: FP-Growth (lọc size ≤ max_k) = vét cạn; luật một vế phải của gen_rules = associationRules của MLlib (lift làm tròn 1e-9)
4. Thêm test gen_rules trả DataFrame rỗng khi không có tập ≥ 2

**Đầu ra**

- src/ml/fim/rules.py, src/ml/fim/fpgrowth.py
- tests/ml/test_fim.py đủ phép so 4 bên như docs/ds/02 §6.1

**Xong khi**

- [ ] uv run pytest -q tests/ml pass 6/6 seed, gồm phép so FP-Growth = vét cạn (MLlib exhaustive trên dữ liệu đồ chơi)
- [ ] Tập luật một vế phải của gen_rules trùng MLlib, lift lệch < 1e-9

### AD-DS-03

**Viết hàm đọc dữ liệu thực nghiệm data.py và bộ đo measure.py có test local**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G0**
- Issue: [#88](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/88)
- Tài liệu: `docs/ds/01_du_lieu_thuc_nghiem.md §1–2`, `docs/ds/04_thuc_nghiem_danh_gia.md §4`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DS-10](05_data_science.md#ad-ds-10) (bắt đầu)

Hai hàm dùng chung cho mọi lần chạy: lọc giao dịch theo grain/scale/top_n và đo thời gian, bộ nhớ đỉnh, timeout. Kiểm được bằng bảng giả local trước khi có ds_data.

**Việc cần làm**

1. Viết src/ml/data.py: load_transactions (scale_bucket lồng nhau, 200% nhân đôi id âm, lọc idx < top_n, bỏ giỏ rỗng), to_rdd; không cache
2. Viết src/ml/measure.py: PeakRss (psutil, mẫu 0,5 s, peak − baseline), time_limit (setJobGroup + cancelJobGroup)
3. Thêm psutil vào dependency nếu chưa có
4. Tạo fixture tests/fixtures/ds_transactions nhỏ; test load_transactions với scale 10/100/200 và top_n
5. Test time_limit hủy job chạy quá hạn và cờ expired bật; test PeakRss trả delta ≥ 0

**Đầu ra**

- src/ml/data.py, src/ml/measure.py
- tests/ml/test_data.py, tests/ml/test_measure.py, fixture nhỏ

**Xong khi**

- [ ] uv run pytest -q tests/ml pass
- [ ] Test chứng minh tập scale 10 nằm trọn trong scale 25 và scale 200 có đúng 2 lần số giỏ của 100

### AD-DS-04

**Dựng cụm Hadoop HDFS + YARN + Spark on YARN bằng Docker ở local**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G0**
- Issue: [#89](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/89)
- Tài liệu: `docs/ds/03_hadoop_mapreduce_local.md §1–2`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DS-06](05_data_science.md#ad-ds-06) (bắt đầu), [AD-DS-07](05_data_science.md#ad-ds-07) (bắt đầu), [AD-DS-17](05_data_science.md#ad-ds-17) (bắt đầu)

Cụm local thay HDFS/Hadoop của đề tài gốc (không có quota trên Azure). Là nền cho mọi phép so MapReduce và Spark ở E6. Phần Hadoop do P3 làm cùng P4 (docs/00 §7); thẻ giao cho P3, P4 nghiệm thu.

**Việc cần làm**

1. Viết local/hadoop/Dockerfile (Hadoop 3.3.6 + Spark 3.5.3 + Python 3 + Java 11, kiểm SHA-512)
2. Viết conf/core-site.xml, hdfs-site.xml, yarn-site.xml, mapred-site.xml, spark-defaults.conf (AQE tắt, maxPartitionBytes 8m, event log)
3. Viết entrypoint.sh và docker-compose.yml: namenode, 2 datanode, resourcemanager, nodemanager 8 vcores/12 GB, historyserver, spark-history, client
4. Thêm data/ vào .gitignore nếu chưa có; ghi cấu hình Docker Desktop tối thiểu (≥ 8 CPU, ≥ 16 GB)
5. Chạy kiểm tra docs/ds/03 §2.5

**Đầu ra**

- Thư mục local/hadoop/ đủ Dockerfile, compose, conf/*.xml, spark-defaults.conf, entrypoint.sh
- Ảnh chụp HDFS UI, YARN UI cho báo cáo

**Xong khi**

- [ ] docker compose up -d --build chạy được; hdfs dfsadmin -report thấy 2 DataNode Live
- [ ] yarn node -list thấy 1 NodeManager 8 vcores; job pi mẫu của Hadoop chạy xong
- [ ] spark-submit --master yarn một job mẫu chạy xong và hiện trên Spark History :18080

### AD-DS-05

**Viết chương trình MapReduce Java ItemCount và PairCount kèm test MiniMRYarnCluster**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G0**
- Issue: [#90](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/90)
- Tài liệu: `docs/ds/03_hadoop_mapreduce_local.md §4.1–4.3`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DS-06](05_data_science.md#ad-ds-06) (hoàn tất)

Chương trình thống kê bằng Hadoop MapReduce của đề tài: hai pha đầu Apriori (k = 2), khóa cặp nén thành long. Build bằng image Maven, không cần cụm. Phần Hadoop do P3 làm cùng P4 (docs/00 §7); thẻ giao cho P3, P4 nghiệm thu.

**Việc cần làm**

1. Tạo src/mapreduce/pom.xml (Java 11, Hadoop 3.3.6, hadoop-minicluster cho test)
2. Viết instacart.mr.ItemCount (lọc count ≥ min_count) và instacart.mr.PairCount (distributed cache item phổ biến, cặp a < b nén (a << 32) | b)
3. Dùng ToolRunner để nhận -D mapreduce.job.running.map.limit/reduce.limit
4. Viết PairCountTest.java trên MiniMRYarnCluster với 5 giỏ đồ chơi, bảng cặp kỳ vọng viết tay
5. Đảm bảo jar không vào git (target/ trong .gitignore)

**Đầu ra**

- src/mapreduce/ (pom.xml, ItemCount.java, PairCount.java, PairCountTest.java)
- Jar src/mapreduce/target/instacart-mr-1.0.jar build được local

**Xong khi**

- [ ] docker run maven:3.9-eclipse-temurin-11 mvn -B verify pass
- [ ] PairCountTest ra đúng bảng cặp viết tay

### AD-DS-06

**Viết đếm cặp Spark on YARN bản DataFrame và RDD, đối chiếu với MapReduce**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G0**
- Issue: [#91](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/91)
- Tài liệu: `docs/ds/03_hadoop_mapreduce_local.md §5`
- **Bắt đầu sau:** [AD-DS-04](05_data_science.md#ad-ds-04) [#89](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/89)
- **Hoàn tất còn cần:** [AD-DS-05](05_data_science.md#ad-ds-05) [#90](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/90)
- **Mở khóa:** [AD-DS-08](05_data_science.md#ad-ds-08) (bắt đầu)

Hai bản Spark cùng logic với PairCount: DataFrame (thuần JVM, so ngang Java) và RDD Python (giống DApriori). Là điều kiện để E6 so công bằng. Phần Hadoop do P3 làm cùng P4 (docs/00 §7); thẻ giao cho P3, P4 nghiệm thu.

**Việc cần làm**

1. Viết src/ml/local/pair_count_df.py (split, explode, array_intersect item phổ biến, cặp bằng transform/slice)
2. Viết src/ml/local/pair_count_rdd.py (textFile, broadcast item phổ biến, combinations 2)
3. Mount src/ml/local vào container client; tạo file giỏ đồ chơi trên HDFS (block 8m)
4. Chạy 3 bản (MapReduce, spark_df, spark_rdd) với n = 4 luồng trên cùng input
5. Viết script nhỏ so số cặp và tổng count giữa 3 output

**Đầu ra**

- src/ml/local/pair_count_df.py, src/ml/local/pair_count_rdd.py
- Lệnh/script đối chiếu output 3 bản

**Xong khi**

- [ ] Trên dữ liệu đồ chơi, 3 bản cho cùng tập cặp, cùng số cặp và cùng tổng count
- [ ] Hai job Spark chạy với --master yarn và hiện trên Spark History

### AD-DS-07

**Viết script tải dữ liệu export về máy và nạp lên HDFS local**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G5**
- Issue: [#92](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/92)
- Tài liệu: `docs/ds/03_hadoop_mapreduce_local.md §3`, `docs/de/08_du_lieu_cho_ds.md §5`
- **Bắt đầu sau:** [AD-DS-04](05_data_science.md#ad-ds-04) [#89](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/89)
- **Hoàn tất còn cần:** [AD-DE-21](02_de_batch.md#ad-de-21) [#57](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/57)
- **Mở khóa:** [AD-DS-15](05_data_science.md#ad-ds-15) (bắt đầu)

Đưa đúng file txt của DE vào HDFS để Spark và MapReduce đọc cùng một input. Script viết được ngay, chỉ chạy thật khi DE đã xuất v1. Phần Hadoop do P3 làm cùng P4 (docs/00 §7); thẻ giao cho P3, P4 nghiệm thu.

**Việc cần làm**

1. Viết local/hadoop/scripts/fetch_exports.sh (AzCopy, AZCOPY_AUTO_LOGIN_TYPE=AZCLI, scale 10/25/50/100 + _manifest.json)
2. Viết local/hadoop/scripts/load_hdfs.sh (put -f block 8m, tạo scale=200 bằng 2 bản của 100)
3. Chạy fetch + load; đếm dòng từng scale so với _manifest.json
4. Chụp hdfs fsck (block, replication) và hdfs dfs -count cho báo cáo

**Đầu ra**

- local/hadoop/scripts/fetch_exports.sh, local/hadoop/scripts/load_hdfs.sh
- HDFS /ds/v1/product/scale=10..200
- Ảnh chụp hdfs fsck

**Xong khi**

- [ ] Số dòng mỗi scale trên HDFS bằng số giao dịch trong _manifest.json; scale=200 gấp đôi scale=100
- [ ] Chạy lại load_hdfs.sh không tạo file trùng

### AD-DS-08

**Viết bench.py chạy lưới E6 và script upload kết quả lên ds/inbox**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G5**
- Issue: [#93](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/93)
- Tài liệu: `docs/ds/03_hadoop_mapreduce_local.md §6–7, §9`, `docs/ds/00_tong_quan_ds.md §6.2`
- **Bắt đầu sau:** [AD-DS-06](05_data_science.md#ad-ds-06) [#91](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/91)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DS-12](05_data_science.md#ad-ds-12) (hoàn tất), [AD-DS-15](05_data_science.md#ad-ds-15) (bắt đầu)

Bộ chạy đo E6 trên cụm local: 3 cài đặt × quy mô × core × 3 lần, đọc số đo từ JobHistory và Spark History. Kiểm được trên dữ liệu đồ chơi trước khi có dữ liệu thật. Phần Hadoop do P3 làm cùng P4 (docs/00 §7); thẻ giao cho P3, P4 nghiệm thu.

**Việc cần làm**

1. Viết local/hadoop/scripts/bench.py: sinh lưới E6 (nhánh quy mô 10–200 @ 8 core, nhánh core 1/2/4/8 @ 100), vòng ngoài là rep
2. Tính run_key sha1 đúng công thức docs/ds/00 §6.2, platform local_mapreduce / local_spark_yarn, bỏ run_key đã có trong e6.jsonl
3. Đo wall_s; đọc job_s, peak_mem_mb, spill_mb, n_result qua REST JobHistory/Spark History và counter MapReduce
4. Xóa thư mục đích trước mỗi lần; ghi dòng JSONL chỉ sau khi job xong; ghi machine
5. Viết local/hadoop/scripts/upload_results.sh (AzCopy vào ds/inbox/local/e6_<stamp>.jsonl)
6. Chạy thử bench.py với lưới thu nhỏ trên dữ liệu đồ chơi

**Đầu ra**

- local/hadoop/scripts/bench.py, local/hadoop/scripts/upload_results.sh
- File results/e6.jsonl mẫu từ lần chạy thử

**Xong khi**

- [ ] Chạy thử sinh đủ dòng cho cả 3 cài đặt, có đủ cột wall_s, job_s, peak_mem_mb, spill_mb, n_result
- [ ] Chạy bench.py lần hai không chạy lại run_key đã có
- [ ] Schema dòng JSONL khớp cột ml.experiment_runs (docs/ds/04 §5.4)

### AD-DS-09

**Định nghĩa lưới experiments.yml và viết plan_grid.py bung tổ hợp theo run_key**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#94](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/94)
- Tài liệu: `docs/ds/04_thuc_nghiem_danh_gia.md §2–3, §5.1, §5.4, §6`, `docs/ds/00_tong_quan_ds.md §6.2`, `docs/ds/01_du_lieu_thuc_nghiem.md §4`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DS-10](05_data_science.md#ad-ds-10) (bắt đầu), [AD-DS-11](05_data_science.md#ad-ds-11) (hoàn tất)

Task plan của job ds-benchmark: đọc lưới, bung tổ hợp, bỏ tổ hợp đã ok để chạy lại không làm lại. Logic bung lưới là hàm thuần, test được local.

**Việc cần làm**

1. Viết src/ml/experiments.yml E1–E5, E7 theo docs/ds/04 §3 (bản nháp, chốt lại ở bước EDA)
2. Viết src/ml/sql/ddl_ml.sql: CREATE TABLE IF NOT EXISTS ml.experiment_queue, ml.experiment_runs, ml.frequent_itemsets, ml.association_rules, ml.holdout_metrics, ml.champion
3. Viết src/ml/plan_grid.py: hàm thuần expand_grid (defaults, min_confidence mặc định nhỏ nhất theo cấp, gom E7 theo MinSupp), run_key sha1 có algo_version
4. Phần chạy: lọc tổ hợp đã status ok, ghi ml.experiment_queue, đặt task value queue_c1/c2/c4 (run_key 12 ký tự)
5. Viết tests/ml/test_plan_grid.py

**Đầu ra**

- src/ml/experiments.yml, src/ml/plan_grid.py, src/ml/sql/ddl_ml.sql
- tests/ml/test_plan_grid.py

**Xong khi**

- [ ] Test: số lần chạy bung ra khớp bảng docs/ds/04 §2 (E1 3, E2 99, E3 45, E4 36, E5 54, E7 9 tổ hợp)
- [ ] Test: run_key ổn định giữa các lần gọi; tổ hợp đã ok bị loại khỏi hàng đợi; tách đúng theo cores 1/2/4

### AD-DS-10

**Viết run_experiment.py chạy một tổ hợp, ghi ml.experiment_runs và MLflow**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#95](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/95)
- Tài liệu: `docs/ds/04_thuc_nghiem_danh_gia.md §4, §5.3–5.4, §6`, `docs/ds/05_mlflow_serving_bao_cao.md §1`, `docs/ds/02_thuat_toan_spark.md §6.2`
- **Bắt đầu sau:** [AD-DS-01](05_data_science.md#ad-ds-01) [#86](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/86), [AD-DS-02](05_data_science.md#ad-ds-02) [#87](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/87), [AD-DS-03](05_data_science.md#ad-ds-03) [#88](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/88), [AD-DS-09](05_data_science.md#ad-ds-09) [#94](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/94)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DS-12](05_data_science.md#ad-ds-12) (hoàn tất)

Mỗi vòng for_each của ds-benchmark gọi script này: chạy thuật toán, đo, sinh luật, ghi kết quả idempotent theo run_key. E1 (algo all) so khớp 3 thuật toán và ghi check_passed.

**Việc cần làm**

1. Viết src/ml/run_experiment.py theo khung docs/ds/04 §5.3: đếm n_tx trước khi bấm giờ, min_count = ceil
2. Nhánh algo all (E1): chạy 3 thuật toán trên cùng df, so khớp như docs/ds/02 §6.2, ghi check_passed
3. Viết _merge_result (MERGE ON run_key), _persist (replaceWhere theo run_key), _log_mlflow (params, metrics, tags experiment/platform/status/run_key)
4. timeout ghi status timeout không làm task đỏ; failed thì SystemExit(1)
5. Tách phần tính toán thành hàm thuần; test local trên dữ liệu đồ chơi (bỏ qua MERGE/MLflow bằng mock)

**Đầu ra**

- src/ml/run_experiment.py
- tests/ml/test_run_experiment.py

**Xong khi**

- [ ] Test local: một tổ hợp mỗi thuật toán trả dòng kết quả đủ cột, n_itemsets_by_k đúng, nhánh all cho check_passed = true trên dữ liệu đồ chơi
- [ ] Test local: timeout giả lập ghi status timeout, không raise

### AD-DS-11

**Khám phá dữ liệu ds_data v1 và chốt lưới MinSupport theo cấp item**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G3**
- Issue: [#96](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/96)
- Tài liệu: `docs/ds/01_du_lieu_thuc_nghiem.md §1, §3–5`, `docs/de/08_du_lieu_cho_ds.md §7`
- **Bắt đầu sau:** [AD-DE-21](02_de_batch.md#ad-de-21) [#57](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/57)
- **Hoàn tất còn cần:** [AD-DS-09](05_data_science.md#ad-ds-09) [#94](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/94)
- **Mở khóa:** [AD-DS-13](05_data_science.md#ad-ds-13) (bắt đầu)

Bước D2: đo kích thước giỏ và số item phổ biến theo MinSupport để ước tải trước khi bật lưới. Kết quả chốt lại experiments.yml.

**Việc cần làm**

1. Tạo notebooks/ds/01_eda.py chạy các truy vấn docs/ds/01 §3.1–3.4
2. Xác nhận vai ds đọc được ds_data.* và volume exports, không ghi được
3. Tính n(L1), n(C2) theo cấp × MinSupp; chọn MinSupp nhỏ nhất cấp product giữ n(L1) ≤ khoảng 2.000
4. Vẽ phân phối tần suất item log-log cho báo cáo
5. Sửa src/ml/experiments.yml nếu ngưỡng quá nặng/nhẹ, ghi lý do trong commit

**Đầu ra**

- notebooks/ds/01_eda.py có kết quả
- Bảng n_frequent_items theo cấp × MinSupport, biểu đồ log-log
- src/ml/experiments.yml đã chốt

**Xong khi**

- [ ] Có số đo thật avg_len, p95, max_len cho 3 cấp và số đơn holdout val/test
- [ ] Mỗi MinSupp trong lưới có n(L1) và n(C2) đo thật; không ngưỡng nào có n(L1) > 3.000 ở cấp product

### AD-DS-12

**Khai báo job ds-benchmark trong DAB và viết ingest_local.sql**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#97](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/97)
- Tài liệu: `docs/ds/04_thuc_nghiem_danh_gia.md §5.1–5.2`, `docs/ds/03_hadoop_mapreduce_local.md §7`, `docs/devops/05_databricks_platform.md (phần DS trong bundle)`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DS-08](05_data_science.md#ad-ds-08) [#93](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/93), [AD-DS-10](05_data_science.md#ad-ds-10) [#95](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/95), [AD-OPS-30](01_devops.md#ad-ops-30) [#32](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/32), [AD-OPS-22](01_devops.md#ad-ops-22) [#24](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/24)
- **Mở khóa:** [AD-DS-13](05_data_science.md#ad-ds-13) (bắt đầu), [AD-DS-15](05_data_science.md#ad-ds-15) (hoàn tất)

Job chạy lưới trên Databricks: plan → grid_c1 → grid_c2 → grid_c4 → ingest_local → publish_gold, 3 job cluster local[n] nối tiếp để vừa quota 4 vCPU.

**Việc cần làm**

1. Viết bundles/resources/jobs_ds.yml phần ds_benchmark: tham số experiments/data_version, for_each concurrency 1, max_retries 2 + retry 3 phút
2. Khai báo job_clusters c1/c2/c4 (policy ds_policy_id, local[n], shuffle partitions 16, AQE tắt, PYTHONPATH src)
3. Viết src/ml/sql/ingest_local.sql: COPY INTO bảng tạm từ /Volumes/instacart/ml/inbox/local/ rồi MERGE ON run_key
4. Thêm task chạy ddl_ml.sql (hoặc gọi trong plan) để bảng ml.* tồn tại trước
5. Để publish_gold trỏ tới src/ml/sql/publish_gold.sql (file làm ở task view gold)
6. databricks bundle validate -t dev, kiểm workspace.file_path

**Đầu ra**

- bundles/resources/jobs_ds.yml (ds_benchmark)
- src/ml/sql/ingest_local.sql

**Xong khi**

- [ ] databricks bundle validate -t dev không lỗi
- [ ] Deploy dev và chạy job với experiments rỗng/1 tổ hợp đồ chơi: plan ra hàng đợi, grid_c1 chạy xong một vòng
- [ ] ingest_local nạp file e6 mẫu hai lần không sinh dòng trùng

### AD-DS-13

**Chạy spike DS-1 và thực nghiệm E1 kiểm đúng đắn trên dữ liệu thật**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#98](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/98)
- Tài liệu: `docs/ds/04_thuc_nghiem_danh_gia.md §2 (mốc DS-1)`, `docs/ds/02_thuat_toan_spark.md §6.2, §7`, `docs/ds/00_tong_quan_ds.md §5`
- **Bắt đầu sau:** [AD-DS-11](05_data_science.md#ad-ds-11) [#96](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/96), [AD-DS-12](05_data_science.md#ad-ds-12) [#97](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/97)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DS-14](05_data_science.md#ad-ds-14) (bắt đầu), [AD-DS-20](05_data_science.md#ad-ds-20) (bắt đầu)

Mốc DS-1: một lần chạy mỗi thuật toán để có thời gian thật và chốt lưới; E1 xác nhận DApriori = DATID = FP-Growth và kiểm nhận định PFP của bài báo.

**Việc cần làm**

1. Tạm dừng lịch instacart-stream-15m, đảm bảo không có demo stream
2. Chạy ds-benchmark với experiments E1 (3 cấp, 10%)
3. Chạy một điểm mỗi thuật toán ở product 25% / 0,002, ghi thời gian và bộ nhớ
4. So support item bậc 1 với ds_data.item_index.freq_prior ở 100%
5. Nếu một thuật toán vượt 30 phút: tăng MinSupp nhỏ nhất hoặc hạ product xuống 10%, sửa experiments.yml; nếu RDD không chạy nổi thì cân nhắc dapriori_df dự phòng (docs/ds/02 §7)
6. Cập nhật ước lượng ngân sách giờ máy và USD

**Đầu ra**

- Dòng E1 trong ml.experiment_runs cho 3 cấp item, có check_passed
- Bảng thời gian spike DS-1 và lưới đã chốt trong experiments.yml

**Xong khi**

- [ ] E1 có check_passed = true ở cả 3 cấp item
- [ ] Có ước lượng thời gian cho từng thuật toán ở product 25% / 0,002 và lưới E2–E5 vừa ngân sách đã ghi

### AD-DS-14

**Chạy toàn bộ lưới E2–E5 trên Databricks**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#99](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/99)
- Tài liệu: `docs/ds/04_thuc_nghiem_danh_gia.md §1–2, §5–6`
- **Bắt đầu sau:** [AD-DS-13](05_data_science.md#ad-ds-13) [#98](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/98)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DS-22](05_data_science.md#ad-ds-22) (hoàn tất), [AD-DS-26](05_data_science.md#ad-ds-26) (bắt đầu)

Mốc DS-2: số liệu theo MinSupport, quy mô, số item và số core cho 3 thuật toán, nguồn cho biểu đồ trang 7 và báo cáo.

**Việc cần làm**

1. Chạy ds-benchmark với experiments E2,E3,E4,E5 (có thể chia nhiều lần, plan tự bỏ tổ hợp đã ok)
2. Repair run các vòng lỗi; tổ hợp timeout giữ nguyên làm điểm dữ liệu
3. Kiểm mỗi tổ hợp đủ 3 lần đo trong ml.experiment_runs
4. Theo dõi chi phí thực tế so với ngân sách

**Đầu ra**

- ml.experiment_runs đủ dòng E2–E5
- Run MLflow tương ứng trong /Shared/instacart-ds

**Xong khi**

- [ ] Mọi tổ hợp E2–E5 có 3 dòng status ∈ {ok, timeout}, không còn failed
- [ ] Truy vấn speedup/efficiency docs/ds/04 §8 trả kết quả cho E5

### AD-DS-15

**Chạy lưới E6 MapReduce và Spark on YARN ở local, nạp kết quả lên ml.experiment_runs**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G5**
- Issue: [#100](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/100)
- Tài liệu: `docs/ds/03_hadoop_mapreduce_local.md §6–7, §9`, `docs/ds/04_thuc_nghiem_danh_gia.md §2`
- **Bắt đầu sau:** [AD-DS-07](05_data_science.md#ad-ds-07) [#92](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/92), [AD-DS-08](05_data_science.md#ad-ds-08) [#93](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/93)
- **Hoàn tất còn cần:** [AD-DS-12](05_data_science.md#ad-ds-12) [#97](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/97)
- **Mở khóa:** [AD-DS-16](05_data_science.md#ad-ds-16) (bắt đầu), [AD-DS-22](05_data_science.md#ad-ds-22) (hoàn tất), [AD-DS-26](05_data_science.md#ad-ds-26) (bắt đầu)

Mốc DS-3, chạy song song DS-2: đo thật MapReduce và Spark trên cùng cụm, phần bổ sung cho lập luận của bài báo. Kết quả vào Azure qua ds/inbox và task ingest_local. Phần Hadoop do P3 làm cùng P4 (docs/00 §7); thẻ giao cho P3, P4 nghiệm thu.

**Việc cần làm**

1. Ghi cấu hình máy (CPU, RAM, ổ đĩa, cấp cho Docker) vào báo cáo; đóng ứng dụng nặng, cắm sạc
2. Chạy bench.py đủ 81 lần (3 cài đặt × 2 nhánh × 3 lần) trên một máy
3. Đối chiếu n_result giữa MapReduce, spark_df, spark_rdd ở cùng tổ hợp
4. Chạy upload_results.sh rồi task ingest_local
5. Chụp YARN UI, JobHistory cho báo cáo

**Đầu ra**

- local/hadoop/results/e6.jsonl (không commit)
- Dòng platform local_mapreduce, local_spark_yarn trong ml.experiment_runs

**Xong khi**

- [ ] ml.experiment_runs có đủ 81 dòng E6 từ cùng một machine
- [ ] n_result bằng nhau giữa 3 cài đặt ở mọi tổ hợp

### AD-DS-16

**Cài AprioriMR lặp k ≤ 3 trên MapReduce và DApriori trên YARN (mở rộng)**

- Vai dự kiến: **P3 · DE stream**
- Cổng: **G5**
- Issue: [#101](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/101)
- Tài liệu: `docs/ds/03_hadoop_mapreduce_local.md §4.4`
- **Bắt đầu sau:** [AD-DS-15](05_data_science.md#ad-ds-15) [#100](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/100)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** Không có.

Phần mở rộng, chỉ làm khi E6 cơ bản xong: đo đúng lập luận chính của bài báo là Hadoop ghi kết quả từng vòng xuống HDFS còn Spark giữ trong bộ nhớ. Phần Hadoop do P3 làm cùng P4 (docs/00 §7); thẻ giao cho P3, P4 nghiệm thu.

**Việc cần làm**

1. Viết driver AprioriMR trong src/mapreduce: mỗi vòng k một job, Lk qua distributed cache, sinh C(k+1) trong setup() của mapper
2. Thêm test MiniMRYarnCluster cho AprioriMR trên dữ liệu đồ chơi (khớp vét cạn k ≤ 3)
3. Viết src/ml/local/dapriori_yarn.py gọi dapriori/datid trên Spark on YARN
4. Mở rộng bench.py đo HDFS_BYTES_WRITTEN giữa các vòng so với shuffle write của Spark; chạy, upload, ingest

**Đầu ra**

- AprioriMR trong src/mapreduce, src/ml/local/dapriori_yarn.py
- Dòng kết quả AprioriMR / DApriori YARN trong ml.experiment_runs

**Xong khi**

- [ ] mvn -B verify pass gồm test AprioriMR
- [ ] Tập phổ biến k ≤ 3 của AprioriMR bằng DApriori trên YARN ở cùng input; có số byte ghi HDFS giữa vòng và shuffle write để so

### AD-DS-17

**Dựng demo pipeline HDFS + Spark dựng giỏ prior từ CSV gốc (tùy chọn)**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#102](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/102)
- Tài liệu: `docs/ds/03_hadoop_mapreduce_local.md §8`
- **Bắt đầu sau:** [AD-DS-04](05_data_science.md#ad-ds-04) [#89](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/89)
- **Hoàn tất còn cần:** [AD-DE-17](02_de_batch.md#ad-de-17) [#53](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/53)
- **Mở khóa:** Không có.

Demo đúng sản phẩm pipeline HDFS + Spark của đề tài gốc, không dùng làm số liệu so sánh.

**Việc cần làm**

1. Đưa orders.csv, order_products__prior.csv (Zenodo) lên hdfs:///raw/instacart/
2. Viết src/ml/local/build_baskets_yarn.py dựng giỏ prior đúng logic silver.baskets (de/03 §7)
3. Chạy trên YARN, đếm số giỏ

**Đầu ra**

- src/ml/local/build_baskets_yarn.py
- Ảnh chụp lần chạy cho phần demo

**Xong khi**

- [ ] Số giỏ prior bằng con số của checklist G3 (3.214.874 hoặc số chính thức)

### AD-DS-18

**Viết đánh giá luật trên holdout evaluate_holdout.py và baseline phổ biến**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#103](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/103)
- Tài liệu: `docs/ds/04_thuc_nghiem_danh_gia.md §7`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DS-19](05_data_science.md#ad-ds-19) (hoàn tất), [AD-DS-20](05_data_science.md#ad-ds-20) (bắt đầu)

E7: đo luật có dùng được để gợi ý không (HitRate, Precision, Recall @10, coverage) và phải hơn baseline top-k phổ biến. Logic SQL kiểm được trên bảng đồ chơi local.

**Việc cần làm**

1. Viết src/ml/sql/eval_holdout.sql (tham số :run_key, :min_conf, :part, tập con vế trái 1..2, loại món trong context)
2. Viết src/ml/evaluate_holdout.py: chạy SQL cho từng (run_key, min_conf, part), thêm dòng baseline run_key = popularity, MERGE vào ml.holdout_metrics, log metric MLflow
3. Thêm coverage danh mục (số món khác nhau được gợi ý / 49.688)
4. Viết tests/ml/test_evaluate_holdout.py với luật và holdout đồ chơi, kết quả tính tay

**Đầu ra**

- src/ml/sql/eval_holdout.sql, src/ml/evaluate_holdout.py
- tests/ml/test_evaluate_holdout.py

**Xong khi**

- [ ] Test local pass: hit_rate, precision_at_k, recall_at_k, order_coverage khớp số tính tay, cả cho baseline

### AD-DS-19

**Viết model pyfunc BasketRules gợi ý món từ giỏ hàng kèm test**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#104](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/104)
- Tài liệu: `docs/ds/05_mlflow_serving_bao_cao.md §2.2`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DS-18](05_data_science.md#ad-ds-18) [#103](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/103)
- **Mở khóa:** [AD-DS-21](05_data_science.md#ad-ds-21) (bắt đầu)

Model Serving không chạy được model Spark, nên luật champion được bọc trong pyfunc thuần Python. Logic xếp hạng phải giống SQL đánh giá (hai bản cần test so khớp).

**Việc cần làm**

1. Viết src/ml/serving/__init__.py và src/ml/serving/basket_rules.py (load_context từ Parquet, _recommend, predict với tham số k, bù món phổ biến)
2. Thêm mlflow, pandas, pyarrow vào dependency nếu chưa có
3. Test: log_model → load_model → predict trên luật đồ chơi cho đúng thứ tự; signature có product_ids và tham số k
4. Test so khớp: top-k của BasketRules bằng top-k của eval_holdout.sql trên cùng luật và giỏ đồ chơi

**Đầu ra**

- src/ml/serving/basket_rules.py
- tests/ml/test_basket_rules.py

**Xong khi**

- [ ] uv run pytest -q tests/ml pass, gồm test so khớp với SQL
- [ ] Giỏ không kích hoạt luật nào vẫn trả đủ k món nguồn popular

### AD-DS-20

**Chạy thực nghiệm E7 và đánh giá luật trên holdout val/test**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#105](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/105)
- Tài liệu: `docs/ds/04_thuc_nghiem_danh_gia.md §2–3, §7`, `docs/ds/05_mlflow_serving_bao_cao.md §1`
- **Bắt đầu sau:** [AD-DS-13](05_data_science.md#ad-ds-13) [#98](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/98), [AD-DS-18](05_data_science.md#ad-ds-18) [#103](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/103)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DS-21](05_data_science.md#ad-ds-21) (bắt đầu), [AD-DS-26](05_data_science.md#ad-ds-26) (bắt đầu)

Mốc DS-4 phần đánh giá: FP-Growth product 100% ở 3 MinSupp, lưu luật (persist) rồi chấm 9 cấu hình trên holdout. Không cần chờ E2–E5 xong.

**Việc cần làm**

1. Chạy ds-benchmark với experiments E7 (persist ghi ml.frequent_itemsets, ml.association_rules)
2. Chạy evaluate_holdout cho 9 cấu hình × val và baseline × val/test
3. Kiểm MLflow có hit_rate_at_10, precision_at_10, recall_at_10, order_coverage

**Đầu ra**

- ml.association_rules của 3 run E7
- ml.holdout_metrics có 9 cấu hình × val và baseline × val/test

**Xong khi**

- [ ] ml.holdout_metrics đủ 9 dòng val của luật và 2 dòng baseline (val, test)
- [ ] Chạy lại evaluate_holdout không sinh dòng trùng

### AD-DS-21

**Chọn champion, đăng ký model basket_rules vào Unity Catalog và khai báo job ds-champion**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#106](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/106)
- Tài liệu: `docs/ds/05_mlflow_serving_bao_cao.md §2, §7`, `docs/devops/03_terraform_azure.md (ds/serving/)`
- **Bắt đầu sau:** [AD-DS-19](05_data_science.md#ad-ds-19) [#104](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/104), [AD-DS-20](05_data_science.md#ad-ds-20) [#105](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/105)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DS-22](05_data_science.md#ad-ds-22) (hoàn tất), [AD-DS-23](05_data_science.md#ad-ds-23) (bắt đầu), [AD-DS-24](05_data_science.md#ad-ds-24) (hoàn tất), [AD-DS-26](05_data_science.md#ad-ds-26) (hoàn tất)

Bước D6: chọn cấu hình theo quy tắc ghi trước (coverage ≥ 0,5, HitRate@10 cao nhất trên val, phải hơn baseline), rồi đóng gói model có alias @champion.

**Việc cần làm**

1. Ghi quy tắc chọn §2.1 vào báo cáo trước khi xem kết quả
2. Viết src/ml/champion.py: chọn cấu hình, xuất Parquet rules/items, register pyfunc vào instacart.ml.basket_rules, gán alias champion ở bước cuối, ghi ml.champion
3. Xuất thêm 2 file Parquet vào container ds/serving/ cho phương án B của API
4. Chạy evaluate_holdout cho champion × test
5. Thêm job ds_champion vào bundles/resources/jobs_ds.yml: evaluate_holdout → champion → publish_gold, chạy tay

**Đầu ra**

- src/ml/champion.py, job ds_champion trong jobs_ds.yml
- Model instacart.ml.basket_rules có alias @champion; bảng ml.champion
- Parquet rules/items trong ds/serving/

**Xong khi**

- [ ] mlflow.pyfunc.load_model('models:/instacart.ml.basket_rules@champion') predict được trên Databricks
- [ ] ml.champion trỏ đúng run_key; ml.holdout_metrics có champion × test
- [ ] Kết luận champion có hơn baseline hay không đã ghi rõ

### AD-DS-22

**Viết view gold cho DS: association_rules, recommendations, experiment_metrics, holdout_metrics**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#107](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/107)
- Tài liệu: `docs/ds/05_mlflow_serving_bao_cao.md §4`, `docs/ds/00_tong_quan_ds.md §6.1`, `docs/ds/04_thuc_nghiem_danh_gia.md §8`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DS-14](05_data_science.md#ad-ds-14) [#99](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/99), [AD-DS-15](05_data_science.md#ad-ds-15) [#100](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/100), [AD-DS-21](05_data_science.md#ad-ds-21) [#106](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/106), [AD-OPS-20](01_devops.md#ad-ops-20) [#22](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/22)
- **Mở khóa:** [AD-BI-11](04_power_bi.md#ad-bi-11) (hoàn tất), [AD-DS-25](05_data_science.md#ad-ds-25) (bắt đầu)

Bước D7: Power BI chỉ đọc gold; view chạy bằng quyền chủ view nên vai bi không cần vào ml hay ds_data. Viết SQL được ngay, kiểm số liệu khi đã có kết quả.

**Việc cần làm**

1. Viết src/ml/sql/publish_gold.sql (4 view CREATE OR REPLACE, có COMMENT)
2. experiment_metrics: trung vị 3 lần đo, speedup, efficiency, n_ok/n_timeout, so khớp top_n bằng <=>
3. Nối file vào task publish_gold của ds-benchmark và ds-champion
4. Chạy publish_gold; đối chiếu số dòng view với bảng nguồn
5. Kiểm bằng tài khoản vai bi: đọc được gold.association_rules, đọc ml.experiment_runs báo PERMISSION_DENIED

**Đầu ra**

- src/ml/sql/publish_gold.sql
- View gold.association_rules, gold.recommendations, gold.experiment_metrics, gold.holdout_metrics

**Xong khi**

- [ ] Task publish_gold xanh, chạy lại vẫn xanh
- [ ] gold.experiment_metrics có n_ok + n_timeout = 3 cho mọi tổ hợp E2–E5 và có đủ 3 platform
- [ ] gold.recommendations tối đa 10 dòng mỗi product_id, theo luật của champion

### AD-DS-23

**Khai báo Model Serving endpoint instacart-basket-rules trong DAB và kiểm API**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#108](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/108)
- Tài liệu: `docs/ds/05_mlflow_serving_bao_cao.md §3.1`, `docs/devops/05_databricks_platform.md (serving_ds.yml)`
- **Bắt đầu sau:** [AD-DS-21](05_data_science.md#ad-ds-21) [#106](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/106), [AD-OPS-18](01_devops.md#ad-ops-18) [#20](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/20)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-OPS-31](01_devops.md#ad-ops-31) (hoàn tất), [AD-DS-24](05_data_science.md#ad-ds-24) (hoàn tất), [AD-DS-27](05_data_science.md#ad-ds-27) (hoàn tất)

Phương án A của API: endpoint serverless scale-to-zero phục vụ phiên bản champion; đổi champion chỉ cần deploy lại với biến champion_version.

**Việc cần làm**

1. Viết bundles/resources/serving_ds.yml (biến champion_version, workload Small, scale_to_zero, quyền ds CAN_MANAGE, SP func CAN_QUERY)
2. databricks bundle validate rồi deploy dev với champion_version của model
3. Gọi curl invocations với giỏ mẫu, đo thời gian khi đã nóng; chụp ảnh
4. Ghi lại thời gian cold start

**Đầu ra**

- bundles/resources/serving_ds.yml
- Endpoint instacart-basket-rules READY; ảnh chụp curl

**Xong khi**

- [ ] curl theo docs/ds/05 §3.1 trả danh sách món gợi ý
- [ ] Thời gian trả lời khi đã nóng < 2 giây

### AD-DS-24

**Viết Azure Function recommend gọi endpoint, có phương án B tự tra luật**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#109](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/109)
- Tài liệu: `docs/ds/05_mlflow_serving_bao_cao.md §3.2–3.3`, `docs/devops/04_github_actions_cicd.md §3.8`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DS-21](05_data_science.md#ad-ds-21) [#106](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/106), [AD-DS-23](05_data_science.md#ad-ds-23) [#108](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/108), [AD-OPS-31](01_devops.md#ad-ops-31) [#33](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/33)
- **Mở khóa:** [AD-OPS-32](01_devops.md#ad-ops-32) (hoàn tất), [AD-DS-27](05_data_science.md#ad-ds-27) (hoàn tất)

Proxy FaaS cho web, dùng managed identity lấy token Entra, không giữ token Databricks. Nếu Model Serving không dùng được thì Function tự chạy BasketRules từ Parquet trong ds/serving/.

**Việc cần làm**

1. Viết src/serving_func/function_app.py (route recommend, kiểm product_ids rỗng → 400, cắt tối đa 50 id)
2. Viết src/serving_func/requirements.txt, host.json
3. Thêm chế độ phương án B: đọc rules/items Parquet từ ds/serving bằng managed identity, gọi BasketRules._recommend
4. Viết test unit mock requests và credential cho cả 2 chế độ
5. Sau khi deploy: gọi Function bằng function key (lưu Key Vault), chụp ảnh

**Đầu ra**

- src/serving_func/ (function_app.py, requirements.txt, host.json)
- tests/serving_func/test_function_app.py

**Xong khi**

- [ ] Test unit pass cho phương án A và B
- [ ] POST /api/recommend trên Function đã deploy trả món gợi ý cho giỏ mẫu
- [ ] Function key không có trong code hay README

### AD-DS-25

**Làm Power BI trang 6 luật kết hợp và trang 7 thực nghiệm thuật toán**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#110](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/110)
- Tài liệu: `docs/ds/05_mlflow_serving_bao_cao.md §5`, `docs/de/07_power_bi.md §4`, `docs/ds/04_thuc_nghiem_danh_gia.md §8`
- **Bắt đầu sau:** [AD-DS-22](05_data_science.md#ad-ds-22) [#107](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/107), [AD-BI-11](04_power_bi.md#ad-bi-11) [#84](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/84), [AD-BI-09](04_power_bi.md#ad-bi-09) [#82](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/82)
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DS-27](05_data_science.md#ad-ds-27) (bắt đầu)

Trực quan kết quả DS cho rubric mục 4: luật của champion và đường thời gian, speedup, MapReduce so với Spark, HitRate theo ngưỡng.

**Việc cần làm**

1. Import view association_rules, experiment_metrics, holdout_metrics vào model (bảng rời, không quan hệ RLS)
2. Trang 6: bảng luật, scatter support × confidence cỡ theo lift, thẻ số luật, slicer min confidence/min lift/antecedent_size, drill-through sang trang 2
3. Trang 7: thời gian theo MinSupp (E2), quy mô (E3), số item log (E4), speedup/efficiency (E5, E6), cột MapReduce và Spark, HitRate@10 kèm baseline (E7)
4. Thêm trang 6–7 vào app cho 2 audience, làm mobile layout, publish

**Đầu ra**

- Trang 6–7 trong file pbip, đã publish vào app
- Ảnh chụp 2 trang cho báo cáo

**Xong khi**

- [ ] Trang 6–7 refresh thành công sau publish_gold và có trong app + mobile layout
- [ ] Drill-through từ một luật sang trang 2 lọc đúng consequent_product_id

### AD-DS-26

**Phân tích kết quả thực nghiệm và viết phần DS của báo cáo, đối chiếu bài báo**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#111](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/111)
- Tài liệu: `docs/ds/04_thuc_nghiem_danh_gia.md §8`, `docs/ds/00_tong_quan_ds.md §2`, `docs/ds/02_thuat_toan_spark.md §2, §5, §7`
- **Bắt đầu sau:** [AD-DS-14](05_data_science.md#ad-ds-14) [#99](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/99), [AD-DS-15](05_data_science.md#ad-ds-15) [#100](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/100), [AD-DS-20](05_data_science.md#ad-ds-20) [#105](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/105)
- **Hoàn tất còn cần:** [AD-DS-21](05_data_science.md#ad-ds-21) [#106](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/106)
- **Mở khóa:** [AD-DS-27](05_data_science.md#ad-ds-27) (bắt đầu), [AD-RP-06](06_bao_cao_demo.md#ad-rp-06) (bắt đầu)

Đầu ra học thuật chính: biểu đồ và kết luận so với bài báo Fernandez-Basso 2024, nêu rõ giới hạn phần cứng và cách đo.

**Việc cần làm**

1. Vẽ đủ biểu đồ ở docs/ds/04 §8 từ ml.experiment_runs (trung vị 3 lần)
2. Viết bảng đối chiếu đề tài ↔ bài báo ↔ kết quả (docs/ds/00 §2), gồm điểm khác cài đặt bc_items và k ≤ 3 cấp product
3. Kết luận về tính exhaustive của MLlib FP-Growth (E1) và hệ quả cho nhận định về DATID
4. Giải thích speedup/efficiency bằng tỉ lệ thời gian driver; MapReduce và Spark qua spill_mb
5. Kết quả E7 trên test so với baseline (kể cả nếu âm tính); ghi giới hạn đo bộ nhớ, phần cứng, không so chéo platform
6. Ghi DECLAT, YAFIM vào hướng mở rộng

**Đầu ra**

- Chương DS trong báo cáo kèm hình và bảng

**Xong khi**

- [ ] Có đủ 8 nhóm biểu đồ ở docs/ds/04 §8 và bảng đối chiếu với bài báo
- [ ] Mỗi kết luận trỏ được tới run_key hoặc truy vấn trên ml.experiment_runs / ml.holdout_metrics

### AD-DS-27

**Đối chiếu checklist G5 và chốt bằng chứng phần DS**

- Vai dự kiến: **P4 · BI/DS**
- Cổng: **G5**
- Issue: [#112](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/112)
- Tài liệu: `docs/ds/05_mlflow_serving_bao_cao.md §6`, `docs/00_tong_quan_du_an.md (cổng G0–G5)`
- **Bắt đầu sau:** [AD-DS-25](05_data_science.md#ad-ds-25) [#110](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/110), [AD-DS-26](05_data_science.md#ad-ds-26) [#111](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/111)
- **Hoàn tất còn cần:** [AD-DS-23](05_data_science.md#ad-ds-23) [#108](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/108), [AD-DS-24](05_data_science.md#ad-ds-24) [#109](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/109)
- **Mở khóa:** [AD-RP-06](06_bao_cao_demo.md#ad-rp-06) (hoàn tất), [AD-RP-08](06_bao_cao_demo.md#ad-rp-08) (bắt đầu)

Cổng cuối của DS: kiểm từng mục checklist bằng truy vấn, ảnh chụp và lệnh gọi API thật. Endpoint hoặc Function (phương án B) phải có ít nhất một đường trả lời.

**Việc cần làm**

1. Kiểm từng mục docs/ds/05 §6 bằng truy vấn trên gold.experiment_metrics, ml.holdout_metrics, ml.champion
2. Gọi API (endpoint hoặc Function) khi đã nóng, chụp ảnh thời gian trả lời
3. Kiểm quyền vai bi trên gold và ml
4. Đánh dấu checklist, dẫn link bằng chứng; báo thiếu sót còn lại

**Đầu ra**

- Checklist G5 đã đánh dấu kèm bằng chứng (truy vấn, ảnh chụp)

**Xong khi**

- [ ] Mọi mục checklist docs/ds/05 §6 đạt hoặc có ghi chú lý do được nhóm chấp nhận
