"""Materialized view Gold cho đơn hàng đã hợp nhất và giá tại thời điểm đặt."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from common.gold import build_fact_order_lines

spark = SparkSession.active()
CATALOG = spark.conf.get("catalog")
SILVER = spark.conf.get("silver")
GOLD = spark.conf.get("gold")


def table_name(schema: str, name: str) -> str:
    return ".".join("`" + part.replace("`", "``") + "`" for part in (CATALOG, schema, name))


if spark.conf.get("enable_gold_fact", "false").lower() == "true":

    @dp.materialized_view(
        name=table_name(GOLD, "fact_order_lines"),
        cluster_by=["order_date", "department_id"],
        table_properties={"quality": "gold"},
        comment="Một dòng mỗi món trong đơn, giá tại thời điểm đặt và doanh thu giả lập.",
    )
    @dp.expect_or_fail("has_order", "user_id IS NOT NULL AND order_ts_sim IS NOT NULL")
    @dp.expect_or_fail("has_product", "department_id IS NOT NULL AND aisle_id IS NOT NULL")
    @dp.expect("priced", "unit_price IS NOT NULL")
    def fact_order_lines():
        return build_fact_order_lines(
            spark.read.table(table_name(SILVER, "order_items")),
            spark.read.table(table_name(SILVER, "orders")),
            spark.read.table(table_name(SILVER, "dim_product")),
            spark.read.table(table_name(SILVER, "product_prices_history")),
            spark.read.table(table_name(SILVER, "promotions")),
        )

    @dp.materialized_view(name=table_name(GOLD, "kpi_daily"))
    def kpi_daily():
        return spark.sql(f"""
            SELECT order_date, source, count(DISTINCT order_id) AS orders, count(*) AS items,
                   count(DISTINCT user_id) AS users, round(sum(revenue_sim), 2) AS revenue_sim,
                   round(avg(reordered), 4) AS reorder_rate,
                   round(count(*) / count(DISTINCT order_id), 2) AS avg_basket_size
            FROM {table_name(GOLD, "fact_order_lines")}
            GROUP BY order_date, source
        """)

    @dp.materialized_view(name=table_name(GOLD, "kpi_by_dow_hour"))
    def kpi_by_dow_hour():
        # Giữ thứ và giờ gốc của Instacart.
        return spark.sql(f"""
            SELECT order_dow, order_hour_of_day, count(DISTINCT order_id) AS orders,
                   count(*) AS items, round(avg(reordered), 4) AS reorder_rate
            FROM {table_name(GOLD, "fact_order_lines")}
            GROUP BY order_dow, order_hour_of_day
        """)

    @dp.materialized_view(name=table_name(GOLD, "department_sales"), cluster_by=["department_id", "order_date"])
    def department_sales():
        return spark.sql(f"""
            SELECT order_date, department_id, aisle_id, count(DISTINCT order_id) AS orders,
                   count(*) AS items, round(sum(revenue_sim), 2) AS revenue_sim,
                   sum(reordered) AS reordered_items, count_if(discount_pct IS NOT NULL) AS promo_items
            FROM {table_name(GOLD, "fact_order_lines")}
            GROUP BY order_date, department_id, aisle_id
        """)

    @dp.materialized_view(name=table_name(GOLD, "product_360"))
    @dp.expect_or_fail("one_row_per_product", "product_id IS NOT NULL AND n = 1")
    def product_360():
        return spark.sql(f"""
            WITH sales AS (
              SELECT product_id, count(*) AS times_ordered, round(avg(reordered), 4) AS reorder_rate,
                     round(sum(revenue_sim), 2) AS revenue_sim
              FROM {table_name(GOLD, "fact_order_lines")}
              GROUP BY product_id
            )
            SELECT d.product_id, d.product_name, d.aisle_id, d.aisle, d.department_id, d.department,
                   d.current_price, d.on_promotion, d.discount_pct,
                   d.brand, d.nutriscore_grade, d.nova_group, d.energy_kcal_100g, d.sugars_100g,
                   d.image_url, d.off_match_status, d.off_match_score,
                   coalesce(s.times_ordered, 0) AS times_ordered, s.reorder_rate, s.revenue_sim,
                   count(*) OVER (PARTITION BY d.product_id) AS n
            FROM {table_name(SILVER, "dim_product")} d
            LEFT JOIN sales s ON d.product_id = s.product_id
        """)

    @dp.materialized_view(name=table_name(GOLD, "dim_date"))
    def dim_date():
        return spark.sql("""
            SELECT d AS date, year(d) AS year, month(d) AS month, weekofyear(d) AS week,
                   date_format(d, 'yyyy-MM') AS year_month, weekday(d) + 1 AS iso_dow
            FROM (SELECT explode(sequence(DATE'2015-01-01', DATE'2026-04-30', INTERVAL 1 DAY)) AS d)
        """)

    @dp.materialized_view(name=table_name(GOLD, "dim_department"))
    @dp.expect_or_fail("one_row_per_department", "department_id IS NOT NULL AND n = 1")
    def dim_department():
        # Gộp theo department_id thay cho cửa sổ trên DISTINCT: lượt prod Sub D 28/11 (run 917015887430313)
        # đếm n bằng số sản phẩm (4.007) như thể bước DISTINCT bị bỏ qua. n = số tên khác nhau của một mã.
        return spark.sql(f"""
            SELECT department_id, min(department) AS department, count(DISTINCT department) AS n
            FROM {table_name(SILVER, "dim_product")}
            GROUP BY department_id
        """)

    @dp.materialized_view(name=table_name(GOLD, "bi_user_access"), comment="Ánh xạ người xem vào ngành hàng cho RLS.")
    @dp.expect_or_drop("valid_bi_access", "user_email IS NOT NULL AND length(user_email) > 0 AND department_id > 0")
    def bi_user_access():
        seed_schema = spark.conf.get("bi_access_seed_schema", "landing_meta")
        return spark.sql(f"""
            SELECT DISTINCT lower(trim(user_email)) AS user_email, department_id
            FROM {table_name(seed_schema, "bi_access_seed")}
        """)

    if spark.conf.get("enable_baskets", "false").lower() == "true":

        @dp.materialized_view(name=table_name(GOLD, "product_pair_counts"), cluster_by=["p1"])
        @dp.expect_or_fail("ordered_pair", "p1 < p2")
        def product_pair_counts():
            return spark.sql(f"""
                WITH top_products AS (
                  SELECT product_id, times_ordered
                  FROM {table_name(GOLD, "product_360")}
                  WHERE times_ordered > 0
                  ORDER BY times_ordered DESC, product_id
                  LIMIT 5000
                ), prior_baskets AS (
                  SELECT order_id, items FROM {table_name(SILVER, "baskets")} WHERE split = 'prior'
                ), eligible AS (
                  SELECT e.order_id, array_sort(collect_set(e.product_id)) AS items
                  FROM (SELECT order_id, explode(items) AS product_id FROM prior_baskets) e
                  INNER JOIN top_products USING (product_id)
                  GROUP BY e.order_id
                  HAVING count(DISTINCT e.product_id) >= 2
                ), positions AS (
                  SELECT items, i, p1 FROM eligible LATERAL VIEW posexplode(items) pos AS i, p1
                ), counted AS (
                  SELECT p1, p2, count(*) AS pair_count FROM positions
                  LATERAL VIEW explode(slice(items, i + 2, size(items) - i - 1)) tail AS p2
                  GROUP BY p1, p2 HAVING count(*) >= 50
                  ORDER BY pair_count DESC, p1, p2
                  LIMIT 200000
                ), total AS (
                  SELECT count(*) AS n_baskets FROM prior_baskets
                )
                SELECT c.p1, c.p2, c.pair_count,
                       CAST(c.pair_count AS DOUBLE) / n.n_baskets AS support,
                       CAST(c.pair_count AS DOUBLE) / s1.times_ordered AS confidence_p1_to_p2,
                       (CAST(c.pair_count AS DOUBLE) / s1.times_ordered) *
                         (CAST(n.n_baskets AS DOUBLE) / s2.times_ordered) AS lift
                FROM counted c
                INNER JOIN top_products s1 ON s1.product_id = c.p1
                INNER JOIN top_products s2 ON s2.product_id = c.p2
                CROSS JOIN total n
            """)
