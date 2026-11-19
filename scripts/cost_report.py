"""In số liệu chi phí và quota cho bảng hằng tuần ở docs/devops/chi_phi_quota.md (AD-OPS-28).

Chỉ đọc dữ liệu:
- Phía Azure (qua `az`): Cost Management Query API, budget `budget-sub-a`, `az vm list-usage`.
- Phía Databricks (qua `databricks`, khi có `--dbx-profile`): lịch sử truy vấn của warehouse,
  run của job, update của pipeline. Dùng để **ước tính** DBU theo warehouse, job, pipeline,
  vì P1 không có quyền đọc `system.billing`.

Không in subscription ID hay email, nên dán thẳng kết quả vào tài liệu được (repo public).

    az login && az account set --subscription "Azure for Students"
    uv run python scripts/cost_report.py --to 2026-11-19 --dbx-profile instacart
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from urllib.parse import quote

API = "2023-11-01"
BUDGET = "budget-sub-a"
REGION = "eastasia"
WAREHOUSE = "wh-instacart-bi"
SQL_METER = "Premium Serverless SQL DBU"
# DBU/giờ của SQL warehouse serverless theo cỡ (bảng giá Databricks).
WAREHOUSE_DBU_PER_HOUR = {"2X-Small": 4, "X-Small": 6, "Small": 12, "Medium": 24}
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-")


def run(*cmd: str) -> str:
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


def cost_query(sub: str, frm: str, to: str, groups: list[dict], granularity: str = "None") -> list[list]:
    """Chi phí thực tế và số lượng (UsageQuantity) trong [frm, to]. Cost Management hay trả 429 nên thử lại."""
    body = {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {"from": f"{frm}T00:00:00Z", "to": f"{to}T23:59:59Z"},
        "dataset": {
            "granularity": granularity,
            "aggregation": {
                "cost": {"name": "Cost", "function": "Sum"},
                "qty": {"name": "UsageQuantity", "function": "Sum"},
            },
            "grouping": groups,
        },
    }
    url = f"https://management.azure.com/subscriptions/{sub}/providers/Microsoft.CostManagement/query?api-version={API}"
    for attempt in range(6):
        try:
            out = run("az", "rest", "--method", "post", "--url", url, "--body", json.dumps(body), "-o", "json")
            return json.loads(out)["properties"]["rows"]
        except subprocess.CalledProcessError as err:
            if "429" not in err.stderr:
                raise
            time.sleep(20 * (attempt + 1))
    sys.exit("Cost Management vẫn trả 429, thử lại sau vài phút.")


def azure_section(frm: str, to: str) -> dict[str, float]:
    """In phần Azure. Trả về chi phí meter serverless SQL theo ngày (YYYYMMDD → USD) cho phần Databricks."""
    sub = run("az", "account", "show", "--query", "id", "-o", "tsv").strip()

    total = sum(r[0] for r in cost_query(sub, frm, to, []))
    print(f"Lũy kế {frm} → {to}: {total:.2f} USD")

    budget_url = (
        f"https://management.azure.com/subscriptions/{sub}/providers/Microsoft.Consumption/budgets/{BUDGET}"
        "?api-version=2023-05-01"
    )
    b = json.loads(run("az", "rest", "--method", "get", "--url", budget_url, "-o", "json"))["properties"]
    spent = b["currentSpend"]["amount"]
    forecast = (b.get("forecastSpend") or {}).get("amount")
    print(f"Budget {BUDGET}: đã tiêu {spent:.2f}/{b['amount']:.0f} USD ({spent / b['amount']:.0%}), dự báo {forecast}")

    groups = {
        "tag owner": {"type": "TagKey", "name": "owner"},
        "tag stack": {"type": "TagKey", "name": "stack"},
        "tag cost_owner": {"type": "TagKey", "name": "cost_owner"},
        "resource group": {"type": "Dimension", "name": "ResourceGroupName"},
    }
    for label, group in groups.items():
        print(f"\n| {label} | USD |\n|---|---:|")
        for row in sorted(cost_query(sub, frm, to, [group]), key=lambda r: -r[0]):
            if row[0] >= 0.05:
                # Nhóm theo tag: [cost, qty, key, value, currency]; theo dimension: [cost, qty, value, currency].
                value = row[3] if group["type"] == "TagKey" else row[2]
                print(f"| {value or '(không có tag)'} | {row[0]:.2f} |")

    # Đơn giá = cost / quantity: giá DBU thực tế ở region này, dùng để quy đổi phần ước tính Databricks.
    print("\n| Meter | Số lượng | USD | Đơn giá |\n|---|---:|---:|---:|")
    for cost, qty, meter, _cur in sorted(
        cost_query(sub, frm, to, [{"type": "Dimension", "name": "Meter"}]), key=lambda r: -r[0]
    ):
        if cost >= 0.05:
            print(f"| {meter} | {qty:.2f} | {cost:.2f} | {cost / qty if qty else 0:.3f} |")

    sql_daily: dict[str, float] = {}
    for cost, _qty, day, meter, _cur in cost_query(
        sub, frm, to, [{"type": "Dimension", "name": "Meter"}], granularity="Daily"
    ):
        if meter == SQL_METER:
            sql_daily[str(day)] = sql_daily.get(str(day), 0) + cost

    usage = json.loads(run("az", "vm", "list-usage", "--location", REGION, "-o", "json"))
    print(f"\n| Quota vCPU ({REGION}) | Đang dùng | Giới hạn |\n|---|---:|---:|")
    for u in usage:
        if u["name"]["value"] in ("cores", "standardDDSv4Family"):
            print(f"| {u['name']['localizedValue']} | {u['currentValue']} | {u['limit']} |")
    return sql_daily


def dbx(profile: str, *args: str) -> object:
    return json.loads(run("databricks", *args, "--profile", profile, "-o", "json"))


def day_of(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y%m%d")


def warehouse_section(profile: str, start_ms: int, end_ms: int, sql_daily: dict[str, float]) -> None:
    """Ước tính giờ chạy của warehouse từ lịch sử truy vấn, rồi chia chi phí thật theo người chạy truy vấn.

    Warehouse bật khi có truy vấn và tắt sau `auto_stop_mins` rảnh. Gộp các truy vấn cách nhau
    ít hơn auto_stop thành một phiên; mỗi phiên tính thêm auto_stop phút chạy rảnh ở cuối.
    """
    wh = next(w for w in dbx(profile, "warehouses", "list") if w["name"] == WAREHOUSE)
    auto_stop_ms = wh["auto_stop_mins"] * 60_000
    dbu_per_hour = WAREHOUSE_DBU_PER_HOUR.get(wh["cluster_size"])
    me = dbx(profile, "current-user", "me")["userName"]

    queries, token = [], None
    while True:
        # max_results và page_token đi trên URL. Trang đầu gửi filter_by trong body; các trang sau
        # không được gửi body (token đã mang filter, gửi kèm body thì API báo "Malformed token").
        path = "/api/2.0/sql/history/queries?max_results=1000"
        extra = [
            "--json",
            json.dumps(
                {
                    "filter_by": {
                        "warehouse_ids": [wh["id"]],
                        "query_start_time_range": {"start_time_ms": start_ms, "end_time_ms": end_ms},
                    }
                }
            ),
        ]
        if token:
            path, extra = f"{path}&page_token={quote(token, safe='')}", []
        page = json.loads(run("databricks", "api", "get", path, *extra, "--profile", profile))
        queries += page.get("res", [])
        token = page.get("next_page_token")
        if not page.get("has_next_page") or not token:
            break

    # Không in email: người chạy script, service principal (CI, job), những người còn lại đánh số.
    others: dict[str, str] = {}

    def who(user: str) -> str:
        if user == me:
            return "người chạy script"
        if UUID.match(user):
            return "service principal"
        return others.setdefault(user, f"người dùng khác {len(others) + 1}")

    by_day: dict[str, list[tuple[int, int]]] = defaultdict(list)
    share: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for q in queries:
        s, e = q.get("query_start_time_ms"), q.get("query_end_time_ms") or q.get("query_start_time_ms")
        if not s:
            continue
        by_day[day_of(s)].append((s, e))
        share[day_of(s)][who(q.get("user_name") or "")] += max(e - s, 1)

    print(
        f"\n**Warehouse `{WAREHOUSE}`** ({wh['cluster_size']}, auto stop {wh['auto_stop_mins']} phút): "
        "giờ chạy ước tính từ lịch sử truy vấn, so với chi phí thật của meter serverless SQL."
    )
    print(
        "\n| Ngày | Số truy vấn | Số phiên | Giờ chạy ước tính | DBU ước tính | USD thật (Azure) "
        "| Chia theo thời gian truy vấn |"
    )
    print("|---|---:|---:|---:|---:|---:|---|")
    for day in sorted(by_day):
        spans = sorted(by_day[day])
        sessions, (cur_s, cur_e) = 0, spans[0]
        hours = 0.0
        for s, e in spans[1:] + [(None, None)]:
            if s is not None and s <= cur_e + auto_stop_ms:
                cur_e = max(cur_e, e)
                continue
            sessions += 1
            hours += (cur_e - cur_s + auto_stop_ms) / 3_600_000
            if s is not None:
                cur_s, cur_e = s, e
        dbu = f"{hours * dbu_per_hour:.1f}" if dbu_per_hour else "?"
        usd = sql_daily.get(day)
        total = sum(share[day].values())
        split = ", ".join(
            f"{k} {v / total:.0%}" + (f" ≈ {usd * v / total:.2f} USD" if usd else "")
            for k, v in sorted(share[day].items(), key=lambda kv: -kv[1])
        )
        usd_s = f"{usd:.2f}" if usd is not None else "chưa có"
        iso = f"{day[:4]}-{day[4:6]}-{day[6:]}"
        print(f"| {iso} | {len(spans)} | {sessions} | {hours:.1f} | {dbu} | {usd_s} | {split} |")


DEV_PREFIX = re.compile(r"^\[dev ([^\]]+)\]")
_dev_users: dict[str, str] = {}


def mask_dev(name: str) -> str:
    """Tiền tố `[dev <user>]` chứa tên người dùng (mã sinh viên); thay bằng số thứ tự để không lộ."""
    m = DEV_PREFIX.match(name)
    if not m:
        return name
    label = _dev_users.setdefault(m.group(1), f"người {len(_dev_users) + 1}")
    return f"[dev {label}]" + name[m.end() :]


def jobs_section(profile: str, start_ms: int, end_ms: int) -> None:
    """Tổng thời gian chạy của job và pipeline. Không quy ra USD vì DBU/giờ tùy loại compute."""
    totals: dict[str, list[float]] = defaultdict(lambda: [0, 0.0])
    for r in dbx(profile, "jobs", "list-runs"):
        if start_ms <= r["start_time"] <= end_ms:
            t = totals[f"job `{mask_dev(r['run_name'])}`"]
            t[0] += 1
            t[1] += (r.get("run_duration") or 0) / 60_000

    for p in dbx(profile, "pipelines", "list-pipelines"):
        events = json.loads(
            run(
                "databricks",
                "api",
                "get",
                f"/api/2.0/pipelines/{p['pipeline_id']}/events?max_results=250",
                "--profile",
                profile,
            )
        ).get("events", [])
        spans: dict[str, list[int]] = {}
        for e in events:
            if e["event_type"] != "update_progress":
                continue
            ms = int(datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00")).timestamp() * 1000)
            uid = e["origin"].get("update_id", "")
            lo, hi = spans.get(uid, [ms, ms])
            spans[uid] = [min(lo, ms), max(hi, ms)]
        for lo, hi in spans.values():
            if start_ms <= lo <= end_ms:
                t = totals[f"pipeline `{mask_dev(p['name'])}`"]
                t[0] += 1
                t[1] += (hi - lo) / 60_000

    print("\n| Job / pipeline | Số lần chạy | Tổng phút |\n|---|---:|---:|")
    for name, (n, minutes) in sorted(totals.items(), key=lambda kv: -kv[1][1]):
        print(f"| {name} | {n} | {minutes:.0f} |")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="frm", default="2026-11-01", help="Ngày đầu, mặc định ngày bắt đầu budget")
    ap.add_argument("--to", required=True, help="Ngày cuối (YYYY-MM-DD), thường là ngày ghi số")
    ap.add_argument("--dbx-profile", help="Profile Databricks CLI; bỏ trống thì không in phần ước tính Databricks")
    ap.add_argument("--dbx-days", type=int, default=7, help="Số ngày gần nhất lấy lịch sử Databricks (mặc định 7)")
    args = ap.parse_args()

    sql_daily = azure_section(args.frm, args.to)
    if args.dbx_profile:
        end = datetime.combine(date.fromisoformat(args.to) + timedelta(days=1), datetime.min.time(), UTC)
        start = end - timedelta(days=args.dbx_days)
        start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        warehouse_section(args.dbx_profile, start_ms, end_ms, sql_daily)
        jobs_section(args.dbx_profile, start_ms, end_ms)


if __name__ == "__main__":
    main()
