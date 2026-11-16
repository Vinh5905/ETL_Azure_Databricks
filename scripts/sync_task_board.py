"""Đồng bộ `Input status` và `Còn chờ` trên GitHub Project theo Status và phụ thuộc.

Nguồn phụ thuộc: docs/tasks/catalog.json (start = gate bắt đầu, finish = gate hoàn tất).
Nguồn tiến độ: field Status trên Project (Todo, In Progress, Review, Needs Update, Done).

Cách dùng (cần `gh` đã đăng nhập, có scope `project`):
    python3 scripts/sync_task_board.py            # chỉ in các thẻ lệch
    python3 scripts/sync_task_board.py --apply    # ghi lại Input status và Còn chờ
    python3 scripts/sync_task_board.py --report   # in task Sẵn sàng theo Owner

Script không đổi Status, Owner, Assignees và không tạo thẻ.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "docs" / "tasks" / "catalog.json"
OWNER = "Vinh5905"
PROJECT_NUMBER = 8

# Status (cột) → Input status. Riêng Todo tính theo gate bắt đầu.
BY_STATUS = {
    "In Progress": "Đang thực hiện",
    "Review": "Chờ kiểm tra",
    "Needs Update": "Cần cập nhật",
    "Done": "Đã hoàn thành",
}


def gql(query: str, variables: dict) -> dict:
    payload = json.dumps({"query": query, "variables": variables}).encode()
    out = subprocess.run(["gh", "api", "graphql", "--input", "-"], input=payload, capture_output=True, check=True)
    data = json.loads(out.stdout)
    if data.get("errors"):
        raise SystemExit(json.dumps(data["errors"], ensure_ascii=False))
    return data["data"]


def load_project() -> tuple[str, dict, dict]:
    """Trả về (project_id, field theo tên, item theo số issue)."""
    q = """query($o:String!,$n:Int!,$c:String){user(login:$o){projectV2(number:$n){id
      fields(first:40){nodes{... on ProjectV2FieldCommon{id name}
        ... on ProjectV2SingleSelectField{options{id name}}}}
      items(first:100,after:$c){pageInfo{hasNextPage endCursor} nodes{id
        content{... on Issue{number}}
        fieldValues(first:30){nodes{
          ... on ProjectV2ItemFieldSingleSelectValue{name field{... on ProjectV2FieldCommon{name}}}
          ... on ProjectV2ItemFieldTextValue{text field{... on ProjectV2FieldCommon{name}}}}}}}}}}"""
    items, cursor, project = {}, None, None
    while True:
        project = gql(q, {"o": OWNER, "n": PROJECT_NUMBER, "c": cursor})["user"]["projectV2"]
        for node in project["items"]["nodes"]:
            number = (node.get("content") or {}).get("number")
            if number is None:
                continue
            values = {
                v["field"]["name"]: v.get("name", v.get("text"))
                for v in node["fieldValues"]["nodes"]
                if v and v.get("field")
            }
            items[number] = {"id": node["id"], **values}
        page = project["items"]["pageInfo"]
        if not page["hasNextPage"]:
            break
        cursor = page["endCursor"]
    fields = {f["name"]: f for f in project["fields"]["nodes"] if f}
    return project["id"], fields, items


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="ghi thay đổi lên Project")
    ap.add_argument("--report", action="store_true", help="in task Sẵn sàng theo Owner")
    args = ap.parse_args()

    tasks = json.loads(CATALOG.read_text())["tasks"]
    project_id, fields, items = load_project()
    status_of = {t["id"]: items.get(t["issue"], {}).get("Status", "Todo") for t in tasks}

    changes, ready = [], defaultdict(list)
    for t in tasks:
        item = items.get(t["issue"])
        if item is None:
            print(f"Thiếu thẻ trên Project: {t['id']} (#{t['issue']})")
            continue
        status = status_of[t["id"]]
        wait_start = [d for d in t["start"] if status_of.get(d) != "Done"]
        wait_finish = [d for d in t["finish"] if status_of.get(d) != "Done"]
        if status == "Todo" or status not in BY_STATUS:
            want_input = "Chờ đầu vào" if wait_start else "Sẵn sàng"
        else:
            want_input = BY_STATUS[status]
        parts = []
        if wait_start:
            parts.append("Bắt đầu: " + ", ".join(wait_start))
        if wait_finish:
            parts.append("Nên nhận: " + ", ".join(wait_finish))
        want_wait = "; ".join(parts) or "Không"
        if want_input == "Sẵn sàng":
            ready[item.get("Owner", "?")].append(f"{t['id']} — {t['title']}")
        if item.get("Input status") != want_input or item.get("Còn chờ") != want_wait:
            changes.append((t, item, want_input, want_wait))

    for t, item, want_input, want_wait in changes:
        print(f"{t['id']}: {item.get('Input status')} → {want_input} | Còn chờ: {want_wait}")
        if args.apply:
            option = next(o["id"] for o in fields["Input status"]["options"] if o["name"] == want_input)
            mutation = """mutation($p:ID!,$i:ID!,$f:ID!,$v:ProjectV2FieldValue!){
              updateProjectV2ItemFieldValue(input:{projectId:$p,itemId:$i,fieldId:$f,value:$v}){
                projectV2Item{id}}}"""
            gql(
                mutation,
                {
                    "p": project_id,
                    "i": item["id"],
                    "f": fields["Input status"]["id"],
                    "v": {"singleSelectOptionId": option},
                },
            )
            gql(
                mutation,
                {
                    "p": project_id,
                    "i": item["id"],
                    "f": fields["Còn chờ"]["id"],
                    "v": {"text": want_wait},
                },
            )
    print(f"{len(changes)} thẻ lệch" + (" đã cập nhật." if args.apply and changes else "."))

    if args.report:
        for owner in sorted(ready):
            print(f"\n## {owner}: {len(ready[owner])} task Sẵn sàng")
            for line in ready[owner]:
                print(f"- {line}")


if __name__ == "__main__":
    main()
