"""Đồng bộ `Status` và `Waiting on` trên GitHub Project theo PR và phụ thuộc giữa các task.

Nguồn phụ thuộc: docs/tasks/catalog.json (start = gate bắt đầu, finish = gate hoàn tất).
PR gắn với issue qua `Closes #<số>` trong mô tả PR.

Status được suy ra theo thứ tự ưu tiên:
1. Done: issue đã đóng dạng completed (thường do PR merge).
2. PR đang mở, không phải draft:
   - Needs Update: sau commit cuối của PR có người khác tác giả comment/review (không phải approve).
   - Review: còn lại (vừa mở PR, đã push commit mới sau góp ý, hoặc đã approve chờ merge).
3. Chỉ có PR draft: In Progress.
4. Không có PR mở:
   - In Progress nếu người làm tự kéo thẻ sang đó, hoặc PR vừa bị đóng mà không merge.
   - Còn lại: Blocked nếu còn task ở gate bắt đầu chưa Done, ngược lại Ready.

Cách dùng (cần `gh` đã đăng nhập, có scope `project`):
    python3 scripts/sync_task_board.py            # chỉ in các thẻ lệch
    python3 scripts/sync_task_board.py --apply    # ghi lại Status và Waiting on
    python3 scripts/sync_task_board.py --report   # in task Ready theo Owner

Script không đổi Owner, Assignees và không tạo thẻ. Workflow
`.github/workflows/sync-task-board.yml` chạy `--apply` khi có sự kiện issue/PR/review
và theo lịch, dùng secret `PROJECT_TOKEN`.
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
STATUS_FIELD = "Status"
WAITING_FIELD = "Waiting on"

ITEMS_QUERY = """query($o:String!,$n:Int!,$c:String){user(login:$o){projectV2(number:$n){id
  fields(first:40){nodes{... on ProjectV2FieldCommon{id name}
    ... on ProjectV2SingleSelectField{options{id name}}}}
  items(first:50,after:$c){pageInfo{hasNextPage endCursor} nodes{id
    content{... on Issue{number state stateReason
      closedByPullRequestsReferences(first:5,includeClosedPrs:false){nodes{
        number isDraft state author{login}
        commits(last:1){nodes{commit{committedDate}}}
        reviews(last:30){nodes{state submittedAt author{__typename login}}}
        comments(last:30){nodes{createdAt author{__typename login}}}}}}}
    fieldValues(first:30){nodes{
      ... on ProjectV2ItemFieldSingleSelectValue{name field{... on ProjectV2FieldCommon{name}}}
      ... on ProjectV2ItemFieldTextValue{text field{... on ProjectV2FieldCommon{name}}}}}}}}}}"""

SET_VALUE = """mutation($p:ID!,$i:ID!,$f:ID!,$v:ProjectV2FieldValue!){
  updateProjectV2ItemFieldValue(input:{projectId:$p,itemId:$i,fieldId:$f,value:$v}){projectV2Item{id}}}"""


def gql(query: str, variables: dict) -> dict:
    payload = json.dumps({"query": query, "variables": variables}).encode()
    out = subprocess.run(["gh", "api", "graphql", "--input", "-"], input=payload, capture_output=True, check=True)
    data = json.loads(out.stdout)
    if data.get("errors"):
        raise SystemExit(json.dumps(data["errors"], ensure_ascii=False))
    return data["data"]


def load_project() -> tuple[str, dict, dict]:
    """Trả về (project_id, field theo tên, item theo số issue)."""
    items, cursor, project = {}, None, None
    while True:
        project = gql(ITEMS_QUERY, {"o": OWNER, "n": PROJECT_NUMBER, "c": cursor})["user"]["projectV2"]
        for node in project["items"]["nodes"]:
            issue = node.get("content") or {}
            if issue.get("number") is None:
                continue
            values = {
                v["field"]["name"]: v.get("name", v.get("text"))
                for v in node["fieldValues"]["nodes"]
                if v and v.get("field")
            }
            items[issue["number"]] = {"id": node["id"], "issue": issue, **values}
        page = project["items"]["pageInfo"]
        if not page["hasNextPage"]:
            break
        cursor = page["endCursor"]
    fields = {f["name"]: f for f in project["fields"]["nodes"] if f}
    return project["id"], fields, items


def pr_status(pr: dict) -> str:
    """Review hoặc Needs Update cho một PR đang mở, không phải draft."""
    author = (pr.get("author") or {}).get("login")
    last_commit = max((c["commit"]["committedDate"] for c in pr["commits"]["nodes"]), default="")
    feedback = []
    for r in pr["reviews"]["nodes"]:
        feedback.append((r.get("submittedAt") or "", r.get("author"), r["state"]))
    for c in pr["comments"]["nodes"]:
        feedback.append((c["createdAt"], c.get("author"), "COMMENTED"))
    # Chỉ tính góp ý của người khác tác giả, không tính bot (vd. comment plan của tf-plan),
    # và chỉ những góp ý sau commit cuối cùng.
    feedback = [
        (ts, state)
        for ts, who, state in feedback
        if ts > last_commit
        and who
        and who.get("__typename") != "Bot"
        and who.get("login") != author
        and state != "PENDING"
    ]
    if not feedback:
        return "Review"
    _, latest = max(feedback)
    return "Review" if latest == "APPROVED" else "Needs Update"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="ghi thay đổi lên Project")
    ap.add_argument("--report", action="store_true", help="in task Ready theo Owner")
    args = ap.parse_args()

    tasks = {t["issue"]: t for t in json.loads(CATALOG.read_text())["tasks"]}
    project_id, fields, items = load_project()
    status_field = fields[STATUS_FIELD]
    option_id = {o["name"]: o["id"] for o in status_field["options"]}

    def is_done(item: dict) -> bool:
        issue = item["issue"]
        return issue["state"] == "CLOSED" and issue.get("stateReason") == "COMPLETED"

    done_ids = {tasks[n]["id"] for n, it in items.items() if n in tasks and is_done(it)}

    changes, ready = [], defaultdict(list)
    for number, item in sorted(items.items()):
        task = tasks.get(number)
        current = item.get(STATUS_FIELD)
        prs = [p for p in item["issue"]["closedByPullRequestsReferences"]["nodes"] if p["state"] == "OPEN"]
        open_prs = [p for p in prs if not p["isDraft"]]

        wait_start = [d for d in task["start"] if d not in done_ids] if task else []
        wait_finish = [d for d in task["finish"] if d not in done_ids] if task else []

        if is_done(item):
            want = "Done"
        elif open_prs:
            states = {pr_status(p) for p in open_prs}
            want = "Needs Update" if "Needs Update" in states else "Review"
        elif prs or current in ("In Progress", "Review", "Needs Update"):
            # PR bị đóng mà không merge thì thẻ quay về In Progress, không về Ready.
            want = "In Progress"
        elif task:
            want = "Blocked" if wait_start else "Ready"
        else:
            # Issue ngoài catalog (việc phát sinh): không có phụ thuộc để tính.
            want = current or "Ready"

        parts = []
        if wait_start:
            parts.append("Bắt đầu: " + ", ".join(wait_start))
        if wait_finish:
            parts.append("Hoàn tất: " + ", ".join(wait_finish))
        want_wait = ("; ".join(parts) or "Không") if task else item.get(WAITING_FIELD)

        label = task["id"] if task else f"#{number}"
        if want == "Ready" and task:
            ready[item.get("Owner", "?")].append(f"{task['id']} — {task['title']}")
        if current != want or item.get(WAITING_FIELD) != want_wait:
            changes.append((label, item, current, want, want_wait))

    for label, item, current, want, want_wait in changes:
        print(f"{label}: {current} → {want} | Waiting on: {want_wait}")
        if not args.apply:
            continue
        if current != want:
            value = {"singleSelectOptionId": option_id[want]}
            gql(SET_VALUE, {"p": project_id, "i": item["id"], "f": status_field["id"], "v": value})
        if want_wait is not None and item.get(WAITING_FIELD) != want_wait:
            value = {"text": want_wait}
            gql(SET_VALUE, {"p": project_id, "i": item["id"], "f": fields[WAITING_FIELD]["id"], "v": value})
    print(f"{len(changes)} thẻ lệch" + (" đã cập nhật." if args.apply and changes else "."))

    if args.report:
        for owner in sorted(ready):
            print(f"\n## {owner}: {len(ready[owner])} task Ready")
            for line in ready[owner]:
                print(f"- {line}")


if __name__ == "__main__":
    main()
