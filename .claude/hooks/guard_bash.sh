#!/usr/bin/env bash
# PreToolUse (Bash): chặn lệnh có thể phá hạ tầng, lộ secret hoặc state.
# Lớp thứ hai sau permissions.deny, bắt được cả lệnh ghép (cd … && terraform apply).
# Chỉ xét lệnh ở ĐẦU mỗi đoạn (tách theo xuống dòng, |, ;, &&, ||) và bỏ qua thân heredoc,
# nên văn bản nhắc tới "terraform apply" trong file đang viết không bị chặn nhầm.
# Exit 2 = chặn; nội dung stderr được gửi lại cho Claude làm lý do.
# P1 cần apply bootstrap từ máy: đặt "env": {"ALLOW_TF_APPLY": "1"} trong .claude/settings.local.json.
exec python3 -c '
import json, os, re, sys

try:
    cmd = json.load(sys.stdin).get("tool_input", {}).get("command", "") or ""
except Exception:
    sys.exit(0)

# Bỏ thân heredoc: từ dòng chứa <<TAG tới dòng chỉ có TAG.
lines, out, tag = cmd.splitlines(), [], None
for line in lines:
    if tag is not None:
        if line.strip() == tag:
            tag = None
        continue
    m = re.search(r"<<-?\s*[\x27\x22]?(\w+)[\x27\x22]?", line)
    if m:
        tag = m.group(1)
    out.append(line)

segments = [s.strip() for s in re.split(r"\n|\|\||&&|[|;&]", "\n".join(out)) if s.strip()]
prefix = r"^(?:\(\s*)?(?:\w+=\S*\s+)*(?:sudo\s+|exec\s+|command\s+)?"

allow_apply = os.environ.get("ALLOW_TF_APPLY") == "1"
rules = [
    (r"terraform\b(?:\s+-chdir=\S+)?\s+(apply|destroy)\b", "terraform apply/destroy chỉ chạy qua PR → workflow tf-apply (AGENTS.md).", allow_apply),
    (r"terraform\b(?:\s+-chdir=\S+)?\s+(force-unlock|state\s+(rm|mv|push))\b", "Thao tác trực tiếp lên state phải do P1 làm bằng tay.", False),
    (r"databricks\s+bundle\s+(deploy|destroy)\b.*(-t|--target)[ =]?prod\b", "Deploy/destroy bundle prod chỉ chạy qua workflow de-deploy.", False),
    (r"az\s+(group|resource)\s+delete\b", "Không xóa tài nguyên Azure từ Claude; dùng terraform qua PR.", False),
    (r"az\s+keyvault\s+secret\s+(show|download)\b", "Không in giá trị secret ra màn hình.", False),
    (r"git\s+(commit\b.*(--no-verify\b|\s-n\b)|push\b.*--no-verify\b)","Không lách hook (--no-verify); sửa lỗi theo CONTRIBUTING.md.", False),
    (r"(cat|head|tail|less|more|bat|sed|awk|grep|rg|cp|scp|curl)\b.*[\w./-]+\.(auto\.tfvars|tfstate)(?![\w.])", "Không đọc *.auto.tfvars / *.tfstate (chứa ID và giá trị nhạy cảm).", False),
]

def block(reason):
    print(f"Bị chặn bởi .claude/hooks/guard_bash.sh: {reason}", file=sys.stderr)
    sys.exit(2)


def current_branch():
    try:
        import subprocess
        out = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True, timeout=5)
        return out.stdout.strip()
    except Exception:
        return ""


def check_push(args):
    """Chỉ cho --force-with-lease lên nhánh PR; mọi kiểu force lên main đều bị chặn."""
    force = any(a == "--force" or re.fullmatch(r"-[A-Za-z]*f[A-Za-z]*", a) for a in args)
    lease = any(a.startswith("--force-with-lease") for a in args)
    positional = [a for a in args if not a.startswith("-")]
    refspecs = positional[1:]
    if force or any(r.startswith("+") for r in refspecs):
        block("Không dùng --force, -f hay refspec +; nhánh PR chỉ được --force-with-lease (CONTRIBUTING.md §2).")
    if not lease:
        return
    if any(a in ("--all", "--mirror", "--tags") for a in args):
        block("--force-with-lease chỉ dùng cho một nhánh PR, không kèm --all/--mirror/--tags.")
    targets = []
    for ref in refspecs or ["HEAD"]:
        dst = ref.split(":")[-1] or ref
        targets.append(current_branch() if dst == "HEAD" else dst)
    for dst in targets:
        if dst.removeprefix("refs/heads/") in ("main", "") or dst == "HEAD":
            block("Không bao giờ force-push lên main (hoặc không xác định được nhánh đích).")


for seg in segments:
    for pattern, reason, bypass in rules:
        if not bypass and re.search(prefix + pattern, seg):
            block(reason)
    m = re.search(prefix + r"git\s+push\b(.*)", seg)
    if m:
        check_push(m.group(1).split())
sys.exit(0)
'
