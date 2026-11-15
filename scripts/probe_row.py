"""Одна строка трейса probe (читает JSON подов с stdin)."""
import json
import sys

phase, t, old_rs = sys.argv[1], int(sys.argv[2]), sys.argv[3]
d = json.load(sys.stdin)
new_ready = all_ready = new_total = 0
for p in d["items"]:
    name = p["metadata"]["name"]
    rs = name.rsplit("-", 2)[0]
    ready = any(c.get("ready") for c in p["status"].get("containerStatuses", []))
    all_ready += ready
    if rs != old_rs:
        new_total += 1
        new_ready += ready
print(json.dumps({"phase": phase, "t": t, "ready_new": new_ready,
                  "new_total": new_total, "ready_all": all_ready}))
