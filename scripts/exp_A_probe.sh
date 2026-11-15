#!/usr/bin/env bash
# Покадровый трейс Ready по фазам дефекта/исправления (для fig-A):
# каждые 2 с: время, Ready среди подов НОВОГО RS, Ready всего.
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/bin:$PATH" NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
OUT=results/raw/expA-ready-trace.jsonl
LOG=logs/exp-A.log
: > "$OUT"

probe() {  # $1 phase, $2 duration, $3 RS старого релиза (до фазы)
  local phase=$1 dur=$2 old_rs=$3 t0=$SECONDS
  for i in $(seq 0 $((dur / 2))); do
    kubectl get pods -l app=hw11-service -o json 2>/dev/null | python3 - "$phase" "$((SECONDS - t0))" "$old_rs" >> "$OUT" <<'EOF'
import json, sys
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
EOF
    sleep 2
  done
}

OLD_RS=$(kubectl get pods -l app=hw11-service -o jsonpath='{.items[0].metadata.name}' | rev | cut -d- -f3- | rev)
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=20 \
  --set warm.readyBeforeWarmup=true --set limits.cpuBurnSeconds=0.25 >>"$LOG" 2>&1
probe defect 40 "$OLD_RS"
NEW_RS=$(kubectl get pods -l app=hw11-service -o jsonpath='{.items[0].metadata.name}' | rev | cut -d- -f3- | rev)
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=20 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 >>"$LOG" 2>&1
probe fixed 40 "$NEW_RS"
# возврат к рабочей конфигурации опытов
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=5 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 >>"$LOG" 2>&1
kubectl rollout status deploy/hw11-service --timeout=120s >>"$LOG" 2>&1
echo "probe done"
