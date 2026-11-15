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
    # python-код из файла: heredoc подменял бы stdin конвейера
    kubectl get pods -l app=hw11-service -o json 2>/dev/null \
      | python3 scripts/probe_row.py "$phase" "$((SECONDS - t0))" "$old_rs" >> "$OUT"
    sleep 2
  done
}

N=$(kubectl get pods -l app=hw11-service -o jsonpath='{.items[0].metadata.name}'); OLD_RS="${N%-*}"; OLD_RS="${OLD_RS%-*}"
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=20 \
  --set warm.readyBeforeWarmup=true --set limits.cpuBurnSeconds=0.25 >>"$LOG" 2>&1
probe defect 40 "$OLD_RS"
N=$(kubectl get pods -l app=hw11-service -o jsonpath='{.items[0].metadata.name}'); NEW_RS="${N%-*}"; NEW_RS="${NEW_RS%-*}"
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=20 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 >>"$LOG" 2>&1
probe fixed 40 "$NEW_RS"
# возврат к рабочей конфигурации опытов
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=5 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 >>"$LOG" 2>&1
kubectl rollout status deploy/hw11-service --timeout=120s >>"$LOG" 2>&1
echo "probe done"
