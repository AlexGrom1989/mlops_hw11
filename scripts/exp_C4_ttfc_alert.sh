#!/usr/bin/env bash
# C4: управляемый переход HW11TTFCSlowP95 pending→firing→ok.
# Нагрузка выше ёмкости, но ниже порога очереди-отказа (64): очередь растёт,
# p95 TTFC переваливает за 5 с — алерт pending, через for=30s — firing;
# после снятия нагрузки — возврат в норму. Трейс каждые 15 с.
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/bin:$PATH" NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
LOG=logs/exp-C.log
RAW=results/raw
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=5 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 \
  --set modelVersion=sim-cpu-v1 --set autoscaling.enabled=false >>"$LOG" 2>&1
kubectl rollout status deploy/hw11-service --timeout=120s >>"$LOG" 2>&1
kubectl delete pod loadgen-0 loadgen-1 loadgen-2 loadgen-3 --force --grace-period=0 >>"$LOG" 2>&1 || true
say() { echo "[$(date +%H:%M:%S)] $*" >>"$LOG"; }
say "=== C4: переход HW11TTFCSlowP95 (нагрузка 3.6 rps при ёмкости ~2.7)"
for i in 0 1 2 3; do
  kubectl run "loadgen-$i" --image=hw11-loadgen:v6 --restart=Never \
    --env="LG_ARGS=--url http://hw11-service:8000 --client-id c$i --rps 0.9 --duration 300 --stream --sessions 8 --no-keepalive" \
    --env="PYTHONUNBUFFERED=1" >>"$LOG" 2>&1
done
for t in $(seq 0 30); do
  A=$(curl -s --noproxy '*' "http://[::1]:19090/api/v1/alerts" 2>/dev/null)
  echo "$(date +%H:%M:%S) $A" >> "$RAW/expC-ttfc-alerts-trace.log"
  sleep 15
done
for i in 0 1 2 3; do
  kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/loadgen-$i" --timeout=120s >>"$LOG" 2>&1 || true
  kubectl logs "pod/loadgen-$i" > "$RAW/expC-C4-c$i.jsonl" 2>>"$LOG"
  kubectl delete pod "loadgen-$i" --force --grace-period=0 >>"$LOG" 2>&1 || true
done
say "C4: нагрузка снята — наблюдаем возврат в норму"
for t in $(seq 0 6); do
  A=$(curl -s --noproxy '*' "http://[::1]:19090/api/v1/alerts" 2>/dev/null)
  echo "$(date +%H:%M:%S) $A" >> "$RAW/expC-ttfc-alerts-trace.log"
  sleep 15
done
say "=== C4 завершён"
