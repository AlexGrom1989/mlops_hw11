#!/usr/bin/env bash
# C1-повтор с надёжным трейсом: jsonpath desired/current каждые 10 с +
# нагрузка 3 rps (4 клиента × 0.75, --no-keepalive, cpuBurn 0.25).
# Пишет results/raw/expC-hpa-series.jsonl (t, desired, current, cpu_m pods).
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/bin:$PATH" NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
LOG=logs/exp-C.log
RAW=results/raw
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=5 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 \
  --set modelVersion=sim-cpu-v1 --set autoscaling.enabled=true >>"$LOG" 2>&1
kubectl rollout status deploy/hw11-service --timeout=120s >>"$LOG" 2>&1
kubectl delete pod loadgen-0 loadgen-1 loadgen-2 loadgen-3 --force --grace-period=0 >>"$LOG" 2>&1 || true
for i in 0 1 2 3; do
  kubectl run "loadgen-$i" --image=hw11-loadgen:v6 --restart=Never \
    --env="LG_ARGS=--url http://hw11-service:8000 --client-id c$i --rps 0.75 --duration 300 --stream --sessions 8 --no-keepalive" \
    --env="PYTHONUNBUFFERED=1" >>"$LOG" 2>&1
done
say() { echo "[$(date +%H:%M:%S)] $*" >>"$LOG"; }
say "=== C1-mini: трейс HPA на нагрузке ~3 rps"
T0=$(date +%s)
for i in $(seq 0 55); do
  D=$(kubectl get hpa hw11-service -o jsonpath='{.status.desiredReplicas}' 2>/dev/null)
  C=$(kubectl get hpa hw11-service -o jsonpath='{.status.currentReplicas}' 2>/dev/null)
  M=$(kubectl get hpa hw11-service -o jsonpath='{.status.currentMetrics[0].resource.current.averageUtilization}' 2>/dev/null)
  CPU=$(kubectl top pods -l app=hw11-service --no-headers 2>/dev/null | awk '{s+=$2} END {print s}')
  echo "{\"t\": $(( $(date +%s) - T0 )), \"desired\": \"$D\", \"current\": \"$C\", \"cpu_pct\": \"$M\", \"cpu_m_total\": \"$CPU\"}" >> "$RAW/expC-hpa-series.jsonl"
  sleep 10
done
say "снимаю нагрузку; наблюдаю возврат к минимуму"
for i in 0 1 2 3; do
  kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/loadgen-$i" --timeout=120s >>"$LOG" 2>&1 || true
  kubectl logs "pod/loadgen-$i" > "$RAW/expC-C1mini-c$i.jsonl" 2>>"$LOG"
  kubectl delete pod "loadgen-$i" --force --grace-period=0 >>"$LOG" 2>&1 || true
done
RETURN_T=""
for i in $(seq 0 36); do
  D=$(kubectl get hpa hw11-service -o jsonpath='{.status.desiredReplicas}' 2>/dev/null)
  C=$(kubectl get hpa hw11-service -o jsonpath='{.status.currentReplicas}' 2>/dev/null)
  echo "{\"t\": $(( $(date +%s) - T0 )), \"desired\": \"$D\", \"current\": \"$C\", \"cpu_pct\": \"\", \"cpu_m_total\": \"\"}" >> "$RAW/expC-hpa-series.jsonl"
  [ "$C" = "2" ] && [ -z "$RETURN_T" ] && { RETURN_T=$(( $(date +%s) - T0 )); say "возврат к 2 репликам через ${RETURN_T} с от старта"; }
  [ -n "$RETURN_T" ] && [ $(( $(date +%s) - T0 )) -ge $((RETURN_T + 60)) ] && break
  sleep 10
done
say "C1-mini клиентские итоги:"
for i in 0 1 2 3; do python3 scripts/summarize.py "$RAW/expC-C1mini-c$i.jsonl" >>"$LOG" 2>&1; done
say "=== C1-mini завершён"
