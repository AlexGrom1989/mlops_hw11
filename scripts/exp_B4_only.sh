#!/usr/bin/env bash
# Повтор только B4 (hot80 без keep-alive) на loadgen v6, где флаг работает.
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/bin:$PATH" NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
LOG=logs/exp-B.log
RAW=results/raw
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=5 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 \
  --set modelVersion=sim-cpu-v1 --set autoscaling.enabled=false >>"$LOG" 2>&1
kubectl rollout status deploy/hw11-service --timeout=120s >>"$LOG" 2>&1

tag=B4-hot80-noka; svc=hw11-service
for r in 1 2 3; do
  echo "[B] $tag прогон $r/3 (v6, no-keepalive работает) $(date +%H:%M:%S)" >>"$LOG"
  for i in 0 1 2 3; do
    rates=$([[ $i == 0 ]] && echo "--rps 1.0 --hot 100" || echo "--rps 0.083")
    kubectl delete pod "loadgen-$i" --force --grace-period=0 >>"$LOG" 2>&1 || true
    kubectl run "loadgen-$i" --image=hw11-loadgen:v6 --restart=Never \
      --env="LG_ARGS=--url http://$svc:8000 --duration 120 --stream --sessions 8 --client-id c$i $rates --no-keepalive" \
      --env="PYTHONUNBUFFERED=1" >>"$LOG" 2>&1
  done
  sleep 140
  for i in 0 1 2 3; do
    kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/loadgen-$i" \
      --timeout=90s >>"$LOG" 2>&1 || true
    kubectl logs "pod/loadgen-$i" > "$RAW/expB-$tag-run$r-c$i.jsonl" 2>>"$LOG"
    kubectl delete pod "loadgen-$i" --force --grace-period=0 >>"$LOG" 2>&1 || true
  done
  python3 scripts/exp_B_analysis.py "$RAW" "$tag" "$r" | tee -a "$LOG"
done
echo "[B] B4 повтор завершён" >>"$LOG"
