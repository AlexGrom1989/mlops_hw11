#!/usr/bin/env bash
# Точечное доказательство дефекта A1 (журналы реплик), отдельным прогоном:
# дефектный прогрев 20 с → 25 с нагрузки → журнал «до WARMUP_DONE» → откат.
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/bin:$PATH" NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
LOG=logs/exp-A.log
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=20 \
  --set warm.readyBeforeWarmup=true --set limits.cpuBurnSeconds=0.25 >>"$LOG" 2>&1
kubectl delete pod loadgen-pod --force --grace-period=0 >>"$LOG" 2>&1 || true
kubectl run loadgen-pod --image=hw11-loadgen:v6 --restart=Never \
  --env="LG_ARGS=--url http://hw11-service:8000 --rps 2 --duration 25 --stream --sessions 16" \
  --env="PYTHONUNBUFFERED=1" >>"$LOG" 2>&1
sleep 40
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/loadgen-pod --timeout=60s >>"$LOG" 2>&1 || true
kubectl logs loadgen-pod > results/raw/expA1-defect-evidence.jsonl 2>>"$LOG"
python3 scripts/summarize.py results/raw/expA1-defect-evidence.jsonl | tee -a "$LOG"
echo "журнальное доказательство (до WARMUP_DONE):" | tee -a "$LOG"
python3 scripts/exp_A_warmup_evidence.py | tee -a "$LOG"
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=5 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 >>"$LOG" 2>&1
