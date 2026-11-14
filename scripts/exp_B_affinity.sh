#!/usr/bin/env bash
# Опыт B: привязка к Pod и горячая реплика (REPORT раздел 3).
# Комбинации (каждая — 3 прогона, фаза измерения 120 с):
#   B1 обычный Service, равномерный трафик (4 клиента)
#   B2 обычный Service, hot80 (клиент c0 — 80% объёма, горячий префикс)
#   B3 Service с sessionAffinity: ClientIP, hot80
#   B4 обычный Service, hot80, БЕЗ keep-alive (исправление: per-request
#      балансировка — очередь выравнивается, кеш-локальность падает)
#   B5 affinity, равномерный — случай, когда ожидаемый перекос НЕ возникает
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/bin:$PATH" NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
LOG=logs/exp-B.log
RAW=results/raw
mkdir -p logs "$RAW"

helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=5 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 \
  --set modelVersion=sim-cpu-v1 --set autoscaling.enabled=false >>"$LOG" 2>&1
kubectl rollout status deploy/hw11-service --timeout=120s >>"$LOG" 2>&1

# Клиент c0 несёт 1.0 rps (80% из 1.25), остальные — по 0.083.
# --hot 100 у c0: все его промпты с общим горячим префиксом.
run_combo() {  # <тег> <svc> <extra>
  local tag=$1 svc=$2 extra=$3
  local base="--url http://$svc:8000 --duration 120 --stream --sessions 8 $extra"
  local hot_args="--rps 1.0 --hot 100"
  local rest_args="--rps 0.083"
  for r in 1 2 3; do
    echo "[B] $tag прогон $r/3 $(date +%H:%M:%S)" >>"$LOG"
    for i in 0 1 2 3; do
      local rates
      if [[ "$extra" == *"--hot-clients"* ]]; then
        rates=$([[ $i == 0 ]] && echo "$hot_args" || echo "$rest_args")
      else
        rates="--rps 0.25"
      fi
      kubectl delete pod "loadgen-$i" --force --grace-period=0 >>"$LOG" 2>&1 || true
      kubectl run "loadgen-$i" --image=hw11-loadgen:v5 --restart=Never \
        --env="LG_ARGS=$base --client-id c$i $rates" \
        --env="PYTHONUNBUFFERED=1" >>"$LOG" 2>&1
    done
    sleep 140  # фаза 120 с + прогрев/хвост
    for i in 0 1 2 3; do
      kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/loadgen-$i" \
        --timeout=90s >>"$LOG" 2>&1 || true
      kubectl logs "pod/loadgen-$i" > "$RAW/expB-$tag-run$r-c$i.jsonl" 2>>"$LOG"
      kubectl delete pod "loadgen-$i" --force --grace-period=0 >>"$LOG" 2>&1 || true
    done
    python3 scripts/exp_B_analysis.py "$RAW" "$tag" "$r" | tee -a "$LOG"
  done
}

echo "[B] подготовка: стабильные 2 реплики" >>"$LOG"
run_combo "B1-uniform-plain"    hw11-service         ""
run_combo "B2-hot80-plain"      hw11-service         "--hot-clients"
run_combo "B3-hot80-affinity"   hw11-service-affinity "--hot-clients"
run_combo "B4-hot80-noka"       hw11-service         "--hot-clients --no-keepalive"
run_combo "B5-uniform-affinity" hw11-service-affinity ""
echo "[B] опыт B завершён" >>"$LOG"
