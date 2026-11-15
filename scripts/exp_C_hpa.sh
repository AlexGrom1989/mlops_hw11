#!/usr/bin/env bash
# Опыт C: HPA autoscaling/v2 2→3→2 по CPU + алерты (REPORT раздел 4).
# C1: вычислительная нагрузка (cpuBurn 0.25 с/запрос), суммарно ~3 rps
#     через 4 клиента → CPU выше 60% → HPA 2→3; фиксируем время появления
#     Pod и время готовности; затем снятие нагрузки → возврат к 2 с учётом
#     stabilizationWindowSeconds=120 (записываем фактическую длительность).
# C2: повтор горячей реплики при 3 репликах с ClientIP-привязкой:
#     существующие keep-alive соединения остаются на старых репликах.
# C3: алерты: нагрузка выше ёмкости → HW11TTFCSlowP95 и HW11ErrorRateHigh
#     переходят pending→firing, после снятия → ok (наблюдение каждые 15 с).
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/bin:$PATH" NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
LOG=logs/exp-C.log
RAW=results/raw
mkdir -p logs "$RAW"

start_clients() {  # $1 rps, $2 duration, $3 svc
  for i in 0 1 2 3; do
    kubectl delete pod "loadgen-$i" --force --grace-period=0 >>"$LOG" 2>&1 || true
    kubectl run "loadgen-$i" --image=hw11-loadgen:v6 --restart=Never \
      --env="LG_ARGS=--url http://$3:8000 --client-id c$i --rps $1 --duration $2 --stream --sessions 8 --no-keepalive" \
      --env="PYTHONUNBUFFERED=1" >>"$LOG" 2>&1
  done
}
stop_clients() {
  for i in 0 1 2 3; do
    kubectl delete pod "loadgen-$i" --force --grace-period=0 >>"$LOG" 2>&1 || true
  done
}
collect_clients() {  # $1 суффикс файла
  for i in 0 1 2 3; do
    kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/loadgen-$i" \
      --timeout=60s >>"$LOG" 2>&1 || true
    kubectl logs "pod/loadgen-$i" > "$RAW/expC-$1-c$i.jsonl" 2>>"$LOG"
  done
}

say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

say "=== C0: базовая конфигурация: HPA 2..3, cpuBurn 0.25, target 60%"
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=5 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 \
  --set modelVersion=sim-cpu-v1 --set autoscaling.enabled=true >>"$LOG" 2>&1
kubectl rollout status deploy/hw11-service --timeout=120s >>"$LOG" 2>&1
kubectl get hpa >>"$LOG" 2>&1
say "проверка resource metrics API и CPU requests:"
kubectl top pods >>"$LOG" 2>&1
kubectl get pods -o json | python3 -c '
import json,sys
for p in json.load(sys.stdin)["items"]:
    if p["metadata"]["labels"].get("app")=="hw11-service":
        c=p["spec"]["containers"][0]
        print(" ", p["metadata"]["name"], "requests:", c.get("resources",{}).get("requests"))' >>"$LOG" 2>&1

say "=== C1: нагрузка ~3 rps (вычислительная), наблюдение HPA каждые 10 с"
start_clients 0.75 300 hw11-service
T0=$(date +%s)
( for t in $(seq 0 42); do
    H=$(kubectl get hpa hw11-service -o json 2>/dev/null \
      | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d["status"].get("currentReplicas"), d["status"].get("desiredReplicas"), [c.currentValue for c in d["status"].get("currentMetrics",[])])' 2>/dev/null)
    echo "$(date +%H:%M:%S) hpa: $H" >> "$RAW/expC-hpa-trace.log"
    sleep 10
  done ) &
TRACE_PID=$!
# Клиенты идут 300 с: дожидаемся их завершения, собираем журналы и только
# потом удаляем (урок первого прогона: force delete до collect → 4 пустых
# JSONL при живом HPA-трейсе).
sleep 310
collect_clients "C1"
say "снимаю нагрузку; возврат к минимуму наблюдаем отдельно"
stop_clients
# scaleDown stabilizationWindow=120s + периоды: ждём до 6 минут
for t in $(seq 0 35); do
  R=$(kubectl get hpa hw11-service -o jsonpath='{.status.currentReplicas}' 2>/dev/null)
  echo "$(date +%H:%M:%S) replicas=$R" >> "$RAW/expC-scaledown-trace.log"
  [ "$R" = "2" ] && { say "возврат к 2 репликам через $(( $(date +%s) - T0 )) с от старта опыта"; break; }
  sleep 10
done
kill $TRACE_PID 2>/dev/null
say "C1 клиентские итоги:"
for i in 0 1 2 3; do python3 scripts/summarize.py "$RAW/expC-C1-c$i.jsonl" >>"$LOG" 2>&1; done
kubectl get hpa >>"$LOG" 2>&1

say "=== C2: горячая реплика при 3 репликах с ClientIP-привязкой"
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=5 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 \
  --set autoscaling.enabled=true >>"$LOG" 2>&1
# принудительно 3 реплики: расширяю minReplicas на время опыта C2
kubectl patch deploy hw11-service --type=json \
  -p='[{"op":"replace","path":"/spec/replicas","value":3}]' >>"$LOG" 2>&1 || true
sleep 15
start_clients 1.5 90 hw11-service-affinity
sleep 110
collect_clients "C2"
stop_clients
say "C2 распределение при 3 репликах и привязке:"
python3 - <<'EOF' | tee -a "$LOG"
import json, pathlib
rows = []
for i in range(4):
    p = pathlib.Path(f"results/raw/expC-C2-c{i}.jsonl")
    for l in p.read_text().splitlines():
        if l.startswith("{"):
            rows.append(json.loads(l))
ok = [r for r in rows if r.get("outcome") == "completed"]
from collections import Counter
print("  по Pod:", Counter(r["pod"] for r in ok))
print("  по клиенту x Pod:")
for c in sorted({r["client_id"] for r in ok}):
    print("   ", c, Counter(r["pod"].split("-")[-2] for r in ok if r["client_id"] == c))
EOF
kubectl patch deploy hw11-service --type=json \
  -p='[{"op":"replace","path":"/spec/replicas","value":2}]' >>"$LOG" 2>&1 || true

say "=== C3: алерты — нагрузка выше ёмкости"
# 3 rps при ёмкости ~1.9 rps: TTFC p95 > 5с и rejected > 5%.
start_clients 0.75 240 hw11-service
( for t in $(seq 0 20); do
    A=$(curl -s --noproxy '*' "http://[::1]:19090/api/v1/alerts" 2>/dev/null)
    echo "$(date +%H:%M:%S) $A" >> "$RAW/expC-alerts-trace.log"
    sleep 15
  done ) &
ALERT_PID=$!
sleep 240
stop_clients
say "после снятия нагрузки: ожидаем ok алертов; наблюдение ещё 90 с"
for t in $(seq 0 6); do
  A=$(curl -s --noproxy '*' "http://[::1]:19090/api/v1/alerts" 2>/dev/null)
  echo "$(date +%H:%M:%S) $A" >> "$RAW/expC-alerts-trace.log"
  sleep 15
done
kill $ALERT_PID 2>/dev/null
say "=== опыт C завершён"
