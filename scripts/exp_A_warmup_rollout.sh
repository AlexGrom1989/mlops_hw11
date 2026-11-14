#!/usr/bin/env bash
# Опыт A: прогрев, обновление и откат (REPORT раздел 2).
# Фазы:
#   A1 дефект: readiness отдаёт 200 до прогрева (warm.readyBeforeWarmup=true),
#      нагрузка подана сразу после rollout — непрогретые реплики отвечают 503.
#   A2 исправление: readyBeforeWarmup=false, тот же прогон — rejected=0.
#   A3 штатный rolling update A(sim-cpu-v1)→B(sim-cpu-v2) под нагрузкой
#      с длинным потоковым ответом: 0 5xx и незавершённых потоков.
#   A4 неготовая B: артефакт с битым хешем → Pod не Ready → вне EndpointSlice;
#      управляемый откат к A; версия подтверждается по фактическим ответам.
#   A5 принудительное завершение (kill) во время стримов: оборванные запросы,
#      обнаружение отказа, восстановление.
# Каждая фаза пишет журнал logs/exp-A.log и сырые результаты results/raw/.
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/bin:$PATH" NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
LOG=logs/exp-A.log
RAW=results/raw
mkdir -p logs "$RAW"
TS() { date +%H:%M:%S; }
say() { echo "[$(TS)] $*" | tee -a "$LOG"; }

wait_rollout() { kubectl rollout status deploy/hw11-service --timeout=120s >/dev/null 2>&1; }
snapshot() {  # <метка>: Ready + EndpointSlice + версии
  local label=$1
  {
    echo "--- snapshot $label $(date +%H:%M:%S)"
    kubectl get pods -l app=hw11-service -o json \
      | python3 -c 'import json,sys; d=json.load(sys.stdin); [print(p["metadata"]["name"], p["status"].get("phase"), [c.get("ready") for c in p["status"].get("containerStatuses",[])]) for p in d["items"]]'
    kubectl get endpointslice -l kubernetes.io/service-name=hw11-service -o json \
      | python3 -c 'import json,sys; d=json.load(sys.stdin); print("endpoints:", [e["addresses"] for s in d["items"] for e in s.get("endpoints",[])], "ready:", [e.get("conditions",{}).get("ready") for s in d["items"] for e in s.get("endpoints",[])])'
  } >>"$LOG" 2>&1
}

start_clients() {  # $1: LG_ARGS — один клиент-под (с хоста управляем моментом)
  kubectl delete pod loadgen-pod --force --grace-period=0 >/dev/null 2>&1
  kubectl run loadgen-pod --image=hw11-loadgen:v5 --restart=Never \
    -l job-name=loadgen-single \
    --overrides='{"spec":{"containers":[{"name":"loadgen","image":"hw11-loadgen:v5","imagePullPolicy":"IfNotPresent","env":[{"name":"LG_ARGS","value":"'"$1"'"},{"name":"PYTHONUNBUFFERED","value":"1"}],"resources":{"limits":{"memory":"128Mi"}}}],"restartPolicy":"Never"}}' \
    >>"$LOG" 2>&1
}

collect_client() {  # $1 имя файла результатов
  kubectl wait --for=condition=ready pod/loadgen-pod --timeout=30s >>"$LOG" 2>&1 || true
  kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/loadgen-pod \
    --timeout=${2:-240}s >>"$LOG" 2>&1 || true
  kubectl logs loadgen-pod > "$1" 2>>"$LOG"
  kubectl delete pod loadgen-pod --force --grace-period=0 >>"$LOG" 2>&1 || true
}

# ============================== A1: дефектный прогрев
say "=== A1: ДЕФЕКТ readiness до прогрева (readyBeforeWarmup=true, warm=20s)"
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=20 \
  --set warm.readyBeforeWarmup=true --set limits.cpuBurnSeconds=0.25 >>"$LOG" 2>&1
snapshot "после helm upgrade (дефект)"
say "нагрузка сразу после upgrade; 2 rps на 30 с"
start_clients "--url http://hw11-service:8000 --rps 2 --duration 30 --stream --sessions 16"
snapshot "клиент запущен (дефектный прогрев)"
sleep 8
snapshot "середина прогрева (дефект): Ready-поды получают трафик"
collect_client "$RAW/expA1-defect.jsonl" 120
say "A1 итог клиента:"
python3 scripts/summarize.py "$RAW/expA1-defect.jsonl" | tee -a "$LOG"
say "A1 доказательство приёма ДО завершения прогрева (журналы реплик):"
python3 scripts/exp_A_warmup_evidence.py | tee -a "$LOG"

# ============================== A2: исправление
say "=== A2: ИСПРАВЛЕНИЕ readyBeforeWarmup=false, тот же опыт"
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=20 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 >>"$LOG" 2>&1
snapshot "после helm upgrade (исправление)"
say "нагрузка сразу после upgrade"
start_clients "--url http://hw11-service:8000 --rps 2 --duration 30 --stream --sessions 16"
sleep 8
snapshot "середина прогрева (исправление): новые поды вне EndpointSlice"
collect_client "$RAW/expA2-fixed.jsonl" 120
say "A2 итог клиента:"
python3 scripts/summarize.py "$RAW/expA2-fixed.jsonl" | tee -a "$LOG"

# ============================== A3: штатный rollout A→B под нагрузкой
say "=== A3: rolling update v1→v2 под нагрузкой (grace 30с, длинные стримы)"
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=5 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 >>"$LOG" 2>&1
wait_rollout
say "стабильная A (warm 5с) готова; запуск длинного потока + фоновой нагрузки"
# «Длительные» потоковые ответы: каждый 15-й запрос с chunk_delay=2 с —
# полный ответ ~17 с, заведомо пересекает начало обновления и grace-окно.
start_clients "--url http://hw11-service:8000 --rps 1.5 --duration 90 --stream --sessions 16 --long-every 15"
sleep 12
say "начинаю rolling update на v2 (артефакт B) — активные стримы пересекают обновление"
date +%s.%N > "$RAW/expA3-rollout-start"
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=5 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 \
  --set modelVersion=sim-cpu-v2 >>"$LOG" 2>&1
for i in $(seq 1 12); do
  sleep 10
  snapshot "rollout+${i}0с"
done
collect_client "$RAW/expA3-rollout.jsonl" 240
say "A3 итог клиента (ожидание: 0 rejected, 0 interrupted, версии v1 и v2 в окне):"
python3 scripts/summarize.py "$RAW/expA3-rollout.jsonl" | tee -a "$LOG"

# ============================== A4: неготовая B и откат к A
say "=== A4: заведомо неготовая B (битый хеш артефакта) → исключение из трафика"
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=5 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 \
  --set modelVersion=sim-bad-artifact >>"$LOG" 2>&1 || true
sleep 20
snapshot "неготовая B: Ready и EndpointSlice"
say "EndpointSlice обязан содержать только A; проверка версия по ответам"
start_clients "--url http://hw11-service:8000 --rps 0.5 --duration 10 --stream --sessions 4"
collect_client "$RAW/expA4-badB.jsonl" 90
python3 scripts/summarize.py "$RAW/expA4-badB.jsonl" | tee -a "$LOG"
say "управляемый откат к A (helm rollback к предыдущей ревизии с артефактом v1)"
helm history hw11 | tail -5 >>"$LOG" 2>&1
helm rollback hw11 >>"$LOG" 2>&1 || helm upgrade hw11 chart --set image.tag=v4 \
  --set warm.seconds=5 --set warm.readyBeforeWarmup=false \
  --set limits.cpuBurnSeconds=0.25 --set modelVersion=sim-cpu-v1 >>"$LOG" 2>&1
wait_rollout
snapshot "после отката к A"
say "подтверждение версии по фактическим ответам:"
start_clients "--url http://hw11-service:8000 --rps 0.5 --duration 8 --stream --sessions 4"
collect_client "$RAW/expA4-rollback-verify.jsonl" 90
python3 - <<'EOF' | tee -a "$LOG"
import json, pathlib
rows = [json.loads(l) for l in pathlib.Path("results/raw/expA4-rollback-verify.jsonl").read_text().splitlines() if l.startswith("{")]
models = {r.get("model_id") for r in rows}
print("версии в ответах после отката:", models,
      "— ожидается только sim-cpu-v1:", models == {"sim-cpu-v1"})
EOF

# ============================== A5: принудительное завершение
say "=== A5: принудительное завершение процесса во время потоковых ответов"
helm upgrade hw11 chart --set image.tag=v4 --set warm.seconds=5 \
  --set warm.readyBeforeWarmup=false --set limits.cpuBurnSeconds=0.25 \
  --set modelVersion=sim-cpu-v1 >>"$LOG" 2>&1
wait_rollout
start_clients "--url http://hw11-service:8000 --rps 1 --duration 60 --stream --sessions 8 --long-every 10"
sleep 15
# убиваем именно реплику клиента: имя пода берём из его же первого ответа
VICTIM=$(kubectl logs loadgen-pod 2>/dev/null | head -1 \
  | python3 -c "import json,sys; print(json.loads(sys.stdin.readline()).get('pod',''))" 2>/dev/null)
VICTIM=${VICTIM:-$(kubectl get pods -l app=hw11-service -o jsonpath='{.items[0].metadata.name}')}
say "kill -9 Pod $VICTIM (реплика клиента) во время активных стримов"
kubectl delete pod "$VICTIM" --force --grace-period=0 >>"$LOG" 2>&1
sleep 5
snapshot "сразу после kill: Endpoints и восстановление"
collect_client "$RAW/expA5-kill.jsonl" 180
say "A5 итог (ожидаем interrupted в клиенте, ok от выжившей реплики):"
python3 scripts/summarize.py "$RAW/expA5-kill.jsonl" | tee -a "$LOG"
say "=== опыт A завершён"
