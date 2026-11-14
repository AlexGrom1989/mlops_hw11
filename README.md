# ДЗ 11: Kubernetes под нагрузкой

API выпуска ДЗ 8+9+10 в Kubernetes (kind) с `backend_mode=simulated`:
управляемые прогрев, очередь, ограничение одновременной обработки, выдача
ответа частями (SSE), имитация префиксного кеша и вычислительная фаза.
Полный отчёт — `report.html`, инструкция стенда — `STAND_UP.md`.

## Быстрый старт (кратко; полная версия — STAND_UP.md)

```bash
export PATH="$HOME/bin:$PATH" NO_PROXY=127.0.0.1,localhost
kind create cluster --config cluster/kind-config.yaml
docker build -t hw11-service:v4 . && docker build -f Dockerfile.loadgen -t hw11-loadgen:v5 .
kind load docker-image hw11-service:v4 hw11-loadgen:v5 --name hw11
kubectl apply -f cluster/metrics-server.yaml
helm install hw11 chart --set image.tag=v4
# мониторинг: monitoring/values-monitoring.yaml + grafana-setup.yaml (см. STAND_UP.md)
```

## Опыты

| Скрипт | Что показывает | Время |
|---|---|---|
| `scripts/exp_A_warmup_rollout.sh` | дефект прогрева → исправление; rollout под стримами; неготовая B; откат; kill | ~12 мин |
| `scripts/exp_B_affinity.sh` | обычный Service vs `sessionAffinity: ClientIP`, hot-клиент, keep-alive | ~40 мин |
| `scripts/exp_C_hpa.sh` | HPA 2→3→2 по CPU, горячая реплика при 3 репликах, алерты | ~20 мин |
| `scripts/checks.py` | пять собственных проверок (прогрев, откат, замена Pod, обрывы, изоляция диалогов) | ~8 мин |

## Состав

- `app/main.py` — сервис: `/healthz` `/readyz` `/version` `/generate`
  `/generate-stream` (SSE) `/metrics`; request_id, имя Pod и версия артефакта
  в каждом ответе; история диалога передаётся в запросе.
- `chart/` — Helm: Deployment (startup/readiness/liveness разного назначения,
  checksum-аннотации ConfigMap), 2 Service, HPA autoscaling/v2 2..3 по CPU
  с окном стабилизации.
- `monitoring/` — Prometheus (scrape 15s, retention 6h), Grafana с
  provisioning дашборда, правила алертов (p95 TTFC, доля отказов).
- `scripts/` — генератор нагрузки (монотонные часы, расписание, JSONL в stdout
  пода), опыты, проверки, графики.
- `results/` — сырые результаты прогонов (JSONL), фигуры; `logs/` — журналы.
- `model-sim/` — детерминированный артефакт-имитатор (v1, v2) с sha256.
