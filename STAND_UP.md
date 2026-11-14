# Стенд ДЗ 11: запуск и удаление

## Состав и версии

| Компонент | Версия | Лимит |
|---|---|---|
| kind (узел k8s) | kind v0.29.0, kubernetes v1.33.1 | один узел, контейнер Docker |
| kubectl | v1.33.4 | `~/bin` |
| helm | v3.18.4 | `~/bin` |
| Docker | 29.8.0 | хост |
| metrics-server | v0.8.0 | `--kubelet-insecure-tls` (kind) |
| Prometheus (chart prometheus-community/prometheus) | сервер v3.16.0 | retention 6h, RAM limit 512Mi, scrape 15s |
| Grafana (chart grafana/grafana) | 12.3.1 | RAM limit 256Mi |
| kube-state-metrics | v2.20.0 | RAM limit 128Mi |
| образ сервиса | hw11-service:v4 (python:3.12-slim, fastapi 0.115.6, uvicorn 0.34.0, prometheus-client 0.21.1) | 168 МБ, cpu req 300m / limit 1, RAM 128/256Mi |
| образ генератора | hw11-loadgen:v4 (httpx 0.28.1) | 131 МБ, RAM limit 128Mi |
| backend | `backend_mode=simulated` (управляемый прогрев/очередь/чанки/кеш/CPU-burn) | значения в chart/values.yaml |

Условия хоста перед запуском — `logs/stand-baseline.log` (диск 105 ГБ свободно,
RAM 14 ГБ при доступных ~2,5 ГБ — поэтому оператор Prometheus не ставится,
стек мониторинга с жёсткими лимитами, один узел kind).

## Запуск с нуля

```bash
cd mlops-hw11/mlops_hw11
export PATH="$HOME/bin:$PATH" NO_PROXY=127.0.0.1,localhost

# 1. кластер
kind create cluster --config cluster/kind-config.yaml

# 2. образы
docker build -t hw11-service:v4 .
docker build -f Dockerfile.loadgen -t hw11-loadgen:v4 .
kind load docker-image hw11-service:v4 hw11-loadgen:v4 --name hw11

# 3. мониторинг (Prometheus + Grafana + kube-state-metrics + metrics-server)
kubectl apply -f cluster/metrics-server.yaml
kubectl -n kube-system patch deploy metrics-server --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
kubectl create namespace monitoring
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm install monitoring prometheus-community/prometheus --namespace monitoring \
  -f monitoring/values-monitoring.yaml \
  --set prometheus-pushgateway.enabled=false --set prometheus-node-exporter.enabled=false \
  --set kube-state-metrics.enabled=false
helm install ksm prometheus-community/kube-state-metrics --namespace monitoring \
  --set resources.requests.memory=64Mi --set resources.limits.memory=128Mi
helm install grafana grafana/grafana --namespace monitoring \
  --set resources.requests.cpu=50m --set resources.requests.memory=128Mi \
  --set resources.limits.memory=256Mi --set adminPassword=hw11 \
  --set sidecar.dashboards.enabled=false \
  --set extraVolumes[0].name=ds,extraVolumes[0].configMap.name=grafana-datasources \
  --set extraVolumes[1].name=prov,extraVolumes[1].configMap.name=grafana-providers \
  --set extraVolumes[2].name=dash,extraVolumes[2].configMap.name=grafana-dashboard-hw11 \
  --set 'extraVolumeMounts[0].name=ds,extraVolumeMounts[0].mountPath=/etc/grafana/provisioning/datasources' \
  --set 'extraVolumeMounts[1].name=prov,extraVolumeMounts[1].mountPath=/etc/grafana/provisioning/dashboards' \
  --set 'extraVolumeMounts[2].name=dash,extraVolumeMounts[2].mountPath=/var/lib/grafana/dashboards'
kubectl apply -f monitoring/grafana-setup.yaml

# 4. сервис (Deployment 2 реплики, 2 Service, HPA 2..3)
helm install hw11 chart --set image.tag=v4

# 5. доступ: порт-форварды (хост-порты 9090/18000 могут быть заняты — берём 19090/18000, IPv6 [::1])
kubectl -n monitoring port-forward deploy/monitoring-prometheus-server 19090:9090 &
kubectl -n monitoring port-forward deploy/grafana 18300:3000 &
kubectl port-forward svc/hw11-service 18000:8000 &
```

## Опыты и проверки

```bash
scripts/exp_A_warmup_rollout.sh   # опыт A (~12 мин), logs/exp-A.log
scripts/exp_B_affinity.sh         # опыт B (~25 мин), logs/exp-B.log
scripts/exp_C_hpa.sh              # опыт C (~20 мин), logs/exp-C.log
.venv/bin/python scripts/checks.py  # пять проверок (меняют значения helm и восстанавливают)
```

## Удаление стенда и оценка размера

```bash
helm uninstall hw11 grafana ksm monitoring -n monitoring   # ~0.1 GiB релизы
helm uninstall monitoring -n monitoring                     # PV Prometheus ~0.5 GiB
kubectl delete ns monitoring
kind delete cluster --name hw11                             # узел: ~1.6 GiB (volume+образ)
docker rmi hw11-service:v4 hw11-loadgen:v4 quay.io/prometheus/prometheus:v3.16.0 grafana/grafana:12.3.1 registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.20.0 registry.k8s.io/metrics-server/metrics-server:v0.8.0 quay.io/prometheus-operator/prometheus-config-reloader:v0.95.2
```

Фактический размер стенда (замер `docker system df` до/после создания):
узел kind ~1,6 ГиБ + образы ~0,5 ГиБ + PV Prometheus <0,2 ГиБ. Итого меньше
2,5 ГиБ на диске; в RAM — кластер ~1 ГиБ, мониторинг ≤0,9 ГиБ, сервис
≤0,5 ГиБ.
