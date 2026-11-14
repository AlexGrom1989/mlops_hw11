# Лёгкий образ сервиса (simulated backend, CPU): python 3.12-slim,
# база закреплена digest'ом, pip-зависимости закреплены версиями
# (дублируют uv.lock venv'а проекта). Артефакт-имитатор и его хеш
# подкладываются в Pod через ConfigMap (одна версия всем репликам),
# поэтому в образе их нет: версия выбирается values.modelVersion чарта.
FROM python@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

RUN pip install --no-cache-dir \
    fastapi==0.115.6 \
    "uvicorn[standard]==0.34.0" \
    prometheus-client==0.21.1

WORKDIR /app
COPY app/main.py /app/app/main.py

EXPOSE 8000
# uvicorn без --reload; graceful timeout меньше terminationGracePeriodSeconds,
# чтобы активные потоковые ответы успевали завершиться в grace period.
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", \
     "--port", "8000", "--timeout-graceful-shutdown", "25"]
