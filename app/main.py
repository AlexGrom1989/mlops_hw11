"""Сервис ДЗ 11: API выпуска ДЗ 8+9+10 в Kubernetes, backend_mode=simulated.

Лёгкий управляемый backend на CPU: настраиваемые прогрев, очередь,
ограничение одновременно обрабатываемых запросов, выдача ответа частями
(SSE). Явно возвращает backend_mode=simulated во всех ответах и метриках.
Имитатор — не настоящая модель; его числа относятся к модели нагрузки
имитатора (значения в ConfigMap, повторены в REPORT).

Контракт HTTP:
  GET  /healthz            — жив ли процесс (liveness): 200 всегда;
  GET  /readyz             — готов ли к рабочему трафику (readiness):
                             200 только после загрузки артефакта И прогрева;
  GET  /version            — model_id, artifact_sha256, pod, warmup state;
  POST /generate           — обычный JSON-ответ;
  POST /generate-stream    — потоковый ответ (SSE): части data, финальное
                             событие done с полной сводкой;
  GET  /metrics            — Prometheus.

История диалога передаётся в запросе (messages), вне Pod не хранится:
реплика после замены продолжает диалог по присланной истории.
Request ID, имя Pod и фактически загруженная версия — в каждом ответе,
каждой SSE-части и в журнале.

Исходный дефект опыта A включается флагом READY_BEFORE_WARMUP=1:
HTTP-процесс стартует и /readyz отвечает 200 до завершения прогрева.
Исправление — флаг снят (см. chart values warm.readyBeforeWarmup).
"""
import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from prometheus_client import (CONTENT_TYPE_LATEST, Counter, Gauge, Histogram,
                               generate_latest)

# ------------------------------------------------------------------ конфигурация
POD_NAME = os.environ.get("POD_NAME", os.uname().nodename)
ARTIFACT_DIR = Path(os.environ.get("ARTIFACT_DIR", "/app/model"))
WARMUP_SECONDS = float(os.environ.get("WARMUP_SECONDS", "0"))
READY_BEFORE_WARMUP = os.environ.get("READY_BEFORE_WARMUP", "0") == "1"
MAX_CONCURRENCY = max(1, int(os.environ.get("MAX_CONCURRENCY", "1")))
QUEUE_LIMIT = int(os.environ.get("QUEUE_LIMIT", "64"))        # макс. длина очереди
MAX_PROMPT_CHARS = int(os.environ.get("MAX_PROMPT_CHARS", "4000"))
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", "40"))
CHUNK_COUNT = max(1, int(os.environ.get("CHUNK_COUNT", "8")))
CHUNK_DELAY = float(os.environ.get("CHUNK_DELAY", "0.05"))    # межчастевая пауза, с
CACHE_CAPACITY = int(os.environ.get("CACHE_CAPACITY", "64"))  # записей префиксов
CACHE_HIT_COST = float(os.environ.get("CACHE_HIT_COST", "0.10"))   # с
CACHE_MISS_COST = float(os.environ.get("CACHE_MISS_COST", "0.40"))  # с

# ------------------------------------------------------------------ метрики
# Меток немного: pod и mode. Запрос/сессия — только в журнале.
TTFC = Histogram("sim_ttfc_seconds", "Время до первой части ответа",
                 ["pod", "mode"],
                 buckets=(.05, .1, .2, .3, .5, .8, 1, 1.5, 2, 3, 5, 8, 12, 20, 40))
FULL = Histogram("sim_full_seconds", "Длительность полного ответа",
                 ["pod", "mode"],
                 buckets=(.1, .2, .5, 1, 2, 3, 5, 8, 12, 20, 40, 80))
QUEUE_WAIT = Histogram("sim_queue_wait_seconds", "Ожидание в очереди",
                       ["pod", "mode"],
                       buckets=(0, .01, .05, .1, .2, .5, 1, 2, 3, 5, 10, 20))
RUNNING = Gauge("sim_running", "Обрабатывается сейчас", ["pod"])
WAITING = Gauge("sim_waiting", "Ждёт в очереди", ["pod"])
REQUESTS = Counter("sim_requests_total", "Запросы по исходам",
                   ["pod", "mode", "outcome"])  # ok|rejected|interrupted
CACHE = Counter("sim_cache_total", "Имитация префиксного кеша",
                ["pod", "result"])            # hit|miss

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger("hw11")


class SimState:
    """Состояние реплики: артефакт-имитатор, прогрев, семафор вычислений."""

    def __init__(self):
        self.artifact_sha256 = None
        self.model_id = None
        self.warm = asyncio.Event()
        self.load_error = None
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        self.queue_depth = 0                 # ждут семафора прямо сейчас
        self.cache: dict[str, None] = {}     # LRU по вставке, CACHE_CAPACITY записей

    def load_artifact(self) -> None:
        """«Загрузка модели»: прочитать файл-имитатор и сверить хеш."""
        payload = (ARTIFACT_DIR / "artifact.bin").read_bytes()
        expected = (ARTIFACT_DIR / "artifact.sha256").read_text().strip().split()[0]
        actual = hashlib.sha256(payload).hexdigest()
        if actual != expected:
            raise ValueError(f"хеш артефакта не совпал: {actual[:12]} != {expected[:12]}")
        self.artifact_sha256 = actual
        self.model_id = json.loads((ARTIFACT_DIR / "manifest.json").read_text())["model_id"]
        log.info('ARTIFACT_LOADED pod="%s" model_id="%s" sha=%s',
                 POD_NAME, self.model_id, actual[:12])

    async def warmup(self) -> None:
        started = time.monotonic()
        # Прогрев имитатора: управляемая пауза (значение — в values чарта).
        while time.monotonic() - started < WARMUP_SECONDS:
            await asyncio.sleep(min(.25, WARMUP_SECONDS - (time.monotonic() - started)))
        self.warm.set()
        log.info('WARMUP_DONE pod="%s" seconds=%.1f', POD_NAME, WARMUP_SECONDS)

    def cache_lookup(self, prefix: str) -> bool:
        """Имитация локального префиксного кеша: hit по первым 64 байтам."""
        key = hashlib.sha256(prefix.encode()[:64]).hexdigest()
        if key in self.cache:
            CACHE.labels(POD_NAME, "hit").inc()
            return True
        CACHE.labels(POD_NAME, "miss").inc()
        self.cache[key] = None
        while len(self.cache) > CACHE_CAPACITY:
            self.cache.pop(next(iter(self.cache)))
        return False


STATE = SimState()
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)


@app.on_event("startup")
async def _startup() -> None:
    # Дефект опыта A: при READY_BEFORE_WARMUP=1 readiness отдан сразу,
    # прогрев идёт фоном — процесс «готов» раньше вычислений.
    if READY_BEFORE_WARMUP:
        STATE.warm.set()
    try:
        await asyncio.to_thread(STATE.load_artifact)
    except Exception as e:  # noqa: BLE001 — битый артефакт: живы, но не готовы
        STATE.load_error = f"{type(e).__name__}: {e}"
        log.error('ARTIFACT_ERROR pod="%s" error="%s"', POD_NAME, STATE.load_error)
    asyncio.create_task(STATE.warmup())


def _validate(payload) -> tuple[str, list[dict], str, int]:
    """Валидация запроса ДО любого вычисления; HTTPException → 4xx."""
    if not isinstance(payload, dict):
        raise HTTPException(400, "тело запроса должно быть JSON-объектом")
    prompt = payload.get("prompt")
    if not isinstance(prompt, str) or not prompt.strip():
        raise HTTPException(400, "prompt обязателен и должен быть непустой строкой")
    if len(prompt) > MAX_PROMPT_CHARS:
        raise HTTPException(413, f"prompt длиннее {MAX_PROMPT_CHARS} символов")
    history = payload.get("messages", [])
    if not isinstance(history, list) or len(history) > MAX_MESSAGES:
        raise HTTPException(400, f"messages — список до {MAX_MESSAGES} сообщений")
    for m in history:
        if not isinstance(m, dict) or not isinstance(m.get("role"), str) \
                or not isinstance(m.get("content"), str):
            raise HTTPException(400, "сообщение — объект с role и content (строки)")
    max_chunks = payload.get("max_chunks", CHUNK_COUNT)
    if not isinstance(max_chunks, int) or isinstance(max_chunks, bool) \
            or not 1 <= max_chunks <= CHUNK_COUNT:
        raise HTTPException(400, f"max_chunks — целое 1..{CHUNK_COUNT}")
    session_id = payload.get("session_id")
    if session_id is not None and not isinstance(session_id, str):
        raise HTTPException(400, "session_id должен быть строкой")
    return prompt, history, session_id or "", max_chunks


def _reject(outcome: str, mode: str, detail, status: int):
    REQUESTS.labels(POD_NAME, mode, outcome).inc()
    return JSONResponse({"error": detail, "pod": POD_NAME,
                         "backend_mode": "simulated"}, status_code=status)


def _summary(mode: str, request_id: str, session_id: str, started: float,
             first_at: float | None, chunks: int, queued: float,
             prefix_hit: bool) -> dict:
    now = time.monotonic()
    return {
        "request_id": request_id, "session_id": session_id,
        "pod": POD_NAME, "model_id": STATE.model_id,
        "artifact_sha256": STATE.artifact_sha256, "backend_mode": "simulated",
        "warm": STATE.warm.is_set(), "chunks": chunks,
        "ttfc_seconds": round(first_at - started, 4) if first_at else None,
        "full_seconds": round(now - started, 4),
        "queue_wait_seconds": round(queued, 4),
        "cache": "hit" if prefix_hit else "miss",
        "mode": mode,
    }


async def _serve(payload: dict, mode: str):
    """Общая обработка: очередь → стоимость кеша → части ответа → done."""
    started = time.monotonic()
    prompt, history, session_id, max_chunks = _validate(payload)
    request_id = str(uuid.uuid4())
    prefix = "".join(m.get("content", "") for m in history) + prompt

    if not STATE.warm.is_set():
        # Непрогретая реплика не обслуживает рабочий трафик.
        _reject("rejected", mode, "прогрев не завершён", 503)
        yield ('event: error\ndata: ' +
               json.dumps({"error": "прогрев не завершён", "pod": POD_NAME,
                           "request_id": request_id}, ensure_ascii=False) + "\n\n")
        return
    if STATE.queue_depth >= QUEUE_LIMIT:
        _reject("rejected", mode, "очередь переполнена", 429)
        yield ('event: error\ndata: ' +
               json.dumps({"error": "очередь переполнена", "pod": POD_NAME,
                           "request_id": request_id}, ensure_ascii=False) + "\n\n")
        return

    STATE.queue_depth += 1
    WAITING.labels(POD_NAME).set(STATE.queue_depth)
    entered_at = None
    try:
        async with STATE.semaphore:
            entered_at = time.monotonic()
            STATE.queue_depth -= 1
            WAITING.labels(POD_NAME).set(STATE.queue_depth)
            RUNNING.labels(POD_NAME).inc()
            try:
                prefix_hit = STATE.cache_lookup(prefix)
                # Стоимость кеша имитатора: hit дешевле miss (см. values).
                await asyncio.sleep(CACHE_HIT_COST if prefix_hit else CACHE_MISS_COST)
                first_at = None
                for i in range(max_chunks):
                    await asyncio.sleep(CHUNK_DELAY)
                    if first_at is None:
                        first_at = time.monotonic()
                        TTFC.labels(POD_NAME, mode).observe(first_at - started)
                    yield ("data: " + json.dumps(
                        {"i": i, "text": f"часть {i}", "request_id": request_id,
                         "pod": POD_NAME, "model_id": STATE.model_id,
                         "artifact_sha256": STATE.artifact_sha256,
                         "backend_mode": "simulated"}, ensure_ascii=False) + "\n\n")
                done = _summary(mode, request_id, session_id, started, first_at,
                                max_chunks, entered_at - started, prefix_hit)
                FULL.labels(POD_NAME, mode).observe(done["full_seconds"])
                QUEUE_WAIT.labels(POD_NAME, mode).observe(done["queue_wait_seconds"])
                REQUESTS.labels(POD_NAME, mode, "ok").inc()
                log.info('REQ_DONE request_id="%s" session_id="%s" pod="%s" '
                         'outcome=ok ttfc=%.3f full=%.3f', request_id, session_id,
                         POD_NAME, done["ttfc_seconds"], done["full_seconds"])
                yield ("event: done\ndata: " +
                       json.dumps(done, ensure_ascii=False) + "\n\n")
            finally:
                RUNNING.labels(POD_NAME).dec()
    except asyncio.CancelledError:
        # Клиент ушёл или Pod завершается: запрос оборван — считаем честно.
        STATE.queue_depth = max(0, STATE.queue_depth - (1 if entered_at is None else 0))
        WAITING.labels(POD_NAME).set(STATE.queue_depth)
        REQUESTS.labels(POD_NAME, mode, "interrupted").inc()
        log.info('REQ_INTERRUPTED request_id="%s" pod="%s"', request_id, POD_NAME)
        raise


@app.post("/generate")
async def generate(request: Request):
    try:
        payload = await request.json()
    except ValueError:
        return _reject("rejected", "json", "тело не является JSON", 400)
    try:
        _validate(payload)
    except HTTPException as e:
        return _reject("rejected", "json", e.detail, e.status_code)
    if not STATE.warm.is_set():
        return _reject("rejected", "json", "прогрев не завершён", 503)
    chunks = [c async for c in _serve(payload, "json")]
    return JSONResponse(json.loads(chunks[-1].split("data: ", 1)[1].strip()))


@app.post("/generate-stream")
async def generate_stream(request: Request):
    try:
        payload = await request.json()
    except ValueError:
        return _reject("rejected", "sse", "тело не является JSON", 400)
    try:
        _validate(payload)
    except HTTPException as e:
        return _reject("rejected", "sse", e.detail, e.status_code)
    if not STATE.warm.is_set():
        return _reject("rejected", "sse", "прогрев не завершён", 503)
    return StreamingResponse(_serve(payload, "sse"), media_type="text/event-stream",
                             headers={"X-Accel-Buffering": "no"})


@app.get("/healthz")
async def healthz():
    """Liveness: жив ли процесс. Прогрев и артефакт тут не проверяются."""
    return {"ok": True, "pod": POD_NAME}


@app.get("/readyz")
async def readyz():
    """Readiness: рабочий трафик. 200 только после артефакта И прогрева."""
    if STATE.load_error:
        return JSONResponse({"ready": False, "error": STATE.load_error}, status_code=503)
    if not (STATE.warm.is_set() and STATE.artifact_sha256):
        return JSONResponse({"ready": False, "error": "прогрев/загрузка не завершены"},
                            status_code=503)
    return {"ready": True, "pod": POD_NAME, "model_id": STATE.model_id}


@app.get("/version")
async def version():
    return {"pod": POD_NAME, "model_id": STATE.model_id,
            "artifact_sha256": STATE.artifact_sha256,
            "warm": STATE.warm.is_set(), "load_error": STATE.load_error,
            "backend_mode": "simulated"}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
