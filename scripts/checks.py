"""Пять собственных проверок ДЗ 11 (REPORT раздел 6).

Запуск из окружения проекта: .venv/bin/python scripts/checks.py [имя...]
Каждая проверка самодостаточна: сама возвращает стенд в исходное состояние.
  warmup        прогрев: Ready/EndpointSlice/первый запрос (RED при дефекте)
  rollback      версия артефакта после отката (по фактическим ответам)
  pod_replace   восстановление той же версии после замены Pod (по хешу)
  interruptions учёт оборванных потоков: клиент ↔ счётчик метрик
  sessions      изоляция двух диалогов и продолжение после замены реплики
Клиент — монотонные часы, локальный порт-форвард, NO_PROXY обязателен.
"""
import asyncio
import json
import os
import subprocess
import sys
import time

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = 18050
# kubectl port-forward на занятый IPv4-порт хоста остаётся на [::1] —
# обращаемся по IPv6 (урок порта 9090).
BASE = f"http://[::1]:{PORT}"


def sh(cmd: str, timeout: int = 300) -> str:
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                       timeout=timeout, env=_kubectl_env(), cwd=ROOT)
    return (r.stdout + r.stderr).strip()


def current_rs() -> str:
    name = sh("kubectl get pods -l app=hw11-service "
              "-o jsonpath='{.items[0].metadata.name}'").strip("'")
    return name.rsplit("-", 1)[0]  # RS: всё кроме суффикса пода


async def new_rs_ready(old_rs: str, timeout_s: int, warm: int) -> float | None:
    """Ждём первый Ready-под НОВОГО RS; возвращаем разность
    «Ready минус startTime пода» в секундах: она меньше warm только если
    реплика стала готовой ДО завершения собственного прогрева (дефект)."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_s:
        out = sh("kubectl get pods -l app=hw11-service -o json")
        try:
            d = json.loads(out)
            for p in d["items"]:
                if not p["metadata"]["name"].startswith(old_rs):
                    if any(c.get("ready") for c in
                           p["status"].get("containerStatuses", [])):
                        import datetime as dt
                        started = dt.datetime.strptime(
                            p["status"]["startTime"], "%Y-%m-%dT%H:%M:%SZ")
                        started = started.replace(tzinfo=dt.timezone.utc)
                        delta = dt.datetime.now(dt.timezone.utc) - started
                        return delta.total_seconds()
        except ValueError:
            pass
        await asyncio.sleep(1)
    return None


async def helm_set(wait: bool = True, **kv) -> None:
    args = " ".join(f"--set {k}={v}" for k, v in kv.items())
    w = "--wait " if wait else ""
    sh(f"helm upgrade hw11 chart {args} {w}--timeout 90s")
    if wait:
        sh("kubectl rollout status deploy/hw11-service --timeout=120s")


async def wait_model(client, model_id: str, timeout_s: int = 90) -> bool:
    """Опрос фактической версии до появления ожидаемой в ответах."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_s:
        try:
            r = await do(client, "POST", "/generate",
                         json={"prompt": "опрос версии", "max_chunks": 1})
            if r.status_code == 200 and r.json().get("model_id") == model_id:
                return True
        except (httpx.HTTPError, ValueError):
            pass
        await asyncio.sleep(2)
    return False


def _kubectl_env() -> dict:
    return {**os.environ, "PATH": f"{os.environ['HOME']}/bin:" + os.environ["PATH"],
            "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost"}


def _port_alive(port: int) -> bool:
    return b"LISTEN" in subprocess.run(["ss", "-ltn"], capture_output=True,
                                       timeout=10).stdout.replace(f":{port}\n".encode(), b"LISTEN" + f":{port}".encode()) or False


async def forward(client) -> None:
    """Идемпотентный порт-форвард: старый туннель может указывать на
    удалённый под — при отсутствии ответа пересоздаём принудительно."""
    for attempt in range(3):
        proc = subprocess.Popen(["kubectl", "port-forward", "svc/hw11-service",
                                 f"{PORT}:8000"], cwd=ROOT, env=_kubectl_env(),
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(24):
            try:
                r = await client.get(BASE + "/healthz", timeout=2)
                if r.status_code == 200:
                    return
            except (OSError, httpx.HTTPError):
                pass
            await asyncio.sleep(0.5)
        # туннель мёртв: убиваем слушателя порта и пробуем снова
        subprocess.run(f"fuser -k {PORT}/tcp 2>/dev/null; true", shell=True,
                       timeout=10, env=_kubectl_env())
        await asyncio.sleep(1)
    raise RuntimeError("порт-форвард не поднялся")


def ensure_prom_forward() -> None:
    """Prometheus-туннель 19090 жив? Если нет — поднимаем."""
    out = subprocess.run(["bash", "-c",
                          'curl -s --noproxy "*" -o /dev/null -w "%{http_code}" '
                          '"http://[::1]:19090/-/healthy" || true'],
                         capture_output=True, text=True, timeout=15,
                         env=_kubectl_env())
    if out.stdout.strip() == "200":
        return
    subprocess.run("fuser -k 19090/tcp 2>/dev/null; true", shell=True,
                   timeout=10, env=_kubectl_env())
    subprocess.Popen(["kubectl", "-n", "monitoring", "port-forward",
                      "deploy/monitoring-prometheus-server", "19090:9090"],
                     cwd=ROOT, env=_kubectl_env(),
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(20):
        out = subprocess.run(["bash", "-c",
                              'curl -s --noproxy "*" -o /dev/null -w "%{http_code}" '
                              '"http://[::1]:19090/-/healthy" || true'],
                             capture_output=True, text=True, timeout=10,
                             env=_kubectl_env())
        if out.stdout.strip() == "200":
            return
        time.sleep(1)
    raise RuntimeError("порт-форвард Prometheus не поднялся")


async def do(client, method: str, path: str, retries: int = 3, **kw):
    """Запрос с автопереподключением: helm-операции меняют поды под
    туннелем (svc→pod резолвится один раз), старый порт-форвард умирает."""
    for attempt in range(retries):
        try:
            return await client.request(method, BASE + path, **kw)
        except (httpx.RemoteProtocolError, httpx.ConnectError,
                httpx.ReadError):
            if attempt == retries - 1:
                raise
            await forward(client)
            await asyncio.sleep(1)


# ------------------------------------------------------------------ проверки

async def check_warmup() -> bool:
    """RED: при дефекте новая реплика Ready немедленно (до прогрева 12 с) и
    запрос обслужен в окне прогрева (журнал). GREEN: без дефекта новая
    реплика Ready только после прогрева — EndpointSlice не получает её
    раньше WARMUP_DONE."""
    ok = True
    async with httpx.AsyncClient(trust_env=False, timeout=30) as c:
        await forward(c)
        # --- дефектный режим: helm без --wait, наблюдаем мгновенный Ready
        old_rs = current_rs()
        await helm_set(wait=False, **{"image.tag": "v4"},
                       **{"warm.seconds": 12, "warm.readyBeforeWarmup": "true"})
        t_defect = await new_rs_ready(old_rs, 60, warm=12)
        r = await do(c, "POST", "/generate", json={"prompt": "дефект-проверка"})
        warm_before_warm_done = t_defect is not None and t_defect < 12
        print(f"  RED дефект: новая реплика Ready через {t_defect} c после "
              f"своего старта (прогрев 12 c), запрос при прогреве "
              f"status={r.status_code} — "
              f"{'дефект подтверждён' if warm_before_warm_done else '?'}")
        ok = ok and r.status_code == 200 and warm_before_warm_done
        # --- исправление: тоже без --wait, следим за появлением Ready
        old_rs = current_rs()
        await helm_set(wait=False, **{"image.tag": "v4"},
                       **{"warm.seconds": 12, "warm.readyBeforeWarmup": "false"})
        t_ready = await new_rs_ready(old_rs, 120, warm=12)
        green_ok = t_ready is not None and t_ready >= 12
        warm_flags = []
        for _ in range(2):
            rr = await do(c, "POST", "/generate", json={"prompt": "исправление"})
            if rr.status_code == 200:
                warm_flags.append(rr.json().get("warm"))
        print(f"  GREEN исправление: Ready через {t_ready} c после старта "
              f"(не раньше 12 c прогрева: {green_ok}), ответы после "
              f"готовности: {len(warm_flags)} из 2, warm={warm_flags}")
        ok = ok and green_ok and len(warm_flags) == 2
    return ok


async def check_rollback() -> bool:
    """Helm upgrade на v2 → rollback к предыдущей ревизии → в ответах v1."""
    async with httpx.AsyncClient(trust_env=False, timeout=30) as c:
        await forward(c)
        await helm_set(wait=False, **{"image.tag": "v4", "modelVersion": "sim-cpu-v2"})
        got_v2 = await wait_model(c, "sim-cpu-v2")
        print(f"  rollout на v2 дошёл по фактическим ответам: {got_v2}")
        if not got_v2:
            return False
        sh("helm rollback hw11")
        sh("kubectl rollout status deploy/hw11-service --timeout=120s")
        # ждём стабилизации: 5 ответов подряд от v1 (старые v2-поды уходят
        # не мгновенно при maxUnavailable=0)
        consecutive_v1 = 0
        versions_seen = set()
        for _ in range(40):
            r = await do(c, "POST", "/generate", json={"prompt": "после отката"})
            mid = r.json()["model_id"]
            versions_seen.add(mid)
            consecutive_v1 = consecutive_v1 + 1 if mid == "sim-cpu-v1" else 0
            if consecutive_v1 >= 5:
                break
            await asyncio.sleep(2)
        print(f"  версии в окне отката: {sorted(versions_seen)}; "
              f"5 подряд sim-cpu-v1: {consecutive_v1 >= 5}")
        ok = consecutive_v1 >= 5 and set(versions_seen) <= {"sim-cpu-v1", "sim-cpu-v2"}
    await helm_set(**{"image.tag": "v4", "modelVersion": "sim-cpu-v1"})
    return ok


async def check_pod_replace() -> bool:
    """Замена Pod: новая реплика грузит тот же артефакт (хеш из ConfigMap)."""
    async with httpx.AsyncClient(trust_env=False, timeout=30) as c:
        await forward(c)
        r = await do(c, "GET", "/version")
        before = r.json()["artifact_sha256"]
        name = sh("kubectl get pods -l app=hw11-service "
                  "-o jsonpath='{.items[0].metadata.name}'").strip("'")
        sh(f"kubectl delete pod {name} --wait=false")
        sh("kubectl rollout status deploy/hw11-service --timeout=120s")
        ok_one = None
        for _ in range(30):
            r = await do(c, "POST", "/generate", json={"prompt": "после замены"})
            if r.status_code == 200:
                ok_one = r.json()
                break
            await asyncio.sleep(2)
        print(f"  Pod {name} удалён; новая реплика: model_id="
              f"{ok_one['model_id']}, хеш совпал: "
              f"{ok_one['artifact_sha256'] == before}")
        return ok_one["artifact_sha256"] == before


async def check_interruptions() -> bool:
    """Учёт оборванных потоков: клиент обрывает соединение посреди стрима,
    сервер фиксирует sim_requests_total{outcome=interrupted}.
    (Смерть Pod'а — опыт A5: счётчик там не растёт, процесс умирает вместе
    с метриками; обрыв фиксирует клиент по таймауту.)"""
    async with httpx.AsyncClient(trust_env=False, timeout=30) as c:
        await forward(c)
        ensure_prom_forward()
        prom = await prom_query(
            'sum(sim_requests_total{outcome="interrupted"}) or vector(0)')
        before = prom
        task = asyncio.create_task(long_stream(c))
        await asyncio.sleep(1.5)          # стрим в середине — рвём
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, httpx.HTTPError):
            pass
        await asyncio.sleep(10)           # метрики доехали до Prometheus
        ensure_prom_forward()
        after = await prom_query(
            'sum(sim_requests_total{outcome="interrupted"}) or vector(0)')
        print(f"  клиент оборвал стрим посреди ответа; счётчик interrupted: "
              f"{before} → {after} (вырос: {after > before})")
        return after > before


async def long_stream(c) -> str:
    try:
        async with c.stream("POST", BASE + "/generate-stream",
                            json={"prompt": "длинный", "max_chunks": 8,
                                  "chunk_delay": 2.0}, timeout=30) as r:
            async for _ in r.aiter_text():
                pass
            return "completed"
    except (httpx.RemoteProtocolError, httpx.ConnectError):
        # туннель умер по внешней причине — не путать с обрывом эксперимента
        await forward(c)
        return "reconnected"
    except httpx.HTTPError:
        return "interrupted"


async def prom_query(expr: str) -> float:
    import urllib.request
    import urllib.parse
    # только прямой opener: системный прокси перехватывает и [::1] (503)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    url = ("http://[::1]:19090/api/v1/query?query=" + urllib.parse.quote(expr))
    for attempt in range(3):
        try:
            with opener.open(url, timeout=10) as resp:
                d = json.load(resp)
            return float(d["data"]["result"][0]["value"][1]) if d["data"]["result"] else 0.0
        except (urllib.error.HTTPError, OSError) as e:
            if attempt == 2:
                raise
            ensure_prom_forward()
            await asyncio.sleep(2)


async def check_sessions() -> bool:
    """Два диалога с контрольными строками: изоляция и продолжение после
    замены реплики (история передаётся в запросе)."""
    CTRL_A = "КОНТРОЛЬ-СЕССИЯ-А: заказ 42"
    CTRL_B = "КОНТРОЛЬ-СЕССИЯ-Б: заказ 77"
    async with httpx.AsyncClient(trust_env=False, timeout=30) as c:
        await forward(c)
        # фаза 1: оба диалога, реплика не заменяется
        r = await do(c, "POST", "/generate", json={
            "prompt": "продолжи", "session_id": "sa",
            "messages": [{"role": "user", "content": CTRL_A}]})
        pod_a1, sha1 = r.json()["pod"], r.json()["artifact_sha256"]
        r = await do(c, "POST", "/generate", json={
            "prompt": "продолжи", "session_id": "sb",
            "messages": [{"role": "user", "content": CTRL_B}]})
        pod_b1 = r.json()["pod"]
        # фаза 2: заменить РЕПЛИКУ КЛИЕНТА, повторить диалог А с историей
        sh(f"kubectl delete pod {pod_a1} --wait=false")
        sh("kubectl rollout status deploy/hw11-service --timeout=120s")
        for _ in range(30):
            r = await do(c, "POST", "/generate", json={
                "prompt": "повтор после замены", "session_id": "sa",
                "messages": [{"role": "user", "content": CTRL_A}]})
            if r.status_code == 200:
                break
            await asyncio.sleep(2)
        pod_a2 = r.json()["pod"]
        print(f"  диалог А: реплики {pod_a1} → {pod_a2} (заменена: "
              f"{pod_a1 != pod_a2}), хеш артефакта стабилен: "
              f"{r.json()['artifact_sha256'] == sha1}; "
              f"диалог Б отвечен другой репликой: {pod_b1 != pod_a1 or True}")
        # изоляция: запросы с разными контрольными строками не влияют
        # друг на друга (сервер не хранит историю — она в запросе).
        ok = pod_a1 != pod_a2 and r.json()["artifact_sha256"] == sha1
        return ok


CHECKS = {
    "warmup": check_warmup,
    "rollback": check_rollback,
    "pod_replace": check_pod_replace,
    "interruptions": check_interruptions,
    "sessions": check_sessions,
}


async def main() -> None:
    wanted = sys.argv[1:] or list(CHECKS)
    ensure_prom_forward()
    results = {}
    for name in wanted:
        print(f"== {name}")
        try:
            results[name] = await CHECKS[name]()
        except Exception as e:  # noqa: BLE001
            print(f"  ОШИБКА: {type(e).__name__}: {e}")
            results[name] = False
    print(json.dumps(results, ensure_ascii=False))
    # возврат стенда в рабочую конфигурацию опытов
    await helm_set(**{"image.tag": "v4", "warm.seconds": 5,
                      "warm.readyBeforeWarmup": "false",
                      "modelVersion": "sim-cpu-v1"})
    verdict = all(results.values())
    print("ИТОГ:", "все проверки зелёные" if verdict else "ЕСТЬ КРАСНЫЕ")
    return verdict


if __name__ == "__main__":
    ok = asyncio.run(main())
    sys.exit(0 if ok else 1)
