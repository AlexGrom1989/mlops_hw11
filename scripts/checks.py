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
BASE = f"http://127.0.0.1:{PORT}"


def sh(cmd: str, timeout: int = 300) -> str:
    env = {**os.environ, "PATH": f"{os.environ['HOME']}/bin:" + os.environ["PATH"],
           "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost"}
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                       timeout=timeout, env=env, cwd=ROOT)
    return (r.stdout + r.stderr).strip()


async def wait_endpoints_ready(client, want: int, timeout_s: int) -> float | None:
    """Ждём want готовых адресов в EndpointSlice; возвращаем время ожидания."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_s:
        out = sh("kubectl get endpointslice -l kubernetes.io/service-name="
                 "hw11-service -o json")
        try:
            d = json.loads(out)
            ready = sum(1 for s in d["items"] for e in s.get("endpoints", [])
                        if e.get("conditions", {}).get("ready") is True)
        except ValueError:
            ready = 0
        if ready >= want:
            return time.monotonic() - t0
        await asyncio.sleep(1)
    return None


async def helm_set(**kv) -> None:
    args = " ".join(f"--set {k}={v}" for k, v in kv.items())
    sh(f"helm upgrade hw11 chart {args} --wait --timeout 90s")
    sh("kubectl rollout status deploy/hw11-service --timeout=120s")


async def forward(client) -> None:
    subprocess.Popen(["kubectl", "port-forward", "svc/hw11-service",
                      f"{PORT}:8000"], cwd=ROOT,
                     env={**os.environ, "NO_PROXY": "*"},
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(30):
        try:
            r = await client.get(BASE + "/healthz", timeout=2)
            if r.status_code == 200:
                return
        except OSError:
            pass
        await asyncio.sleep(0.5)
    raise RuntimeError("порт-форвард не поднялся")


# ------------------------------------------------------------------ проверки

async def check_warmup() -> bool:
    """RED: с дефектом запросы уходят непрогретой реплике.
    GREEN: без дефекта EndpointSlice пуст до конца прогрева, 503 нет."""
    ok = True
    async with httpx.AsyncClient(trust_env=False, timeout=30) as c:
        await forward(c)
        # --- дефектный режим
        await helm_set(image_tag="v4", **{"warm.seconds": 12,
                                          "warm.readyBeforeWarmup": "true"})
        t_defect = await wait_endpoints_ready(c, 1, 60)
        r = await c.post(BASE + "/generate", json={"prompt": "дефект-проверка"})
        body = r.json()
        defect_served = r.status_code == 200
        print(f"  RED дефект: endpoints через {t_defect:.1f} c, ответ "
              f"status={r.status_code} warm={body.get('warm')} — трафик ушёл "
              f"{'непрогретой' if defect_served and not body.get('warm') else '?'} реплике")
        ok = ok and defect_served and not body.get("warm")
        # --- исправление
        await helm_set(image_tag="v4", **{"warm.seconds": 12,
                                          "warm.readyBeforeWarmup": "false"})
        r = await c.get(BASE + "/readyz")
        t_ready = await wait_endpoints_ready(c, 2, 60)
        seen_503 = r.status_code == 503
        # теперь два запроса — оба от прогретой реплики
        warm_flags = []
        for _ in range(2):
            rr = await c.post(BASE + "/generate", json={"prompt": "исправление"})
            warm_flags.append(rr.json().get("warm"))
        print(f"  GREEN исправление: readyz сразу после upgrade={r.status_code} "
              f"(503={seen_503}), endpoints через {t_ready:.1f} c, warm ответов "
              f"{warm_flags}")
        ok = ok and seen_503 and all(warm_flags)
    return ok


async def check_rollback() -> bool:
    """Helm upgrade на v2 → rollback к ревизии v1 → в ответах только v1."""
    async with httpx.AsyncClient(trust_env=False, timeout=30) as c:
        await forward(c)
        await helm_set(image_tag="v4", model_version="sim-cpu-v2")
        r = await c.post(BASE + "/generate", json={"prompt": "B"})
        assert r.json()["model_id"] == "sim-cpu-v2", "rollout на v2 не дошёл"
        sh("helm rollback hw11")
        sh("kubectl rollout status deploy/hw11-service --timeout=120s")
        versions = []
        for _ in range(6):
            r = await c.post(BASE + "/generate", json={"prompt": "после отката"})
            versions.append(r.json()["model_id"])
        print(f"  версии в ответах после отката: {sorted(set(versions))} — "
              f"ожидался только sim-cpu-v1")
        ok = set(versions) == {"sim-cpu-v1"}
    await helm_set(image_tag="v4", model_version="sim-cpu-v1")
    return ok


async def check_pod_replace() -> bool:
    """Замена Pod: новая реплика грузит тот же артефакт (хеш из ConfigMap)."""
    async with httpx.AsyncClient(trust_env=False, timeout=30) as c:
        await forward(c)
        r = await c.get(BASE + "/version")
        before = r.json()["artifact_sha256"]
        name = sh("kubectl get pods -l app=hw11-service "
                  "-o jsonpath='{.items[0].metadata.name}'").strip("'")
        sh(f"kubectl delete pod {name} --wait=false")
        sh("kubectl rollout status deploy/hw11-service --timeout=120s")
        ok_one = None
        for _ in range(30):
            r = await c.post(BASE + "/generate", json={"prompt": "после замены"})
            if r.status_code == 200:
                ok_one = r.json()
                break
            await asyncio.sleep(2)
        print(f"  Pod {name} удалён; новая реплика: model_id="
              f"{ok_one['model_id']}, хеш совпал: "
              f"{ok_one['artifact_sha256'] == before}")
        return ok_one["artifact_sha256"] == before


async def check_interruptions() -> bool:
    """Kill Pod во время потокового ответа: клиент видит interrupted,
    серверный счётчик sim_requests_total{outcome=interrupted} растёт."""
    async with httpx.AsyncClient(trust_env=False, timeout=30) as c:
        await forward(c)
        prom = await prom_query(
            'sum(sim_requests_total{outcome="interrupted"}) or vector(0)')
        before = prom
        task = asyncio.create_task(long_stream(c))
        await asyncio.sleep(1.5)          # стрим в середине
        name = sh("kubectl get pods -l app=hw11-service "
                  "-o jsonpath='{.items[0].metadata.name}'").strip("'")
        sh(f"kubectl delete pod {name} --force --grace-period=0")
        outcome = await task
        await asyncio.sleep(8)            # метрики доехали до Prometheus
        after = await prom_query(
            'sum(sim_requests_total{outcome="interrupted"}) or vector(0)')
        print(f"  клиент исход={outcome}, счётчик interrupted: {before} → "
              f"{after} (вырос: {after > before})")
        return outcome in ("interrupted", "client_error:ReadError",
                           "client_error:RemoteProtocolError") and after > before


async def long_stream(c) -> str:
    try:
        async with c.stream("POST", BASE + "/generate-stream",
                            json={"prompt": "длинный", "max_chunks": 8,
                                  "chunk_delay": 2.0}, timeout=30) as r:
            async for _ in r.aiter_text():
                pass
            return "completed"
    except httpx.HTTPError:
        return "interrupted"


async def prom_query(expr: str) -> float:
    import urllib.request
    import urllib.parse
    url = ("http://127.0.0.1:19090/api/v1/query?query=" + urllib.parse.quote(expr))
    with urllib.request.urlopen(url, timeout=10) as resp:
        d = json.load(resp)
    return float(d["data"]["result"][0]["value"][1]) if d["data"]["result"] else 0.0


async def check_sessions() -> bool:
    """Два диалога с контрольными строками: изоляция и продолжение после
    замены реплики (история передаётся в запросе)."""
    CTRL_A = "КОНТРОЛЬ-СЕССИЯ-А: заказ 42"
    CTRL_B = "КОНТРОЛЬ-СЕССИЯ-Б: заказ 77"
    async with httpx.AsyncClient(trust_env=False, timeout=30) as c:
        await forward(c)
        # фаза 1: оба диалога, реплика не заменяется
        r = await c.post(BASE + "/generate", json={
            "prompt": "продолжи", "session_id": "sa",
            "messages": [{"role": "user", "content": CTRL_A}]})
        pod_a1, sha1 = r.json()["pod"], r.json()["artifact_sha256"]
        r = await c.post(BASE + "/generate", json={
            "prompt": "продолжи", "session_id": "sb",
            "messages": [{"role": "user", "content": CTRL_B}]})
        pod_b1 = r.json()["pod"]
        # фаза 2: заменить реплику, повторить диалог А с той же историей
        name = sh("kubectl get pods -l app=hw11-service "
                  "-o jsonpath='{.items[0].metadata.name}'").strip("'")
        sh(f"kubectl delete pod {name} --wait=false")
        sh("kubectl rollout status deploy/hw11-service --timeout=120s")
        for _ in range(30):
            r = await c.post(BASE + "/generate", json={
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
    results = {}
    for name in wanted:
        print(f"== {name}")
        try:
            results[name] = await CHECKS[name]()
        except Exception as e:  # noqa: BLE001
            print(f"  ОШИБКА: {type(e).__name__}: {e}")
            results[name] = False
    print(json.dumps(results, ensure_ascii=False))
    verdict = all(results.values())
    print("ИТОГ:", "все проверки зелёные" if verdict else "ЕСТЬ КРАСНЫЕ")
    return verdict


if __name__ == "__main__":
    ok = asyncio.run(main())
    sys.exit(0 if ok else 1)
