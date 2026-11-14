"""Локальный smoke simulated-сервиса: прогрев, валидация, SSE, метрики."""
import asyncio
import os
import subprocess
import sys
import time

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = "8099"
BASE = f"http://127.0.0.1:{PORT}"


async def main() -> None:
    env = {**os.environ, "WARMUP_SECONDS": "2", "CHUNK_DELAY": "0.02"}
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app",
                             "--port", PORT], env=env, cwd=ROOT)
    time.sleep(1.5)
    try:
        async with httpx.AsyncClient(timeout=30, trust_env=False) as c:
            print("healthz:", (await c.get(f"{BASE}/healthz")).status_code)
            r = await c.get(f"{BASE}/readyz")
            print("ready до прогрева:", r.status_code)
            await asyncio.sleep(2.5)
            r = await c.get(f"{BASE}/readyz")
            print("ready после:", r.status_code, r.json()["model_id"])
            r = await c.post(f"{BASE}/generate-stream",
                             json={"prompt": "тест", "max_chunks": 3})
            print("sse:", r.status_code, "| частей:", r.text.count('"i":'))
            r = await c.post(f"{BASE}/generate",
                             json={"prompt": "тест", "max_chunks": 3})
            d = r.json()
            print("json: mode=", d["backend_mode"], "ttfc=", d["ttfc_seconds"],
                  "sha=", d["artifact_sha256"][:8])
            for name, payload in [
                ("пустой prompt", {"prompt": ""}),
                ("max_chunks=999", {"prompt": "x", "max_chunks": 999}),
                ("не объект", [1, 2]),
                ("messages мусор", {"prompt": "x", "messages": [{"role": 1}]}),
            ]:
                r = await c.post(f"{BASE}/generate", json=payload)
                print(f"{name}: {r.status_code}")
            r = await c.get(f"{BASE}/metrics")
            for line in r.text.splitlines():
                if line.startswith(("sim_requests_total", "sim_ttfc_seconds_count",
                                    "sim_cache_total")):
                    print("  ", line)
    finally:
        proc.terminate()
        proc.wait(timeout=10)


if __name__ == "__main__":
    asyncio.run(main())
