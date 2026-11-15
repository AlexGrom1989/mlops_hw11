"""Генератор нагрузки ДЗ 11: монотонные часы, расписание, SSE, JSONL-итог.

Запускается в клиентском Pod (опыт B: 4 клиента с разными IP) или на хосте
(опыты A/C через port-forward). Каждый запрос несёт: запланированное и
фактическое время отправки (различие показывает замедление генератора),
первую часть, завершение, статус, Pod, версию, число частей. Итоговые
счётчики: planned/sent/completed/rejected/interrupted/expired/timeout.
Времена — только монотонные часы клиента (в секундах от старта прогона);
абсолютная стена (для сверок с Prometheus) отдельным полем wall_start.

Env/флаги:
  --url        базовый URL сервиса (в кластере: http://hw11-service:8000)
  --client-id  метка клиента (для распределения по клиентам)
  --rps        целевой темп одного клиента
  --duration   длительность фазы измерения, с
  --sessions   число диалогов (промпты генерируются детерминированно)
  --hot        доля запросов с общим «горячим» префиксом (имитация локальности)
  --stream     SSE (/generate-stream), иначе /generate
  --max-chunks и --timeout-s — параметры запроса и просрочки
"""
import argparse
import asyncio
import json
import os
import sys
import time

import httpx


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://hw11-service:8000")
    p.add_argument("--client-id", default=None,
                   help="метка клиента; по умолчанию hostname пода (индекс Job)")
    p.add_argument("--rps", type=float, default=0.5)
    p.add_argument("--duration", type=float, default=120)
    p.add_argument("--sessions", type=int, default=16)
    p.add_argument("--hot", type=float, default=0.0,
                   help="доля запросов с горячим префиксом (0..1)")
    p.add_argument("--stream", action="store_true")
    p.add_argument("--max-chunks", type=int, default=8)
    p.add_argument("--long-every", type=int, default=0,
                   help="каждый N-й запрос — «длительный» потоковый (chunk_delay=2)")
    p.add_argument("--timeout-s", type=float, default=30)
    p.add_argument("--warmup-s", type=float, default=5,
                   help="прогрев соединения и кеша до фазы измерения")
    p.add_argument("--no-keepalive", action="store_true",
                   help="Connection: close — каждый запрос балансируется заново")
    return p


def prompt_for(session: int, hot: bool) -> tuple[str, list[dict]]:
    """Детерминированные промпты: общий горячий префикс либо свой диалог."""
    if hot:
        return (f"горячий общий префикс карточки организации № 7 "
                f"для локальности кеша, запрос {session}"), []
    return f"отзыв № {session}: обслуживание, кухня, атмосфера", [
        {"role": "user", "content": f"история диалога {session}"}]


async def one_request(client: httpx.AsyncClient, url: str, args,
                      planned_t: float, seq: int, started: float, out):
    """Один запрос: отправка → первая часть → done; исход в JSONL."""
    hot = (seq % 100) < round(args.hot * 100)
    prompt, messages = prompt_for(seq % args.sessions, hot)
    session_id = f"{args.client_id}-{seq % args.sessions}"
    path = "/generate-stream" if args.stream else "/generate"
    payload = {"prompt": prompt, "messages": messages, "session_id": session_id,
               "max_chunks": args.max_chunks}
    long = bool(args.long_every and seq % args.long_every == 0)
    if long:
        # «Длительный» потоковый ответ: пересекает начало обновления (опыт A).
        payload["chunk_delay"] = 2.0
    rec = {"client_id": args.client_id, "seq": seq, "session_id": session_id,
           "hot": hot, "long": long, "planned_t": round(planned_t, 4),
           "sent_t": round(time.monotonic() - started, 4),
           "wall_sent": time.time(), "status": None, "pod": None,
           "model_id": None, "artifact_sha256": None, "chunks_seen": 0,
           "ttfc_seconds": None, "full_seconds": None,
           "queue_wait_seconds": None, "outcome": "sent"}
    sent_at = None
    first_at = None
    try:
        async with client.stream("POST", url + path, json=payload,
                                 timeout=args.timeout_s) as resp:
            sent_at = time.monotonic()
            rec["sent_t"] = round(sent_at - started, 4)
            rec["status"] = resp.status_code
            if resp.status_code != 200:
                rec["outcome"] = "rejected"
                await resp.aread()
            else:
                async for chunk in resp.aiter_text():
                    if first_at is None:
                        first_at = time.monotonic()
                        rec["ttfc_seconds"] = round(first_at - sent_at, 4)
                        rec["first_chunk"] = chunk[:120]
                    if "event: error" in chunk:
                        # Сервер открыл стрим и отказал (непрогрет/перегружен):
                        # HTTP 200, но ответа нет — учитываем как rejected.
                        rec["outcome"] = "rejected"
                        rec["stream_error"] = chunk[:120]
                    if '"request_id"' in chunk:
                        rec["chunks_seen"] += 1
                    if "event: done" in chunk:
                        data_line = chunk.split("data: ", 1)[1].splitlines()[0]
                        d = json.loads(data_line)
                        rec.update(pod=d["pod"], model_id=d["model_id"],
                                   artifact_sha256=d["artifact_sha256"],
                                   full_seconds=d["full_seconds"],
                                   queue_wait_seconds=d["queue_wait_seconds"],
                                   chunks_seen=d["chunks"],
                                   cache=d["cache"],
                                   server_ttfc=d["ttfc_seconds"])
                        rec["outcome"] = "completed"
    except httpx.TimeoutException:
        rec["outcome"] = "expired"
    except (httpx.HTTPError, Exception) as e:  # noqa: BLE001 — обрыв соединения
        rec["outcome"] = ("interrupted" if first_at is not None and
                          rec["status"] == 200 else f"client_error:{type(e).__name__}")
        rec["error"] = str(e)[:120]
    rec["done_t"] = round(time.monotonic() - started, 4)
    out.write(json.dumps(rec, ensure_ascii=False) + "\n")
    out.flush()
    return rec


async def main_async(args) -> None:
    started = time.monotonic()
    # JSONL — в stdout: журнал пода и есть сырой результат (kubectl logs),
    # извлекать файлы из подов не требуется.
    out = sys.stdout
    summary = {"client_id": args.client_id, "rps": args.rps, "duration": args.duration}
    url = args.url
    # Прогрев соединений до фазы измерения (не входит в статистику).
    if args.no_keepalive:
        # Connection: close — каждый запрос балансируется заново (опыт B4)
        limits = httpx.Limits(max_connections=64, max_keepalive_connections=0)
    else:
        limits = httpx.Limits(max_connections=64, keepalive_expiry=300)
    async with httpx.AsyncClient(trust_env=False, limits=limits) as client:
        try:
            await client.get(url + "/healthz", timeout=5)
        except Exception:  # noqa: BLE001 — прогрев может не дожить до сервиса
            pass
        schedule = [i / args.rps for i in range(int(args.rps * args.duration))]
        warm_until = args.warmup_s
        tasks = []
        for i, planned in enumerate(schedule):
            delay = (planned + warm_until) - (time.monotonic() - started)
            if delay > 0:
                await asyncio.sleep(delay)
            tasks.append(asyncio.create_task(
                one_request(client, url, args, planned, i, started, out)))
        results = await asyncio.gather(*tasks, return_exceptions=True)

    counters = {"planned": len(schedule), "sent": 0, "completed": 0,
                "rejected": 0, "interrupted": 0, "expired": 0, "other": 0}
    for r in results:
        if isinstance(r, Exception):
            counters["other"] += 1
            continue
        counters[r.get("outcome", "other").split(":")[0]] = \
            counters.get(r.get("outcome", "other").split(":")[0].replace(
                "client_error", "other"), 0) + 1
    summary.update(counters)
    summary["wall_done"] = time.time()
    summary["monotonic_duration"] = round(time.monotonic() - started, 3)
    print("SUMMARY " + json.dumps(summary, ensure_ascii=False))
    out.close()


def main() -> None:
    args = build_parser().parse_args()
    if not args.client_id:
        # loadgen-<индекс>-…: оставляем индекс Job как метку клиента
        args.client_id = os.uname().nodename.split("-")[1] \
            if "-" in os.uname().nodename else os.uname().nodename
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
