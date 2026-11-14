"""Итоги прогона генератора: p50/p95, счётчики, распределение по Pod.

Вход: JSONL записей loadgen (kubectl logs пода). Читает только строки,
начинающиеся с '{' (SUMMARY и журнал сервиса отбрасываются).
"""
import json
import sys
from pathlib import Path


def percentile(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    idx = min(len(s) - 1, max(0, round(q / 100 * (len(s) - 1))))
    return s[idx]


def main(path: str) -> None:
    rows = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
    ok = [r for r in rows if r.get("outcome") == "completed"]
    counters = {}
    for r in rows:
        outcome = r.get("outcome", "?").split(":")[0]
        counters[outcome] = counters.get(outcome, 0) + 1
    print(f"  записей: {len(rows)} | исходы: {counters}")
    if ok:
        ttfc = [r["ttfc_seconds"] for r in ok if r.get("ttfc_seconds")]
        full = [r["full_seconds"] for r in ok if r.get("full_seconds")]
        queue = [r["queue_wait_seconds"] for r in ok if r.get("queue_wait_seconds")]
        n = len(ok)
        print(f"  успешных: {n} | p50/p95 TTFC: {percentile(ttfc, 50):.3f}/"
              f"{percentile(ttfc, 95):.3f} с | p50/p95 full: "
              f"{percentile(full, 50):.3f}/{percentile(full, 95):.3f} с | "
              f"p50/p95 queue: {percentile(queue, 50):.3f}/{percentile(queue, 95):.3f} с")
        by_pod = {}
        for r in ok:
            by_pod.setdefault(r.get("pod"), []).append(r)
        for pod, rs in sorted(by_pod.items(), key=lambda kv: -len(kv[1])):
            t = [r["ttfc_seconds"] for r in rs if r.get("ttfc_seconds")]
            print(f"    pod {pod}: {len(rs)} шт, TTFC p50 {percentile(t, 50):.3f} с, "
                  f"версии {sorted({r.get('model_id') for r in rs})}")
        versions = {}
        for r in ok:
            versions[r.get("model_id")] = versions.get(r.get("model_id"), 0) + 1
        print(f"  версии в ответах: {versions}")
    delays = [r["sent_t"] - r["planned_t"] for r in rows if "sent_t" in r]
    if delays:
        late = max(delays)
        print(f"  замедление генератора (sent−planned): макс {late:.3f} с, "
              f"медиана {percentile(delays, 50):.3f} с")


if __name__ == "__main__":
    main(sys.argv[1])
