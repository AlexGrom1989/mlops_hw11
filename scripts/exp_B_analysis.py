"""Сводка прогона опыта B по 4 клиентским JSONL: распределение по Pod,
очередь и TTFC по реплике, кеш-хиты, перекос горячей реплики.
"""
import json
import sys
from pathlib import Path


def percentile(values, q):
    if not values:
        return float("nan")
    s = sorted(values)
    return s[min(len(s) - 1, round(q / 100 * (len(s) - 1)))]


def main(raw_dir: str, tag: str, run: str) -> None:
    rows = []
    for i in range(4):
        p = Path(raw_dir) / f"expB-{tag}-run{run}-c{i}.jsonl"
        if not p.exists():
            continue
        for line in p.read_text(encoding="utf-8").splitlines():
            if line.startswith("{"):
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    pass
    ok = [r for r in rows if r.get("outcome") == "completed"]
    by_pod: dict[str, list] = {}
    for r in ok:
        by_pod.setdefault(r.get("pod"), []).append(r)
    total = len(ok) or 1
    print(f"[B {tag} run{run}] успешных {len(ok)} из {len(rows)}")
    for pod, rs in sorted(by_pod.items(), key=lambda kv: -len(kv[1])):
        t = [r["ttfc_seconds"] for r in rs if r.get("ttfc_seconds")]
        q = [r["queue_wait_seconds"] for r in rs if r.get("queue_wait_seconds")]
        hits = sum(1 for r in rs if r.get("cache") == "hit")
        clients = sorted({r["client_id"] for r in rs})
        print(f"  pod {pod.split('-')[-2] + '-' + pod.split('-')[-1]}: "
              f"{len(rs)} ({100 * len(rs) / total:.0f}%), TTFC p50/p95 "
              f"{percentile(t, 50):.2f}/{percentile(t, 95):.2f} с, "
              f"queue p50/p95 {percentile(q, 50):.2f}/{percentile(q, 95):.2f} с, "
              f"cache hit {hits}/{len(rs)}, клиенты {clients}")
    # перекос: доля самой нагруженной реплики
    if by_pod:
        top = max(len(v) for v in by_pod.values())
        print(f"  перекос: max доля реплики {100 * top / total:.0f}% "
              f"(равномерно = 50%)")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
