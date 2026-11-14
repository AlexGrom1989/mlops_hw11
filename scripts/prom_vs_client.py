"""Сверка серверного p95 (Prometheus histogram) с клиентским журналом.

Берёт окно прогона по клиентским JSONL (wall_sent min/max), запрашивает
Prometheus histogram_quantile за то же окно и сравнивает с клиентским p95
TTFC. Расхождение ожидается в пределах двойного скрейпа (15 с) и доли
запросов вне окна (прогрев/хвост) — значения обоих источников приводятся
в отчёте.
"""
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path


def percentile(vals, q):
    s = sorted(v for v in vals if v is not None)
    return s[min(len(s) - 1, round(q / 100 * (len(s) - 1)))] if s else None


def prom_p95(start: float, end: float) -> tuple[float | None, float | None]:
    q = urllib.parse.quote(
        "histogram_quantile(0.95, sum by (le) (rate(sim_ttfc_seconds_bucket[1m])))")
    url = (f"http://127.0.0.1:19090/api/v1/query_range?query={q}"
           f"&start={start}&end={end}&step=15")
    with urllib.request.urlopen(url, timeout=30) as r:
        d = json.load(r)
    vals = [float(x[1]) for res in d["data"]["result"] for x in res["values"]]
    if not vals:
        return None, None
    return min(vals), max(vals)


def main(glob: str) -> None:
    rows = []
    for p in Path("results/raw").glob(glob):
        for line in p.read_text(encoding="utf-8").splitlines():
            if line.startswith("{"):
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    pass
    ok = [r for r in rows if r.get("outcome") == "completed" and r.get("wall_sent")]
    if not ok:
        print("нет данных")
        return
    t0, t1 = min(r["wall_sent"] for r in ok), max(r["wall_sent"] for r in ok)
    client_p95 = percentile([r["ttfc_seconds"] for r in ok], 95)
    lo, hi = prom_p95(int(t0), int(t1))
    print(f"окно: {int(t1 - t0)} с, успешных {len(ok)}")
    print(f"  клиентский p95 TTFC: {client_p95:.3f} с")
    print(f"  Prometheus p95 за окно (step 15s): min {lo:.3f} / max {hi:.3f} с"
          if lo else "  Prometheus: нет данных за окно")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "expC-C1-c*.jsonl")
