"""Кеш-хиты по подам за окна прогонов B из Prometheus.

Клиентские записи первого прогона не хранили поле cache (исправлено в
loadgen v6), поэтому хиты берём из sim_cache_total{result=hit} как прирост
за окно прогона, разбивку — по pod.
"""
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

PROM = "http://[::1]:19090"
# Системный прокси перехватывает и [::1] — только прямой opener (урок ДЗ 6+7).
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def query(expr: str, t: float):
    url = f"{PROM}/api/v1/query?query={urllib.parse.quote(expr)}&time={t}"
    with OPENER.open(url, timeout=15) as r:
        d = json.load(r)
    return {r["metric"].get("pod", "?"): float(r["value"][1])
            for r in d["data"]["result"]}


def hits_for_run(tag: str, run: int) -> None:
    rows = []
    for i in range(4):
        p = Path(f"results/raw/expB-{tag}-run{run}-c{i}.jsonl")
        if not p.exists():
            continue
        for line in p.read_text(encoding="utf-8").splitlines():
            if line.startswith("{"):
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    pass
    if not rows:
        print(f"  run{run}: нет данных")
        return
    t0 = min(r["wall_sent"] for r in rows) - 5
    t1 = max(r["wall_sent"] for r in rows) + 5
    hits0 = query('sum by (pod) (sim_cache_total{result="hit"})', t0)
    hits1 = query('sum by (pod) (sim_cache_total{result="hit"})', t1)
    miss0 = query('sum by (pod) (sim_cache_total{result="miss"})', t0)
    miss1 = query('sum by (pod) (sim_cache_total{result="miss"})', t1)
    print(f"  run{run} окно {int(t1 - t0)} с:")
    for pod in sorted(set(hits1) | set(miss1)):
        dh = hits1.get(pod, 0) - hits0.get(pod, 0)
        dm = miss1.get(pod, 0) - miss0.get(pod, 0)
        total = dh + dm
        if total:
            print(f"    {pod.split('-')[-2]+'-'+pod.split('-')[-1]}: "
                  f"hit {dh:.0f} / {total:.0f} ({100 * dh / total:.0f}%)")


if __name__ == "__main__":
    for tag in sys.argv[1:]:
        print(tag)
        for run in (1, 2, 3):
            hits_for_run(tag, run)
