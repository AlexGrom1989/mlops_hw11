"""Доказательство «запрос обслужен непрогретой репликой» (опыт A, фаза дефекта).

Журналы всех реплик сервиса: на каждый Pod сравниваем времена REQ_DONE и
WARMUP_DONE. Запрос, завершённый раньше собственного WARMUP_DONE реплики,
обслужен непрогретым backend — при дефекте (readiness до прогрева) их много,
при исправлении — ноль (readiness не пропускает трафик до WARMUP_DONE).
"""
import re
import subprocess
from collections import defaultdict

TS = re.compile(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+ INFO (REQ_DONE|WARMUP_DONE)")
POD = re.compile(r"\[pod/([^/\]]+)")


def main() -> None:
    out = subprocess.run(["kubectl", "logs", "-l", "app=hw11-service",
                          "--prefix"],
                         capture_output=True, text=True, timeout=60)
    # --prefix даёт "[pod/name/container] 2026-… ,345 INFO REQ_DONE …"
    warm_done: dict[str, float] = {}
    served_early: dict[str, int] = defaultdict(int)
    served_total: dict[str, int] = defaultdict(int)
    cur_pod = None
    for line in (out.stdout or "").splitlines():
        m = POD.search(line)
        if m:
            cur_pod = m.group(1)
        t = TS.search(line)
        if not t or not cur_pod:
            continue
        from datetime import datetime
        ts = datetime.strptime(t.group(1), "%Y-%m-%d %H:%M:%S").timestamp()
        if t.group(2) == "WARMUP_DONE":
            warm_done[cur_pod] = ts
        else:
            served_total[cur_pod] += 1
            if cur_pod in warm_done and ts < warm_done[cur_pod]:
                served_early[cur_pod] += 1
    total_early = sum(served_early.values())
    total_all = sum(served_total.values())
    for pod, done in sorted(warm_done.items()):
        print(f"  pod {pod}: WARMUP_DONE есть; запросов всего "
              f"{served_total[pod]}, из них до прогрева {served_early[pod]}")
    pods_without_done = set(served_total) - set(warm_done)
    if pods_without_done:
        print(f"  реплики без WARMUP_DONE в журнале (прогрев ещё шёл): "
              f"{sorted(pods_without_done)} — их запросы обслужены непрогретыми")
    early_total = total_early + sum(
        served_total[p] for p in pods_without_done)
    print(f"  ИТОГ: {early_total} из {total_all} запросов обслужены "
          f"ДО завершения прогрева своей реплики — "
          f"{'ДЕФЕКТ ПОДТВЕРЖДЁН' if early_total else 'прогрев не нарушен'}")


if __name__ == "__main__":
    main()
