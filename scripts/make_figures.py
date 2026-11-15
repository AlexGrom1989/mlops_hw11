"""Четыре графика ДЗ 11 (results/figures/*.png) из сырых результатов.

1. fig-A-warmup.png  — опыт A: Ready/EndpointSlice и rejected по времени
   (дефектный прогрев против исправленного).
2. fig-B-pods.png    — опыт B hot80 (обычный Service): TTFC и очередь по Pod.
3. fig-B-policies.png— сравнение политик по трём прогонам (p95 TTFC).
4. fig-C-hpa.png     — опыт C: реплики 2→3→2 и фаза нагрузки по времени.
Единицы: секунды, доля, штуки; условия подписаны на самих графиках.
"""
import json
import re
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RAW = Path("results/raw")
FIG = Path("results/figures")
FIG.mkdir(parents=True, exist_ok=True)
plt.rcParams.update({"font.size": 11, "figure.dpi": 130})


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("{"):
            try:
                out.append(json.loads(line))
            except ValueError:
                pass
    return out


def percentile(vals, q):
    s = sorted(v for v in vals if v is not None)
    return s[min(len(s) - 1, round(q / 100 * (len(s) - 1)))] if s else 0


def fig_a_warmup() -> None:
    """Ready новых подов по времени: дефект против исправления.

    Источник: results/raw/expA-ready-trace.jsonl (scripts/exp_A_probe.sh):
    каждые 2 с Ready-поды нового RS в момент rollout'а с прогревом 20 с.
    """
    rows = load_jsonl(RAW / "expA-ready-trace.jsonl")
    if not rows:
        print("нет трейса expA-ready-trace.jsonl — fig-A пропущено")
        return
    fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    for ax, (phase, title) in zip(axes, [
            ("defect", "ДЕФЕКТ: readiness отдаёт 200 до завершения прогрева"),
            ("fixed", "ИСПРАВЛЕНО: /readyz отвечает после загрузки и прогрева")]):
        pts = [(r["t"], r["ready_new"], r["ready_all"]) for r in rows
               if r["phase"] == phase]
        if not pts:
            continue
        xs = [p[0] for p in pts]
        ax.step(xs, [p[1] for p in pts], where="post", color="#4878cf",
                linewidth=2, label="Ready-поды НОВОЙ версии")
        ax.step(xs, [p[2] for p in pts], where="post", color="#956cb4",
                linewidth=1.5, ls="--", label="Ready всего (старые+новые)")
        ax.axvspan(0, 20, alpha=.15, color="crimson", label="окно прогрева 20 с")
        ax.set_ylim(-0.3, 3.6)
        ax.set_yticks([0, 1, 2, 3])
        ax.set_ylabel("поды")
        ax.set_title(title, fontsize=11, loc="left")
        ax.legend(fontsize=9, loc="center right")
    axes[-1].set_xlabel("секунды после старта rollout (нагрузка 2 rps подана сразу)")
    fig.suptitle("Опыт A:EndpointSlice при прогреве 20 с — новая версия получает "
                 "трафик раньше вычислений при дефекте", y=1.0)
    fig.tight_layout()
    fig.savefig(FIG / "fig-A-warmup.png", bbox_inches="tight")


def fig_b_pods() -> None:
    rows = []
    for i in range(4):
        rows += load_jsonl(RAW / "expB-B2-hot80-plain-run1-c0.jsonl".replace("c0", f"c{i}"))
    ok = [r for r in rows if r.get("outcome") == "completed"]
    by_pod: dict[str, list] = {}
    for r in ok:
        by_pod.setdefault(r.get("pod"), []).append(r)
    pods = sorted(by_pod)
    ttfc = [percentile([r["ttfc_seconds"] for r in by_pod[p]], 50) for p in pods]
    queue = [percentile([r["queue_wait_seconds"] for r in by_pod[p]], 50) for p in pods]
    share = [100 * len(by_pod[p]) / len(ok) for p in pods]
    x = range(len(pods))
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.2))
    a1.bar([i - .2 for i in x], ttfc, .4, label="TTFC p50, с")
    a1.bar([i + .2 for i in x], queue, .4, label="очередь p50, с")
    a1.set_xticks(list(x), [p.split("-")[-2][-4:] + "…" for p in pods])
    a1.set_ylabel("секунды"); a1.legend()
    a1.set_title("Задержка и очередь по реплике\n(hot80, обычный Service, прогон 1)")
    a2.bar(list(x), share, color=["crimson" if s > 60 else "#4878cf" for s in share])
    a2.set_xticks(list(x), [p.split("-")[-2][-4:] + "…" for p in pods])
    a2.set_ylabel("% запросов")
    a2.set_title("Распределение запросов по репликам\n(равномерно = 50%)")
    fig.tight_layout()
    fig.savefig(FIG / "fig-B-pods.png", bbox_inches="tight")


def fig_b_policies() -> None:
    combos = [("B1-uniform-plain", "uniform, обычный svc"),
              ("B2-hot80-plain", "hot80, обычный svc"),
              ("B3-hot80-affinity", "hot80, ClientIP"),
              ("B4-hot80-noka", "hot80, без keep-alive"),
              ("B5-uniform-affinity", "uniform, ClientIP")]
    labels, means, lows, highs = [], [], [], []
    for tag, name in combos:
        p95s = []
        for r in (1, 2, 3):
            rows = []
            for i in range(4):
                rows += load_jsonl(RAW / f"expB-{tag}-run{r}-c{i}.jsonl")
            ok = [x for x in rows if x.get("outcome") == "completed"]
            if ok:
                p95s.append(percentile([x["ttfc_seconds"] for x in ok], 95))
        if p95s:
            labels.append(name)
            means.append(sum(p95s) / len(p95s))
            lows.append(min(p95s))
            highs.append(max(p95s))
    fig, ax = plt.subplots(figsize=(9.5, 4.4))
    err_lo = [m - l for m, l in zip(means, lows)]
    err_hi = [h - m for m, h in zip(means, highs)]
    ax.bar(labels, means, yerr=[err_lo, err_hi], capsize=5,
           color=["#4878cf", "crimson", "#ee8536", "#6acc64", "#956cb4"])
    ax.set_ylabel("TTFC p95, с (среднее по 3 прогонам, полосы — min/max)")
    ax.set_title("Опыт B: сравнение политик (4 клиента, фаза 120 с, 2 реплики,\n"
                 "simulated: cache hit 0.1 с / miss 0.4 с, чанки 8×50 мс)")
    ax.tick_params(axis="x", rotation=12)
    fig.tight_layout()
    fig.savefig(FIG / "fig-B-policies.png", bbox_inches="tight")


def fig_c_hpa() -> None:
    rows = load_jsonl(RAW / "expC-hpa-series.jsonl")
    pts = []
    for r in rows:
        try:
            pts.append((r["t"], int(r.get("desired") or 0),
                        int(r.get("current") or 0)))
        except (ValueError, TypeError):
            continue
    if not pts:
        print("нет expC-hpa-series.jsonl — fig-C пропущено")
        return
    t0 = pts[0][0]
    xs = [p[0] - t0 for p in pts]
    fig, ax = plt.subplots(figsize=(10, 4.2))
    ax.step(xs, [p[2] for p in pts], where="post", linewidth=2,
            label="фактические реплики (current)")
    ax.step(xs, [p[1] for p in pts], where="post", ls="--",
            label="желаемые (HPA desired)")
    # фаза нагрузки: до первого снятия (клиенты идут 300 с)
    ax.axvspan(0, 300, alpha=.15, color="#4878cf", label="нагрузка ~3 rps")
    ax.set_xlabel("секунды от старта опыта C1")
    ax.set_ylabel("реплики")
    ax.set_ylim(1.5, 3.5)
    ax.set_title("Опыт C: HPA 2→3→2 по CPU (target 60%, stabilizationWindow 120 с,\n"
                 "cpuBurn 0.25 с/запрос); фактическая длительность возврата — в REPORT")
    ax.legend(loc="lower right", fontsize=9)
    fig.tight_layout()
    fig.savefig(FIG / "fig-C-hpa.png", bbox_inches="tight")


if __name__ == "__main__":
    fig_a_warmup()
    fig_b_pods()
    fig_b_policies()
    fig_c_hpa()
    print("графики:", *[p.name for p in sorted(FIG.glob("*.png"))])
