"""Детерминированный файл модели-имитатора + хеш + манифест.

Артефакт — учебная фикстура, не веса: 32 КиБ байтов, выведенных из
стабильной строки версии. Одинаковые байты при каждом запуске ⇒ одинаковый
sha256 ⇒ «восстановление той же версии после замены Pod» проверяется
сравнением хешей, а не временем. Результат коммитится в model-sim/ и
распространяется в Pod через ConfigMap (режим чтения, общий всем репликам).
"""
import hashlib
import json
import sys
from pathlib import Path

MODEL_ID = sys.argv[1] if len(sys.argv) > 1 else "sim-cpu-v1"


def main() -> None:
    out = Path(__file__).resolve().parent.parent / "model-sim" / MODEL_ID
    out.mkdir(parents=True, exist_ok=True)
    payload = bytearray()
    seed = MODEL_ID.encode()
    while len(payload) < 32 * 1024:
        seed = hashlib.sha256(seed).digest()
        payload += seed
    payload = bytes(payload[:32 * 1024])
    digest = hashlib.sha256(payload).hexdigest()
    (out / "artifact.bin").write_bytes(payload)
    (out / "artifact.sha256").write_text(digest + "\n", encoding="utf-8")
    (out / "manifest.json").write_text(
        json.dumps({"model_id": MODEL_ID, "artifact_sha256": digest,
                    "backend_mode": "simulated",
                    "size_bytes": len(payload),
                    "kind": "учебная фикстура-имитатор, не веса"},
                   ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"{MODEL_ID}: artifact.bin {len(payload)} B, sha256 {digest}")


if __name__ == "__main__":
    main()
