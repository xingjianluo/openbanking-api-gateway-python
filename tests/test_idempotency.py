"""幂等与并发控制：并发重复提交只生效一次，同键不同内容明确拒绝。"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

logger = logging.getLogger("openbanking.tests.idempotency")


def _pay(client: TestClient, seeded: dict, key: str, amount: int):
    return client.post(
        "/payments",
        json={"account_id": seeded["account_id"], "amount_cents": amount},
        headers={**seeded["headers"], "Idempotency-Key": key},
    )


def test_concurrent_duplicate_submissions_apply_once(
    client: TestClient, seeded: dict
) -> None:
    n = 8
    logger.info("submitting %d concurrent payments, same key, amount=700", n)
    with ThreadPoolExecutor(max_workers=n) as pool:
        responses = list(pool.map(lambda _: _pay(client, seeded, "k-dup", 700), range(n)))

    statuses = [r.status_code for r in responses]
    bodies = [r.json() for r in responses]
    replays = [r.headers.get("x-idempotent-replay") for r in responses]
    logger.info("statuses=%s replays=%s", statuses, replays)
    logger.info("bodies=%s", bodies)

    assert all(s == 201 for s in statuses)
    assert len({str(b) for b in bodies}) == 1, "重复提交必须返回与首次一致的结果"
    assert replays.count("false") == 1 and replays.count("true") == n - 1

    balance = client.get(
        f"/accounts/{seeded['account_id']}", headers=seeded["headers"]
    ).json()["balance_cents"]
    logger.info("final balance=%s (expected 100000-700=99300)", balance)
    assert balance == 100_000 - 700, "并发重复只能产生一次实际扣减"


def test_sequential_replay_returns_first_result(
    client: TestClient, seeded: dict
) -> None:
    first = _pay(client, seeded, "k-seq", 250)
    second = _pay(client, seeded, "k-seq", 250)
    logger.info("first=%s second=%s", first.json(), second.json())
    assert first.json() == second.json()
    assert second.headers["x-idempotent-replay"] == "true"


def test_same_key_different_payload_rejected(client: TestClient, seeded: dict) -> None:
    first = _pay(client, seeded, "k-conflict", 100)
    assert first.status_code == 201
    conflict = _pay(client, seeded, "k-conflict", 999)
    logger.info(
        "same key with amount 100 then 999 -> status=%s body=%s",
        conflict.status_code, conflict.json(),
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"] == "idempotency_conflict"

    balance = client.get(
        f"/accounts/{seeded['account_id']}", headers=seeded["headers"]
    ).json()["balance_cents"]
    logger.info("balance after conflict=%s (expected 99900, 未被静默覆盖)", balance)
    assert balance == 100_000 - 100


def test_missing_idempotency_key_rejected(client: TestClient, seeded: dict) -> None:
    resp = client.post(
        "/payments",
        json={"account_id": seeded["account_id"], "amount_cents": 100},
        headers=seeded["headers"],
    )
    logger.info("missing Idempotency-Key -> status=%s body=%s", resp.status_code, resp.json())
    assert resp.status_code == 400
    assert resp.json()["error"] == "missing_idempotency_key"
