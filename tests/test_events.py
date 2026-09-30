"""事件投递：签名校验、后台异步、失败分类、重试退避、重投链。"""
from __future__ import annotations

import json
import logging

from fastapi.testclient import TestClient

from app.events import verify_signature
from tests.conftest import FakeSender, wait_terminal

logger = logging.getLogger("openbanking.tests.events")

SECRET = "sub-secret-1"


def _subscribe(client: TestClient, *, url: str = "https://partner.example/hook") -> str:
    sub = client.post(
        "/subscriptions",
        json={
            "partner_id": "partner-1",
            "event_types": ["payment.completed"],
            "callback_url": url,
            "secret": SECRET,
        },
    ).json()
    logger.info("subscription created: %s", sub)
    return sub["subscription_id"]


def _pay(client: TestClient, seeded: dict, key: str = "k-evt", amount: int = 100):
    resp = client.post(
        "/payments",
        json={"account_id": seeded["account_id"], "amount_cents": amount},
        headers={**seeded["headers"], "Idempotency-Key": key},
    )
    assert resp.status_code == 201, resp.json()
    return resp


def _deliveries(client: TestClient) -> list[dict]:
    return client.get("/deliveries").json()


def test_delivery_signature_verifies_and_tampering_detected(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    _subscribe(client)
    _pay(client, seeded)
    rows = wait_terminal(client)

    assert sender.call_count == 1
    _, body, headers = sender.calls[0]
    signature = headers["X-Signature"]
    logger.info("delivered body=%s signature=%s", body, signature)

    assert verify_signature(SECRET, body, signature), "合法签名必须校验通过"

    tampered = json.loads(body)
    tampered["payload"]["amount_cents"] = 999_999
    tampered_body = json.dumps(tampered, sort_keys=True, separators=(",", ":")).encode()
    logger.info("tampered body=%s", tampered_body)
    assert not verify_signature(SECRET, tampered_body, signature), (
        "正文被篡改后签名必须校验失败"
    )
    assert not verify_signature("wrong-secret", body, signature), (
        "密钥不符也必须校验失败"
    )

    assert rows[0]["status"] == "delivered"
    assert rows[0]["attempts"] == 1


def test_delivery_retries_with_backoff_then_succeeds(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    sender.failures_before_success = 2
    _subscribe(client)
    _pay(client, seeded)

    rows = wait_terminal(client)
    logger.info("after 2 transient failures: deliveries=%s", rows)
    assert rows[0]["status"] == "delivered"
    assert rows[0]["attempts"] == 3, "前两次失败、第三次成功"
    assert rows[0]["last_error"] is None
    assert sender.call_count == 3


def test_delivery_failure_exhausts_retries_with_reason(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    sender.always_fail = True
    _subscribe(client)
    _pay(client, seeded)

    rows = wait_terminal(client)
    logger.info("always-fail sender: deliveries=%s", rows)
    assert rows[0]["status"] == "failed"
    assert rows[0]["attempts"] == 3, "重试次数耗尽"
    assert "ConnectionError" in rows[0]["last_error"]
    assert rows[0]["failure_kind"] == "network_unreachable"
    # 投递失败不影响支付本身的结果
    balance = client.get(
        f"/accounts/{seeded['account_id']}", headers=seeded["headers"]
    ).json()["balance_cents"]
    assert balance == 100_000 - 100


def test_redrive_failed_delivery_creates_new_trackable_attempt(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    """重试耗尽 -> 重投成功：是一次新的可追踪投递，且订阅方只成功收到一次。"""
    sender.always_fail = True
    _subscribe(client)
    _pay(client, seeded, key="k-redrive")
    rows = wait_terminal(client)
    failed = rows[0]
    assert failed["status"] == "failed"
    assert failed["attempts"] == 3
    assert sender.call_count == 3

    # 重投：发送器恢复正常
    sender.always_fail = False
    resp = client.post(f"/deliveries/{failed['delivery_id']}/redrive")
    logger.info("redrive response=%s", resp.json())
    assert resp.status_code == 202
    redelivery_id = resp.json()["redelivery_id"]
    assert redelivery_id != failed["delivery_id"], "重投必须是一条新记录"

    rows = wait_terminal(client)
    by_id = {r["delivery_id"]: r for r in rows}
    new = by_id[redelivery_id]
    logger.info("redriven delivery=%s", new)
    assert new["status"] == "delivered"
    assert new["attempts"] == 1
    assert new["origin_delivery_id"] == failed["delivery_id"], "重投链必须可追踪"
    # 原失败记录保持 failed，不被改写
    assert by_id[failed["delivery_id"]]["status"] == "failed"
    # 订阅方只成功收到一次（前 3 次为失败尝试）
    assert sender.call_count == 4

    # 已成功重投后，再次重投必须被拒绝，避免订阅方收到两次成功投递
    again = client.post(f"/deliveries/{failed['delivery_id']}/redrive")
    logger.info("second redrive after success -> %s", again.json())
    assert again.status_code == 409
    assert again.json()["error"] == "redrive_not_allowed"
    assert sender.call_count == 4, "拒绝的重投不得产生任何投递调用"

    # 对非失败记录（已 delivered）重投也必须拒绝
    rejected_redrive = client.post(f"/deliveries/{redelivery_id}/redrive")
    assert rejected_redrive.status_code == 409
    # 不存在的投递 -> 404
    assert client.post("/deliveries/nope/redrive").status_code == 404


def test_redrive_success_does_not_duplicate_ledger(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    """重投只重发事件，绝不重放支付：账本与余额保持只扣一次。"""
    sender.always_fail = True
    _subscribe(client)
    _pay(client, seeded, key="k-redrive-ledger", amount=300)
    wait_terminal(client)

    ledger_before = _ledger_count(client, seeded["account_id"])
    sender.always_fail = False
    failed = _deliveries(client)[0]
    client.post(f"/deliveries/{failed['delivery_id']}/redrive")
    wait_terminal(client)

    balance = client.get(
        f"/accounts/{seeded['account_id']}", headers=seeded["headers"]
    ).json()["balance_cents"]
    logger.info(
        "balance after redrive=%s ledger=%s (expected 99700 / 1 entry)",
        balance, ledger_before,
    )
    assert balance == 100_000 - 300
    assert _ledger_count(client, seeded["account_id"]) == ledger_before == 1


def _ledger_count(client: TestClient, account_id: str) -> int:
    # 测试用：通过重复支付观察余额不如直接查内部 store；这里以余额不变佐证，
    # 账本条数通过应用状态读取。
    return len(client.app.state.store.ledger)


def test_failure_categories_are_distinct(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    """策略拒绝、幂等冲突、投递失败三类原因不得混为一谈。"""
    sender.always_fail = True
    _subscribe(client)

    # 1) 策略拒绝：范围不足，且不得产生任何投递
    narrow = client.post(
        "/consents",
        json={
            "partner_id": "partner-1",
            "customer_id": "cust-1",
            "scopes": ["accounts:read"],
            "ttl_seconds": 3600,
        },
    ).json()
    denied = client.post(
        "/payments",
        json={"account_id": seeded["account_id"], "amount_cents": 100},
        headers={
            "X-Partner-Id": "partner-1",
            "X-Consent-Id": narrow["consent_id"],
            "Idempotency-Key": "k-cat-1",
        },
    )
    # 2) 幂等冲突：同键不同内容
    ok = client.post(
        "/payments",
        json={"account_id": seeded["account_id"], "amount_cents": 100},
        headers={**seeded["headers"], "Idempotency-Key": "k-cat-2"},
    )
    conflict = client.post(
        "/payments",
        json={"account_id": seeded["account_id"], "amount_cents": 200},
        headers={**seeded["headers"], "Idempotency-Key": "k-cat-2"},
    )
    # 3) 投递失败：支付成功但订阅投递失败
    rows = wait_terminal(client)

    logger.info(
        "policy denial: %s; idempotency conflict: %s; deliveries: %s",
        denied.json(), conflict.json(), rows,
    )
    assert denied.status_code == 403 and denied.json()["reason"] == "scope_insufficient"
    assert ok.status_code == 201
    assert conflict.status_code == 409
    assert conflict.json()["error"] == "idempotency_conflict"
    assert len(rows) == 1, "策略拒绝与幂等冲突不得产生事件投递"
    assert rows[0]["status"] == "failed"
    assert "ConnectionError" in rows[0]["last_error"]
