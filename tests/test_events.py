"""事件投递：签名校验与篡改识别、失败重试退避、失败原因与策略/幂等错误分离。"""
from __future__ import annotations

import json
import logging

from fastapi.testclient import TestClient

from app.events import verify_signature
from tests.conftest import FakeSender

logger = logging.getLogger("openbanking.tests.events")

SECRET = "sub-secret-1"


def _subscribe(client: TestClient) -> str:
    sub = client.post(
        "/subscriptions",
        json={
            "partner_id": "partner-1",
            "event_types": ["payment.completed"],
            "callback_url": "https://partner.example/hook",
            "secret": SECRET,
        },
    ).json()
    logger.info("subscription created: %s", sub)
    return sub["subscription_id"]


def _pay(client: TestClient, seeded: dict, key: str = "k-evt") -> None:
    resp = client.post(
        "/payments",
        json={"account_id": seeded["account_id"], "amount_cents": 100},
        headers={**seeded["headers"], "Idempotency-Key": key},
    )
    assert resp.status_code == 201, resp.json()


def _deliveries(client: TestClient) -> list[dict]:
    return client.get("/deliveries").json()


def test_delivery_signature_verifies_and_tampering_detected(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    _subscribe(client)
    _pay(client, seeded)

    assert len(sender.calls) == 1
    url, body, headers = sender.calls[0]
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

    deliveries = _deliveries(client)
    logger.info("deliveries=%s", deliveries)
    assert deliveries[0]["status"] == "delivered"
    assert deliveries[0]["attempts"] == 1


def test_delivery_retries_with_backoff_then_succeeds(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    sender.failures_before_success = 2
    _subscribe(client)
    _pay(client, seeded)

    deliveries = _deliveries(client)
    logger.info("after 2 transient failures: deliveries=%s", deliveries)
    assert deliveries[0]["status"] == "delivered"
    assert deliveries[0]["attempts"] == 3, "前两次失败、第三次成功"
    assert deliveries[0]["last_error"] is None
    assert len(sender.calls) == 3


def test_delivery_failure_exhausts_retries_with_reason(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    sender.always_fail = True
    _subscribe(client)
    _pay(client, seeded)

    deliveries = _deliveries(client)
    logger.info("always-fail sender: deliveries=%s", deliveries)
    assert deliveries[0]["status"] == "failed"
    assert deliveries[0]["attempts"] == 3, "重试次数耗尽"
    assert "ConnectionError" in deliveries[0]["last_error"]
    # 投递失败不影响支付本身的结果
    balance = client.get(
        f"/accounts/{seeded['account_id']}", headers=seeded["headers"]
    ).json()["balance_cents"]
    assert balance == 100_000 - 100


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
    deliveries = _deliveries(client)

    logger.info(
        "policy denial: %s; idempotency conflict: %s; deliveries: %s",
        denied.json(), conflict.json(), deliveries,
    )
    assert denied.status_code == 403 and denied.json()["reason"] == "scope_insufficient"
    assert ok.status_code == 201
    assert conflict.status_code == 409
    assert conflict.json()["error"] == "idempotency_conflict"
    assert len(deliveries) == 1, "策略拒绝与幂等冲突不得产生事件投递"
    assert deliveries[0]["status"] == "failed"
    assert "ConnectionError" in deliveries[0]["last_error"]
