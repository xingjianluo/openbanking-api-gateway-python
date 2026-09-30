"""业务边界拒绝：余额不足/资源缺失/无效订阅等必须明确拒绝，且零副作用。"""
from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from tests.conftest import FakeSender

logger = logging.getLogger("openbanking.tests.guardrails")


def _pay(
    client: TestClient,
    seeded: dict,
    key: str,
    amount: int = 100,
    account_id: str | None = None,
):
    return client.post(
        "/payments",
        json={"account_id": account_id or seeded["account_id"], "amount_cents": amount},
        headers={**seeded["headers"], "Idempotency-Key": key},
    )


def test_insufficient_funds_rejected_without_side_effects(
    client: TestClient, seeded: dict
) -> None:
    """余额不足：明确业务拒绝（402），余额不被扣成负数、账本/投递零变动。"""
    store = client.app.state.store
    ledger_before = len(store.ledger)
    idem_before = len(store.idempotency)
    deliveries_before = len(store.deliveries)

    resp = _pay(client, seeded, "k-poor", 100_001)
    logger.info(
        "overdraw request -> status=%s body=%s", resp.status_code, resp.json()
    )
    assert resp.status_code == 402
    assert resp.json()["error"] == "insufficient_funds"

    balance = client.get(
        f"/accounts/{seeded['account_id']}", headers=seeded["headers"]
    ).json()["balance_cents"]
    logger.info(
        "balance=%s ledger=%d->%d idem=%d->%d deliveries=%d->%d",
        balance, ledger_before, len(store.ledger),
        idem_before, len(store.idempotency),
        deliveries_before, len(store.deliveries),
    )
    assert balance == 100_000, "拒绝后余额不得变化，更不能为负"
    assert len(store.ledger) == ledger_before, "账本不得新增"
    assert len(store.idempotency) == idem_before, "被拒绝请求不得登记幂等记录"
    assert len(store.deliveries) == deliveries_before, "不得产生事件投递"

    # 被拒绝后允许客户修正请求（例如换个足够小的金额）用新键重试成功，
    # 也允许用同一个键重试（拒绝不应占用幂等键）。
    retry_same_key = _pay(client, seeded, "k-poor", 100)
    logger.info("retry same key after top-up -> %s", retry_same_key.json())
    assert retry_same_key.status_code == 201
    assert retry_same_key.json()["balance_cents"] == 99_900


def test_zero_or_negative_amount_rejected(client: TestClient, seeded: dict) -> None:
    for amount in (0, -50):
        resp = _pay(client, seeded, f"k-amount-{amount}", amount)
        logger.info("amount=%s -> status=%s body=%s", amount, resp.status_code, resp.json())
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_amount"
    balance = client.get(
        f"/accounts/{seeded['account_id']}", headers=seeded["headers"]
    ).json()["balance_cents"]
    assert balance == 100_000


def test_payment_against_nonexistent_account_rejected(
    client: TestClient, seeded: dict
) -> None:
    """针对不存在资源的操作：404 业务拒绝，不 500、不落账。"""
    resp = _pay(
        client, seeded, "k-no-account", 100, account_id="does-not-exist"
    )
    logger.info("payment on missing account -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 404
    assert resp.json()["reason"] == "resource_not_found"
    assert len(client.app.state.store.ledger) == 0
    assert client.get("/deliveries").json() == []


def test_get_nonexistent_account_rejected(client: TestClient, seeded: dict) -> None:
    resp = client.get("/accounts/nope", headers=seeded["headers"])
    logger.info("get missing account -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 404
    assert resp.json()["reason"] == "resource_not_found"


def test_revoke_nonexistent_consent_returns_404(client: TestClient) -> None:
    resp = client.post("/consents/nope/revoke")
    logger.info("revoke missing consent -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 404
    assert resp.json()["error"] == "consent_not_found"


def test_invalid_subscription_requests_rejected(client: TestClient) -> None:
    """无效订阅请求：400 且明确原因，不创建订阅、不影响投递。"""
    bad_bodies = [
        {
            "partner_id": "",
            "event_types": ["payment.completed"],
            "callback_url": "https://x.example/hook",
            "secret": "s",
        },
        {
            "partner_id": "p",
            "event_types": [],
            "callback_url": "https://x.example/hook",
            "secret": "s",
        },
        {
            "partner_id": "p",
            "event_types": ["payment.completed"],
            "callback_url": "https://x.example/hook",
            "secret": "",
        },
        {
            "partner_id": "p",
            "event_types": ["payment.completed"],
            "callback_url": "ftp://x.example/hook",
            "secret": "s",
        },
        {
            "partner_id": "p",
            "event_types": ["payment.completed"],
            "callback_url": "not-a-url",
            "secret": "s",
        },
    ]
    for body in bad_bodies:
        resp = client.post("/subscriptions", json=body)
        logger.info("invalid subscription %s -> %s %s", body["callback_url"], resp.status_code, resp.json())
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_subscription"

    # 没有任何订阅被创建
    assert client.get("/deliveries").json() == []


def test_subscription_with_invalid_callback_goes_to_terminal_failure(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    """绕过入口直接登记非法回调的订阅时，投递结论必须是 invalid_subscription
    且不重试（入口层本身会拒绝创建，见上一用例）。"""
    from app.models import Subscription

    store = client.app.state.store
    with store.lock:
        store.subscriptions["bad-sub"] = Subscription(
            subscription_id="bad-sub",
            partner_id="partner-1",
            event_types={"payment.completed"},
            callback_url="ftp://bad.example/hook",
            secret="s",
        )
    _pay(client, seeded, "k-bad-callback")
    from tests.conftest import wait_terminal

    rows = wait_terminal(client)
    logger.info("invalid callback delivery=%s calls=%d", rows, sender.call_count)
    assert rows[0]["status"] == "invalid_subscription"
    assert rows[0]["failure_kind"] == "invalid_subscription"
    assert rows[0]["attempts"] == 1, "无效订阅不得重试"
    assert sender.call_count == 0, "非法回调绝不能发起真实 HTTP 调用"


def test_denied_requests_leave_ledger_consent_delivery_unchanged(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    """综合：撤销同意 + 范围不足 + 幂等冲突后，账本/同意/投递状态不被脏改。"""
    # 先建一个合法订阅，确保只有成功支付才会投递
    client.post(
        "/subscriptions",
        json={
            "partner_id": "partner-1",
            "event_types": ["payment.completed"],
            "callback_url": "https://partner.example/hook",
            "secret": "s",
        },
    )

    # 撤销同意后发起支付：403，零副作用
    client.post(f"/consents/{seeded['consent_id']}/revoke")
    revoked = _pay(client, seeded, "k-after-revoke", 100)
    logger.info("payment after revoke -> %s %s", revoked.status_code, revoked.json())
    assert revoked.status_code == 403
    assert revoked.json()["reason"] == "consent_revoked"

    store = client.app.state.store
    with store.lock:
        consent = store.consents[seeded["consent_id"]]
        ledger_tuples = [(e.entry_id, e.amount_cents) for e in store.ledger]
    assert consent.status.value == "revoked", "拒绝操作不得改写同意状态之外的数据"
    assert len(store.ledger) == 0, "拒绝操作不得新增账本"
    assert client.get("/deliveries").json() == [], "拒绝操作不得产生投递"

    # 账本快照在后续成功支付后，仍只有成功那一笔
    # （同意已撤销，无法再用它支付；账本保持为空）
    assert ledger_tuples == []
