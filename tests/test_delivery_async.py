"""后台异步投递：解耦、积压、重投不重复、失败类别可区分、边界拒绝无副作用。"""
from __future__ import annotations

import logging
import time

from fastapi.testclient import TestClient

from app.events import EventDispatcher
from app.store import Store
from tests.conftest import FakeSender

logger = logging.getLogger("openbanking.tests.delivery_async")

SECRET = "sub-secret-async"


def _subscribe(client: TestClient) -> str:
    return client.post(
        "/subscriptions",
        json={
            "partner_id": "partner-1",
            "event_types": ["payment.completed"],
            "callback_url": "https://partner.example/hook",
            "secret": SECRET,
        },
    ).json()["subscription_id"]


def _pay(client: TestClient, seeded: dict, key: str, amount: int = 100):
    return client.post(
        "/payments",
        json={"account_id": seeded["account_id"], "amount_cents": amount},
        headers={**seeded["headers"], "Idempotency-Key": key},
    )


def _balance(client: TestClient, seeded: dict) -> int:
    return client.get(
        f"/accounts/{seeded['account_id']}", headers=seeded["headers"]
    ).json()["balance_cents"]


def test_slow_callback_does_not_slow_payment_api(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher, seeded: dict
) -> None:
    """回调慢 0.5s 时，支付接口必须在明显更短的时间内返回。"""
    sender.delay = 0.5
    _subscribe(client)

    start = time.perf_counter()
    resp = _pay(client, seeded, "k-slow")
    elapsed = time.perf_counter() - start
    logger.info("payment elapsed=%.3fs with callback delay=0.5s", elapsed)

    assert resp.status_code == 201
    assert elapsed < 0.35, "回调耗时不应传导到对外接口（同步投递时至少 0.5s）"

    assert dispatcher.wait_idle(5), "后台投递最终应完成"
    deliveries = client.get("/deliveries").json()
    logger.info("deliveries after slow callback: %s", deliveries)
    assert deliveries[0]["status"] == "delivered"
    assert sender.call_count() == 1


def test_burst_backlog_drains_without_blocking_api(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher, seeded: dict
) -> None:
    """短时间涌入大量事件：接口响应有上限，积压最终全部投递成功、不丢不重。"""
    sender.delay = 0.2  # 每个回调 0.2s；同步投递 20 笔至少 4s
    n = 20
    _subscribe(client)

    start = time.perf_counter()
    for i in range(n):
        resp = _pay(client, seeded, f"k-burst-{i}")
        assert resp.status_code == 201
    elapsed = time.perf_counter() - start
    logger.info("%d payments enqueued in %.3fs (callback delay=0.2s each)", n, elapsed)
    assert elapsed < 2.0, "积压期间接口响应必须有上限（同步投递至少 4s）"

    assert dispatcher.wait_idle(30), "积压最终应全部排空"
    deliveries = client.get("/deliveries").json()
    statuses = [d["status"] for d in deliveries]
    attempts = [d["attempts"] for d in deliveries]
    logger.info("drained: n=%d statuses=%s attempts=%s", len(deliveries), set(statuses), attempts)
    assert len(deliveries) == n, "事件不能丢"
    assert set(statuses) == {"delivered"}
    assert all(a == 1 for a in attempts)
    assert sender.call_count() == n, "每个事件恰好成功投递一次"
    assert _balance(client, seeded) == 100_000 - n * 100


def test_redeliver_after_exhaustion_no_duplicate_effects(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher, seeded: dict
) -> None:
    """重试耗尽 -> 重投成功：新投递可追踪，且不产生重复扣款或重复成功投递。"""
    sender.always_fail = True
    _subscribe(client)
    _pay(client, seeded, "k-redrive")
    assert dispatcher.wait_idle(5)

    original = client.get("/deliveries").json()[0]
    logger.info("exhausted delivery: %s", original)
    assert original["status"] == "failed"
    assert original["attempts"] == 3
    assert "ConnectionError" in original["last_error"]
    assert sender.call_count() == 3

    # 重投：一次新的、可追踪的投递过程
    sender.always_fail = False
    resp = client.post(f"/deliveries/{original['delivery_id']}/redeliver")
    assert resp.status_code == 201
    redelivery = resp.json()
    logger.info("redeliver response: %s", redelivery)
    assert redelivery["redelivery_of"] == original["delivery_id"]
    assert redelivery["delivery_id"] != original["delivery_id"]

    assert dispatcher.wait_idle(5)
    deliveries = {d["delivery_id"]: d for d in client.get("/deliveries").json()}
    new = deliveries[redelivery["delivery_id"]]
    old = deliveries[original["delivery_id"]]
    logger.info("after redeliver: old=%s new=%s", old, new)
    assert new["status"] == "delivered" and new["attempts"] == 1
    assert old["status"] == "redelivered", "成功重投补救后，原记录转为 redelivered"
    assert old["attempts"] == 3, "原记录的失败历史（attempts）保留可追踪"
    assert old["last_error"] is not None, "原记录的失败原因保留可追踪"
    assert old["failure_kind"] == "callback_unreachable"
    assert new["redelivery_of"] == original["delivery_id"], "新记录可溯源到原记录"

    # 不产生重复业务副作用
    assert sender.call_count() == 4, "重投只追加一次成功投递（3 失败 + 1 成功）"
    assert _balance(client, seeded) == 100_000 - 100, "同一笔支付不会被重复扣款"

    # 已成功投递的记录不允许重投，订阅方不会收到第二次成功投递
    conflict = client.post(f"/deliveries/{new['delivery_id']}/redeliver")
    logger.info("redeliver delivered one -> %s %s", conflict.status_code, conflict.json())
    assert conflict.status_code == 409
    assert conflict.json()["error"] == "redeliver_not_allowed"

    # 关键回归：对**原始失败记录**再点一次重投，必须被明确拒绝（可区分的冲突），
    # 订阅方不能因此再收到一次成功投递
    repeat = client.post(f"/deliveries/{original['delivery_id']}/redeliver")
    logger.info(
        "repeat redeliver original -> %s %s", repeat.status_code, repeat.json()
    )
    assert repeat.status_code == 409
    assert repeat.json()["error"] == "redeliver_conflict"
    assert repeat.json()["reason"] == "already_redelivered"
    assert dispatcher.wait_idle(5)
    assert sender.call_count() == 4, "重复重投不得产生任何额外回调"

    missing = client.post("/deliveries/no-such-id/redeliver")
    assert missing.status_code == 404
    assert missing.json()["error"] == "delivery_not_found"


def test_redeliver_in_flight_delivery_rejected(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher, seeded: dict
) -> None:
    """仍在投递过程中（pending/in_progress）的记录不允许重投。"""
    sender.delay = 1.0
    _subscribe(client)
    _pay(client, seeded, "k-inflight")
    delivery = client.get("/deliveries").json()[0]
    logger.info("in-flight delivery: %s", delivery)
    assert delivery["status"] in ("pending", "in_progress")

    resp = client.post(f"/deliveries/{delivery['delivery_id']}/redeliver")
    logger.info("redeliver in-flight -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 409
    assert dispatcher.wait_idle(5)
    assert sender.call_count() == 1, "在途投递不受重投请求干扰，恰好成功一次"


def test_failure_kinds_are_distinguishable(
    client: TestClient,
    sender: FakeSender,
    dispatcher: EventDispatcher,
    seeded: dict,
    app,
) -> None:
    """订阅方拒绝 / 网络不可达 / 订阅无效 / 无匹配订阅者：四类结论可区分。"""
    # 1) 订阅方应答但返回错误状态码
    sender.reject_status = 503
    _subscribe(client)
    resp = _pay(client, seeded, "k-kind-1")
    assert resp.json()["deliveries_created"] == 1
    assert dispatcher.wait_idle(5)
    d1 = client.get("/deliveries").json()[0]
    logger.info("callback rejected -> %s", d1)
    assert d1["status"] == "failed"
    assert d1["failure_kind"] == "callback_rejected"
    assert "503" in d1["last_error"]
    sender.reject_status = None

    # 2) 订阅本身无效：重投时订阅已不存在
    sender.always_fail = True
    _pay(client, seeded, "k-kind-2")
    assert dispatcher.wait_idle(5)
    d2 = client.get("/deliveries?status=failed").json()[0]
    store: Store = app.state.store
    with store.lock:
        store.subscriptions.clear()  # 模拟订阅失效
    sender.always_fail = False
    client.post(f"/deliveries/{d2['delivery_id']}/redeliver")
    assert dispatcher.wait_idle(5)
    d3 = client.get("/deliveries?status=failed").json()[-1]
    logger.info("subscription invalid -> %s", d3)
    assert d3["redelivery_of"] == d2["delivery_id"]
    assert d3["failure_kind"] == "subscription_invalid"
    assert "subscription_not_found" in d3["last_error"]

    # 3) 无任何匹配订阅者：支付成功但明确显示没有投递目标
    calls_before = sender.call_count()
    resp = _pay(client, seeded, "k-kind-3")
    logger.info("no matching subscribers -> %s", resp.json())
    assert resp.status_code == 201
    assert resp.json()["deliveries_created"] == 0
    assert dispatcher.wait_idle(5)
    assert sender.call_count() == calls_before, "没有订阅者时不应发起任何回调"


def test_rejected_boundary_requests_have_no_side_effects(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher, seeded: dict
) -> None:
    """余额不足 / 资源不存在 / 无效订阅：明确拒绝且不改账本、同意与投递状态。"""
    _subscribe(client)

    # 1) 余额不足：422，余额不变、无投递、幂等键可修正后复用
    resp = _pay(client, seeded, "k-bound-1", amount=200_000)
    logger.info("insufficient funds -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 422
    assert resp.json()["error"] == "insufficient_funds"
    assert _balance(client, seeded) == 100_000
    retry = _pay(client, seeded, "k-bound-1", amount=100)
    assert retry.status_code == 201, "被拒绝的请求不应占用幂等键"
    assert _balance(client, seeded) == 100_000 - 100

    # 2) 非正金额：422（参数校验），余额不变
    resp = _pay(client, seeded, "k-bound-2", amount=0)
    logger.info("non-positive amount -> %s", resp.status_code)
    assert resp.status_code == 422
    assert _balance(client, seeded) == 100_000 - 100

    # 3) 不存在的账户：404 account_not_found，无扣款无投递
    resp = client.post(
        "/payments",
        json={"account_id": "no-such-account", "amount_cents": 100},
        headers={**seeded["headers"], "Idempotency-Key": "k-bound-3"},
    )
    logger.info("payment to missing account -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 404
    assert resp.json()["reason"] == "account_not_found"
    resp = client.get("/accounts/no-such-account", headers=seeded["headers"])
    assert resp.status_code == 404
    assert resp.json()["reason"] == "account_not_found"

    # 4) 撤销不存在的同意：404，且不影响既有同意
    resp = client.post("/consents/no-such-consent/revoke")
    logger.info("revoke missing consent -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 404
    assert resp.json()["error"] == "consent_not_found"
    ok = client.get(f"/accounts/{seeded['account_id']}", headers=seeded["headers"])
    assert ok.status_code == 200, "既有同意状态不得被波及"

    # 5) 无效订阅请求：400，不落库
    for bad in (
        {"partner_id": "p", "event_types": [], "callback_url": "https://x.example/h", "secret": "s"},
        {"partner_id": "p", "event_types": ["payment.completed"], "callback_url": "not-a-url", "secret": "s"},
        {"partner_id": "p", "event_types": ["payment.completed"], "callback_url": "https://x.example/h", "secret": ""},
    ):
        resp = client.post("/subscriptions", json=bad)
        logger.info("invalid subscription %s -> %s %s", bad, resp.status_code, resp.json())
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_subscription"

    # 上述拒绝不得产生任何投递副作用：整个过程中只有第 1 步的成功支付
    # 触发了一次事件投递
    assert dispatcher.wait_idle(5)
    deliveries = client.get("/deliveries").json()
    logger.info("deliveries after boundary rejections: %s", deliveries)
    assert len(deliveries) == 1
    assert deliveries[0]["status"] == "delivered"
