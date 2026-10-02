"""重投防重复：同一条失败记录最多派生一次生效的重投（串行与并发一致）。"""
from __future__ import annotations

import logging
import threading

from fastapi.testclient import TestClient

from app.events import EventDispatcher
from tests.conftest import FakeSender

logger = logging.getLogger("openbanking.tests.redeliver_once")

SECRET = "sub-secret-redeliver"


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


def _make_failed_delivery(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher,
    seeded: dict, key: str,
) -> dict:
    """制造一条重试耗尽的 failed 投递并返回其记录。"""
    sender.always_fail = True
    _subscribe(client)
    resp = client.post(
        "/payments",
        json={"account_id": seeded["account_id"], "amount_cents": 100},
        headers={**seeded["headers"], "Idempotency-Key": key},
    )
    assert resp.status_code == 201
    assert dispatcher.wait_idle(5)
    sender.always_fail = False
    failed = client.get("/deliveries?status=failed").json()
    assert len(failed) == 1
    logger.info("failed delivery prepared: %s", failed[0])
    return failed[0]


def test_second_redeliver_of_same_record_rejected(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher, seeded: dict
) -> None:
    """同一条失败记录连续两次重投：第一次成功，第二次被 409 拒绝，
    订阅方只收到一次成功回调，账本不受影响。"""
    original = _make_failed_delivery(client, sender, dispatcher, seeded, "k-r1")
    assert sender.call_count() == 3  # 首次投递重试 3 次均失败

    # 第一次重投：允许，且最终成功
    resp1 = client.post(f"/deliveries/{original['delivery_id']}/redeliver")
    assert resp1.status_code == 201
    new_id = resp1.json()["delivery_id"]
    assert dispatcher.wait_idle(5)
    logger.info("first redeliver accepted -> %s", resp1.json())

    # 第二次对同一条原始失败记录重投：必须被明确拒绝（可区分的冲突错误）
    resp2 = client.post(f"/deliveries/{original['delivery_id']}/redeliver")
    logger.info("second redeliver -> %s %s", resp2.status_code, resp2.json())
    assert resp2.status_code == 409
    body = resp2.json()
    assert body["error"] == "already_redelivered"
    assert body["superseded_by"] == new_id

    assert dispatcher.wait_idle(5)
    # 订阅方只成功收到一次（3 次失败 + 1 次成功），没有第二次成功投递
    assert sender.call_count() == 4
    deliveries = {d["delivery_id"]: d for d in client.get("/deliveries").json()}
    logger.info("deliveries after duplicate redeliver attempt: %s", deliveries)
    assert len(deliveries) == 2, "被拒绝的重投不得产生新投递记录"
    old = deliveries[original["delivery_id"]]
    new = deliveries[new_id]
    assert old["status"] == "failed" and old["superseded_by"] == new_id
    assert new["status"] == "delivered" and new["redelivery_of"] == old["delivery_id"]
    delivered = [d for d in deliveries.values() if d["status"] == "delivered"]
    assert len(delivered) == 1, "同一事件最多成功投递一次"


def test_concurrent_redeliver_only_one_takes_effect(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher, seeded: dict
) -> None:
    """并发对同一条失败记录发起多次重投：最多一次 201，其余 409，
    最终只新增一条投递记录、订阅方只收到一次成功回调。"""
    original = _make_failed_delivery(client, sender, dispatcher, seeded, "k-r2")
    assert sender.call_count() == 3

    n = 8
    barrier = threading.Barrier(n)
    results: list[tuple[int, dict]] = []
    lock = threading.Lock()

    def hit() -> None:
        barrier.wait()  # 尽量同时到达
        resp = client.post(f"/deliveries/{original['delivery_id']}/redeliver")
        with lock:
            results.append((resp.status_code, resp.json()))

    threads = [threading.Thread(target=hit) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    created = [body for code, body in results if code == 201]
    conflicts = [body for code, body in results if code == 409]
    logger.info("concurrent redeliver: %d created, %d conflicts", len(created), len(conflicts))
    assert len(created) == 1, "并发下最多一次重投生效"
    assert len(conflicts) == n - 1
    assert all(b["error"] == "already_redelivered" for b in conflicts)

    assert dispatcher.wait_idle(5)
    deliveries = client.get("/deliveries").json()
    logger.info("deliveries after concurrent redeliver: %s", deliveries)
    assert len(deliveries) == 2, "并发抢跑不得多造投递记录"
    new_id = created[0]["delivery_id"]
    by_id = {d["delivery_id"]: d for d in deliveries}
    assert by_id[new_id]["status"] == "delivered"
    assert by_id[original["delivery_id"]]["superseded_by"] == new_id
    assert sender.call_count() == 4, "并发下也只多一次成功投递（3 失败 + 1 成功）"


def test_failed_redelivery_can_be_redelivered_again(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher, seeded: dict
) -> None:
    """重投本身仍失败时，可对那条新的失败记录继续重投（链式可追踪）。"""
    original = _make_failed_delivery(client, sender, dispatcher, seeded, "k-r3")

    # 第一次重投：回调持续不可达，仍然失败
    sender.always_fail = True
    resp1 = client.post(f"/deliveries/{original['delivery_id']}/redeliver")
    assert resp1.status_code == 201
    second_id = resp1.json()["delivery_id"]
    assert dispatcher.wait_idle(5)
    second = {d["delivery_id"]: d for d in client.get("/deliveries").json()}[second_id]
    logger.info("first redelivery also failed: %s", second)
    assert second["status"] == "failed"

    # 对新的失败记录继续重投：允许；这次回调恢复，投递成功
    sender.always_fail = False
    resp2 = client.post(f"/deliveries/{second_id}/redeliver")
    assert resp2.status_code == 201
    third_id = resp2.json()["delivery_id"]
    assert resp2.json()["redelivery_of"] == second_id
    assert dispatcher.wait_idle(5)

    deliveries = {d["delivery_id"]: d for d in client.get("/deliveries").json()}
    logger.info("redelivery chain: %s", deliveries)
    assert deliveries[third_id]["status"] == "delivered"
    assert deliveries[second_id]["superseded_by"] == third_id
    assert deliveries[original["delivery_id"]]["superseded_by"] == second_id

    # 链上已被取代的旧记录都不能再重投
    for old_id in (original["delivery_id"], second_id):
        resp = client.post(f"/deliveries/{old_id}/redeliver")
        logger.info("redeliver superseded %s -> %s %s", old_id, resp.status_code, resp.json())
        assert resp.status_code == 409
        assert resp.json()["error"] == "already_redelivered"
    assert dispatcher.wait_idle(5)
    assert len(client.get("/deliveries").json()) == 3, "被拒绝的重投不得产生新记录"
