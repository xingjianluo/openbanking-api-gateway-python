"""租约回收：worker 在投递中途崩溃（记录滞留 in_flight）时扫描器可恢复。"""
from __future__ import annotations

import logging
import threading
import time

from app.events import EventDispatcher
from app.models import DeliveryStatus, Subscription
from app.store import Store

logger = logging.getLogger("openbanking.tests.lease")


def test_expired_inflight_lease_is_reclaimed_and_delivered() -> None:
    store = Store()
    now = [1000.0]
    clock = lambda: now[0]
    calls: list[str] = []
    lock = threading.Lock()

    def sender(url, body, headers):
        with lock:
            calls.append(url)

    dispatcher = EventDispatcher(
        store,
        sender=sender,
        max_attempts=3,
        backoff_base=0.0,
        worker_count=1,
        queue_maxsize=4,
        sweep_interval=0.01,
        lease_timeout=10.0,
        clock=clock,
        auto_start=False,  # 手动驱动，确定性地模拟崩溃
    )

    with store.lock:
        store.subscriptions["sub-1"] = Subscription(
            subscription_id="sub-1",
            partner_id="p",
            event_types={"payment.completed"},
            callback_url="https://partner.example/hook",
            secret="s",
        )
    deliveries = dispatcher.publish("payment.completed", {"x": 1})
    delivery_id = deliveries[0].delivery_id

    # 手动领取但“不发送、不回写状态”——模拟 worker 领取后崩溃
    claim = dispatcher._claim(delivery_id)
    assert claim is not None
    with store.lock:
        assert store.deliveries[delivery_id].status == DeliveryStatus.IN_FLIGHT

    # 租约未过期：扫描器不应重复投递
    dispatcher._sweep_once()
    dispatcher._enqueue(delivery_id)  # 即便显式入队也领取不到
    assert calls == []
    with store.lock:
        assert store.deliveries[delivery_id].status == DeliveryStatus.IN_FLIGHT
    logger.info("within lease: not reclaimed, calls=%s", calls)

    # 租约过期后扫描器回收并成功投递
    now[0] += 11.0
    dispatcher.start()
    deadline = time.time() + 3.0
    while time.time() < deadline:
        with store.lock:
            status = store.deliveries[delivery_id].status
        if status == DeliveryStatus.DELIVERED:
            break
        time.sleep(0.01)
    dispatcher.stop()

    with store.lock:
        final = store.deliveries[delivery_id]
    logger.info(
        "after lease expiry: status=%s attempts=%d http_calls=%d",
        final.status.value, final.attempts, len(calls),
    )
    assert final.status == DeliveryStatus.DELIVERED
    assert len(calls) == 1, "租约内不重发，过期后恰好重发一次"
