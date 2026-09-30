"""异步投递解耦：慢回调不拖慢接口、积压有界、事件不丢、失败分类可解释。"""
from __future__ import annotations

import logging
import threading
import time

from fastapi.testclient import TestClient

from tests.conftest import FakeSender, wait_deliveries, wait_terminal

logger = logging.getLogger("openbanking.tests.async_delivery")


def _subscribe(client: TestClient, url: str = "https://partner.example/hook") -> dict:
    resp = client.post(
        "/subscriptions",
        json={
            "partner_id": "partner-1",
            "event_types": ["payment.completed"],
            "callback_url": url,
            "secret": "s3cr3t",
        },
    )
    assert resp.status_code == 201, resp.json()
    return resp.json()


def _pay(client: TestClient, seeded: dict, key: str, amount: int = 100):
    return client.post(
        "/payments",
        json={"account_id": seeded["account_id"], "amount_cents": amount},
        headers={**seeded["headers"], "Idempotency-Key": key},
    )


def test_slow_callback_does_not_block_payment_api(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    """回调被阻塞 5 秒时，支付接口仍须在很短时间内返回。"""
    sender.gate = threading.Event()
    _subscribe(client)

    start = time.perf_counter()
    resp = _pay(client, seeded, "k-slow")
    elapsed = time.perf_counter() - start
    logger.info(
        "payment with blocked callback -> status=%s elapsed=%.3fs",
        resp.status_code, elapsed,
    )
    assert resp.status_code == 201
    assert elapsed < 0.5, f"对外接口被回调拖慢：{elapsed:.3f}s"

    # 接口返回时投递尚未发生（回调被闸门挡住），证明请求线程不参与投递
    assert sender.call_count == 0

    # 放行后台投递，事件最终成功送达、不丢
    sender.gate.set()
    rows = wait_terminal(client)
    assert rows[0]["status"] == "delivered"
    assert sender.call_count == 1


def test_burst_of_events_is_bounded_and_all_events_eventually_delivered(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    """大量事件瞬间涌入：接口响应时间有上限，积压全部最终送达、零丢失。"""
    sender.delay = 0.01  # 每个回调慢一点，制造真实积压
    _subscribe(client)

    n = 60
    start = time.perf_counter()
    statuses: list[int] = []
    for i in range(n):
        r = _pay(client, seeded, f"k-burst-{i}", amount=1)
        statuses.append(r.status_code)
    publish_elapsed = time.perf_counter() - start
    logger.info(
        "published %d events in %.3fs, statuses_ok=%s",
        n, publish_elapsed, all(s == 201 for s in statuses),
    )
    assert all(s == 201 for s in statuses)
    # 60 次请求总耗时远小于串行等待 60 个慢回调（60*0.01=0.6s）
    assert publish_elapsed < 0.4, (
        f"接口被投递积压拖垮：{publish_elapsed:.3f}s"
    )

    rows = wait_deliveries(client, expected=n)
    rows = wait_terminal(client, timeout=10.0)
    delivered = [r for r in rows if r["status"] == "delivered"]
    logger.info(
        "delivered=%d/%d, max attempts=%d",
        len(delivered), n, max(r["attempts"] for r in rows),
    )
    assert len(rows) == n
    assert len(delivered) == n, "有界积压下事件不允许丢失"


def test_no_matching_subscriber_is_observable(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    """没有任何匹配订阅者：给出 no_subscriber 结论，且不产生投递。"""
    # 只订阅别的事件类型，没有任何订阅者匹配 payment.completed
    client.post(
        "/subscriptions",
        json={
            "partner_id": "partner-1",
            "event_types": ["account.opened"],
            "callback_url": "https://other.example/hook",
            "secret": "x",
        },
    )
    resp = _pay(client, seeded, "k-no-sub")
    event_id = None
    # 支付事件 id 未直接暴露；从 store 侧确认零投递并通过内部事件接口验证
    with client.app.state.store.lock:
        events = list(client.app.state.store.events.values())
        assert len(events) == 1
        event_id = events[0].event_id

    outcome = client.get(f"/events/{event_id}/deliveries").json()
    logger.info("payment response=%s event outcome=%s", resp.status_code, outcome)
    assert outcome["outcome"] == "no_subscriber"
    assert outcome["deliveries"] == []
    assert client.get("/deliveries").json() == []


def test_subscriber_4xx_is_rejected_not_retried(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    """订阅方明确拒绝（4xx）：rejected 终态、可区分、不做无意义重试。"""
    sender.reject_status = 400
    _subscribe(client)
    _pay(client, seeded, "k-4xx")

    rows = wait_terminal(client)
    logger.info("4xx response -> delivery=%s", rows[0])
    assert rows[0]["status"] == "rejected"
    assert rows[0]["failure_kind"] == "subscriber_rejected"
    assert "HTTP 400" in rows[0]["last_error"]
    assert rows[0]["attempts"] == 1, "4xx 不应重试"
    assert sender.call_count == 1


def test_network_unreachable_retries_then_fails_with_reason(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    """网络不可达：重试退避耗尽 -> failed + network_unreachable + 次数与原因。"""
    sender.always_fail = True
    _subscribe(client)
    _pay(client, seeded, "k-unreachable")

    rows = wait_terminal(client)
    logger.info("unreachable -> delivery=%s", rows[0])
    assert rows[0]["status"] == "failed"
    assert rows[0]["failure_kind"] == "network_unreachable"
    assert rows[0]["attempts"] == 3
    assert "ConnectionError" in rows[0]["last_error"]


def test_subscription_5xx_retries_and_can_recover(
    client: TestClient, sender: FakeSender, seeded: dict
) -> None:
    """订阅方 5xx 属于可重试的暂时故障，恢复后送达。"""
    calls = {"n": 0}
    lock = threading.Lock()

    from app.events import SubscriberServerError

    def flaky(url, body, headers):
        with lock:
            calls["n"] += 1
            n = calls["n"]
        if n <= 2:
            raise SubscriberServerError("HTTP 503: boom")
        sender.calls.append((url, body, headers))

    client.app.state.dispatcher.sender = flaky
    _subscribe(client)
    _pay(client, seeded, "k-5xx")

    rows = wait_terminal(client)
    logger.info("5xx then recovered -> %s", rows[0])
    assert rows[0]["status"] == "delivered"
    assert rows[0]["attempts"] == 3


def test_queue_overflow_is_recovered_by_sweeper_no_event_lost(
    sender: FakeSender,
) -> None:
    """队列极小且回调被阻塞时，超出容量的事件滞留 PENDING，
    由扫描线程兜底拾取；放开后全部送达，零丢失。"""
    from fastapi.testclient import TestClient

    from app.main import create_app

    app = create_app(
        sender=sender,
        worker_count=1,
        queue_maxsize=2,
        enqueue_timeout=0.01,
        sweep_interval=0.01,
    )
    with TestClient(app) as small_client:
        acc = small_client.post(
            "/accounts", json={"customer_id": "cust-1", "balance_cents": 100_000}
        ).json()
        con = small_client.post(
            "/consents",
            json={
                "partner_id": "partner-1",
                "customer_id": "cust-1",
                "scopes": ["accounts:read", "payments:write"],
                "ttl_seconds": 3600,
            },
        ).json()
        headers = {
            "X-Partner-Id": "partner-1",
            "X-Consent-Id": con["consent_id"],
        }
        sub = small_client.post(
            "/subscriptions",
            json={
                "partner_id": "partner-1",
                "event_types": ["payment.completed"],
                "callback_url": "https://partner.example/hook",
                "secret": "s",
            },
        ).json()
        assert sub["subscription_id"]

        gate = threading.Event()
        sender.gate = gate

        n = 10
        for i in range(n):
            r = small_client.post(
                "/payments",
                json={"account_id": acc["account_id"], "amount_cents": 1},
                headers={**headers, "Idempotency-Key": f"k-overflow-{i}"},
            )
            assert r.status_code == 201, r.json()

        # 此时仅有少数进入队列，其余滞留；放开闸门后扫描器必须全部补救
        gate.set()
        rows = wait_deliveries(small_client, expected=n, timeout=5.0)
        rows = wait_terminal(small_client, timeout=10.0)
        statuses = sorted(r["status"] for r in rows)
        logger.info("overflow recovery statuses=%s", statuses)
        assert len(rows) == n
        assert all(s == "delivered" for s in statuses), statuses
        assert sender.call_count == n


def test_no_duplicate_delivery_under_high_concurrency(
    sender: FakeSender,
) -> None:
    """多 worker + 高事件量 + 部分瞬断：每条订阅成功 HTTP 次数必须恰好 1 次，
    原子领取（in_flight）保证成功投递不重复。"""
    from fastapi.testclient import TestClient

    from app.events import DeliveryNetworkError
    from app.main import create_app

    app = create_app(
        sender=sender,
        max_attempts=5,
        backoff_base=0.001,
        worker_count=8,
        queue_maxsize=16,
        enqueue_timeout=0.001,
        sweep_interval=0.002,
    )
    counter = {"calls": 0}
    lock = threading.Lock()

    import json as _json

    # 每个 event_id 确定性地先失败 2 次再成功：既有大量重试，又保证最终送达，
    # 且单条失败次数（2）严格小于 max_attempts（5），不会误判为耗尽。
    failures_by_event: dict[str, int] = {}
    FAIL_FIRST = 2

    def flaky(url, body, headers):
        event_id = _json.loads(body)["event_id"]
        with lock:
            counter["calls"] += 1
            seen = failures_by_event.get(event_id, 0)
            if seen < FAIL_FIRST:
                failures_by_event[event_id] = seen + 1
                should_fail = True
            else:
                should_fail = False
        if should_fail:
            raise DeliveryNetworkError("transient network error")
        sender.calls.append((url, body, headers))

    app.state.dispatcher.sender = flaky

    with TestClient(app) as c:
        acc = c.post(
            "/accounts", json={"customer_id": "cust-1", "balance_cents": 1_000_000}
        ).json()
        con = c.post(
            "/consents",
            json={
                "partner_id": "p",
                "customer_id": "cust-1",
                "scopes": ["payments:write"],
                "ttl_seconds": 3600,
            },
        ).json()
        c.post(
            "/subscriptions",
            json={
                "partner_id": "p",
                "event_types": ["payment.completed"],
                "callback_url": "https://partner.example/hook",
                "secret": "s",
            },
        )
        n = 100
        for i in range(n):
            r = c.post(
                "/payments",
                json={"account_id": acc["account_id"], "amount_cents": 1},
                headers={
                    "X-Partner-Id": "p",
                    "X-Consent-Id": con["consent_id"],
                    "Idempotency-Key": f"k-conc-{i}",
                },
            )
            assert r.status_code == 201, r.json()

        rows = wait_deliveries(c, expected=n, timeout=10.0)
        rows = wait_terminal(c, timeout=15.0)
        delivered = [r for r in rows if r["status"] == "delivered"]
        logger.info(
            "total http calls=%d (>=%d), delivered=%d/%d",
            counter["calls"], n, len(delivered), n,
        )
        assert len(rows) == n
        assert len(delivered) == n
        # 关键断言：成功的 HTTP 投递次数恰好等于事件数，绝不重复成功
        assert sender.call_count == n, (
            f"成功投递次数 {sender.call_count} != 事件数 {n}，出现重复投递"
        )
        # 每次成功对应唯一的 event_id（正文中携带）
        event_ids = [
            _json.loads(body)["event_id"] for _, body, _ in sender.calls
        ]
        assert len(event_ids) == len(set(event_ids)), "同一 event_id 被成功投递多次"
        # 总调用次数 = n 次成功 + 每个 event 恰好 2 次失败
        assert counter["calls"] == n * (1 + FAIL_FIRST)
