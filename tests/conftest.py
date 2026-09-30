from __future__ import annotations

import logging
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.events import CallbackRejectedError, EventDispatcher
from app.main import create_app

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger("openbanking.tests")


class FakeSender:
    """可控投递发送器：记录每次投递，可注入延迟、瞬时失败、持续失败与拒绝。

    后台 worker 线程会并发调用，因此内部状态用锁保护。
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes, dict[str, str]]] = []
        self.failures_before_success = 0  # 前 N 次抛 ConnectionError
        self.always_fail = False  # 始终抛 ConnectionError（模拟不可达）
        self.reject_status: int | None = None  # 始终抛 CallbackRejectedError
        self.delay = 0.0  # 每次调用前睡眠秒数（模拟慢回调）
        self._lock = threading.Lock()

    def __call__(self, url: str, body: bytes, headers: dict[str, str]) -> None:
        if self.delay > 0:
            time.sleep(self.delay)
        with self._lock:
            self.calls.append((url, body, headers))
            if self.reject_status is not None:
                raise CallbackRejectedError(self.reject_status, "simulated rejection")
            if self.always_fail or self.failures_before_success > 0:
                if not self.always_fail:
                    self.failures_before_success -= 1
                raise ConnectionError("simulated delivery failure")

    def call_count(self) -> int:
        with self._lock:
            return len(self.calls)


@pytest.fixture()
def sender() -> FakeSender:
    return FakeSender()


@pytest.fixture()
def app(sender: FakeSender):
    return create_app(sender=sender)


@pytest.fixture()
def client(app) -> TestClient:
    return TestClient(app)


@pytest.fixture()
def dispatcher(app) -> EventDispatcher:
    return app.state.dispatcher


@pytest.fixture()
def seeded(client: TestClient) -> dict:
    """预置合作方、客户账户与有效同意，返回测试上下文。"""
    account = client.post(
        "/accounts", json={"customer_id": "cust-1", "balance_cents": 100_000}
    ).json()
    consent = client.post(
        "/consents",
        json={
            "partner_id": "partner-1",
            "customer_id": "cust-1",
            "scopes": ["accounts:read", "payments:write"],
            "ttl_seconds": 3600,
        },
    ).json()
    ctx = {
        "partner_id": "partner-1",
        "account_id": account["account_id"],
        "consent_id": consent["consent_id"],
        "headers": {
            "X-Partner-Id": "partner-1",
            "X-Consent-Id": consent["consent_id"],
        },
    }
    logger.info("seeded context: %s", ctx)
    return ctx
