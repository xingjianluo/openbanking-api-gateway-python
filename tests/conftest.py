from __future__ import annotations

import logging
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger("openbanking.tests")


class FakeSender:
    """可控投递发送器：记录每次投递，可注入慢、失败、拒绝等行为。"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes, dict[str, str]]] = []
        self.failures_before_success = 0  # 前 N 次抛连接错误
        self.always_fail = False
        self.delay = 0.0  # 每次调用阻塞时长（模拟慢回调）
        self.reject_status: int | None = None  # 模拟订阅方 4xx/5xx
        self.gate: threading.Event | None = None  # 置位前调用全部阻塞
        self._lock = threading.Lock()

    def __call__(self, url: str, body: bytes, headers: dict[str, str]) -> None:
        if self.gate is not None:
            self.gate.wait(timeout=5.0)
        if self.delay:
            time.sleep(self.delay)
        with self._lock:
            self.calls.append((url, body, headers))
            if self.reject_status is not None:
                from app.events import (
                    SubscriberRejected,
                    SubscriberServerError,
                )

                if 400 <= self.reject_status < 500:
                    raise SubscriberRejected(f"HTTP {self.reject_status}: nope")
                raise SubscriberServerError(f"HTTP {self.reject_status}: boom")
            if self.always_fail or self.failures_before_success > 0:
                self.failures_before_success -= 0 if self.always_fail else 1
                raise ConnectionError("simulated delivery failure")

    @property
    def call_count(self) -> int:
        with self._lock:
            return len(self.calls)


@pytest.fixture()
def sender() -> FakeSender:
    return FakeSender()


@pytest.fixture()
def client(sender: FakeSender) -> TestClient:
    # dispatcher 由 FastAPI lifespan 负责启动与停止
    app = create_app(sender=sender)
    with TestClient(app) as c:
        yield c


def wait_deliveries(client: TestClient, expected: int, timeout: float = 5.0) -> list[dict]:
    """轮询等待出现 expected 条投递记录。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = client.get("/deliveries").json()
        if len(rows) >= expected:
            return rows
        time.sleep(0.01)
    raise AssertionError(
        f"timed out waiting for {expected} deliveries, got "
        f"{client.get('/deliveries').json()}"
    )


def wait_terminal(client: TestClient, timeout: float = 5.0) -> list[dict]:
    """等待所有投递进入终态（delivered/failed/rejected/invalid_subscription）。"""
    deadline = time.time() + timeout
    terminal = {"delivered", "failed", "rejected", "invalid_subscription"}
    while time.time() < deadline:
        rows = client.get("/deliveries").json()
        if rows and all(r["status"] in terminal for r in rows):
            logger.info("all deliveries terminal: %s", rows)
            return rows
        time.sleep(0.01)
    raise AssertionError(f"deliveries not terminal: {client.get('/deliveries').json()}")


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
