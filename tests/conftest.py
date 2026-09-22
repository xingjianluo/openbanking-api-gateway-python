from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger("openbanking.tests")


class FakeSender:
    """可控投递发送器：记录每次投递，可按需注入失败。"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes, dict[str, str]]] = []
        self.failures_before_success = 0  # 前 N 次抛异常
        self.always_fail = False

    def __call__(self, url: str, body: bytes, headers: dict[str, str]) -> None:
        self.calls.append((url, body, headers))
        if self.always_fail or self.failures_before_success > 0:
            self.failures_before_success -= 0 if self.always_fail else 1
            raise ConnectionError("simulated delivery failure")


@pytest.fixture()
def sender() -> FakeSender:
    return FakeSender()


@pytest.fixture()
def client(sender: FakeSender) -> TestClient:
    app = create_app(sender=sender)
    return TestClient(app)


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
