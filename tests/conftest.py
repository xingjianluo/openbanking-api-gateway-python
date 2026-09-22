"""测试夹具与日志配置。

要求：单测日志打印关键输入与判定依据。这里把 obgateway 各模块的日志提到 INFO，
并在关键用例中显式 print 输入/预期/实际原因，便于失败时一眼定位。
"""
from __future__ import annotations

import logging
import sys

import pytest
from fastapi.testclient import TestClient

from obgateway.app import create_app
from obgateway.events import TransportResponse

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logging.getLogger("obgateway").setLevel(logging.INFO)


class FakeTransport:
    """测试用传输层：按调用序列返回预设结果/抛异常，并记录每次投递。"""

    def __init__(self, script=None, *, always: int = 200, exc: Exception | None = None,
                 record_bodies: bool = True) -> None:
        self.script = list(script or [])
        self.always = always
        self.exc = exc
        self.record_bodies = record_bodies
        self.calls = 0
        self.deliveries: list[dict] = []

    def post(self, url: str, headers: dict[str, str], body: bytes) -> TransportResponse:
        idx = self.calls
        self.calls += 1
        if self.record_bodies:
            self.deliveries.append({"url": url, "headers": dict(headers), "body": bytes(body)})
        if self.exc is not None:
            raise self.exc
        if self.script:
            outcome = self.script[min(idx, len(self.script) - 1)]
            if isinstance(outcome, Exception):
                raise outcome
            return TransportResponse(outcome)
        return TransportResponse(self.always)


@pytest.fixture()
def transport():
    return FakeTransport()


@pytest.fixture()
def app(transport):
    application = create_app(
        transport=transport,
        dispatcher_kwargs={"base_delay": 0.01, "jitter": 0.0, "default_max_attempts": 3},
    )
    return application


@pytest.fixture()
def client(app):
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def container(app):
    return app.state.container


@pytest.fixture()
def ok_headers():
    return {"X-Partner-Id": "partner-a", "X-Consent-Id": "consent-ok"}
