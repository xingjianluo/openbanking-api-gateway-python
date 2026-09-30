"""本地存储：线程安全的内存账本与各类记录表。"""
from __future__ import annotations

import threading

from app.models import (
    Account,
    Consent,
    Delivery,
    Event,
    IdempotencyRecord,
    LedgerEntry,
    Subscription,
)


class Store:
    """集中式内存存储，所有表共享一把锁以保证并发下的一致性。"""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)
        self.consents: dict[str, Consent] = {}
        self.accounts: dict[str, Account] = {}
        self.ledger: list[LedgerEntry] = []
        self.idempotency: dict[str, IdempotencyRecord] = {}
        self.subscriptions: dict[str, Subscription] = {}
        self.events: dict[str, Event] = {}
        self.deliveries: dict[str, Delivery] = {}
