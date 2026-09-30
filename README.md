# Open Banking API Gateway（Python / FastAPI）

面向合作方的开放银行接口集成层。用**本地内存账本与本地存储**模拟合作方、
同意授权与事件订阅，使授权校验、幂等处理与事件投递在并发和异常链路下
都能得到一致且可解释的结论。

## 结构

```
app/
  models.py       领域模型：Consent / Account / LedgerEntry / IdempotencyRecord
                  / Subscription / Event / Delivery（含失败分类与重投链）
  store.py        线程安全的内存存储（RLock + Condition）
  errors.py       业务错误：余额不足、资源不存在、订阅无效、重投拒绝等
  consent.py      同意授权校验（所有受保护入口共用一个判定函数）
  idempotency.py  幂等执行器（内容指纹 + 并发等待）
  events.py       后台投递器：有界队列 + worker 池 + 扫描兜底 + 退避重试 + HMAC 签名
  main.py         FastAPI 入口与路由
tests/            pytest 测试（日志打印关键输入与判定依据）
```

## 同意模型

- `Consent` 属于某个合作方（`partner_id`）与某个客户（`customer_id`），
  携带授权范围集合 `scopes` 与过期时间 `expires_at`，状态为
  `active / revoked / expired`。
- 受保护入口通过请求头 `X-Partner-Id` 与 `X-Consent-Id` 定位同意，
  所有入口共用 `app.consent.check_consent`，判定顺序固定，因此同一请求
  在不同入口得到相同结论。
- 拒绝理由可区分（body 中 `reason` 字段）：

  | HTTP | reason                 | 含义                             |
  |------|------------------------|----------------------------------|
  | 403  | `consent_not_found`    | 同意不存在或不属于该合作方       |
  | 403  | `consent_revoked`      | 同意已被撤销                     |
  | 403  | `consent_expired`      | 同意已过期                       |
  | 403  | `scope_insufficient`   | 授权范围不覆盖入口所需 scope     |
  | 403  | `resource_not_owned`   | 目标账户不属于该同意对应的客户   |
  | 404  | `resource_not_found`   | 目标账户不存在/已失效            |

## 幂等语义

- 变更类请求（`POST /payments`）必须携带 `Idempotency-Key` 头，缺失返回
  400 `missing_idempotency_key`。
- 同一幂等键 + 相同内容（按规范化 JSON 的 SHA-256 指纹判定）：
  只执行一次业务动作，重复提交返回与首次完全一致的结果，响应头
  `X-Idempotent-Replay: true` 标识重放，**重放不会再次发布事件**。
- 同一幂等键 + 不同内容：返回 409 `idempotency_conflict`，
  **绝不静默覆盖或重复扣减**。
- 并发下：首个到达的调用方登记 `IN_PROGRESS` 后执行业务动作；
  并发重复方在条件变量上等待，随后返回首次结果。业务动作抛异常时
  清除记录并唤醒等待方，允许修正后重试（包括用同一个键重试）。
- 业务校验（金额为正、账户存在、余额充足）发生在登记幂等记录与账本
  之前；被拒绝的请求不会改动账本、同意、幂等表或投递状态。

## 业务拒绝（不再 500 或静默放行）

| 场景 | HTTP | error 码 |
|------|------|----------|
| 余额不足（含并发下的双重检查） | 402 | `insufficient_funds` |
| 金额 ≤ 0 | 400 | `invalid_amount` |
| 支付/查询不存在的账户 | 404 | `account_not_found` / `resource_not_found` |
| 撤销不存在的同意 | 404 | `consent_not_found` |
| 无效订阅请求（空合作方/空事件类型/空密钥/非 http(s) 回调） | 400 | `invalid_subscription` |
| 对不可重投的投递发起重投 | 409 | `redrive_not_allowed` |
| 投递/事件不存在 | 404 | `delivery_not_found` |

## 事件投递：与对外请求解耦

支付成功后只做两件**本地**操作：落库 `Event` 与每条订阅一条
`Delivery(pending)`，并入一个**有界队列**；真正的 HTTP 回调由后台
**worker 池**完成。因此：

- 合作方回调快、慢、超时或长时间不可达，都只影响后台投递，
  **不会明显拖慢 `/payments` 等对外接口**（后台实现见
  `app.events.EventDispatcher`）。
- 短时间大量事件涌入时，队列长度与 worker 数有界，接口响应时间存在
  上限（入队等待默认最多 `enqueue_timeout`，见下方参数）。
- **事件不丢**：入队时若队列已满，记录保持 `pending` 留在本地存储；
  一个**扫描线程**周期性拾取滞留的 `pending` 以及退避到期的
  `retrying` 记录重新入队。进程内内存存储的边界见下节。

### 投递状态机

```
pending ──worker 原子领取─▶ in_flight ──尝试成功──────────▶ delivered（终态）
   ▲                           │
   │（队列满滞留，扫描器拾取）  ├─尝试失败、仍可重试─▶ retrying
   │                           │                        │退避到期，扫描器重新入队
   └───────────────────────────┴────再次领取────────────┘
                               ├─达到 max_attempts──────▶ failed（终态，可重投）
                               ├─订阅方 4xx ─────────────▶ rejected（终态，不重试）
                               └─订阅无效（回调非法等）──▶ invalid_subscription（终态）
```

- **原子领取，杜绝并发重复投递**：worker 从队列取到任务后，在存储锁内
  把状态从 `pending/retrying` 翻转为 `in_flight`（瞬时态）；只有翻转成功
  的 worker 才会真正发起 HTTP 调用。即使任务因任何原因在队列中重复出现，
  第二个 worker 也领取不到，因此同一事件不会被成功投递两次。
- **租约回收（至少一次）**：`in_flight` 带 `lease_expires_at`
  （默认 60s）。若 worker 崩溃/长时间卡死，扫描器会在租约过期后回收并重投，
  这一极端情况可能产生重复投递，由订阅方按正文中稳定的 `event_id` 去重。
- **重试与退避**：默认最多 3 次尝试，指数退避
  `backoff_base * 2^(attempt-1)`（默认 0.05s 起：0.05s、0.10s）。
  退避由扫描线程按 `next_attempt_at` 到期重新调度，worker 不被睡眠占满。
- **不重复**：`delivered` 是唯一成功终态，worker 与扫描器都不会再拾取
  它，成功路径也不会被迟到的重复结果覆盖；支付本身仍受幂等键保护，
  因此重投/重放都不会重复扣款。
- **失败分类可区分**（`failure_kind` + 状态）：

  | 情形 | status | failure_kind |
  |------|--------|--------------|
  | 订阅方拒绝/返回 4xx | `rejected` | `subscriber_rejected` |
  | 网络不可达/超时（重试耗尽） | `failed` | `network_unreachable` |
  | 订阅无效（非法回调、订阅已消失） | `invalid_subscription` | `invalid_subscription` |
  | 订阅方 5xx | 可重试，耗尽后 `failed` | `server_error` |
  | 没有任何匹配订阅者 | 事件级 `outcome=no_subscriber`，无投递记录 | — |

### 失败归宿与重投（补救入口）

- 重试耗尽的投递进入 `failed`，可通过 `GET /deliveries?status=failed`
  查询，看到总尝试次数 `attempts`、`last_error`（含异常类型与信息）、
  `failure_kind`、时间戳。
- 发起重投：`POST /deliveries/{delivery_id}/redrive` → **202**，
  返回全新的 `redelivery_id`。新记录是一次独立、可追踪的投递过程：
  `attempts` 从 1 重新计数，并用 `origin_delivery_id` 指向最初失败记录；
  原失败记录保持 `failed` 不变。
- 重投的是**事件通知**，不是支付：账本、余额、幂等结果都不会再变。
- 只允许对 `failed` 发起重投；对 `delivered/rejected/...` 重投返回 409。
  一旦某条失败记录已经有**重投成功的后继**，再次重投同样返回 409，
  保证同一订阅方对同一事件不会收到两次成功投递。
- 事件级汇总：`GET /events/{event_id}/deliveries` 返回
  `outcome ∈ delivered / partial / failed / rejected / invalid_subscription / pending / no_subscriber`。

### 签名

投递头 `X-Signature` 为对规范化正文（含稳定 `event_id`）的
HMAC-SHA256（hex），密钥为订阅时登记的 `secret`。订阅方用
`app.events.verify_signature(secret, body, signature)` 校验；
正文被篡改或密钥不符时校验失败。订阅方可用 `event_id` 做去重。

### 并发与容量的预期边界

- 默认参数：`worker_count=4`、`queue_maxsize=256`、
  `enqueue_timeout=0.05s`、`sweep_interval=0.02s`、`max_attempts=3`、
  `lease_timeout=60s`，均可通过 `create_app(...)` 覆盖
  （测试中即以此构造极端场景）。后台线程由 FastAPI lifespan 统一启停，
  导入模块本身不会启动常驻线程。
- 入队是投递链路中唯一可能短暂等待对外请求线程的环节，等待上限为
  `enqueue_timeout`；超时后事件**不丢**，只是转为由扫描线程延迟拾取。
- 当生产速度持续超过投递+重试的消化速度时，积压受队列容量约束，
  超限事件在本地存储中等待扫描，而不是在请求线程里堆积线程/连接；
  代价是投递延迟增大（背压），对外接口延迟仍有界。
- 本实现使用**进程内内存存储与内存队列**：语义在单进程内严格成立，
  重启会丢失未落盘的数据。生产部署应把 Store/队列换成持久化实现
  （如数据库 outbox + 持久队列），状态机与接口语义保持不变。

## 接口一览

| 方法/路径 | 说明 |
|-----------|------|
| `POST /consents` / `POST /consents/{id}/revoke` | 创建/撤销同意 |
| `POST /accounts` / `GET /accounts/{id}` | 开户/查询余额（需授权） |
| `POST /subscriptions` / `GET /subscriptions/{id}` | 订阅管理（创建时严格校验） |
| `POST /payments` | 幂等支付（需授权 + `Idempotency-Key`），成功后后台投递事件 |
| `GET /deliveries[?status=...]` | 投递列表（状态/次数/原因/分类/重投链） |
| `GET /deliveries/{id}` | 单条投递详情 |
| `POST /deliveries/{id}/redrive` | 对最终失败投递发起重投（202，返回新记录 id） |
| `GET /events/{event_id}/deliveries` | 事件级投递汇总（含 `no_subscriber`） |

## 本地验证

```bash
uv sync                                  # 安装依赖
uv run pytest -q                         # 运行全部单测
uv run pytest -q --log-cli-level=INFO    # 查看关键输入与判定依据日志
uv run uvicorn app.main:app --reload     # 启动服务
uv run ruff check app/ tests/            # 静态检查
```

重点场景对应的自动化用例：

| 场景 | 用例 |
|------|------|
| 回调被阻塞 5s，支付接口毫秒级返回；放行后事件送达不丢 | `tests/test_async_delivery.py::test_slow_callback_does_not_block_payment_api` |
| 60 个事件瞬间涌入，接口总耗时有界且全部送达 | `tests/test_async_delivery.py::test_burst_of_events_is_bounded_and_all_events_eventually_delivered` |
| 队列容量=2 时溢出，扫描线程兜底零丢失 | `tests/test_async_delivery.py::test_queue_overflow_is_recovered_by_sweeper_no_event_lost` |
| 8 worker + 100 事件 + 大量重试，成功 HTTP 次数恰好=事件数（不重复） | `tests/test_async_delivery.py::test_no_duplicate_delivery_under_high_concurrency` |
| 重试耗尽→重投成功、生成可追踪新记录、不可二次成功重投 | `tests/test_events.py::test_redrive_failed_delivery_creates_new_trackable_attempt` |
| 重投不产生第二次扣款 | `tests/test_events.py::test_redrive_success_does_not_duplicate_ledger` |
| 4xx/不可达/5xx/无效订阅/无订阅者五类结论可区分 | `tests/test_async_delivery.py` |
| 余额不足/非法金额/缺失资源/无效订阅被拒且零副作用 | `tests/test_business_guardrails.py` |
| 授权、幂等、签名原有用例不回归 | `tests/test_consent.py` `tests/test_idempotency.py` `tests/test_events.py` |

手工冒烟示例：

```bash
# 预置账户与同意
curl -s -X POST localhost:8000/accounts -H 'content-type: application/json' \
  -d '{"customer_id":"cust-1","balance_cents":100000}'
curl -s -X POST localhost:8000/consents -H 'content-type: application/json' \
  -d '{"partner_id":"partner-1","customer_id":"cust-1","scopes":["accounts:read","payments:write"],"ttl_seconds":3600}'

# 注册订阅（可用任意能记录请求的 HTTP 端点观察 X-Signature）
curl -s -X POST localhost:8000/subscriptions -H 'content-type: application/json' \
  -d '{"partner_id":"partner-1","event_types":["payment.completed"],"callback_url":"http://127.0.0.1:9000/hook","secret":"topsecret"}'

# 幂等支付（重复执行同一命令，余额只扣一次；回调速度不影响本响应）
curl -s -X POST localhost:8000/payments \
  -H 'X-Partner-Id: partner-1' -H 'X-Consent-Id: <consent_id>' \
  -H 'Idempotency-Key: pay-1' -H 'content-type: application/json' \
  -d '{"account_id":"<account_id>","amount_cents":500}'

# 观察后台投递状态、次数、失败原因
curl -s localhost:8000/deliveries

# 对最终失败的投递发起重投（拿到新的 redelivery_id 后可再查状态）
curl -s -X POST localhost:8000/deliveries/<failed_delivery_id>/redrive

# 余额不足被明确拒绝（402），账本不变
curl -s -i -X POST localhost:8000/payments \
  -H 'X-Partner-Id: partner-1' -H 'X-Consent-Id: <consent_id>' \
  -H 'Idempotency-Key: pay-2' -H 'content-type: application/json' \
  -d '{"account_id":"<account_id>","amount_cents":999999}'
```
