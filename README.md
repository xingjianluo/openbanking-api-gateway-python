# Open Banking API Gateway（Python / FastAPI）

面向合作方的开放银行接口集成层。用**本地内存账本与本地存储**模拟合作方、
同意授权与事件订阅，使授权校验、幂等处理与事件投递在并发和异常链路下
都能得到一致且可解释的结论。

## 结构

```
app/
  models.py       领域模型：Consent / Account / LedgerEntry / IdempotencyRecord / Subscription / Event / Delivery
  store.py        线程安全的内存存储（RLock + Condition）
  consent.py      同意授权校验（所有受保护入口共用一个判定函数）
  idempotency.py  幂等执行器（内容指纹 + 并发等待）
  events.py       后台异步事件分发器（至少一次、重试退避、HMAC 签名、失败重投）
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

  | reason                 | HTTP | 含义                             |
  |------------------------|------|----------------------------------|
  | `consent_not_found`    | 403  | 同意不存在或不属于该合作方       |
  | `consent_revoked`      | 403  | 同意已被撤销                     |
  | `consent_expired`      | 403  | 同意已过期                       |
  | `scope_insufficient`   | 403  | 授权范围不覆盖入口所需 scope     |
  | `resource_not_owned`   | 403  | 目标账户属于其他客户             |
  | `account_not_found`    | 404  | 目标账户不存在                   |

## 幂等语义

- 变更类请求（`POST /payments`）必须携带 `Idempotency-Key` 头，缺失返回
  400 `missing_idempotency_key`。
- 同一幂等键 + 相同内容（按规范化 JSON 的 SHA-256 指纹判定）：
  只执行一次业务动作，重复提交返回与首次完全一致的结果，响应头
  `X-Idempotent-Replay: true` 标识重放。
- 同一幂等键 + 不同内容：返回 409 `idempotency_conflict`，
  **绝不静默覆盖或重复扣减**。
- 并发下：首个到达的调用方登记 `IN_PROGRESS` 后执行业务动作；
  并发重复方在条件变量上等待，随后返回首次结果。业务动作抛异常时
  清除记录并唤醒等待方，允许修正后重试（因此被业务拒绝的请求不会
  占用幂等键）。

## 事件投递：后台异步、可观察、可补救

### 与对外请求解耦

- `POST /payments` 成功时只**登记** `Event` 与每个匹配订阅的
  `Delivery`（状态 `pending`），随后立即返回；响应体中
  `deliveries_created` 标明匹配到的投递目标数（0 表示无任何匹配订阅者）。
- 实际回调由后台 worker 线程（默认 2 个）按 FIFO 领取执行
  （`pending -> in_progress -> delivered/failed`）。合作方回调慢、超时
  或长时间不可达，都只影响后台投递进度，**不会拖慢对外接口**。
- 投递语义不倒退：
  - **不丢**：投递先落库（内存存储）再处理，积压以 `Delivery` 记录
    形式存在，worker 按自身节奏排空；
  - **可观察**：`GET /deliveries`（支持 `?status=` 过滤，如
    `?status=failed` / `?status=redelivered`）实时查看
    `status / attempts / last_error / failure_kind / redelivery_of`，
    重试退避与重投来源均可见；
  - **不重复成功**：`delivered` 是终态、不可重投；`failed` 记录一旦被
    一次成功的重投补救即转为终态 `redelivered`，不会被任何后续处理
    （含重复点击、并发重投）重复投递。

### 重试、最终失败与重投

- 每次投递最多 `max_attempts`（默认 3）次尝试，指数退避
  `backoff_base * 2^(n-1)`（默认 0.05s 起）。
- 重试耗尽后进入**最终失败状态** `failed`，保留总尝试次数 `attempts`、
  最后一次失败原因 `last_error` 与失败类别 `failure_kind`。
- 失败类别彼此可区分：

  | failure_kind           | 含义                                       |
  |------------------------|--------------------------------------------|
  | `callback_rejected`    | 订阅方应答了但返回 4xx/5xx                 |
  | `callback_unreachable` | 网络不可达 / 超时等传输失败                |
  | `subscription_invalid` | 订阅本身无效（已不存在或回调地址非法）     |

- **重投**：`POST /deliveries/{delivery_id}/redeliver` 仅接受 `failed`
  状态的投递，返回 201 并生成一条**全新的 `Delivery`**
  （`redelivery_of` 指向原记录），走一次完整、可独立追踪的投递过程。

  重投规则（允许与拒绝）：

  | 目标记录状态                         | 结果                                              |
  |--------------------------------------|---------------------------------------------------|
  | `failed` 且无在途/已成功的重投       | 201，登记一条新投递                               |
  | `failed` 但已有**在途**重投          | 409 `redeliver_conflict` / `redelivery_in_progress` |
  | `failed` 但已有过**成功**重投        | 409 `redeliver_conflict` / `already_redelivered`  |
  | `redelivered`（已被成功重投补救）    | 409 `redeliver_conflict` / `already_redelivered`  |
  | `pending` / `in_progress` / `delivered` | 409 `redeliver_not_allowed`                    |
  | 记录不存在                           | 404 `delivery_not_found`                          |

  - **一次成功，永久收口**：一条失败记录一旦被重投成功，原记录（以及同一
    根失败记录下之前重投失败过的兄弟尝试）会被标记为新终态
    `redelivered`，此后再对其中任何一条发起重投都返回**可区分的冲突错误**
    `redeliver_conflict`（`reason=already_redelivered`），订阅方不会收到
    第二次成功回调，也不会新增投递记录或任何其他业务副作用。
  - **并发重投最多生效一次**：判定与登记在同一把存储锁内原子完成，判定
    范围是**同一根失败记录的整棵重投树**——无论请求是对原失败记录发起，
    还是对某条重投失败后的子记录发起，只要树中已存在
    `pending/in_progress` 的在途重投，并发重复发起就返回 409
    `redeliver_conflict`（`reason=redelivery_in_progress`）。并发结果与
    一条条串行发起完全一致，不会因抢跑多造出一次成功投递。
  - **失败不锁死，仍可继续重投**：若新的一次重投最终**仍然失败**，新记录
    为 `failed`、原记录也保持 `failed`，仍然允许继续重投；直到某次重投
    成功，整条链才收敛为 `redelivered`。
  - **状态可追踪**：`GET /deliveries` 可看到每条记录当前状态
    （`failed` / `redelivered` / `delivered` 等）与 `redelivery_of`
    来源；被收口的原记录保留 `attempts / last_error / failure_kind`
    历史失败痕迹，只改状态，不丢排查信息。
  - 重投只重发事件回调，不触碰账本，同一笔支付不会被重复扣款。

### 并发与容量下的预期边界

- 对外接口的响应时间与回调耗时**无关**：支付路径上只有常数时间的
  记录登记，回调再慢、积压再多，接口延迟都有稳定上限（由测试
  `test_slow_callback_does_not_slow_payment_api` 与
  `test_burst_backlog_drains_without_blocking_api` 守护）。
- 排空速率 ≈ `workers / (单次回调耗时 + 退避)`。默认 2 个 worker；
  回调 0.2s 时约 10 笔/秒。短时间涌入超过排空速率的事件时，积压以
  内存中的 `Delivery` 记录形式增长（本集成层为本地模拟，未设积压上限；
  生产化时应替换为有界持久队列并加背压）。
- 投递为**至少一次**语义：回调在 HTTP 层成功但响应丢失等极端情况下，
  订阅方需凭 `event_id` 自行去重。

## 业务拒绝（明确、可区分、无副作用）

以下请求得到明确的业务拒绝而非服务端错误，且**不改动账本、同意或
投递状态**：

| 场景                     | 响应                                        |
|--------------------------|---------------------------------------------|
| 余额不足                 | 422 `insufficient_funds`                    |
| 非正金额                 | 422（参数校验）                             |
| 账户不存在               | 404 `account_not_found`                     |
| 撤销不存在的同意         | 404 `consent_not_found`                     |
| 无效订阅（空事件类型 / 非法回调地址 / 空密钥） | 400 `invalid_subscription` |

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest -q --log-cli-level=INFO   # 运行测试并查看判定依据日志
uv run uvicorn app.main:app --reload    # 启动服务
```

手工冒烟示例：

```bash
# 预置账户与同意
curl -s -X POST localhost:8000/accounts -H 'content-type: application/json' \
  -d '{"customer_id":"cust-1","balance_cents":100000}'
curl -s -X POST localhost:8000/consents -H 'content-type: application/json' \
  -d '{"partner_id":"partner-1","customer_id":"cust-1","scopes":["accounts:read","payments:write"],"ttl_seconds":3600}'

# 登记订阅（回调地址指向一个不可达地址即可观察重试与最终失败）
curl -s -X POST localhost:8000/subscriptions -H 'content-type: application/json' \
  -d '{"partner_id":"partner-1","event_types":["payment.completed"],"callback_url":"http://localhost:9/hook","secret":"s3cret"}'

# 幂等支付（重复执行同一命令，余额只扣一次；接口立即返回，不等回调）
curl -s -X POST localhost:8000/payments \
  -H 'X-Partner-Id: partner-1' -H 'X-Consent-Id: <consent_id>' \
  -H 'Idempotency-Key: pay-1' -H 'content-type: application/json' \
  -d '{"account_id":"<account_id>","amount_cents":500}'

# 观察后台投递：pending/in_progress -> failed（重试耗尽，含 attempts/last_error/failure_kind）
curl -s 'localhost:8000/deliveries?status=failed'

# 对最终失败的投递发起重投（生成新的可追踪投递记录）
curl -s -X POST localhost:8000/deliveries/<delivery_id>/redeliver

# 重投成功后，原记录变为 redelivered（保留失败痕迹），可对比观察
curl -s 'localhost:8000/deliveries'

# 对同一条原始失败记录再点一次重投：409 redeliver_conflict / already_redelivered，
# 订阅方不会收到第二次成功回调
curl -s -i -X POST localhost:8000/deliveries/<delivery_id>/redeliver
```

并发重投"最多生效一次"可在本地用并发请求复现（下面 10 个请求中应恰好
1 个 201，其余 9 个为 409 `redeliver_conflict`，订阅方只被成功回调一次）：

```bash
# 先制造一条 failed 记录（不可达回调 + 等待重试耗尽），再并发重投
for i in $(seq 1 10); do
  curl -s -o /dev/null -w '%{http_code}\n' \
    -X POST localhost:8000/deliveries/<failed_delivery_id>/redeliver &
done; wait
# 结果应只有一行 201；随后再串行重投一次仍为 409 / already_redelivered
```

> 自动化复现无需真实网络：`tests/conftest.py` 的 `FakeSender` 可注入
> 持续失败、成功、慢回调；对应场景由
> `tests/test_redelivery_guard.py` 守护（并发用线程屏障对齐发出时机，
> 可稳定复现）。

关键场景对应的自动化测试：

| 场景                                       | 测试 |
|--------------------------------------------|------|
| 慢回调不拖慢支付接口                       | `tests/test_delivery_async.py::test_slow_callback_does_not_slow_payment_api` |
| 大量事件积压下接口有响应上限且最终全部送达 | `tests/test_delivery_async.py::test_burst_backlog_drains_without_blocking_api` |
| 重试耗尽后重投，不重复扣款、不重复成功投递 | `tests/test_delivery_async.py::test_redeliver_after_exhaustion_no_duplicate_effects` |
| 在途投递不允许重投                         | `tests/test_delivery_async.py::test_redeliver_in_flight_delivery_rejected` |
| 同一失败记录连续两次重投：第二次被拒且订阅方只成功一次 | `tests/test_redelivery_guard.py::test_second_redeliver_after_success_is_conflict` |
| 并发重复重投最多生效一次（HTTP 层）        | `tests/test_redelivery_guard.py::test_concurrent_redeliver_only_one_takes_effect` |
| 并发重复重投最多生效一次（dispatcher 层，在途冲突可区分） | `tests/test_redelivery_guard.py::test_dispatcher_level_concurrent_redeliver_raises_distinct_conflict` |
| 跨链并发重投（分别对原记录与失败子记录重投）只生效一次 | `tests/test_redelivery_guard.py::test_concurrent_redeliver_of_original_and_failed_child_only_one_takes_effect` |
| 重投仍失败可继续重投、成功后全链收口、状态可追踪 | `tests/test_redelivery_guard.py::test_failed_redelivery_keeps_record_retryable_until_success` |
| 四类失败结论可区分                         | `tests/test_delivery_async.py::test_failure_kinds_are_distinguishable` |
| 边界拒绝无副作用                           | `tests/test_delivery_async.py::test_rejected_boundary_requests_have_no_side_effects` |
| 签名与篡改识别、重试退避、失败原因分离     | `tests/test_events.py` |
| 授权校验                                   | `tests/test_consent.py` |
| 幂等与并发控制                             | `tests/test_idempotency.py` |
