# 设计说明：同意模型、幂等语义、事件投递与重试

本服务为演示性质的集成层：合作方（TPP）、同意（Consent）、账户、订阅均存放在
进程内的线程安全账本 `LocalStore`（一把 `RLock` 保护全部可变状态）。
模型定义见 `obgateway/models.py`，时间一律使用时区感知 UTC，金额使用最小货币
单位（分）的整数。

## 1. 同意与授权模型

### 1.1 数据模型

`Consent` 的关键字段：

- `partner_id`：同意归属的合作方。同意不可跨合作方使用。
- `customer_id`：授权客户。
- `scopes: frozenset[str]`：授权范围，如 `accounts.read`、`payments.create`。
- `account_ids: frozenset[str]`：**资源归属白名单**，同意只能访问白名单内账户。
- `status: ConsentStatus`：`PENDING / AUTHORISED / REVOKED / EXPIRED`。
- `expires_at`：有效期截止时刻（含），判定时刻 `now >= expires_at` 即过期。
  注意：即使状态字段因未跑对账仍停留在 `AUTHORISED`，授权判定也以有效期为准。
- `revoked_at / revoke_reason`：撤销信息。

### 1.2 统一判定链（`authz.authorize`）

**所有受保护入口都调用同一个函数**，判定顺序固定，从根本上保证不同入口对同一
请求结论一致：

1. 合作方存在且启用 → 否则 `PARTNER_NOT_FOUND` / `PARTNER_INACTIVE`
2. 同意存在 → 否则 `CONSENT_NOT_FOUND`
3. 同意归属当前合作方 → 否则 `CONSENT_PARTNER_MISMATCH`
4. 状态为 `REVOKED` → `CONSENT_REVOKED`（撤销优先）
5. 状态非 `AUTHORISED` → `CONSENT_STATUS_NOT_AUTHORISED`
6. 当前时刻已过有效期 → `CONSENT_EXPIRED`
7. 同意范围包含入口要求 scope → 否则 `SCOPE_INSUFFICIENT`
8. 目标账户在资源白名单内 → 否则 `RESOURCE_MISMATCH`

优先级设计：**撤销 > 过期 > 范围不足 > 资源不匹配**。撤销是客户最强的意图表达，
因此一个既撤销又过期的同意统一判为 `CONSENT_REVOKED`（有专门测试锁定）。

入口与 scope：

| 入口 | 要求 scope | 资源归属检查 |
| --- | --- | --- |
| `GET /accounts/{id}` | `accounts.read` | 路径账户必须在白名单 |
| `POST /payments` | `payments.create` | 付款账户必须在白名单 |
| `GET /consents/{id}` | `accounts.read`（合作方归属一致） | — |
| `POST /consents/{id}/revoke` | 不要求 scope（撤销不需要既有授权） | 合作方归属一致 |

拒绝响应统一为 HTTP 403：

```json
{"error": "authorization_denied", "category": "AUTHORIZATION",
 "reason": "CONSENT_EXPIRED", "message": "CONSENT_EXPIRED: 同意已于 ... 过期"}
```

撤销接口是幂等视图：重复撤销返回 `alreadyRevoked: true`，状态结论稳定。

## 2. 幂等与并发控制（`idempotency.py`）

### 2.1 键与内容

- 幂等键来自请求头 `Idempotency-Key`（支付必填）。
- “内容指纹”= 规范化 JSON（`sort_keys`、紧凑分隔）后的 SHA-256。
  字段顺序不同不影响指纹；任何值不同即指纹不同。

### 2.2 语义矩阵

| 场景 | 行为 |
| --- | --- |
| 新键 | 成为领导者，执行业务，保存 `(HTTP状态码, 响应体)`，通知等待者 |
| 同键 + 同内容（并发/串行） | 跟随者等待首次完成，**重放完全一致的结果**，业务不再执行 |
| 同键 + 不同内容 | **立即拒绝** `IDEMPOTENCY_PAYLOAD_MISMATCH`（HTTP 422），不等待、不覆盖 |
| 首次请求仍在执行且等待超时（仅显式注入超时） | `IDEMPOTENCY_KEY_IN_FLIGHT`（HTTP 409） |
| 首次业务抛异常 | 记录置 `FAILED` 并通知；同内容等待者中**恰好一个 CAS 晋升**重试，其余等待晋升者的结果 |

要点：

- 业务函数（扣款 + 事件发布）整体包在“首次执行”内，因此**事件也随扣款恰好发布
  一次**，不会因重放产生重复事件。
- 业务在幂等锁**之外**执行，只在状态翻转时取记录上的条件变量锁；账本扣款自身
  在 `LocalStore` 锁内原子完成（检查余额→扣减→落账），杜绝超额扣款。
- 业务级失败结果（如 400 `INSUFFICIENT_FUNDS`）也是“完成结果”，同样被重放，
  不会重复尝试扣款；只有抛异常的技术性失败才允许等待者晋升重试。

HTTP 层并发由测试 `test_http_concurrent_duplicate_payment_debits_once` 锁定：
6 个线程使用同一键同时发起支付，断言 6 个响应全部 201、paymentId 唯一、
支付记录只有 1 条、余额只减少一次。

## 3. 事件订阅、签名、投递与重试（`events.py`）

### 3.1 发布

支付业务成功后发布 `payment.completed`。发布时为**每个启用中的匹配订阅**生成一
条独立的 `EventRecord`（不同订阅彼此独立重试，互不影响）。停用订阅不生成记录；
订阅若在发布后、投递前被停用，则投递时判为 `SUBSCRIBER_INACTIVE` 进入死信。

### 3.2 投递报文与签名

请求体为规范化 JSON：

```json
{"data": {...业务载荷...}, "eventId": "evt-...", "eventType": "payment.completed",
 "timestamp": "2026-09-23T00:00:00+00:00"}
```

签名内容（注意点号分隔，时间戳与头一致）：

```
signingContent = eventId + "." + eventType + "." + timestamp + "." + bodyBytes
signature      = "sha256=" + HMAC_SHA256(subscription.secret, signingContent).hex()
```

通过头部下发：`X-OBG-Event-Id`、`X-OBG-Event-Type`、`X-OBG-Timestamp`、
`X-OBG-Signature`。订阅方用共享密钥按相同规则重组内容并验签
（`hmac.compare_digest` 常量时间比较）。**报文任何字节被篡改、签名被改写或密钥
不符，验签都返回 False**（`test_signature_roundtrip_and_tamper_detection`）。

### 3.3 至少一次与重试退避

后台单工作线程 + 最小堆调度，保证进程内投递顺序与退避时序；投递记录落账，
`start()` 时把所有非终态记录重新入队（进程重启后的至少一次）。

每次尝试的结果分类：

| 情况 | 分类 `DeliveryFailure` | 是否重试 |
| --- | --- | --- |
| 订阅已删除 / 已停用 | `SUBSCRIBER_NOT_FOUND` / `SUBSCRIBER_INACTIVE` | 否（配置性永久失败） |
| 订阅方返回 401/403（验签拒绝） | `SIGNATURE_REJECTED` | 否（同内容重试无意义） |
| 传输异常（拒连/超时等） | `TRANSPORT_ERROR` | 是 |
| 其它非 2xx（如 500/503） | `NON_SUCCESS_STATUS` | 是 |
| 重试达到 `maxAttempts`（默认 5，测试为 2~4） | 末次追加 `MAX_ATTEMPTS_EXHAUSTED` | 终态 `DEAD_LETTER` |

退避：`delay_n = min(baseDelay * 2^(n-1), maxDelay)`，可叠加随机抖动
（`baseDelay=0.5s, maxDelay=30s`；测试用 `0.01s`）。成功为 `DELIVERED`，
耗尽为 `DEAD_LETTER`。每次尝试（含 HTTP 状态码与原因）都写入 `history`，
通过 `GET /events?`（按合作方隔离）可完整审计。

> 至少一次意味着订阅方可能在极端情况下收到重复投递（例如投递成功但响应丢失后
> 重试）；订阅方应使用 `X-OBG-Event-Id` 做消费幂等。

### 3.4 三类失败不混淆

- 授权拒绝：同步 HTTP 403，`category=AUTHORIZATION`。
- 幂等冲突：同步 HTTP 409/422，`category=IDEMPOTENCY`。
- 投递失败：**异步**发生，同步支付响应不受影响；通过 `GET /events` 查看
  `status / attempts / lastFailureReason / history`，`category=DELIVERY` 的
  原因码与前两类枚举互不相交（有集合不相交测试锁定）。

## 4. 本地验证

```bash
uv sync
uv run pytest -s -q          # 41 个测试；-s 可看到关键输入与判定依据日志
uv run uvicorn main:app      # 启动后访问 /docs
```

关键场景对应测试：

| 场景 | 测试 |
| --- | --- |
| 并发重复提交只扣一次、结果一致 | `tests/test_idempotency.py::test_concurrent_same_key_executes_once`、`test_http_concurrent_duplicate_payment_debits_once` |
| 同键异负荷明确拒绝且不扣款 | `test_same_key_different_payload_rejected_without_side_effect`、`test_http_same_key_different_payload_is_422` |
| 同意过期 / 撤销 / 跨方 / 范围 / 归属 | `tests/test_authz.py` 与 `tests/test_api.py` |
| 签名篡改识别 | `tests/test_events.py::test_signature_roundtrip_and_tamper_detection` |
| 订阅方对真实投递验签 | `test_subscriber_can_verify_delivery_signature` |
| 投递失败重试、退避、死信 | `test_retry_with_backoff_then_success`、`test_max_attempts_dead_letter_with_distinguishable_reason`、`test_transport_error_is_distinguishable_and_retried` |
| 验签拒绝不重试 | `test_signature_rejected_no_retry` |
