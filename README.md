# distributed-token-bucket

Python 分布式令牌桶限流库，解决 API 网关扩容后的两个典型问题：

1. **每节点各持一份本地桶，集群限额随节点数翻倍** —— 本库以共享桶为
   唯一权威，节点只持有共享桶「预留」出来的配额租约，令牌不会被
   多个节点重复计数。
2. **同步频率降低 + 节点时钟差，导致同一时间窗被重复使用** —— 租约带
   TTL 与 fencing epoch，节点按漂移安全边距提前放弃租约，存储端时钟
   负责最终回收，旧 incarnation 永远无法复用同一时间窗的配额。

## 架构

```
                 ┌──────────────────────────────┐
                 │  SharedTokenBucket（控制面）  │
                 │  容量 C / 补充速率 R          │
                 │  租约表 + TTL + fencing epoch │
                 └──────────────┬───────────────┘
            acquire/renew/release（带异常：分区/fenced/过期/无配额）
        ┌───────────────┬───────┴────────┬───────────────┐
        ▼               ▼                ▼               ▼
   RateLimiterNode  RateLimiterNode  RateLimiterNode  RateLimiterNode
   租约配额（正常）  租约配额（正常）  SafeBucket（降级） SafeBucket（降级）
```

- `ratelimit/store.py`：`SharedTokenBucket`，共享桶权威状态。申请即预留
  （令牌立刻从桶中扣除），续租时申报真实用量、永久入账，未用预留退回；
  TTL 过期由存储端单调时钟回收；epoch 实现 fencing，支持强制抢占。
- `ratelimit/node.py`：`RateLimiterNode` 与 `SafeBucket`。节点只在持有
  有效租约时按租约配额放行；续租/申请失败立即降级到本地安全桶。
- `ratelimit/clock.py`：`Clock`/`RealClock`/`MockClock`。生产用
  `time.monotonic_ns`，测试可注入时钟频率偏差与零点差。

## 快速开始

```python
from ratelimit import RealClock, RateLimiterNode, SharedTokenBucket

store = SharedTokenBucket(capacity=100, refill_rate=100, clock=RealClock())
node = RateLimiterNode(
    node_id="gateway-1",
    store=store,
    lease_quota=50,      # 每个租约周期预留 50 令牌
    lease_ttl=2.0,       # 租约 2 秒
    safe_rate=12.5,      # 降级速率：R * safe_fraction / N
    safe_capacity=5.0,   # 降级突发容量：总和不超过全局容量
    drift_margin=0.1,    # 时钟漂移安全边距 10%
)

decision = node.allow()
if decision.allowed:
    ...  # 放行，decision.mode 为 "lease" 或 "safe"
```

## 一致性等级

本库提供 **CP 偏向的强配额一致性（全局上限不被打穿）**，具体保证：

- **无重复计数**：配额以预留模型管理，同一批令牌同一时刻只能属于一个
  租约；节点扩容不会放大全局限额（测试见
  `test_scaling_node_count_does_not_multiply_limit`）。
- **fencing 防双花**：每个节点维护单调递增 epoch，重启/抢占后旧 epoch
  的续租与释放一律以 `LeaseFenced` 拒绝；已确认消耗永久入账，换新
  epoch 也不会让旧时间窗配额复活。
- **存储端 TTL 权威**：租约过期判定只使用存储端单调时钟，节点时钟
  快慢不影响回收正确性；节点侧用 `drift_margin` 保证「节点先放弃、
  存储后回收」，不存在节点拿着已过期租约继续放行的窗口。
- **单调时钟语义**：跨节点只比较单调时钟的时长，不比较绝对值，因此
  单调时钟零点差与墙上时间跳变（NTP 步进）不影响限流判断。

严格上界（长度为 `T` 的任意窗口）：

```
放行量 ≤ 容量 C + 速率 R * T + R * ttl
```

多出的 `R * ttl` 是租约模型固有的「一个 TTL 在途预留」上界：节点在
租约到期前确认的消耗，可能对应窗口末端最后一个补充周期的预留量。
零时长突发上界为 `C + Σ 安全桶初始容量`。

## 降级边界

节点在以下任一情况进入 `safe` 模式，且 **绝不继续按全速放行**：

- 续租时收到 `LeaseExpired` / `LeaseFenced` / `NodePartitioned` /
  `QuotaUnavailable`；
- 本地有效期（`ttl * (1 - drift_margin)`）已到，节点主动放弃租约；
- 启动期无法从控制面申请到租约（按短冷却重试）。

降级模式的安全约束：

- 每个节点的安全速率 `safe_rate = R * safe_fraction / N`，因此
  `Σ safe_rate ≤ R`，即使所有节点同时分区，全局限额也不会被打穿；
- 安全桶初始容量按集群规模切分，`Σ safe_capacity ≤ C`；
- 持有有效租约但本周期配额耗尽时直接拒绝，**不挪用**安全桶，避免
  正常路径上绕过共享桶预算；
- 分区恢复/隔离解除后，节点在下一次请求自动重新申请租约并自愈。

代价是可用性取舍：控制面不可达时集群总吞吐被限制在
`R * safe_fraction`（默认 50%），以确定性的保守容量换取不超卖。

## 时钟漂移配置

`drift_margin` 必须覆盖节点时钟相对存储端的最大频率偏差。例如节点
时钟最快走快 5% 时，边距取 10%：本地有效期 = `ttl * 0.9`，即使时钟
以 1.05 倍速运行，节点感知的到期时刻仍早于存储端真实 TTL。

## 测试

```bash
python -m unittest
```

32 个测试覆盖：

- 突发流量、持续流量、节点扩容不放大限额、误拒率；
- 时钟走快/走慢、零点差、墙上时间跳变、漂移边距时序；
- 租约抢占、fencing epoch、过期回收、续租结算与超报拒绝；
- 网络分区（单节点/半数/全体）、降级安全速率与自愈；
- 节点优雅重启与硬崩溃（TTL 回收 + epoch 防复用）；
- 全局令牌守恒不变量（桶内 + 在外预留 + 已消耗 ≤ 初始容量 + 累计补充）。
