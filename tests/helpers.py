"""测试公共工具：集群构造、仿真驱动与断言。"""

from typing import List, Optional, Sequence

from ratelimit import MockClock, RateLimiterNode, SharedTokenBucket


def make_cluster(
    n: int,
    capacity: float,
    refill_rate: float,
    ttl: float = 2.0,
    lease_quota: Optional[float] = None,
    safe_fraction: float = 0.5,
    clock_rates: Optional[Sequence[float]] = None,
    drift_margin: float = 0.1,
) -> "Cluster":
    """构造一个 N 节点集群。

    安全配额采用保守切分：每个节点的安全速率为
    ``refill_rate * safe_fraction / n``，保证所有节点同时降级时，
    安全速率总和 = ``refill_rate * safe_fraction`` 不超过全局补充速率。
    """
    store_clock = MockClock()
    store = SharedTokenBucket(capacity, refill_rate, clock=store_clock)
    # 单节点租约只持有一个 TTL 周期内的公平份额，避免少数节点把
    # 整个桶的令牌长期锁死，降低续租周期之间的利用率抖动
    quota = (
        lease_quota
        if lease_quota is not None
        else min(capacity, refill_rate * ttl / n)
    )
    safe_rate = refill_rate * safe_fraction / n
    # 所有节点安全桶初始容量之和必须落在全局突发预算内（这里取
    # 全局容量的 10%），否则全体同时降级的瞬间会打穿全局上限
    safe_cap = min(safe_rate * ttl, capacity * 0.1 / n)

    nodes: List[RateLimiterNode] = []
    for i in range(n):
        rate = clock_rates[i] if clock_rates else 1.0
        node_clock = MockClock(rate=rate, mono_offset=i * 137.0)
        node = RateLimiterNode(
            node_id=f"node-{i}",
            store=store,
            lease_quota=quota,
            lease_ttl=ttl,
            safe_rate=safe_rate,
            safe_capacity=safe_cap,
            clock=node_clock,
            drift_margin=drift_margin,
        )
        nodes.append(node)
    return Cluster(store, store_clock, nodes)


class Cluster:
    def __init__(
        self,
        store: SharedTokenBucket,
        store_clock: MockClock,
        nodes: List[RateLimiterNode],
    ) -> None:
        self.store = store
        self.store_clock = store_clock
        self.nodes = nodes
        self.time = 0.0

    def advance(self, seconds: float) -> None:
        """以共享基准推进所有 MockClock（节点各自按 rate 漂移）。"""
        self.store_clock.advance(seconds)
        for node in self.nodes:
            node.clock.advance(seconds)  # type: ignore[attr-defined]
        self.time += seconds

    def run(self, duration: float, step: float, per_step_requests: int = 1):
        """按步长推进，每步每个节点发 ``per_step_requests`` 个请求。"""
        steps = int(round(duration / step))
        for _ in range(steps):
            for _ in range(per_step_requests):
                for node in self.nodes:
                    node.allow()
            self.advance(step)

    def total_allowed(self) -> int:
        return sum(node.allowed_total for node in self.nodes)

    def total_rejected(self) -> int:
        return sum(node.rejected_total for node in self.nodes)

    def total_requests(self) -> int:
        return self.total_allowed() + self.total_rejected()

    def false_reject_rate(self) -> float:
        total = self.total_requests()
        if total == 0:
            return 0.0
        return self.total_rejected() / total

    def safe_capacity_total(self) -> float:
        """所有节点本地安全桶初始容量之和（降级突发预算）。"""
        return sum(node.safe_bucket.capacity for node in self.nodes)

    def ideal_bound(self, window_seconds: float) -> float:
        """租约模型下长度为 window_seconds 的窗口的严格放行上界。

        等于标准令牌桶上界（容量 + 速率*时长），再加一个 TTL 窗口的
        在途预留：节点在 TTL 到期前确认的消耗，可能对应窗口结束前
        最后一个补充周期的预留量。
        """
        ttl = self.nodes[0].lease_ttl if self.nodes else 0.0
        return (
            self.store.capacity
            + self.store.refill_rate * window_seconds
            + self.store.refill_rate * ttl
        )

    def burst_bound(self) -> float:
        """零时长突发上界：全局桶容量 + 全体安全桶初始容量。"""
        return self.store.capacity + self.safe_capacity_total()

    def assert_within_global_cap(
        self,
        window_seconds: float,
        slack: float,
        msg: str = "",
    ) -> None:
        allowed = self.total_allowed()
        bound = self.ideal_bound(window_seconds)
        assert allowed <= bound + slack, (
            f"{msg} 全局上限被打穿：放行 {allowed} > "
            f"理论上限 {bound:.2f} + 容差 {slack}"
        )

    def assert_consumed_matches_allowed(self) -> None:
        """存储端确认消耗量与节点租约放行量对账一致（无凭空令牌）。

        节点在本周期内、尚未续租结算的放行量属于「在途消耗」，
        因此允许差值等于各节点未结算的本地用量。
        """
        lease_allowed = sum(
            node.lease_allowed_total for node in self.nodes
        )
        # consumed_total 是跨 incarnation 的累计结算值（租约删除/抢占
        # 后仍保留），与节点侧累计租约放行的差值即未结算的在途量。
        settled_by_store = self.store.consumed_total
        in_flight = lease_allowed - settled_by_store
        assert in_flight >= -1e-9, (
            f"存储端消耗 {self.store.consumed_total} 与租约放行 "
            f"{lease_allowed} 不一致（在途 {in_flight}）"
        )
        # 在途量必须能被所有未过期租约的剩余预留完全覆盖
        assert in_flight <= self.store.outstanding() + 1e-6, (
            f"在途量 {in_flight} 超过在外预留 {self.store.outstanding()}"
        )
