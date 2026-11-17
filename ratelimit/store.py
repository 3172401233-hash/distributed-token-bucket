"""共享令牌桶（权威状态）。

设计要点
--------
1. 配额以「租约（lease）」形式预留：节点申请到的令牌立刻从共享桶扣除，
   其他节点拿不到同一批令牌，因此节点数翻倍不会让全局限额翻倍。
2. 每个租约带 TTL（基于存储端单调时钟）。节点续租时申报本周期真实用量，
   用量永久入账（已消耗的全局容量不会回来），未用完的预留量在续租或
   过期回收时退回桶里继续补充。
3. 每个节点有单调递增的 fencing epoch：节点重启或租约被抢占后 epoch 加一，
   旧 epoch 的请求一律以 :class:`LeaseFenced` 拒绝，防止「同一时间窗被
   重复使用」。
4. 过期租约由存储端时钟判定，节点时钟快慢不影响存储端的 TTL 正确性；
   节点侧另有漂移安全边距（见 :mod:`ratelimit.node`），保证节点永远先于
   存储端放弃租约。
5. 网络分区通过 :meth:`partition` / :meth:`heal_partition` 模拟：分区节点
   的所有租约操作直接失败，模拟真实控制面不可达。
"""

from dataclasses import dataclass
from typing import Dict, Optional

from .clock import Clock, RealClock
from .errors import (
    LeaseExpired,
    LeaseFenced,
    NodePartitioned,
    QuotaExceeded,
    QuotaUnavailable,
)


@dataclass(frozen=True)
class Grant:
    """一次申请/续租返回的租约快照。"""

    node_id: str
    epoch: int
    quota: float
    used: float
    granted_at: float
    """存储端单调时间（秒）。"""
    ttl: float
    """租约时长（秒），过期时间 = ``granted_at + ttl``。"""
    store_time: float
    """响应时的存储端单调时间，供节点做时钟对账。"""

    @property
    def expires_at(self) -> float:
        return self.granted_at + self.ttl


@dataclass(frozen=True)
class LeaseView:
    """租约的只读视图，用于测试与可观测性。"""

    node_id: str
    epoch: int
    quota: float
    used: float
    granted_at: float
    ttl: float
    expired: bool


class _Lease:
    """可变租约内部状态。"""

    __slots__ = ("epoch", "quota", "used", "granted_at", "ttl")

    def __init__(self, epoch: int, quota: float, granted_at: float, ttl: float):
        self.epoch = epoch
        self.quota = quota
        self.used = 0.0
        self.granted_at = granted_at
        self.ttl = ttl


class SharedTokenBucket:
    """共享令牌桶，部署在控制面（如 Redis/etcd 协调服务）。

    参数
    ----
    capacity:
        全局桶容量（突发上限），单位为令牌数。
    refill_rate:
        全局补充速率（令牌/秒）。
    clock:
        存储端时钟，生产环境用 :class:`RealClock`。
    """

    def __init__(
        self,
        capacity: float,
        refill_rate: float,
        clock: Optional[Clock] = None,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity 必须为正数")
        if refill_rate <= 0:
            raise ValueError("refill_rate 必须为正数")
        self.capacity = float(capacity)
        self.refill_rate = float(refill_rate)
        self.clock = clock or RealClock()
        self._tokens = float(capacity)
        self._last_refill = self.clock.mono()
        self._leases: Dict[str, _Lease] = {}
        self._blocked: set = set()
        self._partitions: set = set()
        self.grants_total = 0
        self.reclaims_total = 0.0
        self.consumed_total = 0.0
        self.rejections_total = 0

    # ------------------------------------------------------------------ #
    # 令牌桶
    # ------------------------------------------------------------------ #
    def _refill(self) -> None:
        now = self.clock.mono()
        elapsed = now - self._last_refill
        if elapsed > 0:
            self._tokens = min(
                self.capacity, self._tokens + elapsed * self.refill_rate
            )
            self._last_refill = now

    @property
    def tokens(self) -> float:
        """当前可预留令牌数（先做一次惰性补充）。"""
        self._refill()
        return self._tokens

    def outstanding(self) -> float:
        """所有未过期租约里尚未确认消耗的预留令牌。"""
        now = self.clock.mono()
        return sum(
            lease.quota - lease.used
            for lease in self._leases.values()
            if now < lease.granted_at + lease.ttl
        )

    # ------------------------------------------------------------------ #
    # 租约生命周期
    # ------------------------------------------------------------------ #
    def acquire_lease(
        self,
        node_id: str,
        requested: float,
        ttl: float,
    ) -> Grant:
        """申请新租约。

        若该节点已有未过期租约，等价于用新 epoch 抢占旧租约（旧租约中
        已确认的用量保留，未用量退回桶内）。令牌不足时按可用量部分授予，
        一个令牌都没有时抛 :class:`QuotaUnavailable`。
        """
        if requested <= 0 or ttl <= 0:
            raise ValueError("requested 与 ttl 必须为正数")
        self._check_reachable(node_id)
        self._refill()
        self._purge_expired()

        now = self.clock.mono()
        old = self._leases.pop(node_id, None)
        if old is not None and now < old.granted_at + old.ttl:
            self._return_unused(old)

        if node_id in self._blocked:
            self.rejections_total += 1
            raise LeaseFenced(f"节点 {node_id} 已被隔离，拒绝新租约")

        granted = min(requested, self._tokens)
        if granted <= 0:
            self.rejections_total += 1
            raise QuotaUnavailable("共享桶暂无可用令牌")

        epoch = (old.epoch + 1) if old is not None else 1
        lease = _Lease(epoch, granted, now, ttl)
        self._leases[node_id] = lease
        self._tokens -= granted
        self.grants_total += 1
        return self._snapshot(node_id, lease)

    def renew_lease(
        self,
        node_id: str,
        epoch: int,
        used: float,
        topup: float,
        ttl: float,
    ) -> Grant:
        """续租：结算上一周期用量，并为下一周期追加预留。

        - ``used``：本周期已确认消耗的令牌数，永久入账；
        - ``topup``：新周期希望预留的满额配额。续租后 ``quota`` 变为
          ``used + topup``（受可用令牌限制时部分授予），即
          「已确认用量 + 新周期预留」，配额目标不会因持续消耗缩水；
        - 令牌不足时部分授予，不抛异常，节点靠剩余配额继续工作；
        - 租约失效（过期/fenced/分区）时抛对应异常，节点必须降级。
        """
        if used < 0 or topup < 0 or ttl <= 0:
            raise ValueError("used/topup 不能为负，ttl 必须为正")
        self._check_reachable(node_id)
        self._refill()
        self._purge_expired()

        lease = self._leases.get(node_id)
        if lease is None:
            self.rejections_total += 1
            raise LeaseExpired(f"节点 {node_id} 的租约不存在或已过期")
        if lease.epoch != epoch:
            self.rejections_total += 1
            raise LeaseFenced(
                f"epoch 不匹配：持有 {epoch}，当前 {lease.epoch}"
            )
        now = self.clock.mono()
        if now >= lease.granted_at + lease.ttl:
            self.rejections_total += 1
            raise LeaseExpired(f"节点 {node_id} 的租约已过期")
        if used > lease.quota + 1e-9:
            raise QuotaExceeded(
                f"申报用量 {used} 超过租约配额 {lease.quota}"
            )

        self._settle(lease, used)
        target_quota = used + topup
        want = max(0.0, target_quota - lease.quota)
        refill = min(want, max(0.0, self._tokens))
        if refill > 0:
            self._tokens -= refill
        lease.quota += refill
        lease.granted_at = now
        lease.ttl = ttl
        self.grants_total += 1
        return self._snapshot(node_id, lease)

    def release_lease(self, node_id: str, epoch: int, used: float) -> None:
        """主动释放租约（优雅关闭），未用预留退回桶内。"""
        self._check_reachable(node_id)
        self._refill()
        lease = self._leases.get(node_id)
        if lease is None:
            return
        if lease.epoch != epoch:
            raise LeaseFenced(
                f"epoch 不匹配：持有 {epoch}，当前 {lease.epoch}"
            )
        if used > lease.quota + 1e-9:
            raise QuotaExceeded(
                f"申报用量 {used} 超过租约配额 {lease.quota}"
            )
        self._settle(lease, used)
        self._return_unused(lease)
        del self._leases[node_id]

    def force_preempt(self, node_id: str) -> None:
        """管理面强制抢占：fencing epoch 加一，旧租约立即失效。

        旧租约未用量退回桶内并可立即重新分配给其他节点。被抢占节点
        下次续租会收到 :class:`LeaseFenced`。
        """
        self._refill()
        old = self._leases.pop(node_id, None)
        if old is not None:
            self._return_unused(old)
            self._leases[node_id] = _Lease(
                old.epoch + 1, 0.0, old.granted_at, old.ttl
            )
        else:
            self._leases[node_id] = _Lease(1, 0.0, self.clock.mono(), 0.0)

    def block_node(self, node_id: str) -> None:
        """隔离节点：新租约申请一律拒绝（模拟抢占后禁止重新加入）。"""
        self._blocked.add(node_id)

    def readmit_node(self, node_id: str) -> None:
        """解除隔离。"""
        self._blocked.discard(node_id)

    # ------------------------------------------------------------------ #
    # 网络分区模拟
    # ------------------------------------------------------------------ #
    def partition(self, node_id: str) -> None:
        self._partitions.add(node_id)

    def heal_partition(self, node_id: str) -> None:
        self._partitions.discard(node_id)

    def _check_reachable(self, node_id: str) -> None:
        if node_id in self._partitions:
            self.rejections_total += 1
            raise NodePartitioned(f"节点 {node_id} 处于网络分区")

    # ------------------------------------------------------------------ #
    # 内部结算
    # ------------------------------------------------------------------ #
    def _settle(self, lease: _Lease, used: float) -> None:
        newly_consumed = max(0.0, used - lease.used)
        lease.used = used
        self.consumed_total += newly_consumed

    def _return_unused(self, lease: _Lease) -> None:
        unused = max(0.0, lease.quota - lease.used)
        if unused > 0:
            self._tokens = min(self.capacity, self._tokens + unused)
            self.reclaims_total += unused

    def _purge_expired(self) -> None:
        now = self.clock.mono()
        dead = [
            node_id
            for node_id, lease in self._leases.items()
            if now >= lease.granted_at + lease.ttl
        ]
        for node_id in dead:
            lease = self._leases.pop(node_id)
            self._return_unused(lease)

    def _snapshot(self, node_id: str, lease: _Lease) -> Grant:
        return Grant(
            node_id=node_id,
            epoch=lease.epoch,
            quota=lease.quota,
            used=lease.used,
            granted_at=lease.granted_at,
            ttl=lease.ttl,
            store_time=self.clock.mono(),
        )

    def get_lease(self, node_id: str) -> Optional[LeaseView]:
        """读取租约快照（测试/监控用）。"""
        self._refill()
        self._purge_expired()
        lease = self._leases.get(node_id)
        if lease is None:
            return None
        now = self.clock.mono()
        return LeaseView(
            node_id=node_id,
            epoch=lease.epoch,
            quota=lease.quota,
            used=lease.used,
            granted_at=lease.granted_at,
            ttl=lease.ttl,
            expired=now >= lease.granted_at + lease.ttl,
        )

    def stats(self) -> dict:
        """返回桶级统计。"""
        self._refill()
        self._purge_expired()
        return {
            "tokens": self._tokens,
            "capacity": self.capacity,
            "refill_rate": self.refill_rate,
            "outstanding": self.outstanding(),
            "active_leases": len(self._leases),
            "grants_total": self.grants_total,
            "consumed_total": self.consumed_total,
            "reclaims_total": self.reclaims_total,
            "rejections_total": self.rejections_total,
        }
