"""网关节点限流逻辑。

节点有两种工作模式：

- ``lease``（正常）：持有共享桶授予的带 TTL 配额租约，请求只消耗租约
  内令牌；租约接近到期或余量不足时主动续租，续租时向存储端申报真实
  用量。任何续租失败（过期、fencing、分区、配额不可用）都立即退回
  本地安全配额，绝不按全速放行。
- ``safe``（降级）：只使用本地 :class:`SafeBucket` 的保守速率。集群
  所有节点的安全速率之和必须不超过全局补充速率，这样即使全体节点同时
  被分区，全局限额也不会被打穿。

时钟漂移处理
------------
节点永远先于存储端放弃租约：本地有效期按
``ttl * (1 - drift_margin)`` 计算。``drift_margin`` 必须覆盖节点时钟
相对存储端的最大频率偏差（例如时钟最快走快 5% 时，边距取 10%）。
跨节点只比较单调时钟的「时长」，不比较绝对时间，因此单调时钟零点差
和墙上时间差都不影响正确性。
"""

from dataclasses import dataclass
from typing import Optional

from .clock import Clock, RealClock
from .errors import LeaseError, LeaseFenced, NodePartitioned, QuotaUnavailable
from .store import Grant, SharedTokenBucket

MODE_LEASE = "lease"
MODE_SAFE = "safe"


@dataclass(frozen=True)
class Decision:
    """单次 :meth:`RateLimiterNode.allow` 的判定结果。"""

    allowed: bool
    mode: str
    """``lease`` 或 ``safe``。"""
    amount: float
    reason: str
    epoch: Optional[int]
    lease_remaining: float


class SafeBucket:
    """降级模式使用的本地令牌桶，速率保守、容量很小。"""

    def __init__(
        self,
        refill_rate: float,
        capacity: float,
        clock: Clock,
    ) -> None:
        if refill_rate <= 0 or capacity <= 0:
            raise ValueError("安全桶速率与容量必须为正数")
        self.refill_rate = float(refill_rate)
        self.capacity = float(capacity)
        self.clock = clock
        self._tokens = float(capacity)
        self._last_refill = clock.mono()

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
        self._refill()
        return self._tokens

    def try_consume(self, amount: float = 1.0) -> bool:
        if amount <= 0:
            raise ValueError("消耗令牌数必须为正数")
        self._refill()
        if self._tokens + 1e-12 >= amount:
            self._tokens -= amount
            return True
        return False

    def reset(self) -> None:
        """节点重启后安全桶回到初始满桶状态。"""
        self._tokens = self.capacity
        self._last_refill = self.clock.mono()


class RateLimiterNode:
    """网关节点限流器。

    参数
    ----
    node_id:
        节点唯一标识。
    store:
        共享桶客户端（:class:`SharedTokenBucket`，生产环境替换为对应
        的 Redis/etcd 实现，接口保持一致）。
    lease_quota:
        每次申请/续租希望预留的令牌数。
    lease_ttl:
        租约 TTL（秒，存储端口径）。
    safe_rate / safe_capacity:
        降级模式本地安全桶的速率与容量。
    drift_margin:
        时钟漂移安全边距（0~1），本地有效期 = ``ttl * (1 - margin)``。
    renew_factor:
        本地有效期消耗到该比例时主动续租（默认 0.5，留出一次重试窗口）。
    """

    def __init__(
        self,
        node_id: str,
        store: SharedTokenBucket,
        lease_quota: float,
        lease_ttl: float,
        safe_rate: float,
        safe_capacity: float,
        clock: Optional[Clock] = None,
        drift_margin: float = 0.1,
        renew_factor: float = 0.5,
    ) -> None:
        if lease_quota <= 0 or lease_ttl <= 0:
            raise ValueError("lease_quota 与 lease_ttl 必须为正数")
        if not 0.0 <= drift_margin < 1.0:
            raise ValueError("drift_margin 必须落在 [0, 1)")
        if not 0.0 < renew_factor < 1.0:
            raise ValueError("renew_factor 必须落在 (0, 1)")
        self.node_id = node_id
        self.store = store
        self.lease_quota = float(lease_quota)
        self.lease_ttl = float(lease_ttl)
        self.drift_margin = float(drift_margin)
        self.renew_factor = float(renew_factor)
        self.clock = clock or RealClock()
        self.safe_bucket = SafeBucket(safe_rate, safe_capacity, self.clock)

        self._grant: Optional[Grant] = None
        self._grant_local_mono = 0.0
        self._local_deadline = 0.0
        self._local_used = 0.0
        self._acquire_cooldown_until = 0.0

        self.allowed_total = 0
        self.rejected_total = 0
        self.lease_allowed_total = 0
        self.safe_allowed_total = 0
        self.acquires_total = 0
        self.renewals_total = 0
        self.renew_failures_total = 0
        self.degradations_total = 0
        self.rejections: dict = {}

    # ------------------------------------------------------------------ #
    # 状态
    # ------------------------------------------------------------------ #
    @property
    def mode(self) -> str:
        if self._grant is not None and self.clock.mono() < self._local_deadline:
            return MODE_LEASE
        return MODE_SAFE

    @property
    def has_lease(self) -> bool:
        return self._grant is not None

    def lease_remaining(self) -> float:
        if self._grant is None:
            return 0.0
        return max(0.0, self._grant.quota - self._grant.used - self._local_used)

    def _effective_ttl(self, grant: Grant) -> float:
        return grant.ttl * (1.0 - self.drift_margin)

    def _install_grant(self, grant: Grant) -> None:
        self._grant = grant
        self._grant_local_mono = self.clock.mono()
        self._local_deadline = (
            self._grant_local_mono + self._effective_ttl(grant)
        )
        self._local_used = 0.0

    # ------------------------------------------------------------------ #
    # 租约操作
    # ------------------------------------------------------------------ #
    def _try_acquire(self) -> bool:
        now = self.clock.mono()
        if now < self._acquire_cooldown_until:
            return False
        try:
            grant = self.store.acquire_lease(
                self.node_id, self.lease_quota, self.lease_ttl
            )
        except (QuotaUnavailable, NodePartitioned):
            # 控制面不可达或无配额：短暂退避后快速重试，期间走安全桶。
            # 冷却不能太长，否则健康集群启动竞争会产生不必要的误拒。
            self._acquire_cooldown_until = now + self.lease_ttl * 0.05
            return False
        except LeaseFenced:
            # 旧 fencing 失效后管理面允许重新申请，立即重试一次
            try:
                grant = self.store.acquire_lease(
                    self.node_id, self.lease_quota, self.lease_ttl
                )
            except LeaseError:
                self._acquire_cooldown_until = now + self.lease_ttl * 0.05
                return False
        self._install_grant(grant)
        self.acquires_total += 1
        return True

    def _renew_or_degrade(self) -> None:
        assert self._grant is not None
        grant = self._grant
        try:
            new_grant = self.store.renew_lease(
                node_id=self.node_id,
                epoch=grant.epoch,
                used=self._local_used + grant.used,
                topup=self.lease_quota,
                ttl=self.lease_ttl,
            )
        except LeaseError:
            # 续租失败 = 无法确认配额归属：立即降级，绝不能继续全速放行
            self.renew_failures_total += 1
            self._degrade("renew_failed")
            return
        self._install_grant(new_grant)
        self.renewals_total += 1

    def _degrade(self, reason: str) -> None:
        if self._grant is not None:
            self._grant = None
            self._local_used = 0.0
            self.degradations_total += 1
            self.rejections[reason] = self.rejections.get(reason, 0) + 1

    # ------------------------------------------------------------------ #
    # 入口
    # ------------------------------------------------------------------ #
    def allow(self, amount: float = 1.0) -> Decision:
        """判断 ``amount`` 个令牌的请求是否放行。"""
        if amount <= 0:
            raise ValueError("请求令牌数必须为正数")
        now = self.clock.mono()

        if self._grant is not None:
            if now >= self._local_deadline:
                # 节点时钟视角先到期：主动放弃，等存储端 TTL 回收
                self._degrade("local_ttl_expired")
            else:
                elapsed = now - self._grant_local_mono
                due = elapsed >= self.renew_factor * self._effective_ttl(
                    self._grant
                )
                if due or self.lease_remaining() < amount:
                    self._renew_or_degrade()

        # 持有有效租约期间只使用租约配额；配额耗尽直接拒绝，
        # 绝不挪用本地安全桶——安全桶只服务于「控制面不可达」的降级场景。
        if self._grant is not None and now < self._local_deadline:
            if self.lease_remaining() + 1e-12 >= amount:
                self._local_used += amount
                self.allowed_total += 1
                self.lease_allowed_total += 1
                return Decision(
                    allowed=True,
                    mode=MODE_LEASE,
                    amount=amount,
                    reason="lease_granted",
                    epoch=self._grant.epoch,
                    lease_remaining=self.lease_remaining(),
                )
            self.rejected_total += 1
            self.rejections["lease_quota_empty"] = (
                self.rejections.get("lease_quota_empty", 0) + 1
            )
            return Decision(
                allowed=False,
                mode=MODE_LEASE,
                amount=amount,
                reason="lease_quota_empty",
                epoch=self._grant.epoch,
                lease_remaining=self.lease_remaining(),
            )

        # 无有效租约（启动期/续租失败/本地 TTL 到期）：先尝试向控制面申请，
        # 申请成功走租约；控制面不可达时才退回本地安全配额。
        if self._grant is None and self._try_acquire():
            if self.lease_remaining() + 1e-12 >= amount:
                self._local_used += amount
                self.allowed_total += 1
                self.lease_allowed_total += 1
                return Decision(
                    allowed=True,
                    mode=MODE_LEASE,
                    amount=amount,
                    reason="lease_granted",
                    epoch=self._grant.epoch,
                    lease_remaining=self.lease_remaining(),
                )

        if self.safe_bucket.try_consume(amount):
            self.allowed_total += 1
            self.safe_allowed_total += 1
            return Decision(
                allowed=True,
                mode=MODE_SAFE,
                amount=amount,
                reason="safe_quota",
                epoch=self._grant.epoch if self._grant else None,
                lease_remaining=self.lease_remaining(),
            )

        self.rejected_total += 1
        self.rejections["safe_quota_empty"] = (
            self.rejections.get("safe_quota_empty", 0) + 1
        )
        return Decision(
            allowed=False,
            mode=MODE_SAFE,
            amount=amount,
            reason="safe_quota_empty",
            epoch=self._grant.epoch if self._grant else None,
            lease_remaining=self.lease_remaining(),
        )

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    def release(self) -> None:
        """优雅释放租约，未用预留立即退回共享桶。"""
        if self._grant is None:
            return
        grant = self._grant
        try:
            self.store.release_lease(
                self.node_id, grant.epoch, self._local_used + grant.used
            )
        except LeaseError:
            pass
        self._grant = None
        self._local_used = 0.0

    def restart(self, graceful: bool = True) -> None:
        """模拟节点重启：内存租约状态全部丢失。

        优雅重启会先尝试释放租约；硬崩溃（``graceful=False``）不释放，
        旧租约靠存储端 TTL 到期回收。fencing epoch 保证旧租约即便残留
        也无法被重启后的节点误用。
        """
        if graceful:
            self.release()
        self._grant = None
        self._local_used = 0.0
        self._acquire_cooldown_until = 0.0
        self.safe_bucket.reset()

    def stats(self) -> dict:
        return {
            "node_id": self.node_id,
            "mode": self.mode,
            "has_lease": self._grant is not None,
            "lease_remaining": self.lease_remaining(),
            "safe_tokens": self.safe_bucket.tokens,
            "allowed_total": self.allowed_total,
            "rejected_total": self.rejected_total,
            "lease_allowed_total": self.lease_allowed_total,
            "safe_allowed_total": self.safe_allowed_total,
            "acquires_total": self.acquires_total,
            "renewals_total": self.renewals_total,
            "renew_failures_total": self.renew_failures_total,
            "degradations_total": self.degradations_total,
        }
