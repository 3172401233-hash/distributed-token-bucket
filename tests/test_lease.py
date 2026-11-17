"""租约抢占、fencing、续租与存储端语义测试。"""

import unittest

from ratelimit import (
    LeaseFenced,
    QuotaExceeded,
    SharedTokenBucket,
    MockClock,
)
from ratelimit.node import MODE_LEASE, MODE_SAFE
from tests.helpers import make_cluster


class LeaseLifecycleTest(unittest.TestCase):
    def test_renew_settles_used_quota_and_reclaims_unused(self):
        cluster = make_cluster(n=1, capacity=50.0, refill_rate=10.0, ttl=2.0)
        node = cluster.nodes[0]
        for _ in range(20):
            node.allow()
        self.assertEqual(node.lease_allowed_total, 20)
        # 触发续租
        cluster.advance(1.1)
        node.allow()
        self.assertGreaterEqual(node.renewals_total, 1)
        # 存储端确认前 20 个消耗；续租后的第 21 个属于新周期在途消耗
        self.assertAlmostEqual(
            cluster.store.consumed_total, 20.0, delta=1e-6
        )
        self.assertEqual(node._local_used, 1.0)

    def test_over_reporting_usage_is_rejected(self):
        clock = MockClock()
        store = SharedTokenBucket(100.0, 10.0, clock=clock)
        grant = store.acquire_lease("n", 30.0, 2.0)
        with self.assertRaises(QuotaExceeded):
            store.renew_lease("n", grant.epoch, 31.0, 10.0, 2.0)

    def test_stale_epoch_renewal_is_fenced(self):
        clock = MockClock()
        store = SharedTokenBucket(100.0, 10.0, clock=clock)
        first = store.acquire_lease("n", 30.0, 2.0)
        clock.advance(0.5)
        second = store.acquire_lease("n", 30.0, 2.0)
        self.assertEqual(second.epoch, first.epoch + 1)
        # 旧 epoch 的续租必须被 fencing 拒绝
        with self.assertRaises(LeaseFenced):
            store.renew_lease("n", first.epoch, 5.0, 10.0, 2.0)

    def test_expired_lease_reclaimed_by_store_clock(self):
        clock = MockClock()
        store = SharedTokenBucket(100.0, 50.0, clock=clock)
        grant = store.acquire_lease("n", 80.0, 2.0)
        self.assertAlmostEqual(store.tokens, 20.0, delta=1e-9)
        clock.advance(2.001)
        view = store.get_lease("n")
        self.assertIsNone(view)
        # 未使用的 80 个预留已退回（并受容量上限裁剪）
        self.assertAlmostEqual(store.tokens, 100.0, delta=1e-9)
        # 过期后再续租明确报错，而不是静默新建
        from ratelimit import LeaseExpired

        with self.assertRaises(LeaseExpired):
            store.renew_lease("n", grant.epoch, 0.0, 10.0, 2.0)


class PreemptionTest(unittest.TestCase):
    def test_forced_preemption_drives_node_to_safe_mode(self):
        cluster = make_cluster(n=2, capacity=100.0, refill_rate=50.0, ttl=2.0)
        victim, other = cluster.nodes
        victim.allow()
        other.allow()
        self.assertEqual(victim.mode, MODE_LEASE)

        # 管理面抢占 victim，并隔离其重新申请
        cluster.store.force_preempt(victim.node_id)
        cluster.store.block_node(victim.node_id)
        # 推进到 victim 的续租窗口
        cluster.advance(1.05)

        decision = victim.allow()
        self.assertEqual(decision.mode, MODE_SAFE)
        self.assertEqual(victim.mode, MODE_SAFE)
        self.assertGreaterEqual(victim.renew_failures_total, 1)

        # 抢占期间 victim 只能按安全速率放行，绝不恢复全速
        for _ in range(50):
            victim.allow()
        safe_cap = victim.safe_bucket.capacity
        self.assertLessEqual(
            victim.safe_allowed_total, safe_cap + 2.0
        )

        # 解除隔离后，新 epoch 租约可以重新申请，节点自愈
        cluster.store.readmit_node(victim.node_id)
        cluster.advance(0.6)
        victim.allow()
        self.assertEqual(victim.mode, MODE_LEASE)

    def test_preemption_returns_unused_quota_to_pool(self):
        cluster = make_cluster(n=2, capacity=100.0, refill_rate=20.0, ttl=2.0)
        victim, other = cluster.nodes
        victim.allow()
        tokens_before = cluster.store.tokens
        cluster.store.force_preempt(victim.node_id)
        self.assertGreaterEqual(cluster.store.tokens, tokens_before)
        # 其他节点立刻可以拿到退回的令牌
        grant = cluster.store.acquire_lease(
            other.node_id, requested=90.0, ttl=2.0
        )
        self.assertGreater(grant.quota, 0)

    def test_two_incarnations_cannot_share_one_window(self):
        # 同一节点两次 incarnation 的已确认用量都计入全局，未用量不重复
        clock = MockClock()
        store = SharedTokenBucket(100.0, 10.0, clock=clock)
        first = store.acquire_lease("n", 60.0, 2.0)
        clock.advance(0.5)
        store.renew_lease("n", first.epoch, 40.0, 0.0, 2.0)
        # 新 incarnation 抢占：旧租约只剩 20 未用，退回后重新分配
        second = store.acquire_lease("n", 60.0, 2.0)
        self.assertEqual(second.epoch, first.epoch + 1)
        # 已消耗的 40 个令牌永久入账，不会因为换 epoch 被重复使用
        self.assertAlmostEqual(store.consumed_total, 40.0, delta=1e-9)


if __name__ == "__main__":
    unittest.main()
