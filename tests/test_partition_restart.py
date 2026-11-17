"""网络分区、节点重启与全局限额测试。"""

import unittest

from ratelimit.node import MODE_LEASE, MODE_SAFE
from tests.helpers import make_cluster


class PartitionTest(unittest.TestCase):
    def test_partitioned_node_falls_back_to_safe_quota(self):
        cluster = make_cluster(n=3, capacity=120.0, refill_rate=60.0, ttl=2.0)
        victim = cluster.nodes[0]
        for node in cluster.nodes:
            node.allow()
        self.assertEqual(victim.mode, MODE_LEASE)

        # 分区后 victim 与共享桶完全不可达
        cluster.store.partition(victim.node_id)
        cluster.advance(1.05)  # 到达续租窗口
        decision = victim.allow()
        self.assertEqual(decision.mode, MODE_SAFE)
        self.assertGreaterEqual(victim.renew_failures_total, 1)

        # 分区期间持续施压 3s：只能走安全桶，不能全速
        safe_rate = victim.safe_bucket.refill_rate
        safe_cap = victim.safe_bucket.capacity
        cluster.run(duration=3.0, step=0.02)
        safe_allowed = victim.safe_allowed_total
        # 安全桶上限 = 初始容量 + 速率 * 降级时长（留浮点容差）
        self.assertLessEqual(
            safe_allowed, safe_cap + safe_rate * 3.0 + 2.0
        )
        # 远低于「全速」应有的放行量
        self.assertLess(safe_allowed, 60.0 * 3.0)

        # 恢复分区后应能重新拿回租约
        cluster.store.heal_partition(victim.node_id)
        cluster.advance(0.6)
        victim.allow()
        self.assertEqual(victim.mode, MODE_LEASE)

    def test_partition_never_punches_through_global_limit(self):
        cluster = make_cluster(n=4, capacity=100.0, refill_rate=100.0, ttl=2.0)
        cluster.run(duration=3.0, step=0.02)
        # 半数节点分区，继续跑 5s
        for node in cluster.nodes[:2]:
            cluster.store.partition(node.node_id)
        cluster.run(duration=5.0, step=0.02)

        # 安全速率总和 = 100/s * 0.5，租约侧最多 100/s；
        # 理论上界取 容量 + 全速率 * 总时长，再给降级切换的小容差
        cluster.assert_within_global_cap(
            window_seconds=8.0, slack=15.0, msg="分区场景"
        )

    def test_full_partition_at_start_is_still_bounded(self):
        # 极端场景：所有节点一开始就拿不到租约，只能全靠安全桶
        cluster = make_cluster(n=4, capacity=100.0, refill_rate=100.0, ttl=2.0)
        for node in cluster.nodes:
            cluster.store.partition(node.node_id)
        cluster.run(duration=4.0, step=0.02)

        safe_cap_total = sum(n.safe_bucket.capacity for n in cluster.nodes)
        safe_rate_total = sum(n.safe_bucket.refill_rate for n in cluster.nodes)
        bound = safe_cap_total + safe_rate_total * 4.0
        self.assertLessEqual(cluster.total_allowed(), bound + 2.0)
        # 安全速率总和只有全局速率的 50%，远小于正常放行量
        self.assertLess(
            cluster.total_allowed(),
            cluster.ideal_bound(4.0) * 0.6,
        )


class RestartTest(unittest.TestCase):
    def test_graceful_restart_releases_lease_immediately(self):
        cluster = make_cluster(n=2, capacity=100.0, refill_rate=40.0, ttl=2.0)
        node = cluster.nodes[0]
        for _ in range(10):
            node.allow()
        node.restart(graceful=True)
        self.assertIsNone(cluster.store.get_lease(node.node_id))
        # 重启后重新申请成功
        node.allow()
        self.assertEqual(node.mode, MODE_LEASE)

    def test_hard_crash_lease_reclaimed_by_ttl_and_never_reused(self):
        cluster = make_cluster(n=2, capacity=100.0, refill_rate=40.0, ttl=2.0)
        node = cluster.nodes[0]
        for _ in range(15):
            node.allow()
        old_epoch = node._grant.epoch
        node.restart(graceful=False)

        # 硬崩溃后内存状态清空，旧租约在存储端等待 TTL 回收
        self.assertIsNone(node._grant)
        view = cluster.store.get_lease(node.node_id)
        self.assertIsNotNone(view)
        self.assertEqual(view.epoch, old_epoch)

        # 重启后重新申请得到新 epoch，旧 epoch 永远无法再续租
        node.allow()
        self.assertEqual(node.mode, MODE_LEASE)
        self.assertGreater(node._grant.epoch, old_epoch)

        cluster.advance(2.5)  # 等旧 TTL 窗口完全过去
        # 整个过程全局上限不被打穿
        cluster.run(duration=3.0, step=0.05)
        cluster.assert_within_global_cap(
            window_seconds=5.5, slack=5.0, msg="硬崩溃场景"
        )

    def test_safe_bucket_resets_on_restart(self):
        cluster = make_cluster(n=1, capacity=10.0, refill_rate=10.0)
        node = cluster.nodes[0]
        node.safe_bucket.try_consume(node.safe_bucket.capacity)
        self.assertAlmostEqual(node.safe_bucket.tokens, 0.0, delta=1e-9)
        node.restart(graceful=False)
        self.assertAlmostEqual(
            node.safe_bucket.tokens,
            node.safe_bucket.capacity,
            delta=1e-9,
        )


class GlobalInvariantTest(unittest.TestCase):
    def test_tokens_plus_outstanding_never_exceeds_capacity_plus_refill(self):
        # 任意时刻：桶内令牌 + 未过期租约预留 <= 容量 + 已补充量，
        # 即不存在凭空产生的令牌
        cluster = make_cluster(n=4, capacity=80.0, refill_rate=60.0, ttl=2.0)
        cluster.run(duration=4.0, step=0.03)
        stats = cluster.store.stats()
        upper = (
            cluster.store.capacity
            + cluster.store.refill_rate * cluster.time
        )
        self.assertLessEqual(
            stats["tokens"] + stats["outstanding"], upper + 1e-6
        )
        cluster.assert_consumed_matches_allowed()


if __name__ == "__main__":
    unittest.main()
