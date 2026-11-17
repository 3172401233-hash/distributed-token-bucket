"""降级安全边界与防回归测试。"""

import unittest

from ratelimit.node import MODE_LEASE, MODE_SAFE
from tests.helpers import make_cluster


class DegradeBoundaryTest(unittest.TestCase):
    def test_renew_failure_never_continues_at_full_speed(self):
        # 核心需求：续租失败时必须退回本地安全配额，不能继续全速放行
        cluster = make_cluster(n=2, capacity=100.0, refill_rate=80.0, ttl=2.0)
        node = cluster.nodes[0]
        for _ in range(10):
            node.allow()
        self.assertEqual(node.mode, MODE_LEASE)

        cluster.store.partition(node.node_id)
        cluster.advance(1.05)  # 进入续租窗口
        first = node.allow()
        self.assertEqual(first.mode, MODE_SAFE)
        self.assertGreaterEqual(node.renew_failures_total, 1)

        # 分区期间高速施压：放行速率不得超过安全桶速率
        safe_rate = node.safe_bucket.refill_rate
        before = node.safe_allowed_total
        cluster.run(duration=2.0, step=0.01)
        after = node.safe_allowed_total
        self.assertLessEqual(
            after - before, safe_rate * 2.0 + node.safe_bucket.capacity + 1.0
        )

    def test_safe_rate_sum_never_exceeds_global_refill(self):
        # 任意 N，安全速率总和 <= 全局补充速率（默认 safe_fraction=0.5）
        for n in (1, 2, 4, 8, 16):
            cluster = make_cluster(
                n=n, capacity=100.0, refill_rate=100.0, safe_fraction=0.5
            )
            total_safe_rate = sum(
                node.safe_bucket.refill_rate for node in cluster.nodes
            )
            self.assertLessEqual(
                total_safe_rate, cluster.store.refill_rate + 1e-9
            )
            total_safe_cap = sum(
                node.safe_bucket.capacity for node in cluster.nodes
            )
            self.assertLessEqual(
                total_safe_cap, cluster.store.capacity + 1e-9
            )

    def test_safe_mode_cannot_reach_full_lease_throughput(self):
        cluster = make_cluster(n=1, capacity=100.0, refill_rate=100.0, ttl=2.0)
        node = cluster.nodes[0]
        cluster.store.partition(node.node_id)
        cluster.run(duration=3.0, step=0.01)
        # 单节点安全速率只有 50/s，3s 内（含初始容量）不可能达到全速 300
        self.assertLess(node.safe_allowed_total, 200.0)
        self.assertEqual(node.lease_allowed_total, 0)

    def test_node_recovers_to_lease_mode_after_heal(self):
        cluster = make_cluster(n=1, capacity=100.0, refill_rate=50.0, ttl=2.0)
        node = cluster.nodes[0]
        node.allow()
        self.assertEqual(node.mode, MODE_LEASE)
        cluster.store.partition(node.node_id)
        cluster.advance(1.05)
        node.allow()
        self.assertEqual(node.mode, MODE_SAFE)

        cluster.store.heal_partition(node.node_id)
        cluster.advance(0.2)
        decision = node.allow()
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.mode, MODE_LEASE)


class QuotaAccountingTest(unittest.TestCase):
    def test_no_token_created_outside_store(self):
        # 租约放行的每一个令牌都能在存储端找到对应的预留/消耗记录
        cluster = make_cluster(n=4, capacity=80.0, refill_rate=60.0, ttl=2.0)
        cluster.run(duration=5.0, step=0.05)
        cluster.assert_consumed_matches_allowed()
        stats = cluster.store.stats()
        # 桶内 + 在外预留 + 已确认消耗 <= 初始容量 + 累计补充
        supplied = (
            stats["capacity"] + stats["refill_rate"] * cluster.time
        )
        accounted = (
            stats["tokens"] + stats["outstanding"] + stats["consumed_total"]
        )
        self.assertLessEqual(accounted, supplied + 1e-6)

    def test_partial_grant_does_not_raise(self):
        cluster = make_cluster(n=3, capacity=30.0, refill_rate=10.0, ttl=2.0)
        # 三个节点同时申请，桶里 30 个令牌按顺序部分授予，不允许抛异常
        decisions = [node.allow() for node in cluster.nodes]
        self.assertTrue(all(d.allowed for d in decisions))
        granted_total = sum(
            view.quota
            for node in cluster.nodes
            if (view := cluster.store.get_lease(node.node_id))
        )
        self.assertLessEqual(granted_total, 30.0 + 1e-9)


if __name__ == "__main__":
    unittest.main()
