"""突发、持续流量与误拒率测试。"""

import unittest

from tests.helpers import make_cluster


class BurstTrafficTest(unittest.TestCase):
    def setUp(self) -> None:
        # 全局 100 容量、100/s 补充，4 节点
        self.cluster = make_cluster(n=4, capacity=100.0, refill_rate=100.0)

    def test_initial_burst_never_exceeds_global_budget(self):
        # 每个节点瞬时打 60 个请求，总请求 240 远超 100 容量
        for _ in range(60):
            for node in self.cluster.nodes:
                node.allow()

        allowed = self.cluster.total_allowed()
        # 突发上界 = 共享桶容量 + 全体安全桶初始容量。
        # 拿不到租约的节点只能按降级安全预算放行，绝不全速放行。
        bound = self.cluster.burst_bound()
        self.assertLessEqual(
            allowed,
            bound + 1e-6,
            f"突发流量 {allowed} 超过预算上界 {bound}",
        )
        # 必须真的拒绝了超额请求，而不是全部放行
        self.assertGreater(self.cluster.total_rejected(), 0)
        # 安全桶放行量严格受初始容量约束，不随节点数线性膨胀
        self.assertLessEqual(
            sum(n.safe_allowed_total for n in self.cluster.nodes),
            self.cluster.safe_capacity_total() + 1e-9,
        )
        # 绝大部分突发流量来自共享桶预留，而不是本地安全桶
        lease_allowed = sum(
            n.lease_allowed_total for n in self.cluster.nodes
        )
        self.assertEqual(lease_allowed, self.cluster.store.capacity)

    def test_scaling_node_count_does_not_multiply_limit(self):
        four = make_cluster(n=4, capacity=100.0, refill_rate=100.0)
        eight = make_cluster(n=8, capacity=100.0, refill_rate=100.0)
        for cluster in (four, eight):
            for _ in range(80):
                for node in cluster.nodes:
                    node.allow()
        # 节点数翻倍，突发放行量必须几乎相同（共享桶是唯一权威，
        # 安全预算按集群规模切分，总量恒定）
        self.assertAlmostEqual(
            four.total_allowed(),
            eight.total_allowed(),
            delta=3.0,
        )
        for cluster in (four, eight):
            self.assertLessEqual(
                cluster.total_allowed(),
                cluster.burst_bound() + 1e-6,
            )


class SustainedTrafficTest(unittest.TestCase):
    def test_long_run_rate_capped_at_refill_rate(self):
        cluster = make_cluster(n=4, capacity=100.0, refill_rate=100.0)
        # 每秒每节点 50 个请求（共 200/s，需求是限额的两倍），跑 10s
        cluster.run(duration=10.0, step=0.02, per_step_requests=1)

        # 严格上界 = 容量 + 速率*时长 + 一个 TTL 的在途预留
        cluster.assert_within_global_cap(
            window_seconds=10.0, slack=2.0, msg="持续流量"
        )
        # 且吞吐确实接近限额（限流有效，不是全拒绝）
        self.assertGreater(cluster.total_allowed(), 950)

    def test_steady_demand_below_limit_has_near_zero_rejects(self):
        cluster = make_cluster(n=4, capacity=100.0, refill_rate=100.0)
        # 共 80/s 请求 < 100/s 限额，跑 8s
        cluster.run(duration=8.0, step=0.05, per_step_requests=1)
        cluster.assert_within_global_cap(window_seconds=8.0, slack=2.0)
        # 低于限额的需求几乎不误拒，仅允许启动瞬间极少数节点没抢到租约
        self.assertLessEqual(cluster.total_rejected(), 4)


class FalseRejectRateTest(unittest.TestCase):
    def test_healthy_cluster_false_reject_rate_near_zero(self):
        cluster = make_cluster(n=3, capacity=60.0, refill_rate=60.0)
        cluster.run(duration=6.0, step=0.1, per_step_requests=2)
        self.assertLessEqual(cluster.total_rejected(), 3)
        cluster.assert_consumed_matches_allowed()

    def test_overloaded_cluster_rejects_are_necessary(self):
        cluster = make_cluster(n=4, capacity=50.0, refill_rate=50.0)
        cluster.run(duration=5.0, step=0.02, per_step_requests=2)
        # 过载场景必然有拒绝，但拒绝量必须恰好对应超出全局上限的部分
        cluster.assert_within_global_cap(window_seconds=5.0, slack=4.0)
        self.assertGreater(cluster.total_rejected(), 0)
        # 放行量仍应接近理论上限（没有因为实现保守而饿死）
        standard_bound = (
            cluster.store.capacity
            + cluster.store.refill_rate * 5.0
        )
        self.assertGreater(
            cluster.total_allowed(),
            standard_bound * 0.9,
        )


if __name__ == "__main__":
    unittest.main()
