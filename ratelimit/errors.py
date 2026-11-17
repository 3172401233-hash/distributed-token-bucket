"""限流库异常类型。"""


class LeaseError(Exception):
    """租约相关错误的基类。"""


class LeaseExpired(LeaseError):
    """租约已过期并被共享桶回收，节点不得再使用其中配额。"""


class LeaseFenced(LeaseError):
    """租约 epoch 已失效（被抢占或被新的 incarnation 取代）。

    fencing token 失效，旧持有者的任何续租/释放请求都必须被拒绝。
    """


class QuotaUnavailable(LeaseError):
    """共享桶当前没有足够令牌满足新租约或续租追加请求。"""


class QuotaExceeded(LeaseError):
    """节点申报的用量超过了租约授予量，属于违约调用。"""


class NodePartitioned(LeaseError):
    """节点处于网络分区中，共享桶拒绝其一切租约操作。"""
