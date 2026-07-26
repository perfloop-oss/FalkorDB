from common import *


GRAPH_ID = "memory_usage_overlap_sampler"


def _as_map(reply):
    if isinstance(reply, dict):
        return reply
    return {reply[i]: reply[i + 1] for i in range(0, len(reply), 2)}


class testGraphMemoryUsageOverlapSampler(FlowTestsBase):
    def __init__(self):
        self.env, self.db = Env(env='oss-cluster')
        self.conn = self.env.getConnection()
        self.graph = self.db.select_graph(GRAPH_ID)

    def tearDown(self):
        self.graph.delete()
        self.graph = self.db.select_graph(GRAPH_ID)

    def test_overlapping_attributes_belong_to_the_first_label(self):
        # Label B is registered before A. Every A:B node must therefore be
        # attributed to B, rather than being counted for both label estimates.
        payload = 'x' * 32768
        self.graph.query("CREATE (:B {payload: $payload})", {'payload': payload})
        self.graph.query(
            "UNWIND range(0, 511) AS x CREATE (:A:B {payload: $payload})",
            {'payload': payload},
        )
        self.graph.query(
            "UNWIND range(0, 63) AS x CREATE ({payload: $payload})",
            {'payload': payload},
        )

        reply = self.conn.execute_command(
            "GRAPH.MEMORY", "USAGE", GRAPH_ID, "SAMPLES", 10000
        )
        report = _as_map(reply)
        by_label = _as_map(report["amortized_node_attributes_by_label_sz_mb"])

        self.env.assertGreater(by_label["B"], 0)
        self.env.assertEquals(by_label["A"], 0)
        self.env.assertGreater(
            report["amortized_unlabeled_nodes_attributes_sz_mb"], 0
        )
