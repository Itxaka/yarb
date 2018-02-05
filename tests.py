import unittest
try:
    from mock import Mock, patch, mock_open
except ImportError:
    from unittest.mock import Mock, patch, mock_open
from main import QueueBalancer
import io
try:
    from builtins import range
except ImportError:
    from __builtin__ import range


class TestQueues(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        cls.balanced_queue_list = {
            "node1": ["q1", "q2", "q3"],
            "node2": ["q4", "q5", "q6"],
            "node3": ["q7", "q8", "q9"]
        }

        cls.unbalanced_queue_list = {
            "node1": ["q1", "q2", "q3", "q4", "q5", "q6", "q7"],
            "node2": ["q8"],
            "node3": ["q9"]
        }

        cls.unbalanced_par_queue_list = {
            "node1": ["q1", "q2", "q3", "q4", "q5", "q6", "q7"],
            "node2": ["q8"],
            "node3": ["q9", "q10"]
        }

        cls.queues_list = [
            {"node": "1", "name": "q1"},
            {"node": "1", "name": "q2"},
            {"node": "1", "name": "q3"},
            {"node": "2", "name": "q4"},
            {"node": "3", "name": "q5"}
        ]

        cls.nodes_list = [
            {"name": "1"},
            {"name": "2"},
            {"name": "3"}
        ]

    def setUp(self):
        config = u"""[default]
username = username
password = password
vhost = vhost
hostname = hostname
port = 1
log_level = error
threads = 1
wait_time = 1"""

        with patch('main.open') as mo:
            mo.__enter__ = Mock(return_value=(Mock(), None))
            mo.__exit__ = Mock(return_value=None)
            mo.return_value = io.StringIO(config)
            self.balancer = QueueBalancer()

    def test_distribution_with_balanced_queues(self):
        """
        should not calculate anything if they are already balanced
        """
        distributed = self.balancer.calculate_queue_distribution(self.balanced_queue_list)
        self.assertEqual(distributed, {"node1": 0, "node2": 0, "node3": 0})

    def test_distribution_with_unbalanced_queues(self):
        """
        should properly calculate the distribution
        """
        distributed = self.balancer.calculate_queue_distribution(self.unbalanced_queue_list)
        self.assertEqual(distributed, {"node1": 4, "node2": -2, "node3": -2})

    def test_distribution_returns_integers_even_if_unbalanced(self):
        """
        should not fail and return integers
        """
        distributed = self.balancer.calculate_queue_distribution(self.unbalanced_par_queue_list)
        for k in distributed.keys():
            self.assertIs(type(distributed[k]), int)

    @patch("main.QueueBalancer.get_queues")
    @patch("main.QueueBalancer.get_nodes")
    def test_ordered_queue_list(self, get_nodes_mock, get_queues_mock):
        get_queues_mock.return_value = self.queues_list
        get_nodes_mock.return_value = self.nodes_list
        # from http api list to nice dictionary
        self.assertEqual(self.balancer.ordered_queue_list(), {"1": ["q1", "q2", "q3"], "3": ["q5"], "2": ["q4"]})

    def test_queue_pool(self):
        # should raise IndexError as the queue is empty
        self.assertRaises(IndexError, self.balancer.queue_pool.pop)
        self.balancer.fill_queue_with_overloaded_nodes(self.unbalanced_queue_list, {"node1": 4, "node2": -2, "node3": -2})
        # should have inserted the q1 to q4 queues
        queues = [self.balancer.queue_pool.pop() for _ in range(0, 4)]
        self.assertIn("q1", queues)
        self.assertIn("q2", queues)
        self.assertIn("q3", queues)
        self.assertIn("q4", queues)
        # should not be anything else in the queue left
        self.assertRaises(IndexError, self.balancer.queue_pool.popleft)

    def test_destination_pool(self):
        # should raise IndexError as the queue is empty
        self.assertRaises(IndexError, self.balancer.destiny_pool.pop)
        self.balancer.fill_queue_with_destination_nodes({"node1": 5, "node2": 0, "node3": -4, "node4": -1})
        # should have inserted node3 4 times and node4 once
        nodes = [self.balancer.destiny_pool.pop() for _ in range(0, 5)]
        self.assertIn("node3", nodes)
        self.assertEqual(4, len([n for n in nodes if n == "node3"]))
        self.assertIn("node4", nodes)
        self.assertEqual(1, len([n for n in nodes if n == "node4"]))
        # should raise IndexError as the queue is empty
        self.assertRaises(IndexError, self.balancer.destiny_pool.pop)


if __name__ == '__main__':
    unittest.main()