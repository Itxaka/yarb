import unittest
try:
    from mock import Mock, patch, mock_open
except ImportError:
    from unittest.mock import Mock, patch, mock_open
from main import QueueBalancer
import io


class TestQueues(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        cls.balanced_queue_list = {
            "node1": {
                "q1", "q2", "q3"
            },
            "node2": {
                "q4", "q5", "q6"
            },
            "node3": {
                "q7", "q8", "q9"
            }
        }

        cls.unbalanced_queue_list = {
            "node1": {
                "q1", "q2", "q3", "q4", "q5", "q6", "q7"
            },
            "node2": {
                "q8"
            },
            "node3": {
                "q9"
            }
        }

        cls.unbalanced_par_queue_list = {
            "node1": {
                "q1", "q2", "q3", "q4", "q5", "q6", "q7"
            },
            "node2": {
                "q8"
            },
            "node3": {
                "q9", "q10"
            }
        }

        cls.queues_list = [
            {"node": "1", "name": "q1"},
            {"node": "1", "name": "q2"},
            {"node": "1", "name": "q3"},
            {"node": "2", "name": "q4"},
            {"node": "3", "name": "q5"}
        ]

    def setUp(self):
        config = u"[default]\nusername = username\npassword = password\nvhost = vhost\nhostname = hostname\nport = port"

        with patch('main.open') as mo:
            mo.__enter__ = Mock(return_value=(Mock(), None))
            mo.__exit__ = Mock(return_value=None)
            mo.return_value = io.StringIO(config)
            self.balancer = QueueBalancer()

    def test_distribution_with_balanced_queues(self):
        """
        should not calculate anything if they are already balanced
        """
        distributed = QueueBalancer.calculate_queue_distribution(self.balanced_queue_list)
        self.assertEquals(distributed, {"node1": 0, "node2": 0, "node3": 0})

    def test_distribution_with_unbalanced_queues(self):
        """
        should properly calculate the distribution
        """
        distributed = QueueBalancer.calculate_queue_distribution(self.unbalanced_queue_list)
        self.assertEquals(distributed, {"node1": 4, "node2": -2, "node3": -2})

    def test_distribution_returns_integers_even_if_unbalanced(self):
        """
        should not fail and return integers
        """
        distributed = QueueBalancer.calculate_queue_distribution(self.unbalanced_par_queue_list)
        for k in distributed.keys():
            self.assertIs(type(distributed[k]), int)

    @patch("main.QueueBalancer.get_queues")
    def test_ordered_queue_list(self, get_queues_mock):
        get_queues_mock.return_value = self.queues_list
        # from http api list to nice dictionary
        self.assertEquals(self.balancer.ordered_queue_list(), {"1": ["q1", "q2", "q3"], "3": ["q5"], "2": ["q4"]})

    @unittest.skip("method changed but test not updated yet")
    def test_queue_pool(self):
        # should raise IndexError as the queue is empty
        self.assertRaises(IndexError, self.balancer.queue_pool.pop)
        self.balancer.fill_queue_with_overloaded_nodes({"node1": 10, "node2": 0, "node3": -10})
        # should be able to pop
        self.assertEquals(self.balancer.queue_pool.pop(), {"node1": 10})

    def test_destination_pool(self):
        # should raise IndexError as the queue is empty
        self.assertRaises(IndexError, self.balancer.queue_pool.pop)
        self.balancer.fill_queue_with_destination_nodes({"node1": 10, "node2": 0, "node3": -10})
        # should be able to pop
        self.assertEquals(self.balancer.destiny_pool.pop(), {"node3": -10})


if __name__ == '__main__':
    unittest.main()