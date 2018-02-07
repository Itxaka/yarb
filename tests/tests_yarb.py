import unittest
from mock import Mock, patch
from yarb.yarb import QueueBalancer, ConfigNotFound
from yarb.yarb import main as yarb_main
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
        self.set_balancer_object()

    def set_balancer_object(self):
        self.config = u"""[default]
username = username
password = password
vhost = vhost
hostname = hostname
port = 1
log_level = error
threads = 1
wait_time = 1"""

        with patch('yarb.yarb.open') as mo:
            mo.__enter__ = Mock(return_value=(Mock(), None))
            mo.__exit__ = Mock(return_value=None)
            mo.return_value = io.StringIO(self.config)
            self.balancer = QueueBalancer()

    class MockResponse:
        def __init__(self, json, status_code):
            self.json_response = json
            self.status_code = status_code

        def json(self):
            return self.json_response

    def test_load_config(self):
        self.assertRaises(ConfigNotFound, QueueBalancer)

    @patch("yarb.yarb.QueueBalancer")
    def test_main(self, balancer):
        yarb_main()
        self.assertTrue(balancer.called)

    @patch("yarb.yarb.QueueBalancer.ordered_queue_list")
    @patch("yarb.yarb.QueueBalancer.calculate_queue_distribution")
    @patch("yarb.yarb.QueueBalancer.fill_queue_with_overloaded_nodes")
    def test_prepare(self, fill_queue, calculate_queue, ordered_queue):
        ordered_queue.return_value = self.unbalanced_queue_list
        calculate_queue.return_value = self.balancer.calculate_queue_distribution
        fill_queue.wraps = self.balancer.fill_queue_with_overloaded_nodes
        self.balancer.prepare()
        ordered_queue.assert_called_once()
        calculate_queue.assert_called_once_with(self.unbalanced_queue_list)
        fill_queue.assert_called_once()

    @patch('requests.Session.get')
    def test_get_queues(self, session_get_mock):
        session_get_mock.return_value = self.MockResponse({}, 200)
        self.balancer.get_queues()
        session_get_mock.assert_called_once_with("http://hostname:1/api/queues/vhost")

    @patch('requests.Session.get')
    def test_get_nodes(self, session_get_mock):
        session_get_mock.return_value = self.MockResponse({}, 200)
        self.balancer.get_nodes()
        session_get_mock.assert_called_once_with("http://hostname:1/api/nodes")

    @patch('requests.Session.delete')
    def test_delete_queue(self, session_delete_mock):
        session_delete_mock.return_value = self.MockResponse({}, 204)
        self.balancer.delete_queue("test-queue")
        session_delete_mock.assert_called_once_with(
            "http://hostname:1/api/queues/vhost/test-queue",
            params={"if-empty": "true"},
            timeout=5  # TODO: hardcoded, should be modifiable
        )

    @patch('yarb.yarb.QueueBalancer.delete_queue')
    def test_delete_queue_action(self, delete_queue_mock):
        try:
            # python2
            self.assertEqual(self.balancer.semaphore._Semaphore__value, 1)
        except AttributeError:
            # python3
            self.assertEqual(self.balancer.semaphore._value, 1)
        # we need to manually acquire as we are only testing the release
        self.balancer.semaphore.acquire()
        self.balancer.queue_pool.append("test-queue")
        self.balancer.wait_time = 0.1
        self.balancer.delete_queue_action()
        delete_queue_mock.assert_called_once_with("test-queue")
        try:
            # python2
            self.assertEqual(self.balancer.semaphore._Semaphore__value, 1)
        except AttributeError:
            # python3
            self.assertEqual(self.balancer.semaphore._value, 1)

    def test_delete_queue_action_empty_queue_releases_semaphore(self):
        try:
            # python2
            self.assertEqual(self.balancer.semaphore._Semaphore__value, 1)
        except AttributeError:
            # python3
            self.assertEqual(self.balancer.semaphore._value, 1)
        # we need to manually acquire as we are only testing the release
        self.balancer.semaphore.acquire()
        self.balancer.delete_queue_action()
        try:
            # python2
            self.assertEqual(self.balancer.semaphore._Semaphore__value, 1)
        except AttributeError:
            # python3
            self.assertEqual(self.balancer.semaphore._value, 1)

    @patch('yarb.yarb.QueueBalancer.delete_queue')
    @patch("yarb.yarb.QueueBalancer.get_queues")
    @patch("yarb.yarb.QueueBalancer.get_nodes")
    def test_go(self, get_nodes_mock, get_queues_mock, delete_queue_mock):
        self.balancer.wait_time = 0.1
        self.balancer.queue_pool.append("test-queue")
        self.balancer.go()
        get_nodes_mock.assert_called_once_with()
        get_queues_mock.assert_called_once_with()
        delete_queue_mock.assert_called_once_with("test-queue")

    def test_queuebalancer_class_configures_correctly(self):
        self.assertEqual(self.balancer.wait_time, 1)
        self.assertEqual(self.balancer.nodes_url, "http://hostname:1/api/nodes")
        self.assertEqual(self.balancer.queues_url, "http://hostname:1/api/queues/vhost")
        self.assertEqual(self.balancer.queue_status_url, "http://hostname:1/api/queues/vhost/{}")
        self.assertEqual(self.balancer.queue_delete_url, "http://hostname:1/api/queues/vhost/{}")

    def test_calculate_queue_distribution_with_balanced_queues(self):
        distributed = self.balancer.calculate_queue_distribution(self.balanced_queue_list)
        self.assertEqual(distributed, {"node1": 0, "node2": 0, "node3": 0})

    def test_calculate_queue_distribution_with_unbalanced_queues(self):
        distributed = self.balancer.calculate_queue_distribution(self.unbalanced_queue_list)
        self.assertEqual(distributed, {"node1": 4, "node2": -2, "node3": -2})

    def test_calculate_queue_distribution_returns_integers_even_if_unbalanced(self):
        distributed = self.balancer.calculate_queue_distribution(self.unbalanced_par_queue_list)
        for k in distributed.keys():
            self.assertIs(type(distributed[k]), int)

    @patch("yarb.yarb.QueueBalancer.get_queues")
    @patch("yarb.yarb.QueueBalancer.get_nodes")
    def test_ordered_queue_list(self, get_nodes_mock, get_queues_mock):
        get_queues_mock.return_value = self.queues_list
        get_nodes_mock.return_value = self.nodes_list
        # from http api list to nice dictionary
        self.assertEqual(
            self.balancer.ordered_queue_list(), {"1": ["q1", "q2", "q3"], "3": ["q5"], "2": ["q4"]}
        )

    def test_fill_queue_with_overloaded_nodes(self):
        # should raise IndexError as the queue is empty
        self.assertRaises(IndexError, self.balancer.queue_pool.pop)
        self.balancer.fill_queue_with_overloaded_nodes(
            self.unbalanced_queue_list,
            {"node1": 4, "node2": -2, "node3": -2}
        )
        # should have inserted the q1 to q4 queues
        queues = [self.balancer.queue_pool.pop() for _ in range(0, 4)]
        self.assertIn("q1", queues)
        self.assertIn("q2", queues)
        self.assertIn("q3", queues)
        self.assertIn("q4", queues)
        # should not be anything else in the queue left
        self.assertRaises(IndexError, self.balancer.queue_pool.popleft)


if __name__ == '__main__':
    unittest.main()
