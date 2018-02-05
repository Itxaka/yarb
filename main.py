#
# Copyright 2018, SUSE Linux GmbH
#
# Licensed under the GNU GPL v2. For full terms see the file LICENSE.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from time import sleep

import requests
try:
    from ConfigParser import ConfigParser, NoOptionError
except ImportError:
    from configparser import ConfigParser, NoOptionError
from os.path import expanduser
try:
    from Queue import deque
except ImportError:
    from queue import deque
import copy
try:
    from builtins import range
except ImportError:
    from __builtin__ import range
import logging
import threading
import signal
import time


class ConfigNotFound(Exception):
    pass


class QueueBalancer:
    class SignalHandler:
        def __call__(self, signum, frame):
            print("Exit called, will wait for running threads to stop before exiting.")
            for t in threading.enumerate():
                if t is threading.currentThread():  # this is the main thread, we cannot join it
                    continue
                t.join()  # wait for threads to end before exiting
            exit(0)

    def __init__(self):
        self.retries = 6  # this number * self.wait_time == total number to wait for the check to expire
        signal.signal(signal.SIGINT, self.SignalHandler())
        self.log = logging.getLogger(__name__)
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(thread)d - %(levelname)s - %(funcName)s - %(message)s')
        ch.setFormatter(formatter)
        self.log.addHandler(ch)
        levels = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "error": logging.ERROR
        }

        config = self.load_config()
        username = config.get("default", "username")
        password = config.get("default", "password")
        vhost = config.get("default", "vhost")
        # we need to replace any / in the vhost
        vhost = vhost.replace("/", "%2f")
        hostname = config.get("default", "hostname")
        port = config.getint("default", "port")
        log_level = config.get("default", "log_level")
        threads = config.getint("default", "threads")
        self.wait_time = config.getfloat("default", "wait_time")

        self.log.setLevel(levels.get(log_level, logging.INFO))
        self.semaphore = threading.Semaphore(threads)

        self.log.debug("Starting program")
        self.log.debug("Got hostname {}".format(hostname))
        self.log.debug("Got vhost {}".format(vhost))
        self.log.debug("Got port {}".format(port))
        self.log.debug("Got username {}".format(username))

        # params for http api user
        self.conn = requests.Session()
        self.conn.auth = (username, password)
        self.conn.headers = {"content-type": "application/json"}
        full_host = "http://{}:{}".format(hostname, port)
        self.nodes_url = "{}/api/nodes".format(full_host)
        self.queues_url = "{}/api/queues/{}".format(full_host, vhost)
        self.queue_status_url = self.queues_url + "/{}"
        self.policy_url = "{}/api/policies/{}".format(full_host, vhost) + "/{}"
        self.sync_url = "{}/api/queues/{}".format(full_host, vhost) + "/{}/actions"

        self.policy_new_master = {
            "pattern": "",
            "definition": {
                "ha-mode": "nodes",
                "ha-params": []
            },
            "priority": 600,
            "apply-to": "queues"
        }

        # using deqes to sync all the things
        # queue_pool is a deqe where the "extra" queues go. Extra queues mean, queues that should not be in that node
        # because they are over the total_queues divided by the number of nodes minus the current queues in the node
        # so a node with 20 queues, in a 3-node cluster with 30 queues in total would have:
        # 30/3 = 10 - 20 = +10 (patent pending on this incredible algorithm)
        # this indicates that for the optimal balance this node should get rid of 10 queues
        # those 10 queues, we pick at random* from the node and store them in the pool so we can pop queues from there
        # and work on them safely
        # * not really random right now, sue me.

        # queue to store the nodes that have extra queues on them
        self.queue_pool = deque()
        self.log.debug("Created queue pool: {}".format(self.queue_pool))

        # destiny pool is a more simple deque which contains the target nodes for moving the queues to
        # the values are calculated as above but this are the ones that get a negative value, indicating
        # that they are missing a number of queues to reach the optimal balance(tm)
        # queue to store the nodes that are the destination for extra queues
        self.destiny_pool = deque()
        self.log.debug("Created destiny pool: {}".format(self.destiny_pool))

    def load_config(self):
        config_file = expanduser("~/.queue_balancer.conf")
        config = ConfigParser()

        try:
            with open(config_file) as f:
                config.readfp(f)
                self.log.debug("Config loaded")
        except IOError:
            raise ConfigNotFound("File {} with the config not found.".format(config_file))

        return config

    @staticmethod
    def policy_name(queue_name):
        return "{}-balancer-temp".format(queue_name)

    def get_queues(self):
        response = self.conn.get(self.queues_url).json()
        return response

    def get_nodes(self):
        response = self.conn.get(self.nodes_url).json()
        return response

    def ordered_queue_list(self):
        # type: (None) -> dict
        """

        :return: a dictionary of hosts and their queues
        """
        # FIXME: this assumes that all nodes have at least 1 queue. if they dont they wont appear here.
        # so maybe a different approach to getting the cluster nodes is preferred (/api/nodes ?)
        queues = self.get_queues()
        nodes = self.get_nodes()
        queues_ordered_by_host = {node["name"]: [] for node in nodes}
        self.log.info("There is a total of {} queues".format(len(queues)))
        for queue in queues:
            queues_ordered_by_host[queue["node"]].append(queue["name"])
        return queues_ordered_by_host

    def calculate_queue_distribution(self, queue_list):
        # type: (dict) -> dict
        """
        calculates the difference in queues between nodes
        :param queue_list: an ordered list of queues ordered by host
        :return: a dict with how many queues needs to be removed/added to each host
        """
        total_queues = sum([len(queue_list[i]) for i in queue_list])
        proper_distribution = {}
        for node in queue_list:
            proper_distribution[node] = int(len(queue_list[node]) - (total_queues / len(queue_list.keys())))
        self.log.info("Optimal distribution calculated is: {}".format(proper_distribution))
        return proper_distribution

    def fill_queue_with_overloaded_nodes(self, queues_ordered_by_host, distribution):
        # type: (dict) -> None
        for node, extra_queues in distribution.items():
            if extra_queues > 0:
                self.log.debug("Found that node {} has {} extra queues, adding to queue pool".format(node, extra_queues))
                for q in range(0, extra_queues):
                    self.queue_pool.append(queues_ordered_by_host[node][q])

    def fill_queue_with_destination_nodes(self, distribution):
        # type: (dict) -> None
        for node, extra_queues in distribution.items():
            if extra_queues < 0:
                self.log.debug(
                    "Found that node {} has {} missing queues, adding to destiny pool".format(node, abs(extra_queues))
                )
                for q in range(extra_queues, 0):
                    self.destiny_pool.append(node)

    def apply_policy(self, queue_name, target):
        # type: (str, str) -> None
        policy_name = self.policy_name(queue_name)
        data = copy.deepcopy(self.policy_new_master)
        data["definition"]["ha-params"] = [target]
        data["pattern"] = "^{}$".format(queue_name)
        self.log.info("Applying policy to {}: {}".format(queue_name, data))
        # move queue into its new master
        self.conn.put(self.policy_url.format(policy_name), json=data)

    def wait_until_queue_moved_to_new_master(self, queue_name, target):
        # type: (str, str) -> None
        retries = self.retries
        while True:
            if retries <= 0:
                self.log.error("Queue {} expired the number of retries. Skipping it.".format(queue_name))
                break
            status = self.check_status(queue_name)
            self.log.debug("queue {} status is {}".format(queue_name, status))
            if "error" in status:
                self.log.error("Found an error while checking queue {}: {}".format(queue_name, status["error"]))
                # end checking, queue might have disappear
                break
            else:
                if status["node"] != target:
                    self.log.info("Queue {} still not moved to {}. {} retries left until skipping".format(queue_name, target, retries))
                    retries -= 1
                    sleep(self.wait_time)
                else:
                    break

    def delete_policy(self, queue_name):
        # type: (str) -> None
        policy_name = self.policy_name(queue_name)
        self.log.info("Deleting policy {}".format(policy_name))
        response = self.conn.delete(self.policy_url.format(policy_name))
        self.log.debug("Response of delete_policy: {}".format(response.status_code))

    def check_status(self, queue_name):
        # type: (str) -> dict
        try:
            response = self.conn.get(self.queue_status_url.format(queue_name)).json()
        except requests.ConnectionError:
            response = {"error": "Connection error"}
        return response

    def sync_queue(self, queue_name):
        # type: (str) -> None
        self.log.info("Syncing queue {}".format(queue_name))
        response = self.conn.post(self.sync_url.format(queue_name), json={"action": "sync"})
        self.log.debug("Response from sync_queue: {}".format(response.status_code))

    def prepare(self):
        # get ordered queues
        queues = self.ordered_queue_list()
        # obtain the optimal distribution
        distribution = self.calculate_queue_distribution(queues)
        # fill the overloaded pool
        self.fill_queue_with_overloaded_nodes(queues, distribution)
        # fill the destination pool
        self.fill_queue_with_destination_nodes(distribution)

    def move_queue(self):
        start = time.time()
        try:
            # pop queue from the overloaded pool
            queue = self.queue_pool.pop()
        except IndexError:
            self.log.info("No more queues in the overloaded pool")
            self.semaphore.release()
            return
        try:
            # pop node from the destiny pool
            target = self.destiny_pool.pop()
        except IndexError:
            self.log.info("No more nodes in the destination pool")
            self.semaphore.release()
            return
        self.log.info("Started moving queue {} to {}".format(queue, target))
        self.apply_policy(queue, target)
        # wait until the queue has moved to the new master
        self.wait_until_queue_moved_to_new_master(queue, target)
        # delete policy
        self.delete_policy(queue)
        self.log.info("Finished moving queue {} to {}. It took {} seconds".format(queue, target, time.time() - start))
        self.semaphore.release()

    def go(self):
        start = time.time()
        self.log.info("Starting queue balancing")
        self.prepare()
        self.log.debug("Queue pool has {} items".format(len(self.queue_pool)))
        self.log.debug("Destination pool has {} items".format(len(self.destiny_pool)))
        # start threading here
        while len(self.queue_pool) > 0 or len(self.destiny_pool) > 0:
            self.semaphore.acquire()
            t = threading.Thread(target=self.move_queue)
            self.log.debug("Starting new thread: {}".format(t.getName()))
            t.start()

        # some housekeeping, if we dont have anymore queues to move that's ok but there may still be
        # threads doing some work, so find and join them so we wait for them to finish
        for t in threading.enumerate():
            if t is threading.currentThread():  # this is the main thread, we cannot join it
                continue
            self.log.info("Waiting for thread {} to finish".format(t.getName()))
            t.join()

        self.log.info("Finished moving queues. It took {} seconds".format(time.time() - start))


if __name__ == '__main__':
    q = QueueBalancer()
    q.go()
