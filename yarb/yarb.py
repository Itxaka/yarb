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
            # 128+2 SIGINT <http://www.tldp.org/LDP/abs/html/exitcodes.html>
            exit(130)

    def __init__(self):
        signal.signal(signal.SIGINT, self.SignalHandler())
        self.log = logging.getLogger(__name__)
        ch = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(thread)d - %(levelname)s - %(funcName)s - %(message)s'
        )
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
        self.queue_delete_url = self.queues_url + "/{}"

        # using deqes to sync all the things
        # queue_pool is a deqe where the "extra" queues go. Extra queues mean, queues that should
        # not be in that node because they are over the total_queues divided by the number of
        # nodes minus the current queues in the node so a node with 20 queues, in a 3-node cluster
        # with 30 queues in total would have: 30/3 = 10 - 20 = +10
        # this indicates that for the optimal balance this node should get rid of 10 queues
        # those 10 queues, we pick at random* from the node and store them in the pool
        # * not really random right now, sue me.

        # queue to store the queues to be deleted
        self.queue_pool = deque()
        self.log.debug("Created queue pool: {}".format(self.queue_pool))

    def load_config(self):
        config_file = expanduser("~/.yarb.conf")
        config = ConfigParser()

        try:
            with open(config_file) as f:
                config.readfp(f)
                self.log.debug("Config loaded")
        except IOError:
            raise ConfigNotFound("File {} with the config not found.".format(config_file))

        return config

    def get_queues(self):
        response = self.conn.get(self.queues_url)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            raise Exception(
                "Received a 401 http error. Are your username/password options correct? "
                "Do your user has the 'administrator' tag?"
            )

    def get_nodes(self):
        response = self.conn.get(self.nodes_url).json()
        return response

    def ordered_queue_list(self):
        # type: (None) -> dict
        """

        :return: a dictionary of hosts and their queues
        """
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
        total = sum([len(queue_list[i]) for i in queue_list])
        distribution = {}
        for node in queue_list:
            distribution[node] = int(len(queue_list[node]) - (total / len(queue_list.keys())))
        self.log.info("Optimal distribution calculated is: {}".format(distribution))
        return distribution

    def fill_queue_with_overloaded_nodes(self, queues_ordered_by_host, distribution):
        # type: (dict) -> None
        for node, extra_queues in distribution.items():
            if extra_queues > 0:
                self.log.debug(
                    "Found that node {} has {} extra queues, adding to queue pool".format(
                        node, extra_queues
                    )
                )
                for q in range(0, extra_queues):
                    self.queue_pool.append(queues_ordered_by_host[node][q])

    def prepare(self):
        # type: (None) -> None
        # get ordered queues
        queues = self.ordered_queue_list()
        # obtain the optimal distribution
        distribution = self.calculate_queue_distribution(queues)
        # fill the overloaded pool
        self.fill_queue_with_overloaded_nodes(queues, distribution)

    def delete_queue(self, queue_name):
        # type: (str) -> None
        try:
            response = self.conn.delete(
                self.queue_delete_url.format(queue_name), params={"if-empty": "true"}, timeout=5
            )
            self.log.debug(
                "Response to delete queue {}: {}".format(queue_name, response.status_code)
            )
        except requests.Timeout:
            self.log.error("Response to delete queue {} timed out, skipping.")

    def delete_queue_action(self):
        # type: (None) -> None
        start = time.time()
        try:
            # pop queue from the overloaded pool
            queue = self.queue_pool.pop()
        except IndexError:
            self.log.info("No more queues in the overloaded pool")
            self.semaphore.release()
            return

        self.delete_queue(queue)
        time.sleep(self.wait_time)
        self.log.info(
            "Finished deleting queue {}. It took {} seconds".format(queue, time.time() - start)
        )
        self.semaphore.release()

    def go(self):
        # type: (None) -> None
        start = time.time()
        self.log.info("Starting queue balancing")
        self.prepare()
        self.log.debug("Queue pool has {} items".format(len(self.queue_pool)))
        # start threading here
        while len(self.queue_pool) > 0:
            self.semaphore.acquire()
            t = threading.Thread(target=self.delete_queue_action)
            self.log.debug("Starting new thread: {}".format(t.getName()))
            t.start()

        # some housekeeping, if we dont have anymore queues to move that's ok but there may still be
        # threads doing some work, so find and join them so we wait for them to finish
        for t in threading.enumerate():
            if t is threading.currentThread():  # this is the main thread, we cannot join it
                continue
            self.log.info("Waiting for thread {} to finish".format(t.getName()))
            t.join()

        self.log.info("Finished deleting queues. It took {} seconds".format(time.time() - start))


def main():
    q = QueueBalancer()
    q.go()
