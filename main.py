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
from future.utils import iteritems
from builtins import range
import logging


class ConfigNotFound(Exception):
    pass


class QueueBalancer:
    def __init__(self):
        self.log = logging.getLogger(__name__)
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(process)d - %(levelname)s - %(funcName)s - %(message)s')
        ch.setFormatter(formatter)
        self.log.addHandler(ch)
        levels = {"debug": logging.DEBUG, "info": logging.INFO}

        config = self.load_config()
        username = config.get("default", "username")
        password = config.get("default", "password")
        vhost = config.get("default", "vhost")
        # we need to replace any / in the vhost
        vhost = vhost.replace("/", "%2f")
        hostname = config.get("default", "hostname")
        port = config.get("default", "port")

        try:
            log_level = config.get("default", "log_level")
        except NoOptionError:
            log_level = "info"

        self.log.setLevel(levels.get(log_level, logging.INFO))

        self.log.debug("Starting program")
        self.log.debug("Got hostname {}".format(hostname))
        self.log.debug("Got vhost {}".format(vhost))
        self.log.debug("Got port {}".format(port))
        self.log.debug("Got username {}".format(username))

        # params for http api user
        self.auth = (username, password)
        full_host = "http://{}:{}".format(hostname, port)
        self.nodes_url = "{}/api/nodes".format(full_host)
        self.queues_url = "{}/api/queues/{}".format(full_host, vhost)
        self.queue_status_url = self.queues_url + "/{}"
        self.policy_url = "{}/api/policies/{}".format(full_host, vhost) + "/{}"
        self.sync_url = "{}/api/queues/{}".format(full_host, vhost) + "/{}/actions"

        self.policy_create = {"pattern": "", "ha-mode": "exactly", "ha-params": 1, "priority": 990, "apply-to": "queues"}
        self.policy_new_master = {"pattern": "", "ha-mode": "nodes", "ha-params": [], "priority": 992, "apply-to": "queues"}

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
        # for easy of use (note(itxaka): for whom? for what?)
        self.config = config

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
        response = requests.get(self.queues_url, auth=self.auth).json()
        self.log.debug("Returning response: {}".format(response))
        return response

    def ordered_queue_list(self):
        # type: (None) -> dict
        """

        :return: a dictionary of hosts and their queues
        """
        queues_ordered_by_host = {}
        for q in self.get_queues():
            if q["node"] in queues_ordered_by_host:
                queues_ordered_by_host[q["node"]].append(q["name"])
            else:
                queues_ordered_by_host[q["node"]] = [q["name"]]
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
        self.log.debug("Optimal distribution calculated is: {}".format(proper_distribution))
        return proper_distribution

    def fill_queue_with_overloaded_nodes(self, queues_ordered_by_host, distribution):
        # type: (dict) -> None
        for node, extra_queues in iteritems(distribution):
            if extra_queues > 0:
                self.log.debug("Found that node {} has {} extra queues, adding to queue pool".format(node, extra_queues))
                for q in range(0, extra_queues):
                    self.queue_pool.append(queues_ordered_by_host[node][q])

    def fill_queue_with_destination_nodes(self, distribution):
        # type: (dict) -> None
        for node, extra_queues in iteritems(distribution):
            if extra_queues < 0:
                self.log.debug(
                    "Found that node {} has {} missing queues, adding to destiny pool".format(node, abs(extra_queues))
                )
                for q in range(extra_queues, 0):
                    self.destiny_pool.append(node)

    def apply_policy(self, queue_name, target):
        policy_name = self.policy_name(queue_name)
        # copy, not reference as we are changing it
        data = copy.deepcopy(self.policy_create)
        data["pattern"] = "^{}$".format(queue_name)
        self.log.debug("Applying first policy to {}: {}".format(queue_name, data))
        # move policy to just 1 mirror (so no slaves)
        requests.put(self.policy_url.format(policy_name), json=data, auth=self.auth)
        data = copy.deepcopy(self.policy_new_master)
        data["ha-params"] = [target]
        data["pattern"] = "^{}$".format(queue_name)
        self.log.debug("Applying second policy to {}: {}".format(queue_name, data))
        # move queue into its new master
        requests.put(self.policy_url.format(policy_name), json=data, auth=self.auth)

    def delete_policy(self, queue_name):
        policy_name = self.policy_name(queue_name)
        self.log.debug("Deleting policy {}".format(policy_name))
        requests.delete(self.policy_url.format(policy_name), auth=self.auth)

    def check_status(self, queue_name):
        return requests.get(self.queue_status_url.format(queue_name), auth=self.auth).json()

    def sync_queue(self, queue_name):
        requests.post(self.sync_url.format(queue_name), json={"action": "sync"}, auth=self.auth)

    def go(self):
        self.log.debug("Starting queue balancing")
        # TODO: main, order below
        # get queues
        # order them
        # obtain the optimal distribution
        # fill the overloaded pool
        # fill the destination pool
        # pop queue from the overloaded pool
        # pop node from the destiny pool
        # apply policy to queue
        # sync queue? not sure if needed, may be needed on high loads but it can introduce more load
        # check queue status, slaves should be empty, queue should be moved to new node
        # delete policy
        # check queue status? or leave it to rabbit to sync itself? (it does sync itself, it takes a couple of seconds)
        # add some error catching to reinsert the target into the destiny pool if something fails
        # do the same for the overloaded? maybe we should just ignore the errors for now? it should be ok to relaunch
        # this several times so no biggie


if __name__ == '__main__':
    pass
