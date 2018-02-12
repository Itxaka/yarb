# yarb
Yet Another Rabbit Balancer


This script tackles the issue of unbalanced queues in a rabbitmq mirrored cluster.

Usually if all your nodes are running all the time, your queues will be balanced alrigth, but after a node failure, or even a cluster failure, there is usually one node (lets call it master) that comes before the other ones. This is reflected in the rabbitmq OCF agent[0] as well, which will bring one node as master and then restart the rest of the nodes to join the master.

In environments where the clients have a short reconnection retry, this usually ends up making the master the node which receives all connections and creates all queues, marking itself as the queue master because the other nodes are still not up. This makes the whole cluster really unbalanced and can lead to the master node being overwhelmed, as it has to deal with all the queues and connections.

You could apply a policy to the queues that you want moved and then remove the policy[1], but that has to be done manually or with a script[2] that its really slow, as it does all the queues in a serial way, making it not viable for environments with a high number of queues. You could try to apply the policy to a high number of queues, or automate it, but that has proven to lead to broken queues in my tests[3] so I would recommend testing it before doing so.


How does yarb works?
-----------

 - Calculates the optimal distribution and only applies to extra queues

> There is no need to manually search for the queues that need moving or to do any actions to all of them. Yarb will check the total number of queues in the cluster, know how many are over or under in a given node and just apply actions on those, making it much faster


 - Uses threading to do many queues on parallel

> As it uses the HTTP-API from the rabbitmq-management plugin, we can do hundreds of requests at the same time. Other tools just do actions sequentially which is not viable on environments with a high number of queues. Threading number is user configurable so you can go as low as you want to avoid affecting the cluster performance.

 - Deletes queues that are empty
 
 > If your client is written properly, when connection it will declare the queue, and it will reconnect on failure. This means we can safely delete a queue that has 0 messages and clients will reconnect instantly and recreate the queue. On queue recreation, rabbitmq `queue_master_locator` will trigger in at it will be balanced according to your configuration, with the whole cluster up, so it will be properly set on the appropriate node. **This only works if your clients reconnect automatically, which they should do**
 
 That's it! There is not much to it, as it should be simple and fast.
 
 
 Caveats
 --------
 
 The user configured for yarb should have the `administrator` tag, as listing the nodes in a cluster and deleting queues its and administrator-level task. You can find how to add tags here[4]
 
 There is a hit to performance when running the script and when deleting a high number of queues in parallel with low wait times (<= 1 second) so always try to run the script from outside the cluster and do some testing on the correct number of threads that you should use, bumping it if there is room for it.
 
 
 Configuration
 ---------
 
 Yarb will try to read the config from `~/.yarb.conf` 
 You can find an example file under the tools directory with comments on what each option means.


# Links 
[0] https://github.com/rabbitmq/rabbitmq-server-release/blob/master/scripts/rabbitmq-server-ha.ocf

[1] https://groups.google.com/forum/#!msg/rabbitmq-users/bJNcrDVhWiU/6oMO0DjNQ4oJ

[2] https://github.com/rabbitmq/support-tools/blob/master/scripts/rebalance-queue-masters

[3] https://groups.google.com/forum/#!topic/rabbitmq-users/ocd73pCYlUM

[4] https://www.rabbitmq.com/man/rabbitmqctl.8.html#set_user_tags
