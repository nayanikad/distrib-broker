package com.dist.assignment.kafka

import com.dist.common.ZookeeperTestHarness
import com.dist.simplekafka.util.ZkUtils.Broker

class CreateTopicCommandZookeeperTest extends ZookeeperTestHarness {

  test("should create persistent path for topic with topic partition assignments in zookeeper") {
    val zookeeperClient = new ZookeeperClient(zkClient)
    zookeeperClient.registerBroker(Broker(10, "10.0.0.1", 8000))
    zookeeperClient.registerBroker(Broker(11, "10.0.0.2", 8002))

    val command = new CreateTopicCommand(zookeeperClient)
    command.createTopic("topic1", 2, 3)

    val topics = zookeeperClient.getAllTopics()
    assert(topics.size == 1)

    val partitionReplicas = topics("topic1")
    assert(partitionReplicas.size == 2)
    partitionReplicas.foreach(p => assert(p.brokerIds.size == 3))



  }

}
