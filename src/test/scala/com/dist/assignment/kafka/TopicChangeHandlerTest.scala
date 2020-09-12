package com.dist.assignment.kafka

import com.dist.common.{TestUtils, ZookeeperTestHarness}
import com.dist.simplekafka.PartitionReplicas
import com.dist.simplekafka.util.ZkUtils.Broker

class TopicChangeHandlerTest extends ZookeeperTestHarness {

  class TestContext {
    var replicas: Seq[PartitionReplicas] = List()

    def leaderAndIsr(topicName: String, replicas: Seq[PartitionReplicas]) = {
      this.replicas = replicas
    }
  }

  test("should register for topic change and get replica assignments") {
    val zookeeperClient = new ZookeeperClient(zkClient)
    zookeeperClient.registerBroker(Broker(10, "10.0.0.1", 8000))
    zookeeperClient.registerBroker(Broker(11, "10.0.0.2", 8002))

    val command = new CreateTopicCommand(zookeeperClient)

    val testContext = new TestContext
    val topicChangeHandler = new TopicChangeHandler(zookeeperClient, testContext.leaderAndIsr)
    zookeeperClient.subscribeTopicChangeListener(topicChangeHandler)

    command.createTopic("topic1", 2, 3)

    TestUtils.waitUntilTrue(() => {
      testContext.replicas.size == 2
    }, "Waiting for topic metadata", 1000)


  }

}
