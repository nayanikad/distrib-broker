package com.dist.assignment.kafka

import com.dist.common.{TestUtils, ZookeeperTestHarness}
import com.dist.simplekafka.TestSocketServer
import com.dist.simplekafka.api.RequestKeys
import com.dist.simplekafka.network.InetAddressAndPort
import com.dist.simplekafka.server.Config
import com.dist.simplekafka.util.ZkUtils.Broker

import scala.jdk.CollectionConverters._

class ControllerTest extends ZookeeperTestHarness {

  test("should elect first server as controller and get all live brokers") {
    val zookeeperClient = new ZookeeperClient(zkClient)
    zookeeperClient.registerBroker(Broker(10, "10.0.0.1", 8000))
    zookeeperClient.registerBroker(Broker(11, "10.0.0.2", 8002))

    val controller = new Controller(zookeeperClient, 10, null)

    controller.startUp()

    zookeeperClient.registerBroker(Broker(12, "10.0.0.2", 8002))

    assert(zookeeperClient.getAllBrokers().size == 3)
    assert(controller.liveBrokers == Set(
      Broker(10, "10.0.0.1", 8000),
      Broker(11, "10.0.0.2", 8002),
      Broker(12, "10.0.0.2", 8002)))
  }

  test("should send LeaderAndFollower requests to all leader and follower brokers for given topicandpartition") {
    val zookeeperClient = new ZookeeperClient(zkClient)
    val broker1 = Broker(10, "10.0.0.1", 8000)
    val broker2 = Broker(11, "10.0.0.2", 8002)
    val broker3 = Broker(12, "10.0.0.3", 8002)

    zookeeperClient.registerBroker(broker1)

    val config = Config(10, "10.0.0.1", 8000, zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val socketServer = new TestSocketServer(config)

    val controller = new Controller(zookeeperClient, 10, socketServer)

    controller.startUp()

    zookeeperClient.registerBroker(broker2)
    zookeeperClient.registerBroker(broker3)

    assert(zookeeperClient.getAllBrokers().size == 3)
    assert(controller.liveBrokers == Set(broker1, broker2, broker3))

    val topicCommand = new CreateTopicCommand(zookeeperClient)
    topicCommand.createTopic("topic1", 2, 1)

    TestUtils.waitUntilTrue(() ⇒ {
      socketServer.messages.size() == 5 && socketServer.toAddresses.asScala.toSet.size == 3
    }, "waiting for leader and replica requests handled in all brokers", 2000)

    assert(socketServer.messages.asScala.count(m ⇒ m.requestId == RequestKeys.LeaderAndIsrKey) == 2)
    assert(socketServer.messages.asScala.count(m ⇒ m.requestId == RequestKeys.UpdateMetadataKey) === 3)

    assert(socketServer.toAddresses.asScala.toSet == Set(InetAddressAndPort.create(broker1.host, broker1.port),
      InetAddressAndPort.create(broker2.host, broker2.port),
      InetAddressAndPort.create(broker3.host, broker3.port)))

  }

}
