package com.dist.assignment.kafka

import com.dist.common.{TestUtils, ZookeeperTestHarness}
import com.dist.simplekafka.util.ZKStringSerializer
import com.dist.simplekafka.util.ZkUtils.Broker
import org.I0Itec.zkclient.ZkClient

class ZookeeperClientTest extends ZookeeperTestHarness {

  test("should register broker") {
    val zookeeperClient = new ZookeeperClient(zkClient)
    zookeeperClient.registerBroker(Broker(10, "10.0.0.1", 8000))
    zookeeperClient.registerBroker(Broker(11, "10.0.0.2", 8002))

    assert(2 == zookeeperClient.getAllBrokers().size)
  }

  test("should get notified when a broker is registered") {

    val zkClient1 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val client1 = new ZookeeperClient(zkClient1)

    val listener = new BrokerChangeListener(client1)
    client1.subscribeChangeListener(listener)

    client1.registerBroker(Broker(10, "10.0.0.1", 8000))

    val zkClient2 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val client2 = new ZookeeperClient(zkClient2)
    client2.registerBroker(Broker(11, "10.0.0.2", 8000))

    val zkClient3 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val client3 = new ZookeeperClient(zkClient3)
    client3.registerBroker(Broker(12, "10.0.0.3", 8000))

    TestUtils.waitUntilTrue(() => {
      listener.liveBrokers.size === 3
    }, pause = 1000, msg = "live brokers count should be 3")

    zkClient2.close()
    zkClient3.close()

    TestUtils.waitUntilTrue(() => {
      listener.liveBrokers.size === 1
    }, pause = 1000, msg = "live brokers count should be 1")

  }

}
