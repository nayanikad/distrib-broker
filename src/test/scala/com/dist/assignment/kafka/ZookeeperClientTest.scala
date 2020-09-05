package com.dist.assignment.kafka

import com.dist.common.ZookeeperTestHarness
import com.dist.simplekafka.util.ZkUtils.Broker

class ZookeeperClientTest extends ZookeeperTestHarness {

  test("should register broker") {
    val zookeeperClient = new ZookeeperClient(zkClient)
    zookeeperClient.registerBroker(Broker(10, "10.0.0.1", 8000))
    zookeeperClient.registerBroker(Broker(11, "10.0.0.2", 8002))

    assert(2 == zookeeperClient.getAllBrokers().size)
  }

}
