package com.dist.assignment.kafka

import com.dist.simplekafka.common.JsonSerDes
import com.dist.simplekafka.util.ZkUtils.Broker
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException

import scala.jdk.CollectionConverters._

class ZookeeperClient(client: ZkClient) {
  def subscribeChangeListener(listener: BrokerChangeListener): Option[List[String]] = {
    val result = client.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }

  val BrokerIdsPath = "/brokers/ids"

  def getBrokerInfo(brokerId: Int): Broker = {
    val data: String = client.readData(brokerPathFor(brokerId))
    JsonSerDes.deserialize(data.getBytes, classOf[Broker])
  }

  def getAllBrokers(): Set[Broker] = {
    client.getChildren(BrokerIdsPath).asScala.map(brokerId => getBrokerInfo(brokerId.toInt)).toSet
  }

  private def brokerPathFor(brokerId: Int) = {
    BrokerIdsPath + "/" + brokerId
  }

  def registerBroker(broker: Broker): Any = {
    val data = JsonSerDes.serialize(broker)
    val path = brokerPathFor(broker.id)
    createEphemeralPath(client, path, data)
  }

  def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createEphemeral(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
    }
  }

  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  def createPersistentPath(client: ZkClient, path: String, data: String = ""): Unit = {
    try {
      client.createPersistent(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createPersistent(path, data)
      }
    }
  }

}
