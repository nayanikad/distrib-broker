package com.dist.assignment.kafka

import com.dist.simplekafka.common.JsonSerDes
import com.dist.simplekafka.util.ZkUtils
import com.dist.simplekafka.util.ZkUtils.Broker
import com.dist.simplekafka.{ControllerExistsException, LeaderAndReplicas, PartitionReplicas}
import com.fasterxml.jackson.core.`type`.TypeReference
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}

import scala.jdk.CollectionConverters._

class ZookeeperClient(client: ZkClient) {

  val BrokerIdsPath = "/brokers/ids"
  val BrokerTopicsPath = "/brokers/topics"
  val ControllerPath = "/controller"
  val ReplicaLeaderElectionPath = "/topics/replica/leader"

  def createControllerPath(leaderId: String) = {
    try {
      createEphemeralPath(client, ControllerPath, leaderId)
    } catch {
      case _: ZkNodeExistsException => {
        val existingControllerId: String = client.readData(ControllerPath)
        println("Controller exists exception", existingControllerId)
        throw ControllerExistsException(existingControllerId)
      }
    }
  }

  def getAllTopics(): Map[String, List[PartitionReplicas]] = {
    val topics = client.getChildren(BrokerTopicsPath).asScala

    topics.map(topicName => {
      val data: String = client.readData(BrokerTopicsPath + "/" + topicName)
      val partitionReplicas: List[PartitionReplicas] = JsonSerDes.deserialize[List[PartitionReplicas]](data.getBytes, new TypeReference[List[PartitionReplicas]]() {})
      (topicName, partitionReplicas)
    }).toMap
  }

  def getPartitionAssignmentsFor(topicName: String): Seq[PartitionReplicas] = {
    val data: String = client.readData(BrokerTopicsPath + "/" + topicName)
    JsonSerDes.deserialize[Seq[PartitionReplicas]](data.getBytes, new TypeReference[Seq[PartitionReplicas]]() {})
  }

  def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas]): Unit = {
    val data = JsonSerDes.serialize(partitionReplicas)
    val path = BrokerTopicsPath + "/" + topicName
    createPersistentPath(client, path, data)
  }

  def subscribeTopicChangeListener(topicChangeHandler: TopicChangeHandler): Option[List[String]] = {
    val result = client.subscribeChildChanges(BrokerTopicsPath, topicChangeHandler)
    Option(result).map(_.asScala.toList)
  }

  def subscribeChangeListener(listener: BrokerChangeListener): Option[List[String]] = {
    val result = client.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }

  def subscribeBrokerChangeListener(listener: BrokerChangeListenerWithController): Option[List[String]] = {
    val result = client.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }

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

  def setPartitionLeaderForTopic(topicName: String, leaderAndReplicas: List[LeaderAndReplicas]) = {
    val data = JsonSerDes.serialize(leaderAndReplicas)
    val path = ReplicaLeaderElectionPath + "/" + topicName

    try {
      ZkUtils.updatePersistentPath(client, path, data)
    } catch {
      case e: Throwable => {
        println("Exception while writing data to partition leader data" + e)
      }
    }
  }

}
