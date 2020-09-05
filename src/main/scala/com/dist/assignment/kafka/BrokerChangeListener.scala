package com.dist.assignment.kafka

import java.util

import com.dist.simplekafka.util.ZkUtils.Broker
import org.I0Itec.zkclient.IZkChildListener

import scala.jdk.CollectionConverters._

class BrokerChangeListener(client: ZookeeperClient) extends IZkChildListener {
  var liveBrokers: Set[Broker] = Set()

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {

    val currentBrokerIds = currentChilds.asScala.map(id => id.toInt).toSet
    val newBrokerIds = currentBrokerIds -- liveBrokerIds
    val deadBrokerIds = liveBrokerIds -- currentBrokerIds

    val newBrokers = newBrokerIds.map(brokerId => client.getBrokerInfo(brokerId))

    addNewBrokers(newBrokers)

    removeDeadBrokers(deadBrokerIds)
  }

  private def addNewBrokers(newBrokers: Set[Broker]): Unit = {
    newBrokers.foreach(broker => liveBrokers += broker)
  }

  private def removeDeadBrokers(deadBrokerIds: Set[Int]): Unit = {
    val deadBrokers = liveBrokers.filter(broker => deadBrokerIds.contains(broker.id))
    liveBrokers = liveBrokers -- deadBrokers
  }

  private def liveBrokerIds: Set[Int] = {
    liveBrokers.map(broker => broker.id)
  }
}
