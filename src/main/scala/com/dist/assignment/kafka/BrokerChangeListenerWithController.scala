package com.dist.assignment.kafka

import java.util

import org.I0Itec.zkclient.IZkChildListener

import scala.jdk.CollectionConverters._

class BrokerChangeListenerWithController(controller: Controller, client: ZookeeperClient) extends IZkChildListener {

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {

    val currentBrokerIds = currentChilds.asScala.map(brokerId => brokerId.toInt).toSet
    val newBrokerIds = currentBrokerIds -- controller.liveBrokers.map(broker => broker.id)

    val newBrokers = newBrokerIds.map(client.getBrokerInfo(_))

    newBrokers.foreach(controller.addNewBroker(_))

  }
}
