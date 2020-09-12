package com.dist.assignment.kafka

import java.util

import com.dist.simplekafka.PartitionReplicas
import org.I0Itec.zkclient.IZkChildListener

import scala.jdk.CollectionConverters._

class TopicChangeHandler(zookeeperClient: ZookeeperClient,
                         onTopicChange: (String, Seq[PartitionReplicas]) => Unit) extends IZkChildListener {


  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    currentChilds.asScala.foreach(topicName => {
      val replicas: Seq[PartitionReplicas] = zookeeperClient.getPartitionAssignmentsFor(topicName)
      onTopicChange(topicName, replicas)
    })
  }
}
