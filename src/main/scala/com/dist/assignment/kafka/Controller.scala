package com.dist.assignment.kafka

import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.dist.simplekafka
import com.dist.simplekafka.api.{RequestKeys, RequestOrResponse}
import com.dist.simplekafka.common.{JsonSerDes, TopicAndPartition}
import com.dist.simplekafka.network.InetAddressAndPort
import com.dist.simplekafka.util.ZkUtils.Broker
import com.dist.simplekafka.{ControllerExistsException, LeaderAndReplicaRequest, LeaderAndReplicas, PartitionReplicas, SimpleSocketServer, UpdateMetadataRequest}

import scala.jdk.CollectionConverters._

class Controller(client: ZookeeperClient, brokerId: Int, socketServer: SimpleSocketServer) {

  val correlationId = new AtomicInteger(0)
  var liveBrokers: Set[Broker] = Set()
  private var currentLeader = -1

  def startUp() = {
    elect()
  }

  private def elect() = {
    val leaderId = s"${brokerId}"
    try {
      client.createControllerPath(leaderId)
      this.currentLeader = brokerId
      onBecomingLeader
    } catch {
      case e: ControllerExistsException => {
        println("Controller exists exception", e.controllerId)
        this.currentLeader = e.controllerId.toInt
      }
    }
  }

  private def onBecomingLeader = {
    liveBrokers = liveBrokers ++ client.getAllBrokers()
    client.subscribeTopicChangeListener(new TopicChangeHandler(client, onTopicChange))
    client.subscribeBrokerChangeListener(new BrokerChangeListenerWithController(this, client))
  }

  def addNewBroker(broker: Broker) = {
    liveBrokers += broker
  }

  def onTopicChange(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    val leaderAndReplicas: Seq[LeaderAndReplicas]
    = selectLeaderAndFollowerBrokersForPartitions(topicName, partitionReplicas)

    client.setPartitionLeaderForTopic(topicName, leaderAndReplicas.toList);
    //This is persisted in zookeeper for failover.. we are just keeping it in memory for now.
    sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas, partitionReplicas)
    sendUpdateMetadataRequestToAllLiveBrokers(leaderAndReplicas)

  }

  private def selectLeaderAndFollowerBrokersForPartitions(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    val leaderAndReplicas: Seq[LeaderAndReplicas] = partitionReplicas.map(p => {
      val leaderBrokerId = p.brokerIds.head //This is where leader for particular partition is selected
      val leaderBroker = client.getBrokerInfo(leaderBrokerId)
      val replicaBrokers = p.brokerIds.map(id ⇒ client.getBrokerInfo(id))
      LeaderAndReplicas(TopicAndPartition(topicName, p.partitionId), simplekafka.PartitionInfo(leaderBroker, replicaBrokers))
    })
    leaderAndReplicas
  }

  private def sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas: Seq[LeaderAndReplicas], partitionReplicas: Seq[PartitionReplicas]) = {
    val brokerToLeaderIsrRequest = new util.HashMap[Broker, java.util.List[LeaderAndReplicas]]()
    leaderAndReplicas.foreach(lr ⇒ {
      lr.partitionStateInfo.allReplicas.foreach(broker ⇒ {
        var leaderReplicas = brokerToLeaderIsrRequest.get(broker)
        if (leaderReplicas == null) {
          leaderReplicas = new util.ArrayList[LeaderAndReplicas]()
          brokerToLeaderIsrRequest.put(broker, leaderReplicas)
        }
        leaderReplicas.add(lr)
      })
    })

    val brokers = brokerToLeaderIsrRequest.keySet().asScala
    for (broker ← brokers) {
      val leaderAndReplicas: java.util.List[LeaderAndReplicas] = brokerToLeaderIsrRequest.get(broker)
      val leaderAndReplicaRequest = LeaderAndReplicaRequest(leaderAndReplicas.asScala.toList)
      val request = RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicaRequest), correlationId.getAndIncrement())
      socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
    }
  }

  private def sendUpdateMetadataRequestToAllLiveBrokers(leaderAndReplicas: Seq[LeaderAndReplicas]) = {
    val brokerListToIsrRequestMap =
      liveBrokers.foreach(broker ⇒ {
        val updateMetadataRequest = UpdateMetadataRequest(liveBrokers.toList, leaderAndReplicas.toList)
        val request = RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), correlationId.incrementAndGet())
        socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
      })
  }

}
