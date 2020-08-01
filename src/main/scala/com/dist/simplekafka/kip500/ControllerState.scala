package com.dist.simplekafka.kip500

import java.io.{ByteArrayInputStream, File}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.util
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.stream.Collectors
import java.util.{Map, stream}

import akka.util.Timeout

case class ClientSession(lastModifiedTime: Long, clientId: String, responses: util.Map[Int, String]) {

}

class ControllerState(walDir: File) extends Logging {
  val kv = new mutable.HashMap[String, String]()
  val wal = WriteAheadLog.create(walDir)
  applyLog()
  val activeBrokers = new ConcurrentHashMap[Int, Lease]


  var leaseTracker:LeaseTracker = new FollowerLeaseTracker(activeBrokers)

  def put(key: String, value: String): Unit = {
    wal.writeEntry(SetValueRecord(key, value).serialize())
  }

  def get(key: String): Option[String] = kv.get(key)



  def close = {
    kv.clear()
  }


  def applyEntries(entries: List[WalEntry]): Unit = {
    entries.foreach(entry ⇒ {
      applyEntry(entry)
    })
  }



  def applyEntry(entry: WalEntry) = {
    if (entry.entryType == EntryType.data) {
      val command = Record.deserialize(new ByteArrayInputStream(entry.data))
      command match {
        case setValueCommand: SetValueRecord => {
            kv.put(setValueCommand.key, setValueCommand.value)
        }
        case brokerHeartbeat: BrokerHeartbeat => {
          val brokerId = brokerHeartbeat.brokerId
          info(s"Registering Active Broker with id ${brokerId}")
          leaseTracker.addLease(new Lease(brokerId, TimeUnit.SECONDS.toNanos(2)))
          brokerId
        }
        case topicRecord: TopicRecord => {
          println(topicRecord)
        }
        case partitionRecord: PartitionRecord => {
          println(partitionRecord)
        }
        case fenceBroker:FenceBroker => {
          println(fenceBroker)
        }
      }
    }
  }

  val sessionTimeoutNanos = TimeUnit.SECONDS.toNanos(1)

  def applyLog() = {
    val entries: List[WalEntry] = wal.readAll().toList
    applyEntries(entries)
  }
}
