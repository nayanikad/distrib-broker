package com.dist.assignment.kafka

import java.io.{File, RandomAccessFile}
import java.util
import java.util.concurrent.atomic.AtomicInteger

class Partition(topicName: String, partition: Int, logDir: File) {

  val logFile: File = new File(logDir, s"${topicName}_${partition}.log")
  logFile.createNewFile()
  val logRW = new RandomAccessFile(logFile, "rw")

  val offset = new AtomicInteger()
  val offsetMap = new util.HashMap[Int, Long]()

  def append(key: String, message: Array[Byte]): Int = {
    logRW.seek(logRW.length())
    val filePosition = logRW.getFilePointer

    logRW.writeUTF(key)
    logRW.writeInt(message.length)
    logRW.write(message)

    offsetMap.put(offset.incrementAndGet(), filePosition)
    offset.get()
  }

  def read(offset: Int): List[String] = {

    val filePosition = offsetMap.get(offset)

    logRW.seek(filePosition)

    val key = logRW.readUTF()
    val messageLength = logRW.readInt()

    val message = new Array[Byte](messageLength)
    logRW.read(message)
    List(new String(message))
  }


}
