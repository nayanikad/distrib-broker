package com.dist.assignment.kafka

import com.dist.common.TestUtils
import org.scalatest.FunSuite

class PartitionTest extends FunSuite {

  test("should append messages to file and return offset") {
    val partition = new Partition("topic1", 0, TestUtils.tempDir())

    assert(partition.logFile.exists())

    val offset = partition.append("k1", "m1".getBytes)
    assert(offset == 1)

    val messages = partition.read(offset)
    assert(messages.size == 1)
    assert(messages(0) == "m1")
  }

  test("should append append to the end of the file even if read before write") {
    val partition = new Partition("topic1", 0, TestUtils.tempDir())
    assert(partition.logFile.exists())

    val offset1 = partition.append("k1", "m1".getBytes)
    val offset2 = partition.append("k2", "m2".getBytes)
    val offset3 = partition.append("k3", "m3".getBytes)
    assert(offset3 == 3)

    val messages = partition.read(offset2)
    assert(messages.size == 1)
    assert(messages(0) == "m2")

    val offset4 = partition.append("k4", "m4".getBytes)
    assert(offset4 == 4)

  }

}
