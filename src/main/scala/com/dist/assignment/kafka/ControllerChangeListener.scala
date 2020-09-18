package com.dist.assignment.kafka

import java.util

import org.I0Itec.zkclient.IZkChildListener

class ControllerChangeListener extends IZkChildListener{

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {

  }
}
