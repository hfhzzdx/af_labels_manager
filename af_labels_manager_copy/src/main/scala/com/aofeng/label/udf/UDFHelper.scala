package com.aofeng.label.udf

trait UDFHelper extends Serializable {

  def createUDF(): String => String

  def getColumns: List[String]
}
