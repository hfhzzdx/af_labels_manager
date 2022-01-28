package com.aofeng.label.utils

import java.util.Properties

class ConfigUtils {

  val properties = new Properties()


  def getGlobalConfig(): Properties = {
    val path = this.getClass().getClassLoader().getResourceAsStream("global.properties") //文件要放到resource文件夹下
    properties.load(path)
    properties
  }

  def getConfig(filePath: String): Properties = {
    val path = this.getClass().getClassLoader().getResourceAsStream(filePath) //文件要放到resource文件夹下
    properties.load(path)
    properties
  }

  def getSqlConfig(filePath: String): Properties = {
    val sqlPath = this.getGlobalConfig().get("job.config.path")
    val path = this.getClass().getClassLoader().getResourceAsStream(filePath) //文件要放到resource文件夹下
    properties.load(path)
    properties
  }
}
