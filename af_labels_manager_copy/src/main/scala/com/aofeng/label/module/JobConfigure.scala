package com.aofeng.label.module

import com.aofeng.label.config.AFConfig
import com.aofeng.label.constant.Constants
import com.aofeng.label.helper.YamlHelper
import com.aofeng.label.reader._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

class JobConfigure extends Serializable {
  val yaml: YamlHelper = new YamlHelper
  //  var config: AFConfig = _

  def loadJobConfig(): AFConfig = {
    yaml.loadYamlFromConfig(Constants.JOB_CONF_PATH, classOf[AFConfig]).asInstanceOf[AFConfig]
  }

  def sqlContext(config: AFConfig): SQLContext = {
    val spark = config.getSpark
    val sparkConf = new SparkConf()
    sparkConf.setAppName(spark.getAppName)

    import collection.JavaConverters._
    for ((key, value) <- spark.getConfigMapping.asScala) {
      sparkConf.set(key, value)
    }
    if (StringUtils.isNotEmpty(config.getSpark.getMaster)
      && StringUtils.isNotBlank(config.getSpark.getMaster)) {
      sparkConf.setMaster(config.getSpark.getMaster)
    }
    val sparkContext = new SparkContext(sparkConf)

    if (spark.getNeedsHive) {
      val context = new HiveContext(sparkContext)
      context
    } else {
      val context = new SQLContext(sparkContext)
      context
    }
  }

  def labelReader(config: AFConfig): Reader[String, List[Label]] = {
    new LabelReader(config.getMetaStore)
  }

  def jsonReader(): Reader[String, Map[String, MatchRule]] = {
    new JsonValueReader
  }
}

//object JobConfigure {
//  var jobConfig = new JobConfigure
//  var afConfig: AFConfig = _
//  var context: SQLContext = _
//  var jsonReader: Reader[String, Map[String, MatchRule]] = _
//  var labelReader: Reader[String, List[Label]] = _
//
//  def configure(): Unit = {
//    afConfig = jobConfig.loadJobConfig()
//    context = jobConfig.sqlContext(afConfig)
//    labelReader = jobConfig.labelReader(afConfig)
//    jsonReader = jobConfig.jsonReader()
//  }
//}