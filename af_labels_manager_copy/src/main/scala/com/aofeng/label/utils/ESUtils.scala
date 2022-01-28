package com.aofeng.label.utils


import com.aofeng.label.constant.Constants
import com.aofeng.label.module.TableJobContext
import com.aofeng.label.reader.JobTable
import org.apache.spark.sql.{ DataFrame,  UserDefinedFunction}
import org.elasticsearch.spark.sql.EsSparkSQL




object ESUtils {
  val props = Map("es.write.operation" -> "upsert",
    "es.mapping.id" -> Constants.COLUMN_ID,
    "es.update.script.lang" -> "groovy"
  )

  val propMap: Map[String, String] = props

  def getUp_params(colName: String): String = {
    val up_params = s"new_${colName}:${colName}"
    up_params
  }

  /**
   * 单标签增加和删除方法,参数一致
   *
   * @param jobContext
   * @param df
   * @param index
   * @param colName
   */
  def updateField(jobContext: TableJobContext, df: DataFrame, nodes: String, port: String, index: String, colName: String, map: Map[String, String]) = {
    val up_params: String = getUp_params(colName)
    val up_script = s"if (ctx._source.containsKey('${colName}')) {ctx._source.${colName} = ctx._source.${colName}.concat(',').concat(new_${colName});} else {;}"
    EsSparkSQL.saveToEs(df, index, map ++ propMap + ("es.update.script.params" -> up_params) + ("es.update.script" -> up_script) + ("es.nodes" -> nodes) + ("es.port" -> port))
  }

  def deleteValue(jobContext: TableJobContext, df: DataFrame, nodes: String, port: String, index: String, colName: String, map: Map[String, String]) = {
    val up_params = getUp_params(colName)
    val deleteMap = propMap + ("es.update.script.params" -> up_params)
    val up_script = s"if (ctx._source.containsKey('${colName}')) {ctx._source.${colName} -= ','.concat(new_${colName});} else {;}"
    EsSparkSQL.saveToEs(df, index, map ++ deleteMap + ("es.update.script" -> up_script) + ("es.nodes" -> nodes) + ("es.port" -> port))
  }

  def updateFields(jobContext: TableJobContext, df: DataFrame, nodes: String, port: String, index: String, colName: String, map: Map[String, String]) = {
    //默认df有两个字段  id 和 labels
    val labels = Constants.COLUMN_LABELS
    val up_params = getUp_params(colName)
        val up_script = s"if (ctx._source.containsKey('${colName}')) {ctx._source.${colName} = ctx._source.${colName}.concat(',').concat(new_${colName});} else {;}"
//    val up_script = s"if(ctx._source.${labels}.containsKey('new_${colName}')){;}else{ctx._source.${colName} = ctx._source.${colName}.concat(',').concat(new_${colName});}"
    import org.apache.spark.sql.functions._
    val frame: DataFrame = df.withColumn("es_label_list", split(col(colName), ",")).withColumn(colName, explode(col("es_label_list"))).drop("es_label_list")
    frame.show(false)
    val updateMap = propMap ++ map + ("es.update.script.params" -> up_params) + ("es.update.script" -> up_script)
    println("ESUtils updateFields updateMap is " + updateMap.mkString(","))
    EsSparkSQL.saveToEs(frame, index, updateMap + ("es.nodes" -> nodes) + ("es.port" -> port))
  }

  def deleteValues(jobContext: TableJobContext, jobTable: JobTable, df: DataFrame, nodes: String, port: String, index: String, colName: String, map: Map[String, String]) = {
    import org.apache.spark.sql.functions._
    val labels: String = Constants.COLUMN_LABELS
    val pidList: List[String] = jobTable.pids.split("#").toList
    /**
     * s"""{
     * |  "query":{
     * |       "bool":{
     * |             "must":[{
     * |                  "match": {
     * |                   "${labels}":"${pid}"
     * |                           }
     * |                   }]
     * |             }
     * |          }
     * | }""".stripMargin
     */
    var begin = "{\"query\":{\"bool\":{\"should\":["
    for (id <- pidList) {
      begin += s"""{"match_phrase":{"${labels}":"${id}"}},"""
    }
    var end: String = begin.substring(0, begin.length - 1)
    end += "]}}}"
    val esDF: DataFrame = EsSparkSQL.esDF(jobContext.sqlContext, index, end)
    // 注册udf函数,循环判断list值,如果存在就替换为空
    val function: UserDefinedFunction = udf((str: String) => {
      val strList: List[String] = str.split(",").toList
      val string: String = StrUtils.listToString(strList.intersect(pidList))
      str.replaceAll(string, "")
    })
    val resDF: DataFrame = esDF.select(col("*"), function(col(labels)).as("labels_new")).drop(labels)
      .withColumnRenamed("labels_new", labels)
    EsSparkSQL.saveToEs(resDF, index, propMap ++ map + ("es.nodes" -> nodes) + ("es.port" -> port))
  }

  //删除delete之后的空字符串   经过测试,replace替换逗号不适用,explode之后,个数不确定
  def deleteNull(jobContext: TableJobContext, df: DataFrame, nodes: String, port: String, index: String, colName: String, map: Map[String, String]) = {
    val up_params = getUp_params(colName)
    val up_script = s"if (ctx._source.containsKey('${colName}')) {ctx._source.${colName} = ctx._source.${colName}.replaceAll(',,,','').replaceAll(',,',',') ;} else {;}"
    val esMap: Map[String, String] = propMap ++ map + ("es.update.script.params" -> up_params) + ("es.update.script" -> up_script) + ("es.nodes" -> nodes)
    EsSparkSQL.saveToEs(df, index, esMap + ("es.port" -> port))
  }

  def deleteLabels(jobContext: TableJobContext, jobTable: JobTable, frame: DataFrame, nodes: String, port: String, index: String, colName: String, map: Map[String, String], idList: String) = {
    val str: String = getUp_params(colName)
    val labels = Constants.COLUMN_LABELS
    val es_query = s""" {"query":{"bool":{"must":[{"match":{"${labels}":"${idList}"}}]}}}"""
    val df = EsSparkSQL.esDF(jobContext.sqlContext, index, es_query)
    val up_script = s"if (ctx._source.containsKey('${colName}')) {ctx._source.${colName} -= ${idList};} else {;}"
    val deleteMap = propMap + ("es.update.script.params" -> str)
    EsSparkSQL.saveToEs(df, index,  map ++ deleteMap + ("es.update.script" -> up_script) + ("es.nodes" -> nodes) + ("es.port" -> port))

  }

  def updateES(jobContext: TableJobContext, jobTable: JobTable, df: DataFrame, nodes: String, port: String, index: String, colName: String, action: String, map: Map[String, String], idList:String) = {
    if (action.toUpperCase.equals("ADD")) {
      updateFields(jobContext: TableJobContext, df: DataFrame, nodes, port, index, colName, map)
    } else if (action.toUpperCase.equals("DELETE")) {
//      deleteValues(jobContext: TableJobContext, jobTable: JobTable, df: DataFrame, nodes, port, index, colName, map)
      deleteLabels(jobContext: TableJobContext, jobTable: JobTable, df: DataFrame, nodes, port, index, colName, map,idList)
    }
  }

  /**
   * load dataFrame to ElasticSearch
   *
   * @param df         dataFrame
   * @param index      es's index.  index/type
   * @param nodes      es's nodes
   * @param port       es's port
   * @param idCol      es.mapping.id
   * @param jobContext jobContext
   */
  def saveToES(df: DataFrame, index: String, nodes: String, port: String, idCol: String, jobContext: TableJobContext): Unit = {
    import scala.collection.JavaConverters._
    val esConfig = Map("es.nodes" -> nodes,
      "es.port" -> port,
      "es.mapping.id" -> idCol) ++ jobContext.jobConfig.loadJobConfig().getEs.getConfigMapping.asScala
    import org.elasticsearch.spark.sql._
    EsSparkSQL.saveToEs(df, index, esConfig)
  }
}


case class UserLabels(id: String, labels: String)