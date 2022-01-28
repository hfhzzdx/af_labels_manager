package com.aofeng.label.module

import com.aofeng.label.config.AFConfig
import com.aofeng.label.constant.Constants
import com.aofeng.label.reader.{Label, MatchRule, Reader}
import com.aofeng.label.udf.JsonValueUDFHelper
import com.aofeng.label.utils.{DateUtils, ParameterUtils, SqlParseUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql._

//import org.apache.spark.sql.types.{BooleanType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import scala.collection.immutable

/**
 * <pre>
 * 1. 标签处理流程
 *     1. JobContext.createUDF 传入所有标签ID, 得到所有的UDF, 以及每个UDF需要传入的列
 * 2. 在每个独立的 JOB 中应用 UDF
 * </pre>
 */
class TableJobContext extends Serializable {
  @transient private val logger: Logger = Logger.getLogger(this.getClass)
  var sqlContext: SQLContext = _
  var config: AFConfig = _
  var jsonReader: Reader[String, Map[String, MatchRule]] = _
  var labelReader: Reader[String, List[Label]] = _
  @transient private val sqlParser: SqlParseUtil = new SqlParseUtil
  @transient private val paramUtil: ParameterUtils = new ParameterUtils
  var jobConfig = new JobConfigure

  def configure: Unit = {
    val afConfig = jobConfig.loadJobConfig()
    sqlContext = jobConfig.sqlContext(afConfig)
    labelReader = jobConfig.labelReader(afConfig)
    jsonReader = jobConfig.jsonReader()
  }

  //  private val tbl_schedules: String = config.getMetaStore.getTbl_schedules
  //  private val tbl_report: String = config.getMetaStore.getTbl_report
  def processLabel(labelIds: String*): List[Column] = {
    labelIds.map(labelId => processLabel(labelId)).toList
  }

  def processLabel(labelId: String): Column = {
    import org.apache.spark.sql.functions._
    val udfHelper: JsonValueUDFHelper = new JsonValueUDFHelper(labelId, labelReader, jsonReader)
    val columns: immutable.Seq[Column] = udfHelper.getColumns.map(c => new Column(c))

    val actuallyUDF: UserDefinedFunction = udf(udfHelper.createUDF())
    actuallyUDF(concat_ws(",", columns: _*)) as s"${Constants.PREFIX_LABEL_COL}$labelId"
  }

  def readHiveTable(table: String): DataFrame = {
    sqlContext.read.table(table)
  }

  def readTable(table: String, partitionField: String): DataFrame = {
    var sql: String = s"select * from $table where $partitionField = {$partitionField}"
    sql = sqlParser.parserSql(sql, paramUtil.getAllParams())
    val frame = sqlContext.sql(sql)
    frame
  }

  def readTable(table: String, partitionField: String, dataTime: String): DataFrame = {
    val frame: DataFrame = sqlContext.read.table(table).filter(s"${partitionField}=${dataTime}")
    frame
  }

  def joinDF(df1: DataFrame, df2: DataFrame, index: String): DataFrame = {
    df1.join(df2, Seq(index), "left").drop(df2.col(Constants.COLUMN_ID))
  }

  def saveToHive(df: DataFrame, tableName: String): Unit = {
    lazy val par = sqlContext.udf.register("getPartition", () => DateUtils.getLastMonthId())
    lazy val frame: DataFrame = df.withColumn("month_id", par())
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    val lastYear = DateUtils.getYear()
    sqlContext.sql(s"alter table ${tableName} drop partition (month_id=${lastYear})")
    frame.write.mode(org.apache.spark.sql.SaveMode.Overwrite).format("orc").partitionBy("month_id").insertInto(tableName)
  }

  def saveToHive(df: DataFrame, tableName: String, dataTime: String): Unit = {
    lazy val par = sqlContext.udf.register("getPartition", () => dataTime)
    lazy val frame = df.withColumn("month_id", par())
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    val lastYear = DateUtils.getLastMonth(dataTime, 12)
    sqlContext.sql(s"alter table ${tableName} drop partition (month_id=${lastYear})")
    frame.write.mode(SaveMode.Overwrite).format("orc").partitionBy("month_id").insertInto(tableName)
  }

  def close(): Unit = {
    sqlContext.sparkContext.stop()
  }
}

object TableJobContext extends Serializable{

  def create(): TableJobContext = {
    //    val jobDependencies = new JobDependencies()
    //    val injector: Injector = Guice.createInjector(jobDependencies) //把jobDependencies实例封装一下
    //    injector.getInstance(classOf[TableJobContext]) //获取封装之后的实例
    val jobContext = new TableJobContext()
    jobContext.configure
    jobContext
  }

}

