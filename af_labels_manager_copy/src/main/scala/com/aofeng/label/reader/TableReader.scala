package com.aofeng.label.reader

import com.aofeng.label.config.MetaStoreConfig
import scalikejdbc.{WrappedResultSet, _}

class TableReader(config: MetaStoreConfig) extends Reader[String, List[JobTable]] {

  def read(pid: String): List[JobTable] = {
    implicit val session: AutoSession.type = AutoSession

    Class.forName(config.getDriver)
    ConnectionPool.singleton(
      url = config.getUrl,
      user = config.getUser,
      password = config.getPassword
    )

    sql"""
         |SELECT
         |	source_name as tableName,
         |  partition_filed as partitionField,
         |	group_concat(DISTINCT id SEPARATOR '#') AS pids,
         |	group_concat(DISTINCT source_label SEPARATOR '#') AS labels
         |FROM
         |	tbl_labels
         |WHERE
         |	ISNULL(source_name) = 0
         |AND LENGTH(trim(source_name)) > 0
         |GROUP BY
         |	source_name,partition_filed
        """.stripMargin
      .map(rs => JobTableSupport(rs))
      .list
      .apply()
  }

  def readSingle(sourceName: String): List[JobTable] = {
    implicit val session = AutoSession

    Class.forName(config.getDriver)
    ConnectionPool.singleton(
      url = config.getUrl,
      user = config.getUser,
      password = config.getPassword
    )
    sql"""
         |SELECT
         |	source_name as tableName,
         |  partition_filed as partitionField,
         |	group_concat(DISTINCT id SEPARATOR '#') AS pids,
         |	group_concat(DISTINCT source_label SEPARATOR '#') AS labels
         |FROM
         |	tbl_labels
         |WHERE
         |LENGTH(trim(value)) > 0
         |AND source_name= $sourceName
         |GROUP BY
         |	source_name,partition_filed
        """.stripMargin
      .map(rs => JobTableSupport(rs))
      .list
      .apply()
  }
}

case class JobTable(sourceName: String,  partitionField: String,pids: String, labels: String)
//case class UserDemo(markets: String, userids: String, userid: String)
case class UserMarkets(fieldName1:String,fieldName2:String,col_1:String)
case class  OutPutLabel(lac_ci:String,dn_flag:String,result:String,month_id:String)
object JobTableSupport extends SQLSyntaxSupport[JobTable] {
  //  override val tableName = "tbl_labels"
  def apply(rs: WrappedResultSet): JobTable =
    JobTable(
      rs.string("tableName"),
      rs.string("partitionField"),
      rs.string("pids"),
      rs.string("labels")
    )
}
