package com.aofeng.label.reader

import com.aofeng.label.config.MetaStoreConfig
import scalikejdbc.{WrappedResultSet, _}

class LabelReader(config: MetaStoreConfig) extends Reader[String, List[Label]] {

  def read(id: String): List[Label] = {
    implicit val session: AutoSession.type = AutoSession

    Class.forName(config.getDriver)
    ConnectionPool.singleton(
      url = config.getUrl,
      user = config.getUser,
      password = config.getPassword
    )

    sql"SELECT id, value,source_name,source_label FROM tbl_labels WHERE id=$id"
      .map(rs => LabelSupport(rs))
      .list
      .apply()
  }

}

case class Label(id: String, value: String,source_name:String,source_label:String)

object LabelSupport extends SQLSyntaxSupport[Label] {
  override val tableName = "tbl_labels"

  def apply(rs: WrappedResultSet): Label =
    Label(
      rs.string("id"),
      rs.string("value"),
      rs.string("source_name"),
      rs.string("source_label")
    )
}
