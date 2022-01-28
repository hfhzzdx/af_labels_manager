package com.aofeng.label.utils
import java.sql.{PreparedStatement, ResultSet, ResultSetMetaData, Statement}
import java.util
import java.util.Properties
import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.aofeng.label.module.TableJobContext
import com.aofeng.label.reader.OutPutLabel
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import scala.collection.mutable.ListBuffer

object JdbcUtils {
  def druidPool(): DruidDataSource = {
    import com.alibaba.druid.pool.DruidDataSource
    import com.aofeng.label.config.AFConfig
    import com.aofeng.label.constant.Constants
    import com.aofeng.label.helper.YamlHelper


    val yaml: YamlHelper = new YamlHelper
    val dataSource = new DruidDataSource()
    val afConfig = yaml.loadYamlFromConfig(Constants.JOB_CONF_PATH, classOf[AFConfig]).asInstanceOf[AFConfig]

    dataSource.setDriverClassName(afConfig.getMetaStore.getDriver)
    dataSource.setUrl(afConfig.getMetaStore.getUrl)
    dataSource.setUsername(afConfig.getMetaStore.getUser)
    dataSource.setPassword(afConfig.getMetaStore.getPassword)
    dataSource.setInitialSize(afConfig.getMetaStore.getInitialSize.toInt)
    //最大大小
    dataSource.setMaxActive(afConfig.getMetaStore.getMaxActive.toInt)
    //最小大小
    dataSource.setMinIdle(afConfig.getMetaStore.getMinIdle.toInt)
    //检查时间
    dataSource.setMaxWait(afConfig.getMetaStore.getMaxWait.toInt)
    //防止过期
    dataSource.setValidationQuery("SELECT 'x'")
    dataSource.setTestWhileIdle(true)
    dataSource.setTestOnBorrow(true)
    dataSource
  }

  def druidPoolOther(): DruidDataSource = {
    import com.alibaba.druid.pool.DruidDataSource
    import com.aofeng.label.config.AFConfig
    import com.aofeng.label.constant.Constants
    import com.aofeng.label.helper.YamlHelper


    val yaml: YamlHelper = new YamlHelper
    val dataSource = new DruidDataSource()
    val afConfig = yaml.loadYamlFromConfig(Constants.JOB_CONF_PATH, classOf[AFConfig]).asInstanceOf[AFConfig]
    val prop: Properties = (new PropertiesUtil).getPropertiesByJobName("LoadHiveToMysqlJob", Constants.JOB_CONF_SERVICE_PATH)
    dataSource.setDriverClassName(prop.getProperty("mysqlDriver"))
    dataSource.setUrl(prop.getProperty("mysqlUrl"))
    dataSource.setUsername(prop.getProperty("mysqlUser"))
    dataSource.setPassword(prop.getProperty("mysqlPassword"))
    dataSource.setInitialSize(afConfig.getMetaStore.getInitialSize.toInt)
    //最大大小
    dataSource.setMaxActive(afConfig.getMetaStore.getMaxActive.toInt)
    //最小大小
    dataSource.setMinIdle(afConfig.getMetaStore.getMinIdle.toInt)
    //检查时间
    dataSource.setMaxWait(afConfig.getMetaStore.getMaxWait.toInt)
    //防止过期
    dataSource.setValidationQuery("SELECT 'x'")
    dataSource.setTestWhileIdle(true)
    dataSource.setTestOnBorrow(true)
    dataSource
  }

  /**
   * 读取mysql调度表tbl_schedules,获取表字段
   *
   * @param table which table name need to read
   * @return list contains table's columnName
   */
  def read_mysql(table: String): ListBuffer[String] = {
    val connection: DruidPooledConnection = druidPool().getConnection()
    val sql = s"select * from $table"
    var list: scala.collection.mutable.ListBuffer[String] = ListBuffer[String]()
    val ps: PreparedStatement = connection.prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery()
    val meta: ResultSetMetaData = rs.getMetaData
    val columnCount: Int = meta.getColumnCount
    for (i <- 1 to columnCount) {
      list += meta.getColumnName(i)
    }
    //释放资源
    connectionClose(rs, ps, connection)
    list.remove(0)
    list
  }

  def read_mysql_other(table: String): ListBuffer[String] = {
    val connection: DruidPooledConnection = druidPoolOther().getConnection()
    val sql = s"select * from $table"
    var list: scala.collection.mutable.ListBuffer[String] = ListBuffer[String]()
    val ps: PreparedStatement = connection.prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery()
    val meta: ResultSetMetaData = rs.getMetaData
    val columnCount: Int = meta.getColumnCount
    for (i <- 1 to columnCount) {
      list += meta.getColumnName(i)
    }
    //释放资源
    connectionClose(rs, ps, connection)
    list.remove(0)
    list
  }

  def execSql(sql: String): ResultSet = {
    val connection: DruidPooledConnection = druidPool().getConnection
    val ps: PreparedStatement = connection.prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery()
    rs
  }

  def deleteSql(sql: String): Int = {
    val connection = druidPool().getConnection
    val statement = connection.createStatement()
    val rows: Int = statement.executeUpdate(sql)
    connectionClose(statement, connection)
    rows
  }

  /**
   * 拼接插入语句的sql
   *
   * @param tableName which table need to trans sql
   * @return sql which sql need to return
   */
  def getInsertSql(tableName: String): String = {
    val list: ListBuffer[String] = read_mysql(tableName)
    var sql = s"insert into ${tableName} ("
    for (str <- list) {
      sql += str
      sql += ","
      if (list.last.equals(str)) {
        sql = sql.substring(0, sql.length - 1)
      }
    }
    sql += ") values ("
    for (i <- 0 to list.length - 2) {
      sql += "?,"
      if (i == list.length - 2) {
        sql += "?)"
      }
    }
    println("get sql=" + sql)
    sql
  }

  def getInsertSqlOther(tableName: String): String = {
    val list: ListBuffer[String] = read_mysql_other(tableName)
    var sql = s"insert into ${tableName} ("
    for (str <- list) {
      sql += str
      sql += ","
      if (list.last.equals(str)) {
        sql = sql.substring(0, sql.length - 1)
      }
    }
    sql += ") values ("
    for (i <- 0 to list.length - 2) {
      sql += "?,"
      if (i == list.length - 2) {
        sql += "?)"
      }
    }
    println("get sql=" + sql)
    sql
  }

  /**
   * 将监控结果插入mysql数据库的tbl_schedules表
   *
   * @param listBuffer which need to insert mysql table
   * @param tableName  which need to insert mysql table name
   * @return
   */
  def insertIntoMysql(listBuffer: ListBuffer[String], tableName: String): Unit = {
    val sql = s"insert into ${tableName} (`job_id`,`job_name`,`job_desc`,`job_period`,`project`,`create_time`,`end_time`,`status`,`result`)  values (?,?,?,?,?,?,?,?,?)"
    val connection: DruidPooledConnection = druidPool.getConnection()
    val ps: PreparedStatement = connection.prepareStatement(sql)

    for (i <- 0 to listBuffer.length - 1) {
      ps.setString(i + 1, listBuffer(i))
    }
    try {
      ps.executeUpdate()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      connectionClose(ps, connection)
    }
  }

  /**
   * 将监控结果插入mysql数据库的tbl_schedules表
   *
   * @return
   */
  def insertScheduleLog(job_id: String, job_name: String, job_desc: String, job_period: String,
                        project: String, create_time: String, status: String, result: String): Unit = {
    val sql = s"insert into tbl_schedules (`job_id`,`job_name`,`job_desc`,`job_period`,`project`," +
      s"`create_time`,`status`,`result`)  values (?,?,?,?,?,?,?,?)"
    System.out.println("execute insert sql:" + sql)
    val connection: DruidPooledConnection = druidPool.getConnection()
    val ps: PreparedStatement = connection.prepareStatement(sql)
    try {
      ps.setString(1, job_id)
      ps.setString(2, job_name)
      ps.setString(3, job_desc)
      ps.setString(4, job_period)
      ps.setString(5, project)
      ps.setString(6, create_time)
      ps.setString(7, status)
      ps.setString(8, result)
      ps.executeUpdate()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      connectionClose(ps, connection)
    }
  }

  /**
   * 将监控结果插入mysql数据库的tbl_schedules表
   *
   * @return
   */
  def updateScheduleLog(job_id: String, job_name: String, job_period: String,
                        end_time: String, status: String, result: String): Unit = {
    val new_result: String = result.replaceAll("\'", "\"")
    val sql = s"update tbl_schedules set `end_time`= '${end_time}',`status`='${status}'," +
      s"`result`='${new_result}' where `job_id`='${job_id}' and `job_name`='$job_name' and `job_period`='$job_period'"
    System.out.println("execute update sql:" + sql)
    val connection: DruidPooledConnection = druidPool.getConnection()
    val ps: Statement = connection.createStatement()
    try {
      ps.executeUpdate(sql)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      connectionClose(ps, connection)
    }
  }


  /**
   * 关闭连接,释放资源
   *
   * @param rs
   * @param pstmt
   * @param conn
   */

  def connectionClose(rs: ResultSet, pstmt: PreparedStatement, conn: DruidPooledConnection): Unit = {
    if (rs != null) {
      rs.close()
      if (pstmt != null) {
        pstmt.close()
        if (conn != null) {
          conn.close()
        }
      }
    }
  }

  def connectionClose(statement: Statement, connection: DruidPooledConnection): Unit = {
    if (statement != null) {
      statement.close()
      if (connection != null) {
        connection.close()
      }
    }
  }

  def connectionClose(rs: ResultSet): Unit = {
    if (rs != null) {
      rs.close()
    }
  }

  def connectionClose(pstmt: PreparedStatement, conn: DruidPooledConnection): Unit = {
    if (pstmt != null) {
      pstmt.close()
      if (conn != null) {
        conn.close()
      }
    }
  }

  def getConnection(url: String, userName: String, password: String, className: String): java.sql.Connection = {
    try {
      Class.forName(className)
      java.sql.DriverManager.getConnection(url, userName, password)
    }
  }


  /**
   * 匹配jdbc数据类型,转成sql的StructType
   *
   * @param name    field name
   * @param colType class name of dataType
   * @return StructField
   */
  def createStructField(name: String, colType: String): StructField = {
    colType match {
      case "java.lang.String" => {
        StructField(name, StringType, true)
      }
      case "java.lang.Integer" => {
        StructField(name, IntegerType, true)
      }
      case "java.lang.Long" => {
        StructField(name, LongType, true)
      }
      case "java.lang.Boolean" => {
        StructField(name, BooleanType, true)
      }
      case "java.lang.Double" => {
        StructField(name, DoubleType, true)
      }
      case "java.lang.Float" => {
        StructField(name, FloatType, true)
      }
      case "java.sql.Date" => {
        StructField(name, DateType, true)
      }
      case "java.sql.Time" => {
        StructField(name, TimestampType, true)
      }
      case "java.sql.Timestamp" => {
        StructField(name, TimestampType, true)
      }
      case "java.math.BigDecimal" => {
        StructField(name, DecimalType(10, 0), true)
      }
    }
  }

  /**
   * resultSet trans to DataFrame
   * batch size is 100000
   *
   * @param rs resultSet
   * @return DataFrame
   */
  def createResultSetToDF(rs: ResultSet, jobcontext: TableJobContext): DataFrame = {
    import scala.collection.JavaConverters._
    val rsmd = rs.getMetaData
    val columnTypeList = new util.ArrayList[String]()
    val rowSchemaList = ListBuffer[StructField]()
    for (i <- 1 to rsmd.getColumnCount) {
      var temp = rsmd.getColumnClassName(i)
      temp = temp.substring(temp.lastIndexOf(".") + 1)
      if ("Integer".equals(temp)) {
        temp = "Int"
      }
      columnTypeList.add(temp)
      rowSchemaList.append(createStructField(rsmd.getColumnName(i), rsmd.getColumnClassName(i)))
    }
    val rowSchema = StructType(Seq(rowSchemaList: _*))
    //ResultSet反射类对象
    val sqlContext = jobcontext.sqlContext
    val rsClass = rs.getClass
    var count = 1;
    val resultList = new util.ArrayList[Row]
    var totalDF = sqlContext.createDataFrame(new util.ArrayList[Row], rowSchema)
    while (rs.next()) {
      count = count + 1
      val temp = new util.ArrayList[Object]
      for (i <- 0 to columnTypeList.size - 1) {
        val method = rsClass.getMethod("get" + columnTypeList.get(i), "aa".getClass)
        temp.add(method.invoke(rs, rsmd.getColumnName(i + 1)))
      }
      resultList.add(Row(temp.asScala.toList: _*))
      //        resultList.add(Row(temp:_*))
      if (count % 100000 == 0) {
        val tempDF = sqlContext.createDataFrame(resultList, rowSchema)
        totalDF = totalDF.unionAll(tempDF).distinct()
        resultList.clear()
      }
    }
    val tempDF = sqlContext.createDataFrame(resultList, rowSchema)
    totalDF = totalDF.unionAll(tempDF).distinct()
    totalDF
  }

  def saveToMysql(df: DataFrame, jobContext: TableJobContext, saveMode: SaveMode, driverName: String, url: String, userName: String, password: String, month_id: String, tableName: String): Unit = {
    import java.util.Properties
    val prop: Properties = new Properties()
    prop.setProperty("user", userName)
    prop.setProperty("password", password)
    prop.setProperty("driver", driverName)
    try {
      df.write.mode(saveMode).jdbc(url, tableName, prop)
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def insertMany(tableName: String, list: ListBuffer[OutPutLabel]): Unit = {
    val sql = getInsertSqlOther(tableName)
    var connection: DruidPooledConnection = null
    var pstmt: PreparedStatement = null
    try {
      connection = druidPoolOther().getConnection
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (ele <- list) {
        pstmt.setString(1, ele.lac_ci)
        pstmt.setString(2, ele.dn_flag)
        pstmt.setString(3, ele.result)
        pstmt.setString(4, ele.month_id)
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      connectionClose(pstmt, connection)
    }
  }

  def manySaveToMysql(df: DataFrame, jobContext: TableJobContext, saveMode: SaveMode, driverName: String, url: String, userName: String, password: String, month_id: String, tableName: String): Unit = {
    val colList: List[String] = df.columns.toList

    df.foreachPartition(partitionOfRecords => {
      val list = new ListBuffer[OutPutLabel]
      partitionOfRecords.foreach(info => {
        val lac_ci: String = info.getAs[String]("lac_ci")
        val dn_flag: String = info.getAs[String]("dn_flag")
        val result: String = info.getAs[String]("result")
        val month_id: String = info.getAs[String]("month_id")
        list.append(OutPutLabel(lac_ci, dn_flag, result, month_id))
      })
      insertMany(tableName, list)
    })
  }

  def manyInsertToMysql(df: DataFrame, jobContext: TableJobContext, saveMode: SaveMode, driverName: String, url: String, userName: String, password: String, month_id: String, tableName: String, batchSize: String): Unit = {
    val colList: List[String] = df.columns.toList
    df.repartition(10)
    val sql = getInsertSqlOther(tableName)
    var connection: DruidPooledConnection = null
    var pstmt: PreparedStatement = null
    var statement: Statement = null
    try {
      df.foreachPartition(partitionOfRecord => {
        connection = druidPoolOther().getConnection
        connection.setAutoCommit(false)
        pstmt = connection.prepareStatement(sql)
        statement = connection.createStatement()
        val batch: Integer = Integer.valueOf(batchSize)
        var count = 0
        partitionOfRecord.foreach(row => {
          var listBuffer = scala.collection.mutable.ListBuffer[String]()
          for (col <- colList) {
            val colName = row.getAs(col).toString
            listBuffer.append(colName)
          }
          for (i <- (0 to listBuffer.length - 1)) {
            pstmt.setString(i + 1, listBuffer(i))
          }
          pstmt.addBatch()
          count += 1
          if (count % batch == 0) {
            pstmt.executeBatch()
            connection.commit()
            System.err.println("批次大小为" + batch)
            count = 0
          }

        })
        if (count > 0) {
          pstmt.executeBatch()
          System.err.println("剩余批次为" + count)
          connection.commit()
        }
      })
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      if (statement != null) {
        connectionClose(statement, connection)
        if (pstmt != null) {
          connectionClose(pstmt, connection)
        }
      }
    }
  }


  def batchInsertToMysql(df: DataFrame, jobContext: TableJobContext, saveMode: SaveMode, driverName: String, url: String, userName: String, password: String, month_id: String, tableName: String, batchSize: String): Unit = {
     val colList: List[String] = df.columns.toList
     val dataRows: Array[Row] = df.collect()
    val sql = getInsertSqlOther(tableName)
    val connection: DruidPooledConnection = druidPoolOther().getConnection
    connection.setAutoCommit(false)
    val pstmt: PreparedStatement = connection.prepareStatement(sql)
    val batch: Integer = Integer.valueOf(batchSize)
    var count = 0
    for (row <- dataRows){
      var listBuffer:ListBuffer[String]= scala.collection.mutable.ListBuffer[String]()
      for (col <- colList) {
        val colName = row.getAs(col).toString
        listBuffer.append(colName)
      }
      for (i <- (0 to listBuffer.length - 1)) {
        pstmt.setString(i + 1, listBuffer(i))
      }
      pstmt.addBatch()
      count += 1
      if (count % batch == 0) {
        pstmt.executeBatch()
        connection.commit()
        System.err.println("批次xx大小为" + batch)
        count = 0
      }
    }
      if (count > 0) {
      pstmt.executeBatch()
      System.err.println("剩余xx批次为" + count)
      connection.commit()
    }
  }
}
