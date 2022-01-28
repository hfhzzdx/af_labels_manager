package com.aofeng.label.utils

import com.aofeng.label.config.AFConfig
import com.aofeng.label.constant.Constants
import com.aofeng.label.helper.YamlHelper
import redis.clients.jedis.{Jedis, JedisPool}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.sql.{Column, DataFrame}
import java.util.Properties

import com.aofeng.label.module.TableJobContext
import org.apache.log4j.Logger
import org.apache.spark.TaskContext

/**
 * 获取redis连接的工具
 */
object RedisUtil  {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def getJedis(index: Int): Jedis = {
    val prop: Properties = (new PropertiesUtil).getPropertiesByJobName("LoadHiveToRedisJob", Constants.JOB_CONF_SERVICE_PATH)
    val host = prop.getProperty("host")
    val port = prop.getProperty("port")
    val password = prop.getProperty("password")
    val pool = new JedisPool(new GenericObjectPoolConfig, host, port.toInt)
    //    获取连接
    val resource: Jedis = pool.getResource
    //    选择库
    resource.auth(password)
    resource.select(index)
    //    返回工具
    resource
  }


  def saveToRedis(df: DataFrame, jobContext: TableJobContext,tableName:String,partition:String,month_id:String ,target_columns: List[Column],redis_type: String,redis_key:String, redis_index: Int,prop:Properties): Unit = {
    val start = System.currentTimeMillis()
    if (redis_type.toUpperCase.equals("STRING")) {
      val frame: DataFrame = jobContext.readHiveTable(tableName).filter(s"$partition=$month_id")
        .select(new Column(redis_key), org.apache.spark.sql.functions.concat_ws(",", target_columns: _*))
      frame.rdd.foreachPartition(x => {
        val jedis: Jedis = getJedis(redis_index)
        x.foreach(rdd => {
          jedis.set(rdd.getString(0), rdd.getString(1))
        })
        jedis.close()
        logger.info("LoadHiveToRedis job is done, current partition is " + TaskContext.getPartitionId())
      })
      logger.info("LoadHiveToRedis all job is done, total costTime is " + (System.currentTimeMillis() - start) / 1000 + "s")
    } else if (redis_type.toUpperCase.equals("HASH")) {
      //获取map的key和value
      val mapKey: List[Column] = prop.getProperty("hashMapKey").split("#").toList.map(c => new Column(c))
      val mapValue: List[Column] = prop.getProperty("hashMapValue").split("#").toList.map(c => new Column(c))
      val frame: DataFrame = jobContext.readHiveTable(tableName).filter(s"$partition=$month_id")
        .select(org.apache.spark.sql.functions.concat_ws("_", mapKey: _*), org.apache.spark.sql.functions.concat_ws("_", mapValue: _*))
      frame.rdd.foreachPartition(x => {
        val jedis: Jedis = getJedis(redis_index)
        x.foreach(rdd => {
          jedis.hset(redis_key, rdd.getString(0), rdd.getString(1))
        })
        jedis.close()
        logger.info("LoadHiveToRedis job is done, current partition is " + TaskContext.getPartitionId())
      })
      logger.info("LoadHiveToRedis all job is done, total costTime is " + (System.currentTimeMillis() - start) / 1000 + "s")
    }
    else if (redis_type.toUpperCase.equals("SET")) {
      df.rdd.foreachPartition(x => {
        val jedis: Jedis = getJedis(redis_index)
        x.foreach(rdd => {
          jedis.sadd(redis_key, rdd.getString(0))
        })
        jedis.close()
        logger.info("LoadHiveToRedis job is done, current partition is " + TaskContext.getPartitionId())
      })
      logger.info("LoadHiveToRedis all job is done, total costTime is " + (System.currentTimeMillis() - start) / 1000 + "s")
    }
    else if (redis_type.toUpperCase.equals("LIST")) {
      df.rdd.foreachPartition(x => {
        val jedis: Jedis = getJedis(redis_index)
        x.foreach(rdd => {
          jedis.lpush(redis_key, rdd.getString(0))
        })
        jedis.close()
        logger.info("LoadHiveToRedis job is done, current partition is " + TaskContext.getPartitionId())
      })
      logger.info("LoadHiveToRedis all job is done, total costTime is " + (System.currentTimeMillis() - start) / 1000 + "s")
    }
    else if (redis_type.toUpperCase.equals("SORTED SET") || redis_type.toUpperCase.replaceAll("\\s*", "").equals("SORTEDSET")) {
      var i: Double = 0.0
      df.rdd.foreachPartition(x => {
        val jedis: Jedis = getJedis(redis_index)
        x.foreach(rdd => {
          jedis.zadd(redis_key, i, rdd.getString(0))
          i += 1.0
        })
        jedis.close()
        logger.info("LoadHiveToRedis job is done, current partition is " + TaskContext.getPartitionId())
      })
      logger.info("LoadHiveToRedis all job is done, total costTime is " + (System.currentTimeMillis() - start) / 1000 + "s")
    }
  }

}
