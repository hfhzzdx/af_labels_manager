package com.aofeng.label.config;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@ToString
@Getter
@Setter
public class AFConfig {
    MetaStoreConfig metaStore;
    SparkConfig spark;
    ESConfig es;
    HiveConfig hive;
    List<String> tableOrder;
    String mainJoinTable;
    String idColumn;
    RedisConfig redis;
    String dataTime;
    public String getDataTime() {
        return dataTime;
    }

    public void setDataTime(String dataTime) {
        this.dataTime = dataTime;
    }


    public RedisConfig getRedis() {
        return redis;
    }
    public void setRedis(RedisConfig redis) {
        this.redis = redis;
    }


    public MetaStoreConfig getMetaStore() {
        return metaStore;
    }

    public void setMetaStore(MetaStoreConfig metaStore) {
        this.metaStore = metaStore;
    }

    public SparkConfig getSpark() {
        return spark;
    }

    public void setSpark(SparkConfig spark) {
        this.spark = spark;
    }

    public ESConfig getEs() {
        return es;
    }

    public void setEs(ESConfig es) {
        this.es = es;
    }

    public HiveConfig getHive() {
        return hive;
    }

    public void setHive(HiveConfig hive) {
        this.hive = hive;
    }

    public List<String> getTableOrder() {
        return tableOrder;
    }

    public void setTableOrder(List<String> tableOrder) {
        this.tableOrder = tableOrder;
    }

    public String getMainJoinTable() {
        return mainJoinTable;
    }

    public void setMainJoinTable(String mainJoinTable) {
        this.mainJoinTable = mainJoinTable;
    }

    public String getIdColumn() {
        return idColumn;
    }

    public void setIdColumn(String idColumn) {
        this.idColumn = idColumn;
    }
}




