package com.aofeng.label.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@Getter
@Setter
public class HiveConfig {
    String tableName;
    String partitionField;
    String idColumn;
    public String getIdColumn() {
        return idColumn;
    }

    public void setIdColumn(String idColumn) {
        this.idColumn = idColumn;
    }


    public String getPartitionField() {
        return partitionField;
    }

    public void setPartitionField(String partitionField) {
        this.partitionField = partitionField;
    }


    public String getTable() {
        return tableName;
    }

    public void setTable(String table) {
        this.tableName = table;
    }



}
