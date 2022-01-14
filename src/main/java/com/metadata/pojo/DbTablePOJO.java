package com.metadata.pojo;

public class DbTablePOJO {
    String dbName ;
    String tableName;

    public DbTablePOJO() {
    }

    public DbTablePOJO(String dbName, String tableName) {
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "DbTablePOJO{" +
                "dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
