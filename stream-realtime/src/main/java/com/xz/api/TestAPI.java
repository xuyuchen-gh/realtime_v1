package com.xz.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.lang.annotation.ElementType;

/**
 * @author xz
 * @date 2024/12/30
 */
public class TestAPI {
    private static final String  ZOOKEEPER_SERVER_HOST_LIST = "";
    private static final String HBASE_NAME_SPACE = "info";
    private static final String HBASE_CONNECTION_VERSION = "hbase-2.2";

    private static final String createHbaseDimBaseDicDDL = "create table hbase_dim_base_dic (" +
            " rk string, " +
            " info row<dic_name String, parent_code String > ," +
            "primary key (rk) not enforced " +
            ")" +
            "with (" +
            " 'connector' =  '" + HBASE_CONNECTION_VERSION+ "', " +
            " 'table-name' = '" + HBASE_NAME_SPACE + ":dim_base_dic'," +
            " 'zookeeper.quorum' = '"+ZOOKEEPER_SERVER_HOST_LIST +"' " +
            ")";

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        HiveCatalog catalog = new HiveCatalog("hive-catalog", "default", "D:\\soft\\soft\\IDE\\Projects\\shixun1\\stream-dev\\stream-realtime\\src\\main\\resources");
        tEnv.registerCatalog("hive-catalog",catalog);
        tEnv.useCatalog("hive-catalog");
        tEnv.executeSql(createHbaseDimBaseDicDDL);
    }

}
