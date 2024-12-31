package com.xz.api;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author xz
 * @date 2024/12/30
 */
public class ApiSelect {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        HiveCatalog hiveCataLog = new HiveCatalog("hive-catalog", "default",
                "D:\\soft\\soft\\IDE\\Projects\\shixun1\\stream-dev\\stream-realtime\\src\\main\\resources");
        tEnv.registerCatalog("hive-catalog",hiveCataLog);
        tEnv.useCatalog("hive-catalog");

        tEnv.executeSql("select rk," +
                "  info.dic_name  as dic_name," +
                "  info.parent_code as parent_code " +
                "from hbase_dim_base_dic");

        env.execute();

    }


}
