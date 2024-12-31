package com.xz.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author xz
 * @date 2024/12/30
 */
public class CreateHbaseDDlL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String createHiveCateLogDDL = "create catalog hive_catalog with" +
                " 'type' = 'hive' ," +
                " 'default-database' = 'default' ," +
                " 'hive-conf-dir' = '' " +
                ")";


    }
}
