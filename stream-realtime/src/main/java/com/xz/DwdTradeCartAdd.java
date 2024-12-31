package com.xz;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author xz
 * @date 2024/12/27
 */
public class DwdTradeCartAdd {

    private static final  String topic_db = ConfigUtils.getString("kafka.topic.db");
    private static final  String bootServerList = ConfigUtils.getString("kafka.bootstrap.servers");
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);


        DataStreamSource<String> topic_db_stream = env.fromSource(KafkaUtils.buildKafkaSource(bootServerList,
                        topic_db, "test",
                        OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(), "kafka_source_db");


        SingleOutputStreamOperator<JSONObject> jsonTopicDb = topic_db_stream.map(JSONObject::parseObject).uid("dwd_convent_json").name("dwd_convent_json");



        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

//        4> {"before":null,"after":{"id":3631,"log":"{\"common\":{\"ar\":\"23\",\"ba\":\"SAMSUNG\",\"ch\":\"xiaomi\",\"is_new\":\"1\",\"md\":\"SAMSUNG Galaxy S21\",\"mid\":\"mid_128\",\"os\":\"Android 13.0\",\"sid\":\"49726527-a27b-4511-ad98-d808179b770d\",
//        \"uid\":\"185\",\"vc\":\"v2.1.134\"},\"page\":{\"during_time\":15000,\"item\":\"185\",\"item_type\":\"user_id\",\"last_page_id\":\"good_detail\",\"page_id\":\"register\"},
//        \"ts\":1654694867226}"},
//        "source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall","sequence":null,"table":"z_log","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
//        "op":"r","ts_ms":1735300776806,"transaction":null}


        tEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  op string," +
                "db string," +
                "before map<String,String>," +
                "after map<String,String>," +
                "source map<String,String>," +
                "ts_ms bigint," +
                "row_time as TO_TIMESTAMP_LTZ(ts_ms,3)," +
                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic_db + "',\n" +
                "  'properties.bootstrap.servers' = '" + bootServerList + "',\n" +
                "  'properties.group.id' = '1',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        Table table1 = tEnv.sqlQuery("select * from topic_db");
//        tEnv.toDataStream(table1).print();

        Table table = tEnv.sqlQuery("select " +
                "`after` ['id'] as id ,\n" +
                "`after` ['user_id'] as user_id ,\n" +
                "`after` ['sku_id'] as sku_id ,\n" +
                "`after` ['cart_price'] as cart_price ,\n" +
                "if(op='c',cast(after['sku_num'] as bigint),cast(after['sku_num'] as bigint)-cast(before['sku_num'] as bigint)) sku_num ,\n" +
                "`after` ['img_url'] as img_url ,\n" +
                "`after` ['sku_name'] as sku_name,\n" +
                "`after` ['is_checked'] as is_checked ,\n" +
                "`after` ['create_time'] as create_time ,\n" +
                "`after` ['operate_time'] as operate_time ,\n" +
                "`after` ['is_ordered'] as is_ordered ,\n" +
                "`after` ['order_time'] as order_time ," +
                "ts_ms as ts_ms " +
                "from topic_db " +
                "where source['table']='cart_info'   " +
                "and (op='c' or (op='u' and before['sku_num'] is not null " +
                "and cast (after['sku_num'] as bigint) > cast(before['sku_num'] as bigint)))");
        tEnv.toDataStream(table).print();


        env.execute();

    }
}
