package com.xz;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.xz.utils.ProcessDwd2kafkaFunction;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author xz
 * @date 2024/12/25
 */
public class DbusCdcDwdDb2Kafka {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // TODO: 2024/12/25  1 cdc读取主表和dwd维表
//        主表
        MySqlSource<String> mySQLMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
//        配置表
        MySqlSource<String> mySQLDwdCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "gmall_config.table_process_dwd",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

        DataStreamSource<String> cdcMainStream = env.fromSource(mySQLMainCdcSource, WatermarkStrategy.noWatermarks(),
                "cdc_mysql_main_source");
        DataStreamSource<String> cdcDwdStream = env.fromSource(mySQLDwdCdcSource, WatermarkStrategy.noWatermarks(),
                "dwd_mysql_dwd_source");

        SingleOutputStreamOperator<JSONObject> cdcMainStreamMap= cdcMainStream.map(JSONObject::parseObject)
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1);

        SingleOutputStreamOperator<JSONObject> cdcDwdStreamMap = cdcDwdStream.map(JSONObject::parseObject)
                .uid("dwd_data_convert_json")
                .name("dwd_data_convert_json")
                .setParallelism(1);

//        输出主表和维度表数据
//        cdcMainStreamMap.print("main=============>");
//        cdcDwdStreamMap.print("dwd=============>");

        // TODO: 2024/12/25222  2 清洗数据
        SingleOutputStreamOperator<JSONObject> cdcDwdClean = cdcDwdStreamMap.map(s -> {
                    s.remove("source");
                    s.remove("transaction");
                    JSONObject jsonObject = new JSONObject();
                    if ("d".equals(s.getString("op"))) {
                        jsonObject.put("before", s.getJSONObject("before"));
                    } else {
                        jsonObject.put("after", s.getJSONObject("after"));
                    }
                    jsonObject.put("op", s.getString("op"));
                    return jsonObject;
                }).uid("clean_json_column_map")
                .name("clean_json_column_map");

//        cdcMainStreamMap.print("main======>");
//        cdcDwdClean.print("dwd=======>");

//        创建描述器
        MapStateDescriptor<String, JSONObject> mapDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, JSONObject.class);
//         创建广播流
        BroadcastStream<JSONObject> broadcastStream = cdcDwdClean.broadcast(mapDescriptor);
//        主流和广播流连接
        BroadcastConnectedStream<JSONObject, JSONObject> connect = cdcMainStreamMap.connect(broadcastStream);

//        存入kafka
        connect.process(new ProcessDwd2kafkaFunction(mapDescriptor));


        env.disableOperatorChaining();
        env.execute();
    }
}
