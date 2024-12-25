package com.xz;



import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.MapUpdateHbaseDimTableFunc;
import com.retailersv1.func.ProcessSpiltStreamToHBaseDim;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DbusCdc2DimHbase {
    //自定义两个方法 1. zookeper的服务 2.hbase的命名空间
    private  static final  String CDH_ZOOKEEPRER_SERVE= ConfigUtils.getString("zookeeper.server.host.list");
    private static  final  String CDH_HBASE_NASME_SPACE=ConfigUtils.getString("hbase.namespace");
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        MySqlSource<String> mySQLMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
        MySqlSource<String> mySQLDimCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "gmall_config.table_process_dim",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
        DataStreamSource<String> cdcDbMainStream = env.fromSource(mySQLMainCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");
        DataStreamSource<String> cdcDbDimStream = env.fromSource(mySQLDimCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dim_source");


        SingleOutputStreamOperator<JSONObject> cdcMainStreamMap= cdcDbMainStream.map(JSONObject::parseObject)
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1);
        SingleOutputStreamOperator<JSONObject> cdcDimStreamMap = cdcDbDimStream.map(JSONObject::parseObject)
                .uid("dim_data_convert_json")
                .name("dim_data_convert_json")
                .setParallelism(1);
//        // TODO: 2024/12/22 输出主表和配置表数据
//        cdcMainStreamMap.print("====gmall====================>a");
//        cdcDimStreamMap.print("=====gmall_config=========================>");

        SingleOutputStreamOperator<JSONObject> cdcDimClean = cdcDimStreamMap.map(s -> {
//                    System.out.println(s+"=====================>");
                    s.remove("source");
                    s.remove("transaction");
                    JSONObject resJson = new JSONObject();
                    if ("d" .equals(s.getString("op"))) {
                        resJson.put("before", s.getJSONObject("before"));
                    } else {
                        resJson.put("after", s.getJSONObject("after"));
                    }
                    resJson.put("op", s.getString("op"));
                    return resJson;
                })
                 .uid("clean_json_column_map")
                .name("clean_json_column_map");
//        cdcDimClean.print();


        SingleOutputStreamOperator<JSONObject> tpDS = cdcDimClean.map(new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPRER_SERVE, CDH_HBASE_NASME_SPACE));

        // TODO:  创建描述器
        MapStateDescriptor<String, JSONObject> broadcastDs = new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);
//
//        // TODO:   配置表和描述器绑定
        BroadcastStream<JSONObject> broadcast = tpDS.broadcast(broadcastDs);

        // TODO:  双流连接 配置表 和所有数据
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = cdcMainStreamMap.connect(broadcast);

//        todo 存入hbase
        connectDs.process(new ProcessSpiltStreamToHBaseDim(broadcastDs));
//        process.print();


//        process.print("process======>");

        env.disableOperatorChaining();
        env.execute();
    }
}
