package com.xz.utils;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.TableProcessDim;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.JdbcUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * @author xz
 * @date 2024/12/25
 */
public class ProcessDwd2kafkaFunction extends BroadcastProcessFunction<JSONObject,JSONObject,JSONObject> {
    private MapStateDescriptor<String, JSONObject> mapStateDescriptor;
    private HashMap<String, TableProcessDim> configMap =  new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));

        String querySQL = "select * from gmall_config.table_process_dwd";
        List<TableProcessDim> tableProcessDwd = JdbcUtils.queryList(connection, querySQL, TableProcessDim.class, true);
        for (TableProcessDim jsonObject : tableProcessDwd) {
            configMap.put(jsonObject.getSourceTable(),jsonObject);
        }
        connection.close();

    }

    public ProcessDwd2kafkaFunction(MapStateDescriptor<String, JSONObject> mapDescriptor) {
        this.mapStateDescriptor = mapDescriptor;
    }

    @Override
    public void processElement(JSONObject jsonObject,
                               BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext ctx,
                               Collector<JSONObject> collector) throws Exception {

        ReadOnlyBroadcastState<String, JSONObject> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String tableName = jsonObject.getJSONObject("source").getString("table");
        JSONObject broadData = broadcastState.get(tableName);

//        判断不能为空
        if (broadData != null|| configMap.get(tableName) != null ) {
            if (configMap.get(tableName).getSourceTable().equals(tableName)) {
                if (!jsonObject.getString("op").equals("d")) {
                    JSONObject after = jsonObject.getJSONObject("after");

                    System.out.println(after);
                    JSONObject kafkaData = new JSONObject();
                    kafkaData.put("tableName",tableName);
                    kafkaData.put("operation",jsonObject.getString("op"));
                    kafkaData.put("data",after);

                    ArrayList<JSONObject> list = new ArrayList<>();
                    list.add(kafkaData);

                    KafkaUtils.sinkJson2KafkaMessage(tableName,list);

                }
            }
        }


    }

    @Override
    public void processBroadcastElement(JSONObject jsonObject,
                                        BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context context,
                                        Collector<JSONObject> collector) throws Exception {
        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String op = jsonObject.getString("op");
        if (jsonObject.containsKey("after")) {
            String source_table = jsonObject.getJSONObject("after").getString("source_table");
            if ("d".equals(op)) {
                broadcastState.remove(source_table);
            }else  {
                broadcastState.put(source_table,jsonObject);
            }
        }
    }
}
