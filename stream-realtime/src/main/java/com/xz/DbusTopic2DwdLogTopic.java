package com.xz;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.DateFormatUtil;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;


/**
 * @author xz
 * @date 2024/12/24
 */
public class DbusTopic2DwdLogTopic {
    @SneakyThrows
    public static void main(String[] args) {
//        创建流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);


        KafkaSource<String> stringKafkaSource = KafkaUtils.buildKafkaSource("cdh01:9092", "topic_log", "test", OffsetsInitializer.earliest());

        //读取kafka_topic 的数据
        DataStreamSource<String> kafkaSourceStream = env.fromSource(stringKafkaSource,
                WatermarkStrategy.noWatermarks(), "kafka_topic");

//        kafkaSourceStream.print();

        // TODO: 2024/12/24  过滤非json格式
        SingleOutputStreamOperator<JSONObject> etlStream = kafkaSourceStream.filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        try {
                            JSON.parseObject(value );
                            return true;
                        } catch (Exception e) {
                            System.out.println("非正确json格式: " + value);
                            return false;
                        }
                    }
                })
                .map(JSON::parseObject);

//        etlStream.print();

        // TODO: 2024/12/24  新老用户
        SingleOutputStreamOperator<JSONObject> validateNewOrOldStream = etlStream.keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<String> firstVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<JSONObject> out) throws Exception {

                        JSONObject common = obj.getJSONObject("common");
                        String isNew = common.getString("is_new");

                        String ts = obj.getString("ts");
                        String today = DateFormatUtil.tsToDate(Long.parseLong(ts));


                        // 从状态中获取首次访问日志
                        String firstVisitDate = firstVisitDateState.value();

                        if ("1".equals(isNew)) {
                            if (firstVisitDate == null) {
                                // 这个 mid 的首次访问
                                firstVisitDateState.update(today);
                            }else if(!today.equals(firstVisitDate)){
                                // 今天和首次访问不一致
                                common.put("is_new", "0");  // 把新用户修改为老用户
                            }
                        }else{
                            if (firstVisitDate == null) {
                                // 一个老用户, 他的首次访问日志还是 null
                                // 把他的首次访问日期设置为昨天
                                firstVisitDateState.update(DateFormatUtil.tsToDate(Long.parseLong(ts) - 24 * 60 * 60 * 1000));
                            }
                        }
                        out.collect(obj);
                    }
                });

//        validateNewOrOldStream.print();


        //3 分流
        OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("start"){};
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display"){};
        OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("action"){};
        OutputTag<JSONObject> errTag = new OutputTag<JSONObject>("err"){};
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page"){};

        SingleOutputStreamOperator<JSONObject> DwdLogProcess = validateNewOrOldStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject,
                                       ProcessFunction<JSONObject, JSONObject>.Context context,
                                       Collector<JSONObject> collector) throws Exception {
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = Long.parseLong(jsonObject.getString("ts"));

                // TODO: 2024/12/24 错误日志
                JSONObject err = jsonObject.getJSONObject("err");
                if (err != null) {
                    context.output(errTag, jsonObject);
                    jsonObject.remove("err");
                }
                // TODO: 2024/12/24 启动日志
                JSONObject start = jsonObject.getJSONObject("start");
                if (start != null) {
                    context.output(startTag, start);
                    jsonObject.remove("start");
                }
                // TODO: 2024/12/24 页面日志
                JSONObject page = jsonObject.getJSONObject("page");
                if (page != null) {
                    // TODO: 2024/12/24 曝光日志
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page", page);
                            display.put("common", common);
                            display.put("ts", ts);
                            context.output(displayTag, display);
                        }
                        jsonObject.remove("displays");
                    }
                }
                // TODO: 2024/12/24  活动日志
                JSONArray actions = jsonObject.getJSONArray("actions");
                if (actions != null) {
                    for (int i = 0; i < actions.size(); i++) {
                        JSONObject action = actions.getJSONObject(i);
                        action.put("page", page);
                        action.put("common", common);
                        action.put("ts", ts);
                        context.output(actionTag, action);
                    }
                    jsonObject.remove("actions");
                }
                context.output(pageTag,jsonObject);
                collector.collect(jsonObject);
            }
        });

        SideOutputDataStream<JSONObject> start = DwdLogProcess.getSideOutput(startTag);
        SideOutputDataStream<JSONObject> display = DwdLogProcess.getSideOutput(displayTag);
        SideOutputDataStream<JSONObject> action = DwdLogProcess.getSideOutput(actionTag);
        SideOutputDataStream<JSONObject> err = DwdLogProcess.getSideOutput(errTag);
        SideOutputDataStream<JSONObject> page = DwdLogProcess.getSideOutput(pageTag);
        start.print("start=====>>>>>");
        display.print("display=====>>>>>");
        action.print("action=====>>>>>");
        err.print("err=====>>>>>");
        page.print("page=====>>>>>");

        start.map(m -> m.toString()).sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_log_start"));
        display.map(m -> m.toString()).sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_log_display"));
        action.map(m -> m.toString()).sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_log_action"));
        err.map(m -> m.toString()).sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_log_err"));
        page.map(m -> m.toString()).sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_log_page"));




        env.execute();
    }
}
