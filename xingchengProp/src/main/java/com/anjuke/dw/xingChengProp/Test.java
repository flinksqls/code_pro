package com.anjuke.dw.xingChengProp;


import com.anjuke.dw.xingChengProp.bean.Order;
import com.anjuke.dw.xingChengProp.bean.XingChengBean;
import com.anjuke.dw.xingChengProp.function.MessageFilterFunction;
import com.anjuke.dw.xingChengProp.function.MsgMapFunction;
import com.anjuke.dw.xingChengProp.function.OrderSourceFunction;
import com.anjuke.dw.xingChengProp.function.TestMapFunction;
import com.anjuke.dw.xingChengProp.util.EnvUtil;
import com.anjuke.dw.xingChengProp.util.EnvironmentConfiguration;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;

/**
 *本类用来做读取配置文件
 */


public class Test {
    public static void main(String[] args) throws Exception {
        System.out.println("job begining ...");
        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<Order> source  = env.addSource(new OrderSourceFunction());

        SingleOutputStreamOperator<Order> map = source.map(new TestMapFunction());
        map.print();
        env.execute();


    }


}
