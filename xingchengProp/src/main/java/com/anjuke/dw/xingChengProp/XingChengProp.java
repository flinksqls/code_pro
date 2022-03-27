package com.anjuke.dw.xingChengProp;

import com.anjuke.dw.xingChengProp.DTO.XingchengPropDto;
import com.anjuke.dw.xingChengProp.bean.XingChengBean;
import com.anjuke.dw.xingChengProp.function.MessageFilterFunction;
import com.anjuke.dw.xingChengProp.function.MsgMapFunction;
import com.anjuke.dw.xingChengProp.function.Prc;
import com.anjuke.dw.xingChengProp.function.TidbSinkFunction;
import com.anjuke.dw.xingChengProp.util.EnvUtil;
import com.anjuke.dw.xingChengProp.util.EnvironmentConfiguration;
import com.anjuke.dw.xingChengProp.util.PropertiesUtil;
import lombok.val;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class XingChengProp {
    public static void main(String[] args) throws Exception {
        // kafka 默认组id
       String  groupId  = "12345";
       String  checkpointPath ;
       String  checkpointInterval ;

        // 主类参数
        ParameterTool params = ParameterTool.fromArgs(args);
        groupId = params.get("groupId", groupId);
        System.out.println(groupId);
        checkpointPath = params.get("checkpointPath");
        System.out.println(checkpointPath);
        checkpointInterval = params.get("checkpointInterval");
        System.out.println(checkpointInterval);
        //配置文件参数
        EnvironmentConfiguration envConf = EnvironmentConfiguration.getInstance();

        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer =
                new FlinkKafkaConsumer<>(envConf.getKafka_topic(), new SimpleStringSchema()
                        ,envConf.getKafkaProperties(groupId) );
        stringFlinkKafkaConsumer.setStartFromEarliest();

        StreamExecutionEnvironment env = EnvUtil.getEnv(checkpointInterval,checkpointPath);

        DataStreamSource<String> source  = env.addSource(stringFlinkKafkaConsumer);

        SingleOutputStreamOperator<XingChengBean> map =
                source.filter(new MessageFilterFunction())
                .map(new MsgMapFunction());

        SingleOutputStreamOperator<XingchengPropDto> process = map
                .keyBy(TableKey::build)
                .process(new Prc());

        process.addSink(new TidbSinkFunction(envConf));
        //proces.print();
        env.execute();
    }

}
