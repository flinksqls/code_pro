package com.anjuke.dw.xingChengProp;

import com.anjuke.dw.xingChengProp.DTO.XingchengPropDto;
import com.anjuke.dw.xingChengProp.bean.TableKey;
import com.anjuke.dw.xingChengProp.bean.XingChengBean;
import com.anjuke.dw.xingChengProp.function.*;
import com.anjuke.dw.xingChengProp.util.EnvUtil;
import com.anjuke.dw.xingChengProp.util.EnvironmentConfiguration;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class XingChengProp {
    public static void main(String[] args) throws Exception {
        // kafka 默认组id
       String  groupId  = "12345";
       String  checkpointPath ;
       String  checkpointInterval ;

        // 主类参数
        ParameterTool params = ParameterTool.fromArgs(args);
        groupId = params.get("groupId", groupId);
        checkpointPath = params.get("checkpointPath");
        checkpointInterval = params.get("checkpointInterval");
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

        // process.addSink(new TidbSinkFunction2(envConf));
        process.addSink( new TidbSink(envConf).getSink());

        //proces.print();
        env.execute();
    }

}
