package com.anjuke.dw.xingChengProp.util;

import jdk.nashorn.internal.ir.ReturnNode;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

@Data
public class EnvironmentConfiguration implements Serializable {
    public static EnvironmentConfiguration instance = null;

    @PropertyKey("kafka.servers")
    public String kafka_servers;
    @PropertyKey("kafka.client.id")
    public String kafka_client_id;
    @PropertyKey("kafka.topic")
    public String kafka_topic;
    @PropertyKey("kafka.offset")
    public String kafka_offset;

    @PropertyKey("tidb.url")
    public String tidb_url;
    @PropertyKey("tidb.table")
    public String tidb_table;
    @PropertyKey("tidb.driver")
    public String tidb_driver;
    @PropertyKey("tidb.username")
    public String tidb_username;
    @PropertyKey("tidb.password")
    public String tidb_password ;

    public static EnvironmentConfiguration getInstance(){
        if(Objects.isNull(instance)) {
          instance =  PropertiesUtil.inintProperties("config.properties", new EnvironmentConfiguration());
        }
        return instance ;
    }

    public Properties getKafkaProperties(String group_id){
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,this.kafka_servers);

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,this.kafka_client_id);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                (StringUtils.isBlank(this.kafka_offset) ? "earliest":this.kafka_offset));
        return properties;
    }
}
