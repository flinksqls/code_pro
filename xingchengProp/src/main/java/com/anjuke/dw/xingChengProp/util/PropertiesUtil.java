package com.anjuke.dw.xingChengProp.util;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.mqtt.MqttPublishVariableHeader;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Objects;
import java.util.Properties;

public class PropertiesUtil {

    public static<T> T inintProperties(String path ,T obj ){
        InputStream inputStream = PropertiesUtil.class.getClassLoader().getResourceAsStream(path);
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
            Field[] fields = obj.getClass().getDeclaredFields();
            for (Field field : fields) {
                //取消访问权限的检查
                field.setAccessible(true);
                PropertyKey annotation = field.getAnnotation(PropertyKey.class);
                if(Objects.nonNull(annotation)){
                    String propertyKey = annotation.value();
                    String value = properties.getProperty(propertyKey);
                    field.set(obj,value);
                }
            }
           if(inputStream != null ){
               inputStream.close();
           }
           return obj;

        } catch (IOException | IllegalAccessException e) {
            throw new IllegalStateException(String.format("读取配置文件时出错。%s", e.getMessage()),e) ;
        }

    }

}
