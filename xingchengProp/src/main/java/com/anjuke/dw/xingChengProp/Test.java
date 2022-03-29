package com.anjuke.dw.xingChengProp;

import com.anjuke.dw.xingChengProp.util.PropertiesUtil;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class Test {
    public static void main(String[] args) throws IOException {
        ClassLoader classLoader = Test.class.getClassLoader();
        System.out.println(classLoader);
        URL resource = classLoader.getResource("config.properties");
        InputStream resourceAsStream
                = classLoader.getResourceAsStream("config.properties");
        System.out.println(resource);

        Properties properties = new Properties();

        properties.load(resourceAsStream);
        System.out.println(properties);


    }


}
