package com.anjuke.dw.xingChengProp.function;

import com.anjuke.dw.xingChengProp.bean.Order;
import com.anjuke.dw.xingChengProp.util.EnvironmentConfiguration;
import com.anjuke.dw.xingChengProp.util.PropertiesUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import javax.swing.*;
import  org.apache.log4j.Logger;

public class TestMapFunction extends RichMapFunction<Order,Order> {
    Logger logger = null ;
    @Override
    public void open(Configuration parameters) throws Exception {
        logger = Logger.getLogger("TestMapFunction");

        logger.info("info test");
        logger.error("error!!!");
        logger.fatal("fatal !!!");

    }

    @Override
    public Order map(Order value) throws Exception {
        logger.error(value.toString());
        return value;
    }
}
