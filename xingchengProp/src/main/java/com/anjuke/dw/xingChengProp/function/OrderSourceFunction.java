package com.anjuke.dw.xingChengProp.function;

import com.anjuke.dw.xingChengProp.bean.Order;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.expressions.In;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OrderSourceFunction implements SourceFunction<Order> {
    private boolean flag = true ;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        Random random = new Random();
        int i = 0 ;
        int order_id = 1 ;
        int user_id = 1 ;
        List<Integer> l = new ArrayList();
        l.add(1);
        l.add(2);
        l.add(3);
        l.add(4);
        l.add(5);
        int good_id = 0 ;

        while(flag){
             good_id = random.nextInt(10);
             sourceContext.collect(
             new Order(
                     order_id++
                     ,l.get(random.nextInt(5))
                     ,good_id
                     ,random.nextInt(10)
                     ,System.currentTimeMillis())
             );
             Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
         flag = false ;
    }
}
