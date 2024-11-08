package com.anjuke.dw.xingChengProp.bean;

import lombok.Data;

import java.util.Date;
@Data
public class Order {
    int order_id ;
    int user_id ;
    int goods_id ;
    int qty ;
    Long  create_time ;

    public Order(int order_id, int user_id, int goods_id, int qty, Long create_time) {
        this.order_id = order_id;
        this.user_id = user_id;
        this.goods_id = goods_id;
        this.qty = qty;
        this.create_time = create_time;
    }



}
