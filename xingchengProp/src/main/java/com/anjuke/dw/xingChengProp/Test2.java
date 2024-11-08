package com.anjuke.dw.xingChengProp;

import com.esotericsoftware.minlog.Log;

import java.util.logging.Logger;

public class Test2 {
    public static void main(String[] args) throws InterruptedException {

        boolean flag = true ;
        Logger log = Logger.getLogger("Test2");
        log.info("abc");
        while(flag){
            System.out.println("哥哥，我想你了");
            Thread.sleep(2000);
        }
    }
}
