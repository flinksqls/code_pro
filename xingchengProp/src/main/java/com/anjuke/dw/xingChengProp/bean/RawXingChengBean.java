package com.anjuke.dw.xingChengProp.bean;

import lombok.Data;

import java.util.HashMap;

@Data
public class RawXingChengBean {
    String msgId          ;
    String id;
    String companyUuid;
     String propertyUuid ;
     String employeeUuid;
     String msgType ;
     String type ;
     HashMap<String,String> typeMap ;
     int status;
     int      count  ;
     String deptUuid ;
    String       deptName ;
    String      deptUuid1 ;
    String      deptUuid2 ;
    String      deptUuid3 ;
    String      deptUuid4 ;
    String      deptUuid5 ;
    String      deptUuid6 ;
    String      deptUuid7 ;
    String      deptUuid8 ;
    long    addTime     ;
    long    createTime   ;
    long    updateTime    ;
    String op             ;
    String outingStatusCfgUuid  ;
}
