package com.anjuke.dw.xingChengProp.bean;

import lombok.Data;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
@Data
public class XingChengBean {
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
    String    addTime_date     ;
    long    createTime   ;
    long   updateTime    ;
    String   updateTime_date    ;
    String op             ;
    String outingStatusCfgUuid  ;
    String tradeKind ;
    String dept_uuid_path;
    String dept_uuid_substr_concat;

    public XingChengBean(String msgId, String id, String companyUuid, String propertyUuid, String employeeUuid, String msgType, String type, HashMap<String, String> typeMap, int status, int count, String deptUuid, String deptName, String deptUuid1, String deptUuid2, String deptUuid3, String deptUuid4, String deptUuid5, String deptUuid6, String deptUuid7, String deptUuid8, long addTime, String addTime_date, long createTime, long updateTime, String updateTime_date, String op, String outingStatusCfgUuid, String tradeKind, String dept_uuid_path, String dept_uuid_substr_concat) {
        this.msgId = msgId;
        this.id = id;
        this.companyUuid = companyUuid;
        this.propertyUuid = propertyUuid;
        this.employeeUuid = employeeUuid;
        this.msgType = msgType;
        this.type = type;
        this.typeMap = typeMap;
        this.status = status;
        this.count = count;
        this.deptUuid = deptUuid;
        this.deptName = deptName;
        this.deptUuid1 = deptUuid1;
        this.deptUuid2 = deptUuid2;
        this.deptUuid3 = deptUuid3;
        this.deptUuid4 = deptUuid4;
        this.deptUuid5 = deptUuid5;
        this.deptUuid6 = deptUuid6;
        this.deptUuid7 = deptUuid7;
        this.deptUuid8 = deptUuid8;
        this.addTime = addTime;
        this.addTime_date = addTime_date;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.updateTime_date = updateTime_date;
        this.op = op;
        this.outingStatusCfgUuid = outingStatusCfgUuid;
        this.tradeKind = tradeKind;
        this.dept_uuid_path = dept_uuid_path;
        this.dept_uuid_substr_concat = dept_uuid_substr_concat;
    }
}
