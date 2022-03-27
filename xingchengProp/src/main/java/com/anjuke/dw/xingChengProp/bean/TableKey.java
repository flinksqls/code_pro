package com.anjuke.dw.xingChengProp.bean;

import lombok.Data;

@Data
public class TableKey {
    String companyUuid ;
    String employeeUuid ;
    String deptUuid ;
    String deptUuid1 ;
    String deptUuid2 ;
    String deptUuid3 ;
    String deptUuid4 ;
    String deptUuid5 ;
    String deptUuid6 ;
    String deptUuid7 ;
    String deptUuid8 ;
    String addTime_date;
    String updateTime_date;
    String tradeKind;
    String dept_uuid_path;
    String dept_uuid_substr_concat;

    public TableKey(String companyUuid, String employeeUuid, String deptUuid, String deptUuid1, String deptUuid2, String deptUuid3, String deptUuid4, String deptUuid5, String deptUuid6, String deptUuid7, String deptUuid8, String addTime_date, String updateTime_date, String tradeKind, String dept_uuid_path, String dept_uuid_substr_concat) {
        this.companyUuid = companyUuid;
        this.employeeUuid = employeeUuid;
        this.deptUuid = deptUuid;
        this.deptUuid1 = deptUuid1;
        this.deptUuid2 = deptUuid2;
        this.deptUuid3 = deptUuid3;
        this.deptUuid4 = deptUuid4;
        this.deptUuid5 = deptUuid5;
        this.deptUuid6 = deptUuid6;
        this.deptUuid7 = deptUuid7;
        this.deptUuid8 = deptUuid8;
        this.addTime_date = addTime_date;
        this.updateTime_date = updateTime_date;
        this.tradeKind = tradeKind;
        this.dept_uuid_path = dept_uuid_path;
        this.dept_uuid_substr_concat = dept_uuid_substr_concat;
    }

    public static TableKey build(XingChengBean xingcheng){
        return new TableKey(
                xingcheng.getCompanyUuid(),
                xingcheng.getEmployeeUuid(),
                xingcheng.getDeptUuid(),
                xingcheng.getDeptUuid1(),
                xingcheng.getDeptUuid2(),
                xingcheng.getDeptUuid3(),
                xingcheng.getDeptUuid4(),
                xingcheng.getDeptUuid5(),
                xingcheng.getDeptUuid6(),
                xingcheng.getDeptUuid7(),
                xingcheng.getDeptUuid8(),
                xingcheng.getAddTime_date(),
                xingcheng.getUpdateTime_date(),
                xingcheng.getTradeKind(),
                xingcheng.getDept_uuid_path(),
                xingcheng.getDept_uuid_substr_concat()
        );
    }
}
