package com.anjuke.dw.xingChengProp.function;

import com.anjuke.dw.xingChengProp.bean.RawXingChengBean;
import com.anjuke.dw.xingChengProp.bean.XingChengBean;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.beans.binding.ObjectExpression;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.netty4.io.netty.util.internal.StringUtil;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import static org.apache.calcite.runtime.SqlFunctions.md5;

public class MsgMapFunction implements MapFunction<String, XingChengBean> {
    SimpleDateFormat dateFormat;
    RawXingChengBean raw ;
    private final ObjectMapper mapper  ;
    public MsgMapFunction() {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.dateFormat =  new SimpleDateFormat("yyyy-MM-dd");
    }
    @Override
    public XingChengBean map(String str) throws Exception {

        raw = mapper.readValue(str,RawXingChengBean.class);

        HashMap<String, String> typeMap = raw.getTypeMap();
        String verificationType = typeMap.get("verificationType");
        String propertyTradeKind = typeMap.get("propertyTradeKind");
        String propType = typeMap.get("propType");
        String tradeKind = "" ;
        if(verificationType == "sellVerification" ) {
            tradeKind = "sell";
        } else if(verificationType == "rentVerification" ) {
            tradeKind = "rent";
        }else if( !StringUtils.isBlank(propertyTradeKind) ) {
            tradeKind = propertyTradeKind ;
        }else {
            tradeKind = propType ;
        }
     //   CONCAT('woshizuishangcengdebumenuuid',if(coalesce(deptUuid1,'')='','',CONCAT(',',deptUuid1)),if(coalesce(deptUuid2,'')='','',CONCAT(',',deptUuid2)),if(coalesce(deptUuid3,'')='','',CONCAT(',',deptUuid3)),if(coalesce(deptUuid4,'')='','',CONCAT(',',deptUuid4)),if(coalesce(deptUuid5,'')='','',CONCAT(',',deptUuid5)),if(coalesce(deptUuid6,'')='','',CONCAT(',',deptUuid6)),if(coalesce(deptUuid7,'')='','',CONCAT(',',deptUuid7)),if(coalesce(deptUuid8,'')='','',CONCAT(',',deptUuid8))) as dept_uuid_path ,
     //   md5(CONCAT('woshizuishangcengdebumenuuid',if(coalesce(deptUuid1,'')='','',CONCAT(',',deptUuid1)),if(coalesce(deptUuid2,'')='','',CONCAT(',',deptUuid2)),if(coalesce(deptUuid3,'')='','',CONCAT(',',deptUuid3)),if(coalesce(deptUuid4,'')='','',CONCAT(',',deptUuid4)),if(coalesce(deptUuid5,'')='','',CONCAT(',',deptUuid5)),if(coalesce(deptUuid6,'')='','',CONCAT(',',deptUuid6)),if(coalesce(deptUuid7,'')='','',CONCAT(',',deptUuid7)),if(coalesce(deptUuid8,'')='','',CONCAT(',',deptUuid8))) )   dept_uuid_path_md5
        String deptUuid1 = raw.getDeptUuid1();
        String deptUuid2 = raw.getDeptUuid2();
        String deptUuid3 = raw.getDeptUuid3();
        String deptUuid4 = raw.getDeptUuid4();
        String deptUuid5 = raw.getDeptUuid5();
        String deptUuid6 = raw.getDeptUuid6();
        String deptUuid7 = raw.getDeptUuid7();
        String deptUuid8 = raw.getDeptUuid8();

        if( StringUtils.isBlank(deptUuid1)) deptUuid1 = ""; else deptUuid1 = ","+  deptUuid1;
        if( StringUtils.isBlank(deptUuid2)) deptUuid2 = ""; else deptUuid2 = ","+  deptUuid2;
        if( StringUtils.isBlank(deptUuid3)) deptUuid3 = ""; else deptUuid3 = ","+  deptUuid3;
        if( StringUtils.isBlank(deptUuid4)) deptUuid4 = ""; else deptUuid4 = ","+  deptUuid4;
        if( StringUtils.isBlank(deptUuid5)) deptUuid5 = ""; else deptUuid5 = ","+  deptUuid5;
        if( StringUtils.isBlank(deptUuid6)) deptUuid6 = ""; else deptUuid6 = ","+  deptUuid6;
        if( StringUtils.isBlank(deptUuid7)) deptUuid7 = ""; else deptUuid7 = ","+  deptUuid7;
        if( StringUtils.isBlank(deptUuid8)) deptUuid8 = ""; else deptUuid8 = ","+  deptUuid8;



        String dept_uuid_path = "woshizuishangcengdebumenuuid" +
                deptUuid1+
                deptUuid2+
                deptUuid3+
                deptUuid4+
                deptUuid5+
                deptUuid6+
                deptUuid7+
                deptUuid8
                ;
       // System.out.println(dept_uuid_path);
        String dept_uuid_substr_concat = md5(dept_uuid_path);

        return new XingChengBean(
                raw.getMsgId(),
                raw.getId(),
                raw.getCompanyUuid(),
                raw.getPropertyUuid(),
                raw.getEmployeeUuid(),
                raw.getMsgType(),
                raw.getType(),
                raw.getTypeMap(),
                raw.getStatus(),
                raw.getCount(),
                raw.getDeptUuid(),
                raw.getDeptName(),
                raw.getDeptUuid1(),
                raw.getDeptUuid2(),
                raw.getDeptUuid3(),
                raw.getDeptUuid4(),
                raw.getDeptUuid5(),
                raw.getDeptUuid6(),
                raw.getDeptUuid7(),
                raw.getDeptUuid8(),
                raw.getAddTime(),
                dateFormat.format(new Date(raw.getAddTime())),
                raw.getCreateTime(),
                raw.getUpdateTime(),
                dateFormat.format(new Date(raw.getUpdateTime())),
                raw.getOp(),
                raw.getOutingStatusCfgUuid(),
                tradeKind,
                dept_uuid_path,
                dept_uuid_substr_concat
        )
                ;
    }
}
