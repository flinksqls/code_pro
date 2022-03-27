package com.anjuke.dw.xingChengProp.function;

import com.anjuke.dw.xingChengProp.DTO.XingchengPropDto;
import com.anjuke.dw.xingChengProp.TableKey;
import com.anjuke.dw.xingChengProp.XingChengProp;
import com.anjuke.dw.xingChengProp.bean.XingChengBean;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.checkerframework.checker.units.qual.K;

import java.util.HashMap;


public class Prc extends KeyedProcessFunction<TableKey, XingChengBean, XingchengPropDto> {

    ValueState<XingchengPropDto> state ;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<XingchengPropDto> stateDesc
                = new ValueStateDescriptor<>("stateDesc", XingchengPropDto.class);
        state = getRuntimeContext().getState(stateDesc);
    }

    @Override
    public void processElement(XingChengBean xingChengBean,
                               Context context, Collector<XingchengPropDto> collector) throws Exception {
       // System.out.println("进入 process。。。");
       // System.out.println(xingChengBean);
        if(state.value() == null ){
              // System.out.println("state is null ");
               XingchengPropDto xingchengPropDto = new XingchengPropDto();
               xingchengPropDto.setCompanyUuid(xingChengBean.getCompanyUuid());
               xingchengPropDto.setEmployeeUuid(xingChengBean.getEmployeeUuid());
               xingchengPropDto.setDeptUuid(xingChengBean.getDeptUuid());
               xingchengPropDto.setDeptUuid1(xingChengBean.getDeptUuid1());
               xingchengPropDto.setDeptUuid2(xingChengBean.getDeptUuid2());
               xingchengPropDto.setDeptUuid3(xingChengBean.getDeptUuid3());
               xingchengPropDto.setDeptUuid4(xingChengBean.getDeptUuid4());
               xingchengPropDto.setDeptUuid5(xingChengBean.getDeptUuid5());
               xingchengPropDto.setDeptUuid6(xingChengBean.getDeptUuid6());
               xingchengPropDto.setDeptUuid7(xingChengBean.getDeptUuid7());
               xingchengPropDto.setDeptUuid8(xingChengBean.getDeptUuid8());
               xingchengPropDto.setAddTime_date(xingChengBean.getAddTime_date());
               xingchengPropDto.setUpdateTime_date(xingChengBean.getUpdateTime_date());
               xingchengPropDto.setTradeKind(xingChengBean.getTradeKind());
               xingchengPropDto.setDept_uuid_path(xingChengBean.getDept_uuid_path());
               xingchengPropDto.setDept_uuid_substr_concat(xingChengBean.getDept_uuid_substr_concat());
               //消息记录数
               xingchengPropDto.setRecord_num(1);
               int count = xingChengBean.getCount();
               //    sum(case when typeMap['module'] = 'property' and typeMap['actType'] = 'add'
           //    then `count` end)  property_add_num,
               HashMap<String, String> typeMap = xingChengBean.getTypeMap();
               String module = typeMap.get("module");
               String actType = typeMap.get("actType");
               String propertyTradeKind = typeMap.get("propertyTradeKind");
               if(module.equals("property") ){
                   if(actType.equals("add")){
                       xingchengPropDto.setProperty_add_num(count);
                   }else if(actType.equals("activate")){
                       xingchengPropDto.setProperty_activate_num(count);
                   }
               }else if(module.equals("ZlfollowAddsell")){
                   xingchengPropDto.setKey_num(count);
               }else if(module.equals("followAdd")){
                    xingchengPropDto.setProp_follow_num(count);
                }
              // System.out.println("xingchengPropDto:");
             //  System.out.println(xingchengPropDto);
               state.update(xingchengPropDto);
               collector.collect(xingchengPropDto);
           }else {
              // System.out.println("state is not null ");
               XingchengPropDto xingchengPropDto =  state.value();

               int count = xingChengBean.getCount();
              // System.out.println("count:" + count);
               //    sum(case when typeMap['module'] = 'property' and typeMap['actType'] = 'add'
               //    then `count` end)  property_add_num,
               HashMap<String, String> typeMap = xingChengBean.getTypeMap();
               String module = typeMap.get("module");
               String actType = typeMap.get("actType");
               if(module.equals("property") ){
                   if(actType.equals("add")){
                       xingchengPropDto.setProperty_add_num(count + xingchengPropDto.getProperty_add_num());
                   } else if(actType.equals("activate")){
                   xingchengPropDto.setProperty_activate_num(count + xingchengPropDto.getProperty_activate_num());
               }
             }else if(module.equals("ZlfollowAddsell")){
                 xingchengPropDto.setKey_num(count + xingchengPropDto.getKey_num());
             }else if(module.equals("followAdd")){
                   xingchengPropDto.setProp_follow_num(count + xingchengPropDto.getProp_follow_num());
             }
               //消息记录数
                xingchengPropDto.setRecord_num(xingchengPropDto.getRecord_num() + 1 );
             //  System.out.println("xingchengPropDto:");
              // System.out.println(xingchengPropDto);
                state.update(xingchengPropDto);
                collector.collect(xingchengPropDto);
           }


    }
}
