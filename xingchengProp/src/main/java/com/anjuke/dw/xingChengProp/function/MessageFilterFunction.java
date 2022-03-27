package com.anjuke.dw.xingChengProp.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import jdk.nashorn.internal.ir.ReturnNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.netty4.io.netty.util.internal.StringUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MessageFilterFunction implements FilterFunction<String> {

    private final ObjectMapper objectMapper;

    public MessageFilterFunction() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public boolean filter(String  str ) throws Exception {
        if(StringUtils.isBlank(str)){
            return false;
        }

        Map<Object,Object> map = objectMapper.readValue(str, Map.class);

        Object count = map.get("count");
        int i = Integer.parseInt(count.toString());
        if( i == 0 ){
            return false ;
        }

        Object addTime = map.get("addTime");

        if(Objects.isNull(addTime)){
            return false ;
        }

        String RawTypeMap = String.valueOf(map.get("typeMap"));


        Map typeMap = (HashMap)map.get("typeMap");
        if(Objects.isNull(typeMap)){
            return false ;
        }
        Object module = typeMap.get("module");
        if(Objects.isNull(module)){
            return false ;
        }

        Object propType = typeMap.get("propType");
        Object propertyTradeKind = typeMap.get("propertyTradeKind");
        if(Objects.isNull(typeMap) && Objects.isNull(propertyTradeKind) ){
            return false ;
        }

        Object obj = map.get("msgType");
        if(Objects.isNull(obj)){
            return false ;
        }

        int msgType = Integer.parseInt(String.valueOf(obj));
        if( msgType != 1 ){
            return  false ;
        }

        return  true ;
    }
}
