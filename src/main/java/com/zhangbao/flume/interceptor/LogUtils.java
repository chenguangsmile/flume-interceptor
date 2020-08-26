package com.zhangbao.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;

public class LogUtils {


    public static boolean validateStart(String log) {
        //json
        if(log == null){
            return false;
        }
       if(!log.startsWith("{") || !log.endsWith("}")){
           return false;
       }
        return true;
    }

    public static boolean validateEvent(String log) {
        //时间|json
        //1 切割
        String[] split = log.split("\\|");
        if(split.length != 2){
            return false;
        }
        if(split[0].length() != 13 || !NumberUtils.isDigits(split[0])){
            return false;
        }

        if(!split[1].trim().startsWith("{") || !split[1].trim().endsWith("}")){
            return false;
        }

        return true;
    }
}
