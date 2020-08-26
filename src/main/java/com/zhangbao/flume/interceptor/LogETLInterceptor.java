package com.zhangbao.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //获取数据
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        //启动日志
        if(log.contains("start")){
            if(LogUtils.validateStart(log)){
                return event;
            }
        }else {
            if(LogUtils.validateEvent(log)){
                return event;
            }
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> intercepters = new ArrayList<>();
        for (Event event : list) {
            Event intercept = intercept(event);
            if(intercept!=null){
                intercepters.add(intercept);
            }
        }
        return intercepters;
    }

    @Override
    public void close() {

    }

    //静态内部类
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
