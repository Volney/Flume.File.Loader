package com.cloudera.sa.flume.file.loader;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.hadoop.hbase.util.Bytes;

public class FilePrepInterceptor implements Interceptor {

  @Override
  public void initialize() {
    
  }

  @Override
  public Event intercept(Event event) {
    String filename = event.getHeaders().get(FileHeaderConst.FILENAME);
    String recordNum = event.getHeaders().get(FileHeaderConst.RECORD_NUM);
    
    Event newEvent = EventBuilder.withBody(Bytes.toBytes(filename + "\t" + recordNum + "\t" + Bytes.toString(event.getBody())));
    
    return newEvent;
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    List<Event> result = new ArrayList<Event>();
    
    for (Event event: events) {
      result.add(intercept(event));
    }
    
    return result;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }


}
