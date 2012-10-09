package com.cloudera.sa.flume.file.loader;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.Interceptor.Builder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilePrepInterceptor implements Interceptor, Builder {

  private static final Logger logger = LoggerFactory
      .getLogger(FilePrepInterceptor.class);
  
  @Override
  public Interceptor build() {
    return new FilePrepInterceptor();
  }
  
  @Override
  public void initialize() {
    logger.info("initialize: FilePrepInterceptor");
  }

  @Override
  public Event intercept(Event event) {
    String filename = event.getHeaders().get(FileHeaderConst.FILENAME);
    String recordNum = event.getHeaders().get(FileHeaderConst.RECORD_NUM);
    
    Event newEvent = EventBuilder.withBody(Bytes.toBytes(filename + "||" + Bytes.toString(event.getBody())));
    
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
    logger.info("close: FilePrepInterceptor");
  }

  @Override
  public void configure(Context context) {
    // TODO Auto-generated method stub
    
  }


}
