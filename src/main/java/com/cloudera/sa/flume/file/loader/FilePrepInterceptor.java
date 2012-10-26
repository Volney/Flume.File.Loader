package com.cloudera.sa.flume.file.loader;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.Interceptor.Builder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilePrepInterceptor implements Interceptor, Builder {

  final static Pattern p = Pattern.compile("\\|");
  
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
    
    String eventBody = Bytes.toString(event.getBody());
    
    String[] bodySplit = p.split(eventBody);
    
    Event newEvent = EventBuilder.withBody(Bytes.toBytes(filename + "||" + eventBody));
    
    if (bodySplit.length > 1) {
      newEvent.getHeaders().put(FileHeaderConst.YEAR_MONTH, bodySplit[1]);
      logger.info(FileHeaderConst.YEAR_MONTH + " is set to " + bodySplit[1]);
    } else {
      logger.info(FileHeaderConst.YEAR_MONTH + " is set to black");
    }
    
    
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
