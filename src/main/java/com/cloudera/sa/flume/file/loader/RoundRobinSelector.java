package com.cloudera.sa.flume.file.loader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.AbstractChannelSelector;

public class RoundRobinSelector extends AbstractChannelSelector {

  int numOfChannels = 0;
  int currentChannel = 0;
  private final List<Channel> emptyList = Collections.emptyList();
  
  
  public void setChannels(List<Channel> channels) {
    super.setChannels(channels);
    numOfChannels = channels.size();
    if (numOfChannels == 0) {
      throw new RuntimeException("No Channels for RoundRobinSelector");
    }
  }
  
  @Override
  public List<Channel> getRequiredChannels(Event event) {

    List<Channel> returnList = new ArrayList<Channel>();
    returnList.add(this.getAllChannels().get(currentChannel % numOfChannels));
    
    currentChannel++;
    
    return returnList;
  }

  @Override
  public List<Channel> getOptionalChannels(Event event) {
    return emptyList;
  }

  @Override
  public void configure(Context context) {
    // TODO Auto-generated method stub
  }

}
