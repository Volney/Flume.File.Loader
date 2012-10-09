package com.cloudera.sa.flume.file.loader;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileSource extends AbstractSource implements PollableSource,
		Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(LocalFileSource.class);
  
  public static final String INPUT_DIR = "input.dir";
  public static final String SUCCESS_DIR = "success.dir";
  public static final String FAIL_DIR = "fail.dir";
  public static final String PROCESS_DIR = "process.dir";
  public static final String THREAD_POOL_COUNT = "reader.count";
  
  File inputDir;
  File successDir;
  File failDir;
  File processDir;
  ThreadPoolExecutor threadPool;
  
  @Override
  public void configure(Context context) {
    //Set up directories
    try {
      inputDir = prepDirectory(context.getString(INPUT_DIR));
      processDir = prepDirectory(context.getString(PROCESS_DIR));
      successDir = prepDirectory(context.getString(SUCCESS_DIR));
      failDir = prepDirectory(context.getString(FAIL_DIR));
    } catch (IOException e) {
      throw new RuntimeException("Unable to configure LocalFileSource", e);
    }
    //Set up thread pool
    int threadCount = context.getInteger(THREAD_POOL_COUNT);
    threadPool = new ThreadPoolExecutor(threadCount, threadCount, 1, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(threadCount, true));;  
  }

  Object foo = new Object();
  
  @Override
	public Status process() throws EventDeliveryException {
		try {
		  
	    logger.warn("get number of files");
      
		  File[] files = inputDir.listFiles();
		  
		  logger.warn("Number of files:" + files.length);
		  
		  if (files.length == 0) {
		    return Status.BACKOFF;
		  }
		  
		  for (File file: files) { 
		    //The file is moved to the processing directory and is ready to load
		    FileReaderThread fileReader = new FileReaderThread(getChannelProcessor(), file, processDir, successDir, failDir);
        threadPool.execute(fileReader);
		  }
		} catch (Exception ex) {
			return Status.BACKOFF;
		}
		return Status.BACKOFF;
	}

	@Override
	public void start() {
		System.out.println("LocalFileSource starting v1.1");

		super.start();

		System.out.println("LocalFileSource started");
	}

	@Override
	public void stop() {
		System.out.println("LocalFileSource stopping");

		super.stop();

		System.out.println("LocalFileSource stopped. Metrics:{}");
	}
	
	private static File prepDirectory(String dirPath) throws IOException {
    File dir = new File(dirPath);
    if (dir.exists() == false) {
      if (dir.mkdirs() == false) {
        throw new IOException(dirPath + " is not a valid directory.");
      }
    } else if (dir.isDirectory() == false) {
      throw new IOException(dirPath + " is not a valid directory.");
    }
    return dir;
  }
}