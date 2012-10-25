package com.cloudera.sa.flume.file.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileReaderThread implements Runnable {

  File inputFile;
  File successDir;
  File failDir;
  File processDir;

  long byteCounter = 0;
  long recordCounter = 0;
  
  FileReaderListener listener;
  
  
  ChannelProcessor channelProcessor;
  
  private static final Logger logger = LoggerFactory
      .getLogger(FileReaderThread.class);
  
  public FileReaderThread(ChannelProcessor channelProcessor, File inputFile, File processDir, File successDir, File failDir, FileReaderListener listener) {
    this.inputFile = inputFile;
    this.successDir = successDir;
    this.failDir = failDir;
    this.processDir = processDir;
    this.channelProcessor = channelProcessor;
    this.listener = listener;
  }
  
  @Override
  public void run() {
    BufferedReader reader = null;
    byteCounter = 0;
    recordCounter = 0;
    
    logger.info("Start processing '" + inputFile.getName() + "'");
    
    try {

      moveFileToProcessDir();
      
      reader = getReaderForFileType(inputFile);
      readInputFile(reader);
      
      moveFileToSuccessDir();
      
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (Exception e2) {
        logger.error("Unable to close reader for file '" + inputFile.getName() + "'", e2);
      }
    } catch (Exception e) {
      logger.error("Problem reading '" + inputFile.getName() + "'", e);
      
      //close the reader
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (Exception e2) {
        logger.error("Unable to close reader for file '" + inputFile.getName() + "'", e2);
      }
      //move file to fail directory
      moveFileToFailDir(e);
    }
    
    logger.info("Closing Thread '" + inputFile.getName() + "'");
    
  }

  private void moveFileToFailDir(Exception e) {
    File failLocation = new File(failDir, inputFile.getName());
    if (inputFile.renameTo(failLocation) == false){
      logger.error("Unable to move file '" + inputFile.getName() + "' to fail directory", e);
    } else {
      logger.error("Moved file '" + inputFile.getName() + "' to fail directory");
    }
    inputFile = failLocation;
  }

  private void moveFileToSuccessDir() {
    File successLocation = new File(successDir, inputFile.getName());
    if (inputFile.renameTo(successLocation) == false){
      logger.error("Unable to move file '" + inputFile.getName() + "' to success directory");
    } else {
      logger.warn("Moved '" + inputFile.getName() + "' to success directory.");
    }
    inputFile = successLocation;
  }

  private void moveFileToProcessDir() {
    File processLocation = new File(processDir, inputFile.getName());
    if (inputFile.renameTo(processLocation) == false) {
      logger.warn("Unable to move " + inputFile.getName() + " from the inputDir " +
                  "to the processDir.");
      if (inputFile.renameTo(new File(failDir, inputFile.getName())) == false) {
        logger.error("Unable to move " + inputFile.getName() + " from the inputDir " +
            "to the failDir.");
      }
    } else {
      
      logger.warn("Moved " + inputFile.getName() + " to processed dir");
    }
    listener.movedFileToProcessing(inputFile.getName());
    inputFile = processLocation;
  }

  private void readInputFile(BufferedReader reader) throws IOException,
      InterruptedException {
    String line = reader.readLine();
    
    while (line != null) {
      
      Event event = EventBuilder.withBody(Bytes.toBytes(line));
      event.getHeaders().put(FileHeaderConst.FILENAME, inputFile.getName());
      event.getHeaders().put(FileHeaderConst.BYTE_OFF_SITE, "" + byteCounter);
      event.getHeaders().put(FileHeaderConst.RECORD_NUM, "" + recordCounter);
      
      pushEvent(event);
      
      byteCounter += line.length();
      recordCounter++;
      
      line = reader.readLine();
    }
    logger.info("Finished processing '" + inputFile.getName() + "'");
  }

  private void pushEvent(Event event) throws InterruptedException {
    while (true)
    {
      try {
        channelProcessor.processEvent(event);
        logger.debug("Processed event: {byteCounter:" + byteCounter + ",recordCounter:" + recordCounter + "}");
        break;
      } catch (ChannelException ce) {
        logger.warn("Waiting {byteCounter:" + byteCounter + ",recordCounter:" + recordCounter + "}");
        Thread.sleep(500);
      }
    }
  }
  
  /**
   * If the file is a gzip file then use the GZipInputStream.  Otherwise use the normal inputStream.
   * @param inputFile
   * @return
   * @throws IOException
   */
  protected BufferedReader getReaderForFileType(File inputFile) throws IOException
  {
    String fileName = inputFile.getName().toLowerCase();
    if (fileName.endsWith(".gz"))
    {
      GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(inputFile));
     
      logger.info("Opening " + inputFile.getName() + " with GZIPInputStream.");
      
      return new BufferedReader(new InputStreamReader(gzip));
      
    }else
    {
      logger.info("Opening " + inputFile.getName() + " with FileReader.");
      return new BufferedReader( new FileReader(inputFile));
    }
  }
  
}