package com.cloudera.sa.flume.file.loader;

public interface FileReaderListener {
  public void movedFileToProcessing(String fileName);
}
