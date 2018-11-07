package com.globo.bigdata.ghdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

class FileSystemFactory {

  def get(conf: Configuration): FileSystem = {
    FileSystem.get(conf)
  }

}
