package com.globo.bigdata.ghdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
 *
 */
class FileSystemFactory {

  /**
   *
   * @param conf
   * @return
   */
  def get(conf: Configuration): FileSystem = {
    FileSystem.get(conf)
  }

}
