package com.globo.bigdata.ghdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
 * File System Factory class
 */
class FileSystemFactory {

  /**
   * Returns the configured FileSystem implementation.
   *
   * @param conf configuration to use
   * @return uri of filesystem
   */
  def get(conf: Configuration): FileSystem = {
    FileSystem.get(conf)
  }

}
