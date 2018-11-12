package com.globo.bigdata.ghdfs

import org.apache.hadoop.fs.{FileSystem, LocalFileSystem}

class LocalHdfsManager(fs: FileSystem) extends LocalFileSystem with HdfsManager {
  override protected val hdfs: FileSystem = fs
}
