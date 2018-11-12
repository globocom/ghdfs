package com.globo.bigdata.ghdfs

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem


class DistributedHdfsManager(fs: FileSystem) extends DistributedFileSystem with HdfsManager{
  override protected val hdfs: FileSystem = fs
}