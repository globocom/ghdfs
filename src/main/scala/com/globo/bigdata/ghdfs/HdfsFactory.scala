package com.globo.bigdata.ghdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


class HdfsFactory(hadoopConfDir: String) {

  private def makeConf(conf: Configuration): Configuration = {
    if (!hadoopConfDir.isEmpty) {
      val confHdfs: Path = new Path(hadoopConfDir, "hdfs-site.xml")
      val confCore: Path = new Path(hadoopConfDir, "core-site.xml")
      conf.addResource(confHdfs)
      conf.addResource(confCore)
    }
    conf
  }

  def fromConfiguration(conf: Configuration = new Configuration): FileSystem = {
    FileSystem.get(makeConf(conf))

  }

}
