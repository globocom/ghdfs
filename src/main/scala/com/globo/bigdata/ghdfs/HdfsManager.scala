package com.globo.bigdata.ghdfs

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, LocatedFileStatus, Path}
import org.apache.commons.io.IOUtils

class HdfsManager(hdfsFactory: HdfsFactory,
                  configuration: Configuration = new Configuration){

  val hdfs: FileSystem = hdfsFactory.fromConfiguration(configuration)


  private def validatePath(hadoopPath: Path): Path = {

    if (!hdfs.exists(hadoopPath)) {
      throw new IOException(s"Path ${hadoopPath.toString} not found in hadoop")
    }
    hadoopPath
  }

  def status(hadoopPath: Path): FileStatus = {
    hdfs.getFileStatus(validatePath(hadoopPath))
  }

  def read(hadoopPath: Path): FSDataInputStream = {
    hdfs.open(validatePath(hadoopPath))
  }

  def write(hadoopPath: Path): FSDataOutputStream = {
    hdfs.create(hadoopPath, true)
  }

  def listFiles(hadoopPath: Path, recursive: Boolean = false): Iterator[LocatedFileStatus] =  {
    RemoteIteratorWrapper[LocatedFileStatus](
      hdfs.listFiles(validatePath(hadoopPath), recursive)
    )
  }

  def move(sourcePath: Path, destinationPath: Path): Unit = {
    val destinationFile = this.write(destinationPath)
    val sourceFile = this.read(sourcePath)

    IOUtils.copy(sourceFile, destinationFile)
    destinationFile.close()
    sourceFile.close()
    hdfs.delete(sourcePath, false)
  }
}
