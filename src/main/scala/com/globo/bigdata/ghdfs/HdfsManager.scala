package com.globo.bigdata.ghdfs

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, LocatedFileStatus, Path, FileUtil}
import org.apache.commons.io.IOUtils

class HdfsManager(hdfs: FileSystem) {

  private def validatePath(hadoopPath: Path): Path = {

    if (!hdfs.exists(hadoopPath)) {
      throw new GHdfsIOException(s"Path ${hadoopPath.toString} not found in hadoop")
    }
    hadoopPath
  }

  /**
    * @param hadoopPath The path we want information from
    * @return a FileStatus object that represents the path
    * @throws GHdfsIOException when the path does not exist
    * @see [[com.globo.bigdata.ghdfs.GHdfsIOException]]
    */
  def status(hadoopPath: Path): FileStatus = {
    try {
      hdfs.getFileStatus(validatePath(hadoopPath))
    } catch {
      case e: java.io.IOException => throw new GHdfsIOException(e)
      case e: _ => throw e
    }
  }

  def read(hadoopPath: Path): FSDataInputStream = {
    hdfs.open(validatePath(hadoopPath))
  }

  def write(hadoopPath: Path): FSDataOutputStream = {
    hdfs.create(hadoopPath, true)
  }

  def listFiles(hadoopPath: Path, recursive: Boolean = false): Iterator[LocatedFileStatus] = {
    RemoteIteratorWrapper[LocatedFileStatus](
      hdfs.listFiles(validatePath(hadoopPath), recursive))
  }

  def delete(hadoopPath: Path, recursive: Boolean = false) = {
    if (hdfs.exists(hadoopPath)) {
      hdfs.delete(hadoopPath, recursive)
    }
  }

  def move(sourcePath: Path, destinationPath: Path): Unit = {
    val destinationFile = this.write(destinationPath)
    val sourceFile = this.read(sourcePath)

    IOUtils.copy(sourceFile, destinationFile)
    destinationFile.close()
    sourceFile.close()
    hdfs.delete(sourcePath, false)
  }

  def copyMerge(sourcePath: Path, destinationPath: Path, deleteSource: Boolean, separatorString: String): Boolean = {
    if (!hdfs.exists(sourcePath)) {
      throw new IOException(s"Path ${sourcePath.toString} not found in hadoop")
    }

    FileUtil.copyMerge(hdfs, sourcePath, hdfs, destinationPath, deleteSource, hdfs.getConf, separatorString)
  }
}

object HdfsManager {

  def apply(): HdfsManager = apply(None)

  def apply(hadoopConfDir: String): HdfsManager = apply(Option(hadoopConfDir))

  def apply(hadoopConfDir: Option[String]): HdfsManager = apply(hadoopConfDir, new Configuration())

  def apply(hadoopConfDir: Option[String], conf: Configuration): HdfsManager = {

    if (hadoopConfDir.nonEmpty) {
      val confHdfs: Path = new Path(hadoopConfDir.get, "hdfs-site.xml")
      val confCore: Path = new Path(hadoopConfDir.get, "core-site.xml")
      conf.addResource(confHdfs)
      conf.addResource(confCore)
    }

    val fs = FileSystem.get(conf)
    new HdfsManager(fs)
  }
}