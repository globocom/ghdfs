package com.globo.bigdata.ghdfs

import java.io.IOException

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.hdfs.DistributedFileSystem

trait HdfsManager extends FileSystem {

  protected val hdfs: FileSystem

  private def validatePath(hadoopPath: Path): Path = {

    if (!hdfs.exists(hadoopPath)) {
      throw new IOException(s"Path ${hadoopPath.toString} not found in hadoop")
    }
    hadoopPath
  }


  def status(hadoopPath: Path): FileStatus = {
    hdfs.getFileStatus(validatePath(hadoopPath))
  }

  override def open(hadoopPath: Path): FSDataInputStream = {
    hdfs.open(validatePath(hadoopPath))
  }

  def read(hadoopPath: Path): FSDataInputStream = open(hadoopPath)

  override def create(hadoopPath: Path): FSDataOutputStream = {
    hdfs.create(hadoopPath, true)
  }


  def write(hadoopPath: Path): FSDataOutputStream = create(hadoopPath: Path)

  def list(hadoopPath: Path, recursive: Boolean = false): Iterator[LocatedFileStatus] = {
    RemoteIteratorWrapper[LocatedFileStatus](
      hdfs.listFiles(validatePath(hadoopPath), recursive))
  }

  override def delete(f: Path, recursive: Boolean): Boolean = {
    if (hdfs.exists(f)) {
      hdfs.delete(f, recursive)
    }
    false
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

  def apply(hadoopConfDir: Option[String], conf: Configuration, fsFactory: FileSystemFactory = new FileSystemFactory()): HdfsManager = {

    if (hadoopConfDir.nonEmpty) {
      val confHdfs: Path = new Path(hadoopConfDir.get, "hdfs-site.xml")
      val confCore: Path = new Path(hadoopConfDir.get, "core-site.xml")
      conf.addResource(confHdfs)
      conf.addResource(confCore)
    }

    val fs: FileSystem = fsFactory.get(conf)
    if (fs.isInstanceOf[DistributedFileSystem]){
      new DistributedHdfsManager(fs)
    } else {
      new LocalHdfsManager(fs)
    }
  }
}