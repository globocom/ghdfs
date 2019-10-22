package com.globo.bigdata.ghdfs

import java.io.IOException
import java.io.InputStream

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

class HdfsManager(hdfs: FileSystem) {

  private def validatePath(hadoopPath: Path): Path = {
    if (!hdfs.exists(hadoopPath)) {
      throw new IOException(s"Path ${hadoopPath.toString} not found in hadoop")
    }
    hadoopPath
  }

  def getFS: FileSystem = hdfs

  def status(hadoopPath: Path): FileStatus = {
    hdfs.getFileStatus(validatePath(hadoopPath))
  }

  def open(hadoopPath: Path): FSDataInputStream = {
    hdfs.open(validatePath(hadoopPath))
  }

  def read(hadoopPath: Path): FSDataInputStream = open(hadoopPath)

  def create(hadoopPath: Path): FSDataOutputStream = {
    hdfs.create(hadoopPath, true)
  }


  def write(hadoopPath: Path): FSDataOutputStream = create(hadoopPath: Path)

  def write(hadoopPath: Path, input: InputStream): Boolean = {
    val fsOutputStream = write(hadoopPath)
    
    try {
      IOUtils.copy(input, fsOutputStream)
      true
    }
    finally {
        input.close()
        fsOutputStream.close()
    }
  }

  def list[T](hadoopPath: Path, caller: Path => RemoteIterator[T]): Iterator[T] = {
    RemoteIteratorWrapper[T](
      caller(validatePath(hadoopPath))
    )
  }

  def listFiles(hadoopPath: Path, recursive: Boolean = false): Iterator[LocatedFileStatus] = {
    list[LocatedFileStatus](hadoopPath: Path, hdfs.listFiles(_, recursive))
  }

  def listCorruptFileBlocks(hadoopPath: Path): Iterator[Path] = {
    list[Path](hadoopPath: Path, hdfs.listCorruptFileBlocks(_))
  }

  def listLocatedStatus(hadoopPath: Path): Iterator[LocatedFileStatus] = {
    list[LocatedFileStatus](hadoopPath: Path, hdfs.listLocatedStatus(_))
  }

  def listStatusIterator(hadoopPath: Path): Iterator[FileStatus] = {
    list[FileStatus](hadoopPath: Path, hdfs.listStatusIterator(_))
  }

  def delete(f: Path, recursive: Boolean): Boolean = {
    if (hdfs.exists(f)) {
      hdfs.delete(f, recursive)
    }
    false
  }

  def move(sourcePath: Path, destinationPath: Path): Unit = {
    hdfs.rename(sourcePath, destinationPath)
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
    new HdfsManager(fs)

  }
}