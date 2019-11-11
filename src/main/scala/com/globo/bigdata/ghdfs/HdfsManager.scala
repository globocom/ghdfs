package com.globo.bigdata.ghdfs

import java.io.IOException
import java.io.InputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

/**
 * Hdfs manager
 *
 * @param hdfs receive data in FileSystem format
 */
class HdfsManager(hdfs: FileSystem) {

  /**
   * Validate if hadoopPath exists
   *
   * @param hadoopPath hadoopPath's value
   * @return hadoop path if exists, otherwise throws an exception
   */
  private def validatePath(hadoopPath: Path): Path = {
    if (!hdfs.exists(hadoopPath)) {
      throw new IOException(s"Path ${hadoopPath.toString} not found in hadoop")
    }
    hadoopPath
  }

  /**
   * Get FileSystem
   *
   * @return fileSystem
   */
  def getFS: FileSystem = hdfs

  /**
   * Get Status from FileStatus object
   *
   * @param hadoopPath hadoopPath's value
   * @return status from FileStatus object
   */
  def status(hadoopPath: Path): FileStatus = {
    hdfs.getFileStatus(validatePath(hadoopPath))
  }

  /**
   * Open a file from validated path
   *
   * @param hadoopPath hadoopPath's value to open
   * @return fileSystem data
   */
  def open(hadoopPath: Path): FSDataInputStream = {
    hdfs.open(validatePath(hadoopPath))
  }

  /**
   * Open a file from path
   *
   * @param hadoopPath hadoopPath's value to read
   * @return fileSystem data
   */
  def read(hadoopPath: Path): FSDataInputStream = open(hadoopPath)

  /**
   * Create a file at the indicated path, if already exists with this name, the file will be overwritten
   *
   * @param hadoopPath hadoopPath's value
   * @return a file at the indicated path
   */
  def create(hadoopPath: Path): FSDataOutputStream = {
    hdfs.create(hadoopPath, true)
  }

  /**
   * Create a file at the indicated path
   *
   * @param hadoopPath hadoopPath's value
   * @return a file at the indicated path
   */
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

  /**
   * Function to List objects type that is passed on T
   *
   * @param hadoopPath hadoopPath's directory
   * @param caller     call RemoteIterator
   * @tparam T command return type
   * @return a list of object type T
   */
  def list[T](hadoopPath: Path, caller: Path => RemoteIterator[T]): Iterator[T] = {
    RemoteIteratorWrapper[T](
      caller(validatePath(hadoopPath))
    )
  }

  /**
   * List LocatedFileStatus and block locations of the files in the path
   *
   * @param hadoopPath hadoopPath's directory
   * @param recursive  true if subdirectories need to be traversed recursively
   * @return files statuses
   */
  def listFiles(hadoopPath: Path, recursive: Boolean = false): Iterator[LocatedFileStatus] = {
    list[LocatedFileStatus](hadoopPath: Path, hdfs.listFiles(_, recursive))
  }

  /**
   * List corrupted file blocks
   *
   * @param hadoopPath hadoopPath's directory
   * @return corrupted files
   */
  def listCorruptFileBlocks(hadoopPath: Path): Iterator[Path] = {
    list[Path](hadoopPath: Path, hdfs.listCorruptFileBlocks(_))
  }

  /**
   * List the statuses of the files/directories
   *
   * @param hadoopPath hadoopPath's directory
   * @return an iterator that traverses statuses of the files/directories in the given path
   */
  def listLocatedStatus(hadoopPath: Path): Iterator[LocatedFileStatus] = {
    list[LocatedFileStatus](hadoopPath: Path, hdfs.listLocatedStatus(_))
  }

  /**
   * Returns a remote iterator allowing followup calls on demand
   *
   * @param hadoopPath hadoopPath's directory
   * @return remote iterator
   */
  def listStatusIterator(hadoopPath: Path): Iterator[FileStatus] = {
    list[FileStatus](hadoopPath: Path, hdfs.listStatusIterator(_))
  }

  /**
   * Delete a file
   *
   * @param f         path to delete
   * @param recursive if directory, true will delete all directory, if file recursive can be se to either true or false
   * @return true if delete is succesfull or false
   */
  def delete(f: Path, recursive: Boolean): Boolean = {
    if (hdfs.exists(f)) {
      hdfs.delete(f, recursive)
    }
    false
  }

  /**
   * Rename path source to path destination
   *
   * @param sourcePath      path to be renamed
   * @param destinationPath new path after rename
   */
  def move(sourcePath: Path, destinationPath: Path): Unit = {
    hdfs.rename(sourcePath, destinationPath)
  }

  /**
   * Copy all files in a directory to one output file
   *
   * @param sourcePath      file's source path
   * @param destinationPath destination after copy
   * @param deleteSource    true if delete source, false if don't
   * @param separatorString string separator
   * @return true if copy and merge is success, false if don't
   */
  def copyMerge(sourcePath: Path, destinationPath: Path, deleteSource: Boolean, separatorString: String): Boolean = {
    validatePath(sourcePath)

    FileUtil.copyMerge(hdfs, sourcePath, hdfs, destinationPath, deleteSource, hdfs.getConf, separatorString)
  }
}

/**
 * Hdfs manager
 */
object HdfsManager {

  def apply(): HdfsManager = apply(None)

  /**
   * Apply Hadoop Configuration Directory
   *
   * @param hadoopConfDir directory of hadoop configuration
   * @return
   */
  def apply(hadoopConfDir: String): HdfsManager = apply(Option(hadoopConfDir))

  /**
   * Apply new Configuration
   *
   * @param hadoopConfDir directory of hadoop configuration
   * @return
   */
  def apply(hadoopConfDir: Option[String]): HdfsManager = apply(hadoopConfDir, new Configuration())

  /**
   * Aplly hadoop configurations
   *
   * @param hadoopConfDir directory of hadoop configuration
   * @param conf          configuration
   * @param fsFactory     factory file system
   * @return
   */
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