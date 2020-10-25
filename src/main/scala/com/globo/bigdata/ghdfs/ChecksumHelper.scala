package com.globo.bigdata.ghdfs

import java.security.MessageDigest

import javax.xml.bind.DatatypeConverter
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

class ChecksumHelper(algorithm: String) {

  def generate(inputStream: FSDataInputStream): String = {
    val byteArray = inputStream.readAllBytes()
    val hash = MessageDigest.getInstance(algorithm).digest(byteArray)
    val stringHash = DatatypeConverter.printHexBinary(hash)
    stringHash
  }

  /**
   * Get the checksum of a file
   *
   * @param hdfs hadoopPath's value
   * @param path file's path
   * @return the string checksum of a file
   */
    def getFileChecksum(hdfs: FileSystem, path: Path): String = {
      hdfs.getFileChecksum(path).toString
  }


}

