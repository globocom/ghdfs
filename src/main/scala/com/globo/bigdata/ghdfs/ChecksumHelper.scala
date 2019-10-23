package com.globo.bigdata.ghdfs

import java.security.MessageDigest

import org.apache.hadoop.fs.FSDataInputStream
import javax.xml.bind.DatatypeConverter

class ChecksumHelper(algorithm: String) {

  def generate(inputStream: FSDataInputStream): String = {
    val byteArray = inputStream.readAllBytes()
    val hash = MessageDigest.getInstance(algorithm).digest(byteArray)
    val stringHash = DatatypeConverter.printHexBinary(hash)
    stringHash
  }

  def verify(checksum:String, inputStream: FSDataInputStream): Boolean = {
    val newChecksum = this.generate(inputStream)
    checksum.equals(newChecksum)
  }


}

