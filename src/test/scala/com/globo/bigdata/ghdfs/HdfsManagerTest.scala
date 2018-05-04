package com.globo.bigdata.ghdfs

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

class HdfsManagerTest extends FlatSpec with Matchers with MockFactory with BeforeAndAfter {

  it should "Get status from hadoop path" in {

    val hdfsReader = HdfsManager()
    val returnedClass = hdfsReader.status(new Path("src/test/resources/segment_ids.csv"))
    returnedClass.isDirectory shouldEqual false
    returnedClass.getLen shouldEqual 746
  }

  it should "Read from hadoop path" in {

    val hdfsReader = HdfsManager()
    val returnedClass = hdfsReader.read(new Path("src/test/resources/segment_ids.csv")).getClass.getName
    returnedClass shouldEqual "org.apache.hadoop.fs.ChecksumFileSystem$FSDataBoundedInputStream"
  }

  it should "Write to hadoop path" in {

    val testPath = new Path("src/test/resources/new.csv")
    val hdfsReader = HdfsManager()
    val returnedClass = hdfsReader.write(testPath).getClass.getName
    returnedClass shouldEqual "org.apache.hadoop.fs.FSDataOutputStream"

    new File(testPath.toString).delete
    new File("src/test/resources/.new.csv.crc").delete
  }

  it should "List files from hadoop path" in {

    val hdfsReader = HdfsManager()
    val returnedClass = hdfsReader.listFiles(new Path("src/test/resources/")).getClass.getName
    returnedClass shouldEqual "com.globo.bigdata.ghdfs.RemoteIteratorWrapper"
  }

  it should "Merge files from hadoop path to another hadoop path" in {
    val hdfsReader = HdfsManager()
    val toMergePath = new Path("src/test/resources/mergetest/")
    val destinationPath = new Path("src/test/resources/merged_file")

    val isMerged = hdfsReader.copyMerge(toMergePath, destinationPath, false, "\n")

    isMerged shouldEqual true

    FileSystem.get(new Configuration()).delete(destinationPath, false)
  }
}
