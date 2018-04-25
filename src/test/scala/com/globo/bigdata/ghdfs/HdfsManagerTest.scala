package com.globo.bigdata.ghdfs

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.hadoop.fs.{FileSystem, Path}

class HdfsManagerTest extends FlatSpec with Matchers with MockFactory with BeforeAndAfter {

  var factoryMock: HdfsFactory = _

  before {
    factoryMock = mock[HdfsFactory]
  }

  it should "Get status from hadoop path" in {

    (factoryMock.fromConfiguration _).expects(*).returning(FileSystem.getLocal(new Configuration()))

    val hdfsReader = new HdfsManager(factoryMock)
    val returnedClass = hdfsReader.status(new Path("src/test/resources/segment_ids.csv"))
    returnedClass.isDirectory shouldEqual false
    returnedClass.getLen shouldEqual 746
  }

  it should "Read from hadoop path" in {

    (factoryMock.fromConfiguration _).expects(*).returning(FileSystem.getLocal(new Configuration()))

    val hdfsReader = new HdfsManager(factoryMock)
    val returnedClass = hdfsReader.read(new Path("src/test/resources/segment_ids.csv")).getClass.getName
    returnedClass shouldEqual "org.apache.hadoop.fs.ChecksumFileSystem$FSDataBoundedInputStream"
  }

  it should "Write to hadoop path" in {

    (factoryMock.fromConfiguration _).expects(*).returning(FileSystem.getLocal(new Configuration()))
    val testPath = new Path("src/test/resources/new.csv")
    val hdfsReader = new HdfsManager(factoryMock)
    val returnedClass = hdfsReader.write(testPath).getClass.getName
    returnedClass shouldEqual "org.apache.hadoop.fs.FSDataOutputStream"

    new File(testPath.toString).delete
    new File("src/test/resources/.new.csv.crc").delete
  }

  it should "List files from hadoop path" in {

    (factoryMock.fromConfiguration _).expects(*).returning(FileSystem.getLocal(new Configuration()))

    val hdfsReader = new HdfsManager(factoryMock)
    val returnedClass = hdfsReader.listFiles(new Path("src/test/resources/")).getClass.getName
    returnedClass shouldEqual "com.globo.bigdata.ghdfs.RemoteIteratorWrapper"
  }
}
