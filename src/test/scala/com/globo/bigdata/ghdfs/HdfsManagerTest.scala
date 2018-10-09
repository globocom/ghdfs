package com.globo.bigdata.ghdfs

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import java.io.IOException

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


  it should "Raise IOException if source path does not exist in copyMerge" in {
    val hdfsReader = HdfsManager()
    val toMergePath = new Path("doesnt_exists")
    val destinationPath = new Path("src/test/resources/merged_file")

    assertThrows[IOException]{
      hdfsReader.copyMerge(toMergePath, destinationPath, false, "\n")
    }

  }

}

class HdfsManagerDeleteTest extends FlatSpec with Matchers with MockFactory with BeforeAndAfter {

  behavior of "delete"

  val workDir = "/tmp/ghdfs-" + System.currentTimeMillis()

  before {
    new File(workDir).mkdir()
  }

  after {
    FileUtils.deleteDirectory(new File(workDir))
  }

  it should "recursively" in {
    val dir = workDir + "/1"

    new File(dir).mkdir()
    new File(dir + "/file").createNewFile()

    val hdfsReader = HdfsManager()
    hdfsReader.delete(new Path(dir), true)

    new File(dir).exists() shouldBe false
  }

  it should "Directory not recursively " in {
    val dir = workDir + "/2"

    new File(dir).mkdir()
    new File(dir + "/file").createNewFile()

    val hdfsReader = HdfsManager()

    assertThrows[IOException] {
      hdfsReader.delete(new Path(dir), false)
    }

    new File(dir).exists() shouldBe true
  }

  it should "File not recursively" in {
    val dir = workDir + "/3"

    new File(dir).createNewFile()

    val hdfsReader = HdfsManager()
    hdfsReader.delete(new Path(dir), false)

    new File(dir).exists() shouldBe false
  }

  it should "Unexisted file without error" in {
    val dir = workDir + "/4"

    val hdfsReader = HdfsManager()
    hdfsReader.delete(new Path(dir), false)

    new File(dir).exists() shouldBe false
  }
}
