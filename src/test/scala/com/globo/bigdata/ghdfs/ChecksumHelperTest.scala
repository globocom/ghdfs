package com.globo.bigdata.ghdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.FlatSpec

class ChecksumHelperTest extends FlatSpec {
  val algorithm = "MD5"

  it should "Returns a string that represents a checksum given a file" in {
    val fs = FileSystem.get(new Configuration())
    val hdfsReader = new HdfsManager(fs)
    val inputStream = hdfsReader.read(new Path("src/test/resources/segment_ids.csv"))
    val helper = new ChecksumHelper(algorithm)
    val result = helper.generate(inputStream)
    assert(result != null)
    assert(!result.isBlank)
    result
  }

  it should "It should assert true given different files and generate differente hash" in {
    val fs = FileSystem.get(new Configuration())
    val hdfsReader = new HdfsManager(fs)
    val inputStream1 = hdfsReader.read(new Path("src/test/resources/segment_ids.csv"))
    val helper1 = new ChecksumHelper(algorithm)
    val result1 = helper1.generate(inputStream1)

    val inputStream2 = hdfsReader.read(new Path("src/test/resources/segment_ids2.csv"))
    val helper2 = new ChecksumHelper(algorithm)
    val result2 = helper2.generate(inputStream2)
    assert(!result1.equals(result2))
  }

}
