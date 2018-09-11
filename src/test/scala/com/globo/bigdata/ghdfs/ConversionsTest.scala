package com.globo.bigdata.ghdfs

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

class ConversionsTest extends FlatSpec with Matchers with MockFactory with BeforeAndAfter {

  it should "Invoke hasNext of RemoteIterator on hasNext of RemoteIteratorWrapper" in {

    val mockRemote = mock[org.apache.hadoop.fs.RemoteIterator[String]]

    (mockRemote.hasNext _).expects().once()

    val wrapper = RemoteIteratorWrapper[String](mockRemote)

    wrapper.hasNext

  }

}
