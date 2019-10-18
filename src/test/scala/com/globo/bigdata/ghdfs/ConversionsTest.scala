package com.globo.bigdata.ghdfs

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

/**
 * Integration tests for [[Conversions]]
 */
class ConversionsTest extends FlatSpec with Matchers with MockFactory with BeforeAndAfter {

  it should "Invoke hasNext of RemoteIterator on hasNext of RemoteIteratorWrapper" in {

    val mockRemote = mock[org.apache.hadoop.fs.RemoteIterator[String]]

    (mockRemote.hasNext _).expects().once()

    val wrapper = RemoteIteratorWrapper[String](mockRemote)

    wrapper.hasNext

  }

  it should "Returns true when has next" in {
    val mockRemote = stub[org.apache.hadoop.fs.RemoteIterator[String]]

    val wrapper = RemoteIteratorWrapper[String](mockRemote)

    (mockRemote.hasNext _).when().returns(true)

    assert(wrapper.hasNext)

  }

  it should "Returns false when doesn't have next" in {
    val mockRemote = stub[org.apache.hadoop.fs.RemoteIterator[String]]

    val wrapper = RemoteIteratorWrapper[String](mockRemote)

    (mockRemote.hasNext _).when().returns(false)

    assert(wrapper.hasNext == false)

  }

  it should "Invoke RemoteIterator next method on RemoteIteratorWrapper next" in {

    val mockRemote = mock[org.apache.hadoop.fs.RemoteIterator[String]]

    (mockRemote.next _).expects().once()

    val wrapper = RemoteIteratorWrapper[String](mockRemote)

    wrapper.next

  }

  it should "Returns RemoteIterator next value on RemoteIteratorWrapper next" in {

    val mockRemote = stub[org.apache.hadoop.fs.RemoteIterator[String]]

    (mockRemote.next _).when().returns("munaro id")

    val wrapper = RemoteIteratorWrapper[String](mockRemote)

    assert(wrapper.next() == "munaro id")

  }

}
