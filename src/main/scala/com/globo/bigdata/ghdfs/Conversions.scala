package com.globo.bigdata.ghdfs

case class RemoteIteratorWrapper[T](underlying: org.apache.hadoop.fs.RemoteIterator[T]) extends
  scala.collection.AbstractIterator[T] with scala.collection.Iterator[T] {
  def hasNext: Boolean = underlying.hasNext
  def next(): T = underlying.next()
}

object Conversions {
  implicit def remoteIterator2ScalaIterator[T](underlying: org.apache.hadoop.fs.RemoteIterator[T]):
  scala.collection.Iterator[T] = RemoteIteratorWrapper[T](underlying)
}