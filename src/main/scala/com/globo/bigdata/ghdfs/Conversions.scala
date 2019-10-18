package com.globo.bigdata.ghdfs

/**
 * Convert a collection to a list
 * @param underlying hadoop path
 * @tparam T command return type
 */
case class RemoteIteratorWrapper[T](underlying: org.apache.hadoop.fs.RemoteIterator[T]) extends
  scala.collection.AbstractIterator[T] with scala.collection.Iterator[T] {
  /**
   *
   * @return
   */
  def hasNext: Boolean = underlying.hasNext

  /**
   *
   * @return
   */
  def next(): T = underlying.next()
}
