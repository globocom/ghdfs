package com.globo.bigdata.ghdfs

import java.io.IOException

class GHdfsIOException(e: IOException) extends IOException(e) {
  def this(message: String) = this(new IOException(message))
}
