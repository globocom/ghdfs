[![Build Status](https://travis-ci.com/globocom/ghdfs.svg?branch=master)](https://travis-ci.com/globocom/ghdfs)

# GHDFS

> Works with HDFS for common operations and Scala compatibility.

## Installation

Package is under com.globo.bigdata.ghdfs

- Include in your dependencies:

```branch
    "com.globo.bigdata" %% "ghdfs" % "0.0.13"
```

## Usage

```branch
    val hdfs = HdfsManager(Properties.envOrNone("HADOOP_CONF_DIR"))
    
    hdfs.write(Path)
    
    hdfs.write(Path, InputStream)

    hdfs.read(Path)
    
    hdfs.status(Path)
    
    hdfs.move(Path, Path)
    
    hdfs.listFiles(Path, recursive = false).foreach(...)
    
    hdfs.delete(Path, recursive = true)

    etc...
```

### Get Filesystem Instance

```branch
    hdfs.getFS.exists(hadoopPath)
```

## Contribute

For development and contributing, please follow [Contributing Guide](https://github.com/globocom/ghdfs/blob/master/CONTRIBUTING.md) and ALWAYS respect the [Code of Conduct](https://github.com/globocom/ghdfs/blob/master/CODE_OF_CONDUCT.md)
