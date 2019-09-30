# GHDFS

Works with HDFS for common operations and Scala compatibility.

## Installation

Package is under com.globo.bigdata.ghdfs

Include in your dependencies:

```
    "com.globo.bigdata" %% "ghdfs" % "0.0.13"
```

## Usage

```
    val hdfs = HdfsManager(Properties.envOrNone("HADOOP_CONF_DIR"))
    
    hdfs.write(Path)
    
    hdfs.read(Path)
    
    hdfs.status(Path)
    
    hdfs.move(Path, Path)
    
    hdfs.listFiles(Path, recursive = false).foreach(...)
    
    hdfs.delete(Path, recursive = true)

    etc...
```

### Get Filesystem Instance

```
    hdfs.getFS.exists(hadoopPath)
```

## Contribute

For development and contributing, please follow [Contributing Guide](https://github.com/globocom/ghdfs/blob/master/CONTRIBUTING.md) and ALWAYS respect the [Code of Conduct](https://github.com/globocom/ghdfs/blob/master/CODE_OF_CONDUCT.md)
