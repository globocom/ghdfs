# GHDFS

Works with HDFS for common operations.

## Installation

Package is under com.globo.bigdata.ghdfs_2.11-0.0.1

Include in your dependencies:

```
    "com.globo.bigdata" %% "ghdfs" % "0.0.1"
```

## Usage

```
    val hdfs = new HdfsManager(new HdfsFactory(Properties.envOrNone("HADOOP_CONF_DIR")))
    
    hdfs.write(Path).write(String)
    
    hdfs.read(Path)
    
    hdfs.status(Path)
    
    hdfs.move(Path, Path)
    
    hdfs.listFiles(Path, recursive = false).foreach(...)

```

