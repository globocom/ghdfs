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
    
    hdfs.write(Path).write(String)
    
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

# Tests

```bash
make test
```

# Publish Test

Follow this tutorial: https://leonard.io/blog/2017/01/an-in-depth-guide-to-deploying-to-maven-central/

```
make snapshot
```

# Publish Test

```
make release
```
