## PyJava 

This library is an ongoing effort towards bringing the data exchanging ability 
between Java/Scala and Python. PyJava introduces Apache Arrow as the exchanging data format,
this means we can avoid ser/der between Java/Scala and Python which can really speed up the 
communication efficiency than traditional way.   
 
When you invoke python code in Java/Scala side, PyJava will start some python workers automatically
and send the data to python worker, and once they are processed, send them back. The python workers are reused  
by default.


> The initial code in this lib is from Apache Spark.


Before you can run PyJava in Java/Scala side, please do the following command 
in the target python env.

```sql
pip uninstall pyjava && pip install pyjava
```

## Example In MLSQL

### None Interactive Mode:

```sql
!python env "PYTHON_ENV=source activate streamingpro-spark-2.4.x";
!python conf "schema=st(field(a,long),field(b,long))";

select 1 as a as table1;

!python on table1 '''

import pandas as pd
import numpy as np
for item in data_manager.fetch_once():
    print(item)
df = pd.DataFrame({'AAA': [4, 5, 6, 8],'BBB': [10, 20, 30, 40],'CCC': [100, 50, -30, -50]})
data_manager.set_output([[df['AAA'],df['BBB']]])

''' named mlsql_temp_table2;

select * from mlsql_temp_table2 as output; 
```

### Interactive Mode:

```sql
!python start;

!python env "PYTHON_ENV=source activate streamingpro-spark-2.4.x";
!python env "schema=st(field(a,integer),field(b,integer))";


!python '''
import pandas as pd
import numpy as np
''';

!python  '''
for item in data_manager.fetch_once():
    print(item)
df = pd.DataFrame({'AAA': [4, 5, 6, 8],'BBB': [10, 20, 30, 40],'CCC': [100, 50, -30, -50]})
data_manager.set_output([[df['AAA'],df['BBB']]])
''';
!python close;
```

## Example In Normal Java/Scala Application

```scala
val envs = new util.HashMap[String, String]()
envs.put(str(PythonConf.PYTHON_ENV), "source activate streamingpro-spark-2.4.x")

val dataSchema = StructType(Seq(StructField("value", StringType)))
val enconder = RowEncoder.apply(dataSchema).resolveAndBind()
val batch = new ArrowPythonRunner(
  Seq(ChainedPythonFunctions(Seq(PythonFunction(
    """
      |import pandas as pd
      |import numpy as np
      |for item in data_manager.fetch_once():
      |    print(item)
      |df = pd.DataFrame({'AAA': [4, 5, 6, 7],'BBB': [10, 20, 30, 40],'CCC': [100, 50, -30, -50]})
      |data_manager.set_output([[df['AAA'],df['BBB']]])
    """.stripMargin, envs, "python", "3.6")))), dataSchema,
  "GMT", Map()
)
val newIter = Seq(Row.fromSeq(Seq("a1")), Row.fromSeq(Seq("a2"))).map { irow =>
  enconder.toRow(irow)
}.iterator
val javaConext = new JavaContext
val commonTaskContext = new AppContextImpl(javaConext, batch)
val columnarBatchIter = batch.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)
columnarBatchIter.flatMap { batch =>
  batch.rowIterator.asScala
}.foreach(f => println(f.copy()))
javaConext.markComplete
javaConext.close
```

## Example In Spark

```scala
val session = spark
import session.implicits._
val timezoneid = session.sessionState.conf.sessionLocalTimeZone
val df = session.createDataset[String](Seq("a1", "b1")).toDF("value")
val struct = df.schema
val abc = df.rdd.mapPartitions { iter =>
  val enconder = RowEncoder.apply(struct).resolveAndBind()
  val envs = new util.HashMap[String, String]()
  envs.put(str(PythonConf.PYTHON_ENV), "source activate streamingpro-spark-2.4.x")
  val batch = new ArrowPythonRunner(
    Seq(ChainedPythonFunctions(Seq(PythonFunction(
      """
        |import pandas as pd
        |import numpy as np
        |for item in data_manager.fetch_once():
        |    print(item)
        |df = pd.DataFrame({'AAA': [4, 5, 6, 7],'BBB': [10, 20, 30, 40],'CCC': [100, 50, -30, -50]})
        |data_manager.set_output([[df['AAA'],df['BBB']]])
      """.stripMargin, envs, "python", "3.6")))), struct,
    timezoneid, Map()
  )
  val newIter = iter.map { irow =>
    enconder.toRow(irow)
  }
  val commonTaskContext = new SparkContextImp(TaskContext.get(), batch)
  val columnarBatchIter = batch.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)
  columnarBatchIter.flatMap { batch =>
    batch.rowIterator.asScala
  }
}

val wow = SparkUtils.internalCreateDataFrame(session, abc, StructType(Seq(StructField("AAA", LongType), StructField("BBB", LongType))), false)
wow.show()
```

## Run Python Project



```scala
import tech.mlsql.arrow.python.runner.PythonProjectRunner

val runner = new PythonProjectRunner("./pyjava/examples/pyproject1", Map())
val output = runner.run(Seq("bash", "-c", "source activate streamingpro-spark-2.4.x && python train.py"), Map(
  "tempDataLocalPath" -> "/tmp/data",
  "tempModelLocalPath" -> "/tmp/model"
))
output.foreach(println)
```

## Arrow Server/Client

Server side:

```scala
val socketRunner = new SparkSocketRunner("wow", NetUtils.getHost, "Asia/Harbin")

val dataSchema = StructType(Seq(StructField("value", StringType)))
val enconder = RowEncoder.apply(dataSchema).resolveAndBind()
val newIter = Seq(Row.fromSeq(Seq("a1")), Row.fromSeq(Seq("a2"))).map { irow =>
  enconder.toRow(irow)
}.iterator
val javaConext = new JavaContext
val commonTaskContext = new AppContextImpl(javaConext, null)

val Array(_, host, port) = socketRunner.serveToStreamWithArrow(newIter, dataSchema, 10, commonTaskContext)
println(s"${host}:${port}")
Thread.currentThread().join()
```   

Client side:

```python  
import os
import socket

from pyjava.serializers import \
    ArrowStreamPandasSerializer

out_ser = ArrowStreamPandasSerializer(None, True, True)

out_ser = ArrowStreamPandasSerializer("Asia/Harbin", False, None)
HOST = ""
PORT = -1
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect((HOST, PORT))
    buffer_size = int(os.environ.get("SPARK_BUFFER_SIZE", 65536))
    infile = os.fdopen(os.dup(sock.fileno()), "rb", buffer_size)
    outfile = os.fdopen(os.dup(sock.fileno()), "wb", buffer_size)
    kk = out_ser.load_stream(infile)
    for item in kk:
        print(item)
```

## How to configure python worker runs in Docker (todo)


