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

Java Server side:

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

Python Client side:

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

Python Server side:

```python
import os

import pandas as pd

os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"
from pyjava.api.serve import OnceServer

ddata = pd.DataFrame(data=[[1, 2, 3, 4], [2, 3, 4, 5]])

server = OnceServer("127.0.0.1", 11111, "Asia/Harbin")
server.bind()
server.serve([{'id': 9, 'label': 1}])
```     

Java Client side:

```scala      
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.runner.SparkSocketRunner
import tech.mlsql.common.utils.network.NetUtils

val enconder = RowEncoder.apply(StructType(Seq(StructField("a", LongType),StructField("b", LongType)))).resolveAndBind()
val socketRunner = new SparkSocketRunner("wow", NetUtils.getHost, "Asia/Harbin")
val javaConext = new JavaContext
val commonTaskContext = new AppContextImpl(javaConext, null)
val iter = socketRunner.readFromStreamWithArrow("127.0.0.1", 11111, commonTaskContext)
iter.foreach(i => println(enconder.fromRow(i.copy())))
javaConext.close
```

## How to configure python worker runs in Docker (todo)


