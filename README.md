## PyJava 

This library is designed for Java/Scala invoking python code using Apache 
Arrow as the exchanging data format. This means if you want to run some python code 
in Scala/Java, the Scala/Java will start some python workers automatically,
then send the data/code to python workers by socket and once the python workers process the data with user-provided python code 
finished ,the result will finally return back to Scala/Java. 

The initial code in this lib is from Apache Spark.


Notice: 
Make sure you python env have pyjava installed before testing following examples.

```sql
pip uninstall pyjava && pip install pyjava
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

## Example In MLSQL

None Interactive Mode:

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

Interactive Mode:

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


## How to configure python worker runs in Docker (todo)
