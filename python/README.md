## Python side in PyJava 

This library is designed for Java/Scala invoking python code with Apache 
Arrow as the exchanging data format. This means if you want to run some python code 
in Scala/Java, the Scala/Java will start some python workers with this lib's help.
Then the Scala/Java will send the data/code to python worker in socket and the python will
return the processed data back. 

The initial code in this lib is from Apache Spark.  

