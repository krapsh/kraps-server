# kraps-server

This is the server part for the Kraps project. It provides a simple REST API to execute
data pipelines with Spark in a language independent manner. It complements (on the JVM side) the
Haskell bindings available in Krapsh.

This project is only a technological preview. The API may change in the future.

The simplest way to run the server is to use the prebuilt Spark package.

## Developer instructions

The spark package has been tested with Spark 2.0 and Spark 2.1.

```
./build/sbt assembly && $SPARK_HOME/bin/spark-submit \
    ./target/scala-2.11/krapsh-server-assembly-0.1.1.jar --name krapsh-server\
     --class org.krapsh.Boot --master "local[1]" -v
```
