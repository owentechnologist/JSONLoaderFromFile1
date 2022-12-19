This simple example shows how to load JSON objects into Redis from a file

(your file format may be different)

To execute - provide the path to filename and other options like pipe batch size: 
* if you do not provide --pipebatchsize it will default to 200

``` 
 mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-10400.homelab.local --port 10400 --filepath /Users/owentaylor/wip/java/JSONLoaderFromFile1/src/main/resources/jsonkeyvalue.tldf --pipebatchsize 12"

```