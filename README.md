# Akka Trading Engine

## Usage

### build

``` shell script
mvn package
```

### run

```shell script
cd target
# Run normal node
java -Dconfig.file="../config/local_node.conf" -jar app-1.0-allinone.jar
# Run gateway node
java -Dconfig.file="../config/local_gateway.conf" -jar app-1.0-allinone.jar
```
 