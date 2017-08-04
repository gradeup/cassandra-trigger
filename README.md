# cassandra-trigger

It syncs data from cassandra to ElasticSearch.

## How to run

* Copy this code dir in directory cassandra/examples/triggers/

* Modify constants in Constants.java file

* Download jars listed in conf/lib-files file and copy them to cassandra/lib/ directory.

* Create a folder conf in cassandra directory.

* Copy InvertedIndex.properties file from project's conf directory to cassandra conf directory.

* Modify InvertedIndex.properties values as per your config.

* Build the jar by running

```
ant jar
```

* copy build/trigger-example.jar to cassandra/conf/triggers/ directory.

* Reload triggers by running

```
bin/nodetool reloadtriggers
```

## Example

* Create Trigger

```
CREATE TRIGGER test1 ON "Keyspace1"."Standard1" USING 'org.apache.cassandra.triggers.InvertedIndex';
```
