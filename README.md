# cassandra-trigger

It syncs data from cassandra to ElasticSearch. 
It works with cassandra version 3.x and ElasticSearch 5.x.
It can also be used to sync cassandra with any database, just replace ElasticSearch class with a class specific to your database.

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

## Just for fun

Incase your elasticsearch/other database is down or not working, it sends the message(with data) to rabbitmq server. You can run a rabbitmq consumer to read the data from queue and insert it into elasticsearch.
Incase you don't need that functionality just comment out function 'queueMessage' from ElasticQueue.java file.

## General Tip
For all the updates in cassandra, we get value of primary key and clustering key in trigger code and the updated column and its value.
So, whenever creating a table in cassandra, try to keep routing key(of the corresponding index in elasticsearch) part of primary key or cluster key as you would require routing key when updating document in ES.

For eg. 
Cassandra Table Structure : 
user_post (userid, postid, text, somecol) with primary key userid and clustering key as postid.

Corresponding ES Index : 
'user_post' with routing userid or postid as you would get these values in your cassandra trigger directly. In case you would have kept 'somecol' as routing key, then you would need to query ES for value of 'somecol' which would make write slow.


