package org.apache.cassandra.triggers;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

public class Constants {
	public static final ArrayList<String> ELASTIC_CLUSTER_MAP;
	public static final String DEFAULT_ELASTIC_CLUSTER_NAME;
	public static final String RABBIT_URL = "";
	public static final String MEMCACHE_URL = null;
	public static final int MEMCACHE_PORT = 11211;
	public static final HashMap<String, String> INDEX_KEY_MAP;
	public static final HashMap<String, String> TABLE_INDEX_MAP;
	public static final HashMap<String, String> INDEX_ROUTING_MAP;
	public static final HashMap<String, String> INDEX_CLUSTER_MAP;
	public static final String QUEUE_NAME = "CASSANDRA-ES-QUEUE";
	static {
		/*Config of all es clusters are stored in ELASTIC_CLUSTER_MAP variable. 
		Trigger code supports multiple ES clusters if required. 
		It has a primary cluster and rest can be added which are added as secondary.
		*/
		ELASTIC_CLUSTER_MAP= new ArrayList<>();
		//primary elasticsearch cluster name
		DEFAULT_ELASTIC_CLUSTER_NAME="elasticsearch";
		//primary elasticsearch clusterName:ip:port
		ELASTIC_CLUSTER_MAP.add("elasticsearch:127.0.0.1:9300");
		//add secondary mutliple es cluster urls if required
		ELASTIC_CLUSTER_MAP.add("elasticsearch1:127.0.0.1:9301");
		ELASTIC_CLUSTER_MAP.add("elasticsearch1:127.0.0.1:9302");

		/*if any index is not avaible on primary elasticsearch and is available on 
		other elasticsearch cluster, add its index name and es clustername here.
		*/
		INDEX_CLUSTER_MAP=new HashMap<String,String>();	
		INDEX_CLUSTER_MAP.put("index1","elasticsearch1");

		/*For each elasticsearch index, define _id key.
		*/
		INDEX_KEY_MAP = new HashMap<String, String>();
		INDEX_KEY_MAP.put("index","key1#key2#key3");
		INDEX_KEY_MAP.put("index2", "key");

		/*if there is a routing key associated with es index, define it here.
		*/
		INDEX_ROUTING_MAP = new HashMap<String, String>();
		INDEX_ROUTING_MAP.put("index", "key1");

		/*Define cassandra table to elasticsearch index mapping here.
		*/
		TABLE_INDEX_MAP = new HashMap<String, String>();
		TABLE_INDEX_MAP.put("table1", "index1");

		ElasticClient.createClients();

	}
}
