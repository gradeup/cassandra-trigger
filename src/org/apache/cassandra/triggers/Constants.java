package org.apache.cassandra.triggers;

import java.util.HashMap;

public class Constants {
	
	public static final ArrayList<String> ELASTIC_CLUSTER_MAP;
	public static final String DEFAULT_ELASTIC_CLUSTER_NAME;		
	public static final String RABBIT_URL = "";	
	public static final HashMap<String, String> ES_INDEX_KEY_MAP;
	public static final HashMap<String, String> TABLE_INDEX_MAP;
	public static final HashMap<String, String> INDEX_ROUTING_MAP;
	public static final HashMap<String, String> INDEX_CLUSTER_MAP;
	public static final String QUEUE_NAME = "CASSANDRA-ES-QUEUE";
	
	

	static {
				
		ELASTIC_CLUSTER_MAP= new HashMap<>();	
		DEFAULT_ELASTIC_CLUSTER_NAME="elasticsearch";			
		ELASTIC_CLUSTER_MAP.add("elasticsearch:127.0.0.1:9300");
		
		//add more es server urls
		ELASTIC_CLUSTER_MAP.put("elasticsearch:127.0.0.1:9303");
		ELASTIC_CLUSTER_MAP.put("elasticsearch1:127.0.0.1:9301");
		ELASTIC_CLUSTER_MAP.put("elasticsearch2:127.0.0.1:9302");					

		INDEX_CLUSTER=new HashMap<String,String>();
		INDEX_CLUSTER_MAP.put("index1","elasticsearch1");

		INDEX_KEY_MAP = new HashMap<String, String>();
		INDEX_KEY_MAP.put("index","key1#key2#key3");
		INDEX_KEY_MAP.put("index2", "key");
		
		INDEX_ROUTING_MAP = new HashMap<String, String>();
		INDEX_ROUTING_MAP.put("index", "key1");
		
		


		TABLE_INDEX_MAP = new HashMap<String, String>();
		TABLE_INDEX_MAP.put("table1", "index1");

		ElasticClient.createClients();
	}


}
