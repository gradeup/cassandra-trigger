package org.apache.cassandra.triggers;

import java.util.HashMap;

public class Constants {

	public static final String ELASTIC_URL = "10.0.1.55";
	public static final int ELASTIC_PORT = 9302;
	public static final String RABBIT_URL = "amqp://localhost";
	public static final String ELASTIC_CLUSTER_NAME = "elasticsearch";
	public static final HashMap<String, String> ES_INDEX_KEY_MAP;
	public static final HashMap<String, String> TABLE_INDEX_MAP;
	public static final HashMap<String, String> INDEX_ROUTING_MAP;
	public static final String QUEUE_NAME = "CASSANDRA-ES-QUEUE";

	static {
		ES_INDEX_KEY_MAP = new HashMap<String, String>();
		ES_INDEX_KEY_MAP.put("user", "email");
		ES_INDEX_KEY_MAP.put("group_post", "postid");
		ES_INDEX_KEY_MAP.put("post_counts","postid");
		ES_INDEX_KEY_MAP.put("group_postcount", "groupid");
		ES_INDEX_KEY_MAP.put("user_postcount", "email");
		ES_INDEX_KEY_MAP.put("post_comment","commentid");
		ES_INDEX_KEY_MAP.put("bookmarks","userid#type#id");
		
		INDEX_ROUTING_MAP = new HashMap<String, String>();
		INDEX_ROUTING_MAP.put("group_post", "postid");
		INDEX_ROUTING_MAP.put("post_counts","postid");
		INDEX_ROUTING_MAP.put("post_comment","postid");
		INDEX_ROUTING_MAP.put("bookmarks","userid");

		TABLE_INDEX_MAP = new HashMap<String, String>();
		TABLE_INDEX_MAP.put("table1", "index1");
		TABLE_INDEX_MAP.put("post_counts","group_post");
		TABLE_INDEX_MAP.put("group_postcount","group");
		TABLE_INDEX_MAP.put("user_postcount","user");

	}

}
