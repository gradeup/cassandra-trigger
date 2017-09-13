package org.apache.cassandra.triggers;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class ElasticSearchForRouting {
	private static final Logger logger = LoggerFactory
			.getLogger(ElasticSearchForRouting.class);
	private  TransportClient client = null;

	public static int instanceCount = 0;

	private String esIndex;
	public ElasticSearchForRouting(String esIndex) {
		this.esIndex=esIndex;
		initialize();
	}

	public void initialize() {

		if(client==null){
			client=ElasticClient.getClient(esIndex);
			if(client==null){
				return;
			}
		}
	}

	public String getDocumentRouting(String index, String type, String id,
			boolean refresh, Map<String, Object> primaryKeyData,
			Map<String, Object> clusteringKeyData) {
		String result = null;
		String searchindex = Constants.INDEX_ROUTING_MAP.get(Constants.TABLE_INDEX_MAP.get(index));
		if (searchindex == null) {
			return null;
		}
		if (client == null) {
			initialize();
		}
		Object value = primaryKeyData.get(searchindex);
		if (value != null) {
			return value.toString();
		}
		value = clusteringKeyData.get(searchindex);
		if (value != null) {
			return value.toString();
		}
		String searchkey = Constants.INDEX_KEY_MAP.get(Constants.TABLE_INDEX_MAP.get(indexColumnFamily));
		SearchResponse getResponse = null;
		try {
			getResponse = client.prepareSearch(index).setTypes(index)
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
					.setQuery(QueryBuilders.matchQuery(searchkey, id))
					.setExplain(true).get();
			for (SearchHit hit : getResponse.getHits().getHits()) {
				result = hit.getFields().get("_routing").value();
				return result.toString();
			}

		} catch (NullPointerException e) {
			logger.error("ex",e);
			logger.info("entry not found: " + e.getMessage());
		}
		return null;
	}
}
