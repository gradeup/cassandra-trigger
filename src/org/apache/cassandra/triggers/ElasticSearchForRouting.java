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
			logger.info("Connecting ES " + instanceCount);			
			bulkRequest = client.prepareBulk();						
		}
	}

	public String getDocumentRouting(String index, String type, String id,
			boolean refresh, Map<String, Object> primaryKeyData,
			Map<String, Object> clusteringKeyData) {
		Map<String, Object> result = null;
		String searchindex = Constants.INDEX_ROUTING_MAP.get(index);
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

		SearchResponse getResponse = null;
		try {
			getResponse = client.prepareSearch(index).setTypes(index)
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
					.setQuery(QueryBuilders.matchQuery(searchindex, id))
					.setExplain(true).get();
			for (SearchHit hit : getResponse.getHits()) {
				result = hit.field("_source").<Map<String, Object>> getValue();
				break;
			}

		} catch (NullPointerException e) {
			logger.info("entry not found: " + e.getMessage());
		}
		return result.get(searchindex).toString();
	}
}
