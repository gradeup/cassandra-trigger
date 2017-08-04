package org.apache.cassandra.triggers;

import com.rabbitmq.client.*;
import java.io.IOException;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
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
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.SerializationUtils;

public class ElasticSearch {
	private static final Logger logger = LoggerFactory
			.getLogger(ElasticSearch.class);
	private static Client client = null;
	private BulkRequestBuilder bulkRequest = null;

	public ElasticSearch() {

		initialize();
	}

	public static int instanceCount = 0;
	long lastupdated = System.currentTimeMillis();

	public void closeClient() {
		if (client != null) {
			client.close();
			client = null;
			logger.info("CLOSED");
		}
	}

	public void initialize() {
		if (client == null) {
			instanceCount++;
			logger.info("Connecting ES " + instanceCount);
			try {
				Settings settings = Settings.builder()
						.put("cluster.name", Constants.ELASTIC_CLUSTER_NAME)
						.build();
				client = new PreBuiltTransportClient(settings)
						.addTransportAddress(new InetSocketTransportAddress(
								InetAddress.getByName(Constants.ELASTIC_URL),
								Constants.ELASTIC_PORT));
				bulkRequest = client.prepareBulk();
			} catch (Exception e) {
				e.printStackTrace();
				if (client != null) {
					try {
						client.close();
					} catch (Exception e1) {

					}

				}

				client = null;
			}

		} else {
			bulkRequest = client.prepareBulk();
		}
	}

	public void close() {
		logger.info("node-end: " + instanceCount);
	}

	private class BulkRequest {
		public BulkRequest() {
			response = null;
			failure = null;
		}

		public Response response = new Response();
		public Response failure = new Response();

		private class Response {
			public String index;
			public String id;
			public String type;
		}
	}

	public void executeBulk() {
		if (client == null) {
			initialize();
		}
		if (bulkRequest.numberOfActions() > 0) {
			BulkResponse bres = bulkRequest.execute().actionGet();
			BulkItemResponse[] bulkItemResponses = bres.getItems();
			if (bulkItemResponses.length > 0) {
				for (BulkItemResponse tempRequest : bulkItemResponses) {
					String temp = "";
					if (tempRequest.isFailed()) {
						String index = tempRequest.getIndex();
						temp = index + tempRequest.getType()
								+ tempRequest.getId();
					} else {
						String index = tempRequest.getIndex();
						if (index.contains("1")) {
							index = index.replace("1", "");
						}
						temp = index + tempRequest.getType()
								+ tempRequest.getId();
					}
					documentMap.remove(temp);
					while (true) {
						if (mutex.contains(temp)) {
							mutex.remove(temp);
						} else {
							break;
						}
					}

					if (mutex.isEmpty()) {
						break;
					}
				}
			}

			bulkRequest = null;
			bulkRequest = client.prepareBulk();
		}
		close();
	}

	public void addNewFieldInDocument(String index, String type, String id,
			String field, String newValue) {
		if (client == null) {
			initialize();
		}
		UpdateRequestBuilder prepareUpdate = client.prepareUpdate(index, type,
				id);

		Script script = new Script("ctx._source." + field + "=\"" + newValue
				+ "\"");
		prepareUpdate.setScript(script);
		bulkRequest.add(prepareUpdate);
	}

	public void updateCounterFieldInDocument(String index, String type,
			String id, String routing, Object key, long newValue) {
		if (client == null) {
			initialize();
		}
		getDocument(index, type, id, routing, false);

		logger.info("counter value: " + key + " newValue: " + newValue);
		String fieldName = "ctx._source." + key;
		Script script = new Script("if(" + fieldName + "==null) {" + fieldName
				+ "=" + newValue + "}else{" + fieldName + "+=" + newValue + "}");
		UpdateRequestBuilder prepareUpdate = client.prepareUpdate(index, type,
				id).setScript(script);
		if (routing != null) {
			prepareUpdate.setRouting(routing);
		}

		bulkRequest.add(prepareUpdate);
	}

	public void updateFieldsInDocument(String index, String type, String id,
			String routing, Map<Object, Object> data,
			Map<Object, String> updateColumnCollectionInfo) {
		GetResponse getResponse = getDocument(index, type, id, routing, true);
		Map<String, Object> source = getResponse.getSource();
		if (null == source) {
			source = new HashMap<String, Object>();
		}
		for (int i = 0; i < data.size(); i++) {
			String key = (String) data.keySet().toArray()[i];
			Object value = data.get(key);
			Object object = source.get(key);
			if (null != updateColumnCollectionInfo && object != null
					&& updateColumnCollectionInfo.containsKey(key)) {
				Object modifiedValueOfKey = handleCollectionColumnUpdate(
						source.get(key), data.get(key),
						updateColumnCollectionInfo.get(key), key);
			} else {
				if (updateColumnCollectionInfo == null
						|| !updateColumnCollectionInfo.containsKey(key)) {
					source.put(key, value);
				} else {
					Object modifiedValueOfKey = handleCollectionColumnAdd(
							source.get(key), data.get(key),
							updateColumnCollectionInfo.get(key));
					source.put(key, modifiedValueOfKey);
				}

			}
		}

		updateDocument(index, type, id, routing, source);
	}

	public void deleteFieldsInDocument(String index, String type, String id,
			String routing, Map<Object, Object> data,
			Map<Object, String> updateColumnCollectionInfo) {
		GetResponse getResponse = getDocument(index, type, id, routing, true);
		Map<String, Object> source = getResponse.getSource();
		for (int i = 0; i < data.size(); i++) {
			String key = (String) data.keySet().toArray()[i];
			Object value = data.get(key);
			if (source != null) {
				source.put(key, value);
			}
		}
		if (source != null) {
			postDocument(index, type, id, routing, source);
		}
	}

	public void updateCounterFieldsInDocument(String index, String type,
			String id, String routing, Map<Object, Object> data) {
		Set<Object> keySet = data.keySet();
		for (Object key : keySet) {
			updateCounterFieldInDocument(index, type, id, routing, key,
					(Long) data.get(key));
		}
	}

	public void deleteDocument(String index, String type, String id,
			String routing) {
		if (client == null) {
			initialize();
		}
		try {
			DeleteRequestBuilder response = client.prepareDelete(index, type,
					id);
			if (routing != null) {
				response.setRouting(routing);
			}
			bulkRequest.add(response);
		} catch (NullPointerException e) {
			logger.info("entry not found: " + e.getMessage());
		}
	}

	static List<String> mutex = new ArrayList<String>();
	public ConcurrentHashMap<String, GetResponse> documentMap = new ConcurrentHashMap<String, GetResponse>();

	public synchronized boolean checkForContain(String s) {
		if (!mutex.contains(s)) {
			mutex.add(s);
			return true;
		}
		return false;
	}

	public synchronized boolean checkForInitialContain(String s) {
		if (mutex.contains(s)) {
			return true;
		}
		return false;
	}

	public GetResponse getDocument(String index, String type, String id,
			String routing, boolean refresh) {

		String s = index + type + id;
		if (client == null) {
			initialize();
		}
		if (documentMap.containsKey(s)) {
			return documentMap.get(s);
		}
		if (checkForInitialContain(s)) {
			int hitCount = 0;
			while (hitCount < 100) {

				if (checkForContain(s)) {
					break;
				}
				try {
					hitCount++;
					Thread.sleep(3);

				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} else {
			mutex.add(s);
		}
		GetResponse getResponse = null;
		try {
			if (routing != null) {
				getResponse = client.prepareGet(index, type, id)
						.setRefresh(refresh).setRouting(routing).execute()
						.actionGet();
			} else {
				if (!index.equalsIgnoreCase("post_comment")
						&& !index.equalsIgnoreCase("reply")) {
					getResponse = client.prepareGet(index, type, id)
							.setRefresh(refresh).execute().actionGet();
				}
			}
		} catch (NullPointerException e) {
			logger.info("entry not found: " + e.getMessage());
		}
		if (getResponse != null) {
			documentMap.put(s, getResponse);
		}
		return getResponse;
	}

	public void postDocument(String index, String type, String id,
			String routing, Map<String, Object> dataMap) {
		if (client == null) {
			initialize();
		}
		IndexRequestBuilder response = client.prepareIndex(index, type, id)
				.setSource(dataMap);
		if (routing != null) {
			response.setRouting(routing);
		}

		bulkRequest.add(response);
	}

	public void updateDocument(String index, String type, String id,
			String routing, Map<String, Object> dataMap) {
		if (client == null) {
			initialize();
		}

		UpdateRequestBuilder response = client.prepareUpdate(index, type, id)
				.setDocAsUpsert(true).setDoc(dataMap);
		if (routing != null) {
			response.setRouting(routing);
		}
		bulkRequest.add(response);
	}

	private Object handleCollectionColumnDelete(Object sourceKey,
			Object dataMap, String collectionColumnType) {

		switch (collectionColumnType) {

		case "org.apache.cassandra.db.marshal.SetType":
		case "org.apache.cassandra.db.marshal.ListType": {
			String[] dataList = ((String) dataMap).split(",");
			ArrayList<Object> sourceList = (ArrayList<Object>) sourceKey;
			for (int i = 0; i < dataList.length; i++)
				if (!sourceList.contains(dataList[i])) {
					sourceList.remove(dataList[i]);
				}
			return sourceList.toArray();
		}

		case "org.apache.cassandra.db.marshal.MapType": {
			return dataMap;
		}

		}
		return (String) dataMap;
	}

	private Object handleCollectionColumnUpdate(Object sourceKey,
			Object dataMap, String collectionColumnType, String key) {

		switch (collectionColumnType) {

		case "org.apache.cassandra.db.marshal.SetType":
		case "org.apache.cassandra.db.marshal.ListType": {
			ArrayList<Object> sourceList = (ArrayList<Object>) sourceKey;
			if (dataMap != null) {
				ArrayList<Object> arrayList = (ArrayList<Object>) dataMap;
				int size = arrayList.size();
				for (Object object : arrayList) {
					if (!sourceList.contains(object)) {
						sourceList.add(object);
					}
				}
			}
			return sourceList.toArray();
		}

		case "org.apache.cassandra.db.marshal.MapType": {
			Gson gson = new Gson();
			Map<String, Object> fromJson = (HashMap<String, Object>) sourceKey;
			for (Entry<String, Object> entry : ((HashMap<String, Object>) dataMap)
					.entrySet()) {

				if (entry.getValue() != null) {
					fromJson.put(entry.getKey() + "", entry.getValue());
				}

			}
			return dataMap;
		}
		}
		return (String) dataMap;
	}

	private Object handleCollectionColumnAdd(Object sourceKey, Object dataMap,
			String collectionColumnType) {
		switch (collectionColumnType) {

		case "org.apache.cassandra.db.marshal.SetType":
		case "org.apache.cassandra.db.marshal.ListType":
			ArrayList<Object> objectList = new ArrayList<Object>();
			if (dataMap != null) {
				try {
					objectList.addAll((Collection<? extends Object>) dataMap);
				} catch (RuntimeException e) {
					logger.info("CAUTION : Unable to typecast list object");
				}
			}
			return objectList.toArray();

		case "org.apache.cassandra.db.marshal.MapType": {
			Gson gson = new Gson();
			return dataMap;
		}
		}
		return (String) dataMap;
	}

}
