package org.apache.cassandra.triggers;

import java.net.InetAddress;
import java.util.ArrayList;
import org.elasticsearch.client.Client;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ElasticSearch {
	private static final Logger logger = LoggerFactory
			.getLogger(ElasticSearch.class);
	private Client client = null;
	private BulkRequestBuilder bulkRequest = null;

	private String esIndex;
	public ElasticSearch(String esIndex) {
		this.esIndex=esIndex;
		initialize();
	}

	public static int instanceCount = 0;
	long lastupdated = System.currentTimeMillis();

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
		logger.info("actions " + bulkRequest.numberOfActions());
		if (bulkRequest.numberOfActions() > 0) {
			BulkResponse bres = bulkRequest.execute().actionGet();
			BulkItemResponse[] bulkItemResponses = bres.getItems();
			if (bulkItemResponses.length > 0) {
				for (BulkItemResponse tempRequest : bulkItemResponses) {
					logger.info("actions complete");
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
				}
			}

			bulkRequest = null;
			bulkRequest = client.prepareBulk();
		}
		close();
	}

	public void updateCounterFieldInDocument(String index, String type,
			String id, String routing, Object key, long newValue) {
		if (client == null) {
			initialize();
		}
		logger.info("counter value: " + key + " newValue: " + newValue);
		String fieldName = "ctx._source." + key;
		HashMap<String,Object> scriptParamsMap= new HashMap<String,Object>();
		String scriptString=null;
		if(newValue<0){
				scriptParamsMap.put("count",-newValue);
				scriptString="if(" + fieldName + "==null) {" + fieldName+ "=params.count}else{" + fieldName + "-=params.count}";
		}else{
				scriptParamsMap.put("count",newValue);
				scriptString="if(" + fieldName + "==null) {" + fieldName+ "=params.count}else{" + fieldName + "+=params.count}";
		}
		Script script = new Script(ScriptType.INLINE, "painless", scriptString, scriptParamsMap);

		UpdateRequestBuilder prepareUpdate = client.prepareUpdate(index, type, id).setScript(script);
		if (routing != null) {
			prepareUpdate.setRouting(routing);
		}
		bulkRequest.add(prepareUpdate);
	}

	public void updateFieldsInDocument(String index, String type, String id,
			String routing, Map<Object, Object> data,
			Map<Object, String> updateColumnCollectionInfo,
			Map<Object, Object> deletedDataMap) {

		Map<String, Object> scriptParamsMap = new HashMap<String, Object>();
		String tempScript = "";
		if (null != updateColumnCollectionInfo
				&& updateColumnCollectionInfo.size() > 0) {
			for (Map.Entry<Object, String> entry : updateColumnCollectionInfo
					.entrySet()) {
				if (data.containsKey(entry.getKey())) {
					tempScript += handleCollectionColumnUpdate(
							data.get(entry.getKey()), entry.getValue(),
							(String) entry.getKey(), scriptParamsMap);
					data.remove(entry.getKey());
				}
			}

		}
		if (null != deletedDataMap && deletedDataMap.size() > 0) {

			for (Map.Entry<Object, Object> entry : deletedDataMap.entrySet()) {
				tempScript += handleCollectionColumnDelete(entry.getValue(),
						(String) entry.getKey(), scriptParamsMap);

			}
		}

		updateDocument(index, type, id, routing, data);
		if (scriptParamsMap != null && scriptParamsMap.size() > 0) {

			updateDocument(index, type, id, routing, tempScript,
					scriptParamsMap);
		}
	}

	private String handleCollectionColumnFullUpdate(String key, Object csvalue,
			String collectionColumnType) {

		String scriptString = "ctx._source." + key + "=[";
		switch (collectionColumnType) {

		case "org.apache.cassandra.db.marshal.SetType":
		case "org.apache.cassandra.db.marshal.ListType": {
			ArrayList<Object> csList = (ArrayList<Object>) csvalue;
			for (Object csValue : csList) {
				scriptString += csValue + ",";
			}
			scriptString = scriptString.substring(0, scriptString.length() - 1)
					+ "]";
			return scriptString;
		}
		}
		return null;

	}

	public void fullFieldUpdateInDocument(String index, String type, String id,
			String routing, Map<Object, Object> data,
			Map<Object, String> updateColumnCollectionInfo) {
		updateDocument(index, type, id, routing, data);
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

	public ConcurrentHashMap<String, GetResponse> documentMap = new ConcurrentHashMap<String, GetResponse>();

	public void updateDocument(String index, String type, String id,
			String routing, Object data) {
		if (client == null) {
			initialize();
		}

		logger.info(index + type + id + routing);
		UpdateRequest response = new UpdateRequest(index, type, id).doc(
				new GsonBuilder().setDateFormat("YYYY-MM-dd").serializeNulls()
						.create().toJson(data)).docAsUpsert(true);
		logger.info("log "+   new Gson().toJson(data));
		if (routing != null) {
			response.routing(routing);
		}
		bulkRequest.add(response);
	}

	public void updateDocument(String index, String type, String id,
			String routing, String scriptString,
			Map<String, Object> scriptParamsMap) {
		if (client == null) {
			initialize();
		}

		UpdateRequest response = new UpdateRequest(index, type, id)
				.script(new Script(ScriptType.INLINE, "painless", scriptString,
						scriptParamsMap));

		if (routing != null) {
			response.routing(routing);
		}
		bulkRequest.add(response);
	}

	private String handleCollectionColumnDelete(Object dataMap, String key,
			Map<String, Object> scriptMap) {
		String scriptString = "if(ctx._source." + key + "==null){ctx._source."
				+ key + "=[];}";
		if (dataMap != null) {
			ArrayList<Object> arrayList = (ArrayList<Object>) dataMap;
			int size = arrayList.size();
			int i = 0;
			for (Object object : arrayList) {
				scriptString += "ctx._source." + key + ".remove(ctx._source."
						+ key + ".indexOf(" + object + "));";
				// scriptString += "ctx._source." + key + ".remove(0);";
				scriptMap.put(key + i, object);
				i++;
			}
		}
		return scriptString;

	}

	private String handleCollectionColumnUpdate(Object dataMap,

	String collectionColumnType, String key, Map<String, Object> scriptMap) {

		String scriptString = "if(ctx._source." + key + "==null){ctx._source."
				+ key + "=[];}";
		switch (collectionColumnType) {

		case "org.apache.cassandra.db.marshal.SetType": {
			if (dataMap != null) {
				ArrayList<Object> arrayList = (ArrayList<Object>) dataMap;
				int size = arrayList.size();
				int i = 0;
				for (Object object : arrayList) {
					scriptString += "if(!(ctx._source." + key + ".contains("
							+ object + "))){ctx._source." + key
							+ ".add(params." + key + i + ");}";
					scriptMap.put(key + i, object);
					i++;
				}
				return scriptString;
			}
		}
		case "org.apache.cassandra.db.marshal.ListType": {

			if (dataMap != null) {
				ArrayList<Object> arrayList = (ArrayList<Object>) dataMap;
				int size = arrayList.size();
				int i = 0;
				for (Object object : arrayList) {
					scriptString += "ctx._source." + key + ".add(params." + key
							+ i + ");";
					scriptMap.put(key + i, object);
					i++;
				}
			}
			return scriptString;
		}

		default:
			scriptString += "ctx._source." + key + "=" + dataMap + ";";
		}
		return scriptString;
	}

}
