package org.apache.cassandra.triggers;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class utils {
	private static final Logger logger = LoggerFactory.getLogger(utils.class);

	public static String getIndex(String indexColumnFamily) {
		String index = Constants.TABLE_INDEX_MAP.get(indexColumnFamily);
		if (index != null) {
			return index;
		}
		return indexColumnFamily;
	}

	public static String getType(String indexColumnFamily,
			Map<String, Object> partitionKeyValueList) {
		String type = Constants.TABLE_INDEX_MAP.get(indexColumnFamily);
		if (type != null) {
			return type;
		}
		return indexColumnFamily;
	}

	public static String getESId(String indexColumnFamily,
			Map<String, Object> partitionKeyValueList,
			Map<String, Object> clusterKeyValueList) {
		String id = "";
		logger.info("index:" + indexColumnFamily);
		Map<String, Object> allKeyValueList = new HashMap<String, Object>();
		allKeyValueList.putAll(partitionKeyValueList);
		allKeyValueList.putAll(clusterKeyValueList);
		String key = Constants.ES_INDEX_KEY_MAP.get(indexColumnFamily);
		try {
			if (key != null) {
				return allKeyValueList.get(key).toString();
			}
		} catch (Exception e) {

		}
		return null;

	}

}
