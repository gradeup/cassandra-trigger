package org.apache.cassandra.triggers;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class ElasticQueue {

	static ConnectionFactory factory = new ConnectionFactory();

	public static void queueMessage(String currentEsId, String currentType,
			String currentIndex, String updateType,
			HashMap<Object, Object> currentDataMap,
			Map<Object, String> currentUpdateColumnCollectionInfo, Logger logger) {

		try {
			factory.setUri(Constants.RABBIT_URL);
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();
			channel.queueDeclare(Constants.QUEUE_NAME, false, false, false,
					null);
			HashMap<String, Object> messageObject = new HashMap<String, Object>();
			messageObject.put("dataMap", currentDataMap);
			messageObject.put("updateColumnCollectionInfo",
					currentUpdateColumnCollectionInfo);
			messageObject.put("esId", currentEsId);
			messageObject.put("index", currentIndex);
			messageObject.put("type", currentType);
			messageObject.put("updateType", updateType);

			channel.basicPublish("", Constants.QUEUE_NAME, null,
					SerializationUtils.serialize(messageObject));

			channel.close();

			connection.close();
		} catch (RuntimeException e) {
			logger.error("CAUTION RUNTIME : " + e.getMessage(), e);
		} catch (Exception e) {
			logger.error("CAUTION : " + e.getMessage(), e);
		}
	}
}
