package org.apache.cassandra.triggers;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.config.ColumnDefinition;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.FileUtils;
import org.elasticsearch.common.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
 
public class CassandraRiver {
	private static final Logger logger = LoggerFactory
			.getLogger(InvertedIndex.class);

	private static CassandraFactory cassandraFactory = CassandraFactory
			.getInstance();

	ArrayList<Object> primaryKeyValueList = new ArrayList<Object>();
	ArrayList<Object> clusterKeyValueList = new ArrayList<Object>();

	public interface DeleteDataListener {
		public void onDataObtained(com.datastax.driver.core.ResultSet resultSet);
	}

	public DeleteDataListener deleteDataListener;

	public static void deleteData(String keyspaceName,Map<String, Object> hashMap,
			String indexColumnFamily, Map<String, Object> partitionKeyData,
			Map<String, Object> map, final DeleteDataListener deleteDataListener) {

		if (hashMap == null || hashMap.size() == 0) {
			return;
		}
		// GET primary column values
		String primaryKeyQueryString = "";
		String clusteringKeyQueryString = " and ";
		ArrayList<Object> dataObjects = new ArrayList<Object>();

		for (Entry<String, Object> entry : partitionKeyData.entrySet()) {
			primaryKeyQueryString += entry.getKey() + " = ? and ";
			dataObjects.add(entry.getValue());
		}
		for (Entry<String, Object> entry : map.entrySet()) {
			clusteringKeyQueryString += entry.getKey() + " = ? and ";
			dataObjects.add(entry.getValue());
		}

		primaryKeyQueryString = primaryKeyQueryString.substring(0,
				primaryKeyQueryString.length() - 4);

		clusteringKeyQueryString = clusteringKeyQueryString.substring(0,
				clusteringKeyQueryString.length() - 4);

		Set<Entry<String, Object>> keySet = hashMap.entrySet();
		String SQL = "select ";
		for (Entry<String, Object> entry : keySet) {
			logger.info("key: " + entry.getKey());
			SQL += entry.getKey() + ",";
		}
		SQL = SQL.substring(0, SQL.length() - 1) + " from " + indexColumnFamily
				+ " where " + primaryKeyQueryString + clusteringKeyQueryString
				+ ";";
		Session session = cassandraFactory.getSession(keyspaceName);
		PreparedStatement statement = session.prepare(SQL);
		BoundStatement bndStm = new BoundStatement(statement);
		BoundStatement bind = bndStm.bind(dataObjects.toArray());
		final int batchSize = 1000;
		bndStm.setFetchSize(batchSize);

		ResultSetFuture resultSetFuture = session.executeAsync(bind);

		Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
			public void onSuccess(ResultSet resultSet) {
				deleteDataListener.onDataObtained(resultSet);
			};

			public void onFailure(Throwable throwable) {
				logger.info("Failed with: %s\n", throwable);
			};

		});

	}
}
