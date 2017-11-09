package org.apache.cassandra.triggers;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TimerTask;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Row.Deletion;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.ConnectionFactory;

public class InvertedIndex implements ITrigger {
	private static final Logger logger = LoggerFactory
			.getLogger(InvertedIndex.class);
	private static Properties properties = loadProperties();
	static String indexKeySpace = properties.getProperty("keyspace");
	static String hosts = properties.getProperty("hosts");
	static String port = properties.getProperty("port");
	static String datacenter = properties.getProperty("datacenter");
	static String username = properties.getProperty("username");
	static String password = properties.getProperty("password");
	static String clusterName = properties.getProperty("clusterName");
	static ConnectionFactory factory = new ConnectionFactory();

	public static String getRoutingFromEs(String key, String index,
			Map<String, Object> primaryKeyData,
			Map<String, Object> clusteringKeyData) {
		ElasticSearchForRouting es = new ElasticSearchForRouting(index);

		if (es == null) {
			return null;
		}
		try {
			String routing = es.getDocumentRouting(index, index, key, true,
					primaryKeyData, clusteringKeyData);
			return routing;
		} catch (Exception ex) {
			return null;
		}
	}

	private class MyString {
		public String updateType;
	}

	private Map<String, Object> getPartitionKeyData(ByteBuffer key,
			CFMetaData update) {
		List<ColumnDefinition> partitionKeyList = new ArrayList<ColumnDefinition>();
		Map<String, Object> partitionKeyValueList = new HashMap<String, Object>();
		Object dataObject = null;
		partitionKeyList = update.partitionKeyColumns();
		if (partitionKeyList.size() == 1) {

			AbstractType<?> ptype = partitionKeyList.get(0).type;
			dataObject = ptype.compose(key);

			if (dataObject != null) {
				partitionKeyValueList.put(
						partitionKeyList.get(0).name.toString(), dataObject);
			}
		} else {
			for (int pkIndex = 0; pkIndex < partitionKeyList.size(); pkIndex++) {
				String pname = partitionKeyList.get(pkIndex).name.toString();
				AbstractType<?> ptype = partitionKeyList.get(pkIndex).type;

				ByteBuffer tempKey = CompositeType.extractComponent(key,
						pkIndex);
				dataObject = ptype.compose(tempKey);

				if (dataObject != null) {
					partitionKeyValueList.put(pname, dataObject);
				}
			}
		}

		return partitionKeyValueList;
	}

	private Map<String, Object> getClusterKeyData(Partition update,
			Unfiltered next) {
		List<ColumnDefinition> clusterKeyList = new ArrayList<ColumnDefinition>();
		Map<String, Object> clusterKeyValueList = new HashMap<String, Object>();

		clusterKeyList = update.metadata().clusteringColumns();
		ClusteringPrefix clustering = next.clustering();
		int clSize = clustering.size();
		for (int i = 0; i < clSize; i++) {
			ColumnDefinition columnDefinition = clusterKeyList.get(i);
			String columnName = columnDefinition.name.toString();
			AbstractType<?> columnType = columnDefinition.type;
			Object compose = columnType.compose(clustering.get(i));
			clusterKeyValueList.put(columnName, compose);
		}
		return clusterKeyValueList;
	}

	public Collection<Mutation> augment(Partition partition) {
		ElasticSearch es = null;
		String currentEsId = null, currentType = null, currentIndex = null;
		MyString currentMyString = null;
		HashMap<Object, Object> currentDataMap = new HashMap<Object, Object>();
		Map<Object, String> currentUpdateColumnCollectionInfo = new HashMap<Object, String>();

		try {
			PartitionColumns columns = partition.columns();
			final String indexColumnFamily = partition.metadata().cfName;
			final int updatedColumnCount = columns.size();
			Map<String, Object> partitionKeyData = new HashMap<String, Object>();
			Map<String, Object> clusterKeyData = new HashMap<String, Object>();
			DecoratedKey partitionKey = partition.partitionKey();
			partitionKeyData = getPartitionKeyData(partitionKey.getKey(),
					partition.metadata());

			DeletionTime levelDeletion = partition.partitionLevelDeletion();

			String tempIndex = utils.getIndex(indexColumnFamily);
			final String index = tempIndex;
			final String type = utils.getType(index, partitionKeyData);
			boolean shouldRun = false;
			if (!levelDeletion.isLive()) {

				final String esId = utils.getESId(indexColumnFamily,
						partitionKeyData, clusterKeyData);
				MyString myString = new MyString();
				myString.updateType = "DELETE_ROW";
				logger.info("deleted rowww: " + index + "--" + type + "--"
						+ esId);
				if (esId == null) {
					return null;
				}
				String routing = getRoutingFromEs(esId, index,
						partitionKeyData, clusterKeyData);

				es = new ElasticSearch(index);
				es.deleteDocument(index, type, esId, routing);
				currentEsId = esId;
				currentIndex = index;
				currentType = type;
				currentMyString = myString;
				es.executeBulk();
				return null;
			}

			UnfilteredRowIterator unfilteredIterator = partition
					.unfilteredIterator();
			int loopCount = 0;
			while (unfilteredIterator.hasNext()) {
				loopCount++;
				Map<String, Object> updateLaterCollectionList = new HashMap<>();
				MyString myString = new MyString();
				myString.updateType = "UPDATE_ROW";

				Unfiltered next = unfilteredIterator.next();

				ClusteringPrefix clustering = next.clustering();
				Row row = partition.getRow((Clustering) clustering);
				clusterKeyData = getClusterKeyData(partition, next);

				final String esId = utils.getESId(indexColumnFamily,
						partitionKeyData, clusterKeyData);
				Iterable<Cell> cells = row.cells();

				final Map<Object, String> updateColumnCollectionInfo = new HashMap<Object, String>();
				HashMap<Object, Object> dataMap = new HashMap<Object, Object>();
				HashMap<Object, Object> deletedDataMap = new HashMap<Object, Object>();

				Deletion deletion = row.deletion();

				if (deletion != null && !deletion.isLive()) {
					myString.updateType = "DELETE_ROW";
				} else {

					for (Cell cell : cells) {

						ColumnDefinition column = cell.column();
						String columnName = column.name + "";
						// AbstractType<?> type = column.type;
						AbstractType<Object> cellValueType = (AbstractType<Object>) column
								.cellValueType();
						Object cellValue = cellValueType.compose(cell.value());
						AbstractType<?> columnType = column.type;
						if (cell.isCounterCell()) {
							myString.updateType = "UPDATE_COUNTER";
							dataMap.put(columnName, cellValue);
						} else if (columnType instanceof MapType<?, ?>) {
							MapType<Object, Object> mapType = (MapType<Object, Object>) columnType;

							AbstractType<Object> keysType = mapType
									.getKeysType();
							CellPath path = cell.path();
							int size = path.size();
							for (int i = 0; i < size; i++) {
								ByteBuffer byteBuffer = path.get(i);
								Object cellKey = keysType.compose(byteBuffer);

								if (!dataMap.containsKey(columnName)) {
									Map<Object, Object> map = new HashMap<Object, Object>();
									dataMap.put(columnName, map);

									if (cell.isLive(0)) {
										map.put(cellKey.toString(), cellValue);
									} else {
										if(!updateColumnCollectionInfo.containsKey(columnName)){
											updateColumnCollectionInfo.put(columnName,
												                                                                        columnType.getClass().getName());}
										map.put(cellKey.toString(), null);
									}
								} else {
									Map<Object, Object> map = (Map<Object, Object>) dataMap
											.get(columnName);

									if (cell.isLive(0)) {
										map.put(cellKey.toString(), cellValue);
									} else {
										if(!updateColumnCollectionInfo.containsKey(columnName)){
											                                                                                        updateColumnCollectionInfo.put(columnName,

																							                                      columnType.getClass().getName());}
										map.put(cellKey.toString(), null);
									}
								}
							}
						} else if (columnType instanceof SetType<?>) {
							// ListType<Object> listType = (ListType<Object>)
							// type;
							CellPath path = cell.path();
							int size = path.size();
							for (int i = 0; i < size; i++) {
								ByteBuffer byteBuffer = path.get(i);
								AbstractType<Object> keysType = ((SetType) columnType)
										.getElementsType();
								cellValue = keysType.compose(byteBuffer);

							}
							updateColumnCollectionInfo.put(columnName,
									columnType.getClass().getName());
							if (cell.isLive(0)) {
								if (!dataMap.containsKey(columnName)) {
									ArrayList<Object> arrayList = new ArrayList<Object>();
									arrayList.add(cellValue);
									dataMap.put(columnName, arrayList);
								} else {
									ArrayList<Object> arrayList = (ArrayList<Object>) dataMap
											.get(columnName);
									if (!arrayList.contains(cellValue)) {
										arrayList.add(cellValue);
									}
								}
							} else {

								if (!deletedDataMap.containsKey(columnName)) {
									ArrayList<Object> arrayList = new ArrayList<Object>();
									arrayList.add(cellValue);
									deletedDataMap.put(columnName, arrayList);
								} else {
									ArrayList<Object> arrayList = (ArrayList<Object>) deletedDataMap
											.get(columnName);
									if (!arrayList.contains(cellValue)) {
										arrayList.add(cellValue);
									}
								}

							}
						} else if (columnType instanceof ListType<?>) {
							// ListType<Object> listType = (ListType<Object>)
							// type;
							updateColumnCollectionInfo.put(columnName,
									columnType.getClass().getName());
							if (cell.isLive(0)) {
								if (!dataMap.containsKey(columnName)) {
									ArrayList<Object> arrayList = new ArrayList<Object>();
									arrayList.add(cellValue);
									dataMap.put(columnName, arrayList);
								} else {
									ArrayList<Object> arrayList = (ArrayList<Object>) dataMap
											.get(columnName);
									if (!arrayList.contains(cellValue)) {
										arrayList.add(cellValue);
									}
								}
							} else {

								final String finalColumnName = columnName;
								// myString.updateType = "DONE";
								final AbstractType<?> finalColumnType = columnType;

								updateLaterCollectionList.put(columnName,
										columnType);

							}
						} else {
							if (cell.isLive(0)) {
								dataMap.put(columnName, cellValue);
							} else {
								dataMap.put(columnName, null);
							}
						}

					}

					if (!myString.updateType.equalsIgnoreCase("UPDATE_COUNTER")) {
						dataMap.putAll(partitionKeyData);
						dataMap.putAll(clusterKeyData);
					}

					Iterator<ColumnDefinition> iterator = columns.iterator();
					while (iterator.hasNext()) {
						ColumnDefinition columnDefinition = (ColumnDefinition) iterator
								.next();
						String columnName = columnDefinition.name + "";
						if (!dataMap.containsKey(columnName) && !deletedDataMap.containsKey(columnName)) {
							AbstractType<?> columnType = columnDefinition.type;
							updateLaterCollectionList.put(columnName,
									columnType);
						}

					}

					Set<Entry<Object, Object>> keySet = dataMap.entrySet();

					if (updateLaterCollectionList.size() > 0) {
						// deletedata wala handling
						// shouldRun = false;
						setUpdateForLater(updateLaterCollectionList,
								indexColumnFamily, partitionKeyData,
								clusterKeyData, index, type, esId);

					}
				}
				currentDataMap = dataMap;
				currentEsId = esId;
				currentIndex = index;
				currentType = type;
				currentMyString = myString;
				currentUpdateColumnCollectionInfo = updateColumnCollectionInfo;

				es = new ElasticSearch(index);
				String routing = getRoutingFromEs(currentEsId, currentIndex,
						partitionKeyData, clusterKeyData);
				boolean shouldRefresh=false;
				if (myString.updateType.equalsIgnoreCase("UPDATE_COUNTER")) {
					es.updateCounterFieldsInDocument(index, type, esId,
							routing, dataMap);
					shouldRun = true;
				} else if (myString.updateType.equalsIgnoreCase("UPDATE_ROW")) {
					es.updateFieldsInDocument(index, type, esId, routing,
							dataMap, updateColumnCollectionInfo, deletedDataMap);
					shouldRun = true;
					shouldRefresh = true;
				} else if (myString.updateType.equalsIgnoreCase("DELETE_ROW")) {
					es.deleteDocument(index, type, esId, routing);
					shouldRun = true;
				}
				if (shouldRun) {
					es.executeBulk();
				}
				if(shouldRefresh){es.refreshEs(index);}
			}
		} catch (RuntimeException e) {

			if (es != null) {
				es.close();
			}
			logger.info("CAUTION : TRIGGER FAILURE : " + e.getMessage());
			logger.error("exception : ", e);
			try {
				ElasticQueue.queueMessage(currentEsId, currentType,
						currentIndex, currentMyString.updateType,
						currentDataMap, currentUpdateColumnCollectionInfo,
						logger);
			} catch (Exception e1) {
				logger.info("Rabbit Error BIG CAUTION : " + e1.getMessage());
			}
		}

		return null;
	}

	private void setUpdateForLater(final Map<String, Object> hashMap,
			final String indexColumnFamily,
			final Map<String, Object> tempPartitionKeyData,
			final Map<String, Object> tempClusterKeyData, final String index,
			final String type, final String esId) {
		java.util.Timer timer = new java.util.Timer();

		timer.schedule(new TimerTask() {

			@Override
			public void run() {

				CassandraRiver
						.deleteData(
								indexKeySpace,
								hashMap,
								indexColumnFamily,
								tempPartitionKeyData,
								tempClusterKeyData,
								new org.apache.cassandra.triggers.CassandraRiver.DeleteDataListener() {

									public void onDataObtained(
											com.datastax.driver.core.ResultSet resultSet) {
										ElasticSearch tempEs = new ElasticSearch(index);
										HashMap<Object, String> typeData = new HashMap<Object, String>();
										HashMap<Object, Object> data = new HashMap<Object, Object>();
										try {

											logger.info("async trigger callback");
											Iterator<com.datastax.driver.core.Row> result = resultSet
													.iterator();

											int temindex = 0;
											while (result.hasNext()) {

												temindex++;
												com.datastax.driver.core.Row row = result
														.next();
												com.datastax.driver.core.ColumnDefinitions columnDefinitions = row
														.getColumnDefinitions();

												for (int i = 0; i < columnDefinitions
														.size(); i++) {
													String columnName = columnDefinitions
															.getName(i);
													Object rowObject = row
															.getObject(i);
													Object columnType = hashMap
															.get(columnName);

													if (columnType instanceof MapType<?, ?>) {
														Map<String, Object> finalObject = new HashMap<String, Object>();
														Map<Object, Object> map = (Map<Object, Object>) rowObject;
														Set<Entry<Object, Object>> keySet = map
																.entrySet();

														for (Entry<Object, Object> entry : keySet) {
															finalObject
																	.put(entry
																			.getKey()
																			.toString(),
																			entry.getValue());
														}
														if(finalObject.size()==0){data.put(columnName,null);}else{
														data.put(columnName,
																finalObject);}

													} else {
														data.put(
																columnName,
																row.getObject(i));
													}

													typeData.put(columnName,
															columnType
																	.getClass()
																	.getName());
												}
											}
											String routing = getRoutingFromEs(
													esId, index,
													tempPartitionKeyData,
													tempClusterKeyData);
											data.putAll(tempPartitionKeyData);
											data.putAll(tempClusterKeyData);
											tempEs.fullFieldUpdateInDocument(
													index, type, esId, routing,
													data, typeData);

											tempEs.executeBulk();
										} catch (RuntimeException e) {
											if (tempEs != null) {
												tempEs.close();
											}

											logger.info("CAUTION : TRIGGER FAILURE : In delete "
													+ e.getMessage());
											logger.error("exception : ", e);
											try {
												ElasticQueue.queueMessage(esId,
														type, index,
														"DELETE_FIELDS", data,
														typeData, logger);
											} catch (Exception e1) {
												logger.info("Rabbit Error BIG CAUTION In delete : "
														+ e1.getMessage());
											}
										}
									}
								});

			}
		}, 2000);
	}

	public static Properties loadProperties() {
		Properties properties = new Properties();
		InputStream stream = InvertedIndex.class.getClassLoader()
				.getResourceAsStream("InvertedIndex.properties");
		try {
			properties.load(stream);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			FileUtils.closeQuietly(stream);
		}

		return properties;
	}

}
