package org.apache.cassandra.triggers;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;

/**
 * User: bsha Date: 09.07.2014 Time: 14:34
 */
public class CassandraFactory {
	private static Cluster cluster;
	private Map<String, Session> sessions = new HashMap<String, Session>();

	private static final Logger LOGGER = LoggerFactory
			.getLogger(CassandraFactory.class);

	private static Properties properties = InvertedIndex.loadProperties();
	static String indexKeySpace = properties.getProperty("keyspace");
	static String hosts = properties.getProperty("hosts");
	static String port = properties.getProperty("port");
	static String datacenter = properties.getProperty("datacenter");
	static String username = properties.getProperty("username");
	static String password = properties.getProperty("password");
	static String clusterName = properties.getProperty("clusterName");
	int connectTimeoutMillis=10000;
	int readTimeoutMillis=10000;

	private CassandraFactory() {
		PoolingOptions poolingOptions = new PoolingOptions();

		poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 8);
		poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 50);

		SocketOptions socketOptions = new SocketOptions().setReadTimeoutMillis(
				readTimeoutMillis)
				.setConnectTimeoutMillis(connectTimeoutMillis);

		Cluster.Builder builder = Cluster.builder().addContactPoints(hosts)
				.withPoolingOptions(poolingOptions)
				.withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
				.withSocketOptions(socketOptions);
		
		if (!username.isEmpty() && !password.isEmpty()) {
			builder.withCredentials(username, password);
		}
		cluster = builder.build();
	}

	public static CassandraFactory getInstance() {
		return new CassandraFactory();
	}

	public Session getSession(String keySpaceName) {
		Session session;
		if (this.sessions.containsKey(keySpaceName)) {
			session = this.sessions.get(keySpaceName);
		} else {
			try{
			session = cluster.connect(keySpaceName);
			}catch(IllegalStateException e){
				getInstance();
				session = cluster.connect(keySpaceName);
			}catch(NoHostAvailableException e){
				getInstance();
				session = cluster.connect(keySpaceName);
			}
			LOGGER.info(String.format("Connection established to %s!",
					keySpaceName));
			this.sessions.put(keySpaceName, session);
		}
		return session;
	}

	public void shutdown() {
		if (cluster != null && !cluster.isClosed()) {
			cluster.close();
			LOGGER.info("Cluster shutting down!!");
		}
	}

}
