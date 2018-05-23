package org.apache.cassandra.triggers;

import java.net.InetSocketAddress;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.lambdaworks.redis.*;
import com.lambdaworks.redis.api.*;
import com.lambdaworks.redis.api.sync.*;
import com.lambdaworks.redis.api.sync.RedisStringCommands;

public class Memcached {
	private static RedisClient mcc;
	private static final Logger logger = LoggerFactory.getLogger(Memcached.class);

	private static RedisStringCommands sync;
	private static StatefulRedisConnection<String, String> connection;
	public static Object getValue(String cacheKey) {
		if(Constants.MEMCACHE_URL==null||Constants.MEMCACHE_URL.length()==0){
			return null;
		}
		try{
		if(mcc==null){
			mcc = new RedisClient(RedisURI.create("redis://"+Constants.MEMCACHE_URL+":"+ Constants.MEMCACHE_PORT));
			connection = mcc.connect();
			sync = connection.sync();
		}
		Object fo = sync.get(cacheKey);
		logger.info("cache key : "+cacheKey+", value : "+fo);
		return fo;
		}catch(Exception e){
			mcc=null;
			logger.error("Exception in Memcache : ",e);
		}
		return null;
	}
}
