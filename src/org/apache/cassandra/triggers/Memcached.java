package org.apache.cassandra.triggers;

import java.net.InetSocketAddress;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.spy.memcached.MemcachedClient;

public class Memcached {
	private static MemcachedClient mcc;
	private static final Logger logger = LoggerFactory.getLogger(Memcached.class);

	public static Object getValue(String cacheKey) {
		if(Constants.MEMCACHE_URL==null||Constants.MEMCACHE_URL.length()==0){
			return null;
		}
		try{
		if(mcc==null){
			mcc = new MemcachedClient(new InetSocketAddress(Constants.MEMCACHE_URL, Constants.MEMCACHE_PORT));
		}
			Object fo = mcc.get(cacheKey);
			return fo;
		}catch(Exception e){
			mcc=null;
			logger.error("Exception in Memcache : ",e);
		}
		return null;
	}
}
