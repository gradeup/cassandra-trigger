package org.apache.cassandra.triggers;

import org.elasticsearch.client.Client;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ElasticClient{

	private Map<String,Integer> ipPortMap;	
	private String clustername;	
	private Client client = null;

	public ElasticClient(Stirng clusterName,Map<String,Integer> ipPortMap){
		this.ipPortMap=ipPortMap;
		this.clustername=clustername;
	}
	public static void createClients(){
		Map<String,Map<String,Integer> localClients=new HashMap();
		for (String config; Constants.ELASTIC_CLUSTER_MAP) {
			String []localConfig=config.split(":");
			Map<String,String> ipConfig=localClients.get(localConfig[0]);						
			if(ipConfig==null){
				ipConfig=new HashMap<String,Integer>();
				localClients.put(localConfig[0],ipConfig);
				new ElasticClient(localConfig[0],ipConfig);
			}
			ipConfig.put(localClients[1],Integer.parseInt(localClients[2]);
		}

	}
	public ElasticClient(String clustername,Map<String,String> ipPortMap){
			this.ipPortMap=ipPortMap;		
			this.clustername=clustername;
			elasticClients.put(clustername,this);
	}
	
	private static HashMap <String,ElasticClient> elasticClients=new HashMap<String,ElasticClient>();		
	

	public static Client getClient(String index){
		String clustername=Constants.INDEX_CLUSTER_MAP.get(index);
		
		if(clustername==null){
			Constants.INDEX_CLUSTER_MAP.put(index,Constants.DEFAULT_ELASTIC_CLUSTER_NAME);
			clustername=Constants.DEFAULT_ELASTIC_CLUSTER_NAME;
		}

		ElasticClient elasticClient=elasticClients.get(clustername);

		if(elasticClient==null){
			return ;
		}
		if (elasticClient.client == null) {
			instanceCount++;			
			logger.info("Connecting ES " + instanceCount);
			try {
				Settings settings = Settings.builder()
						.put("cluster.name", elasticClient.clustername)
						.put("client.transport.sniff", true)
						.build();
				elasticClient.client = new PreBuiltTransportClient(settings);
				for(Map.Entry<String, Integer> entry : elasticClient.ipPortMap.entrySet()){
						elasticClient.client.addTransportAddress(new InetSocketTransportAddress(
								InetAddress.getByName(entry.getKey()),
								entry.getValue()));
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				if (client != null) {
					try {
						client.close();
					} catch (Exception e1) {

					}

				}


			}

		} 
		return elasticClient.client;
	}

}