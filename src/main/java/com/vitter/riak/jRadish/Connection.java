package com.vitter.riak.jRadish;

import java.io.File;
import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;

public class Connection {
	
	private String[] nodesArray = {"127.0.0.1"};
	private String stringBucketType = "jradish";
	private String stringBucket = "string";
	private String counterBucketType = "jradish-counter";
	private String counterBucket = "counter";
	private String setBucketType = "jradish-set";
	private String setBucket = "set";
	private String mapBucketType = "jradish-map";
	private String mapBucket = "hash";
	private int rvalue = 2;
	private int wvalue = 2;
	private int readRetry = 1;
	private RiakClient riakClient;
	private RiakCluster riakCluster;
	
	
	public Connection() {
		loadProperties();
		connect();
	}
	
	
	public void cleanup()
	{
		try {
			riakCluster.shutdown();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	private void connect() {
		final RiakNode.Builder builder = new RiakNode.Builder();
        List<RiakNode> nodes;
		try {
			nodes = RiakNode.Builder.buildNodes(builder, Arrays.asList(nodesArray));
			riakCluster = new RiakCluster.Builder(nodes).build();
	        riakCluster.start();
	        riakClient = new RiakClient(riakCluster);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	
	private boolean loadProperties() {
		Properties defaultProps = new Properties();
		try
		{
			File f = getPropertiesFile();
			if(f.exists() && !f.isDirectory()) {
				FileInputStream propertyFile = new FileInputStream(f);
				defaultProps.load(propertyFile);
				propertyFile.close();
				
				nodesArray = defaultProps.getProperty("NODES", "127.0.0.1").split(",");
				stringBucketType = defaultProps.getProperty("STRING_BUCKET_TYPE","jradish");
				stringBucket = defaultProps.getProperty("STRING_BUCKET","string");
				counterBucketType = defaultProps.getProperty("COUNTER_BUCKET_TYPE","jradish-counter");
				counterBucket = defaultProps.getProperty("COUNTER_BUCKET","counter");
				setBucketType = defaultProps.getProperty("SET_BUCKET_TYPE","jradish-set");
				setBucket = defaultProps.getProperty("SET_BUCKET","set");
				mapBucketType = defaultProps.getProperty("MAP_BUCKET_TYPE","jradish-map");
				mapBucket = defaultProps.getProperty("MAP_BUCKET","map");
				rvalue = Integer.parseInt( defaultProps.getProperty("R_VALUE", "2") );
				wvalue = Integer.parseInt( defaultProps.getProperty("W_VALUE", "2") );
				readRetry = Integer.parseInt( defaultProps.getProperty("READ_RETRY", "5") );
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	
	private File getPropertiesFile() {
		File f = new File("target/riak.properties");
		if(f.exists() && !f.isDirectory()) {
			return f;
		}
		else {
			URL url = Connection.class.getProtectionDomain().getCodeSource().getLocation();
			String jarPath = null;
			try {
				jarPath = URLDecoder.decode(url.getFile(), "UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				return null;
			}
			f = new File(jarPath + "riak.properties");
			return f;
		}
	}

	
	// Public Getters for the Connection Class

	public String getStringBucketType() {
		return stringBucketType;
	}

	public String getStringBucket() {
		return stringBucket;
	}
	
	public String getCounterBucketType() {
		return counterBucketType;
	}

	public String getCounterBucket() {
		return counterBucket;
	}

	public String getSetBucketType() {
		return setBucketType;
	}

	public String getSetBucket() {
		return setBucket;
	}
	
	public String getMapBucketType() {
		return mapBucketType;
	}

	public String getMapBucket() {
		return mapBucket;
	}
	
	public int getRValue() {
		return rvalue;
	}
	
	public int getWValue() {
		return wvalue;
	}
	
	public int getReadRetry() {
		return readRetry;
	}

	public RiakClient getRiakClient() {
		return riakClient;
	}

	public RiakCluster getRiakCluster() {
		return riakCluster;
	}

}
