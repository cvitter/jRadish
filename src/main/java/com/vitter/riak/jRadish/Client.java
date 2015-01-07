package com.vitter.riak.jRadish;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.StoreValue.Option;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.util.BinaryValue;

public class Client {
	
	private String[] NODES_ARRAY = {"127.0.0.1"};
	private String BUCKET_TYPE = "jradish";
	private String BUCKET = "kv";
	private String MAP_BUCKET_TYPE = "jradish-map";
	private String MAP_BUCKET = "hash";
	private int R_VALUE = 2;
	private int W_VALUE = 2;
	private int READ_RETRY_COUNT = 5;
	private RiakClient riakClient;
	private RiakCluster riakCluster;
	
	

	/**
	 * set - stores a string under the specified key
	 * @param key
	 * @param value
	 * @return true if item is successfully saved
	 */
	public boolean set(String key, String value) {
		if (key != null && value != null) {
			connect();
			try {
				Location location = new Location(new Namespace(BUCKET_TYPE, BUCKET), key);
				RiakObject object = new RiakObject()
					.setContentType("text/plain")
					.setValue(BinaryValue.create(value));
				StoreValue store = new StoreValue
					.Builder(object)
					.withLocation(location)
            		.withOption(Option.W, new Quorum(W_VALUE))
            		.build();
				riakClient.execute(store);
			}
			catch (Exception e) {
				e.printStackTrace();
				return false;
			}
			finally {
				closeConnection();
			}
			return true;
		}
		else {
			return false;
		}
	}
	
	
	/**
	 * get - returns a string stored under the specified key
	 * @param key
	 * @return
	 */
	public String get(String key) {
		if (key != null) {
			connect();
			try {
				Location location = new Location(new Namespace(BUCKET_TYPE, BUCKET), key);
				final FetchValue fv = new FetchValue.Builder(location)
            		.withOption(FetchValue.Option.R, new Quorum(R_VALUE))
            		.build();
				final FetchValue.Response response = fetch(fv);
	            final RiakObject obj = response.getValue(RiakObject.class);
	            return obj.getValue().toString();
			}
			catch (Exception e) {
				e.printStackTrace();
				return null;
			}
			finally {
				closeConnection();
			}
		}
		else {
			return null;
		}
	}
	
	
	
	/**
	 * fetch
	 * @param fv
	 * @return FetchValue.Response
	 */
	private FetchValue.Response fetch(FetchValue fv) {
		try {
			FetchValue.Response response = null;
			for (int i = 0; i < READ_RETRY_COUNT; i++) {
				response = riakClient.execute(fv);
				if (response.isNotFound() == false) break;
			}
			return response;
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	
	
	
	private void connect() {
		final RiakNode.Builder builder = new RiakNode.Builder();
        List<RiakNode> nodes;
		try {
			nodes = RiakNode.Builder.buildNodes(builder, Arrays.asList(NODES_ARRAY));
			riakCluster = new RiakCluster.Builder(nodes).build();
	        riakCluster.start();
	        riakClient = new RiakClient(riakCluster);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public void closeConnection()
	{
		try {
			riakCluster.shutdown();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
