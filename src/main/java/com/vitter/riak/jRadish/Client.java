package com.vitter.riak.jRadish;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.datatypes.CounterUpdate;
import com.basho.riak.client.api.commands.datatypes.FetchCounter;
import com.basho.riak.client.api.commands.datatypes.FetchSet;
import com.basho.riak.client.api.commands.datatypes.UpdateCounter;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.StoreValue.Option;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.crdt.types.RiakCounter;
import com.basho.riak.client.core.util.BinaryValue;

public class Client {
	
	private String[] nodesArray = {"127.0.0.1"};
	private String stringBucketType = "jradish";
	private String stringBucket = "string";
	private String mapBucketType = "jradish-map";
	private String mapBucket = "hash";
	private String counterBucketType = "jradish-counter";
	private String counterBucket = "counter";
	private String setBucketType = "jradish-set";
	private String setBucket = "set";
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
				Location location = new Location(new Namespace(stringBucketType, stringBucket), key);
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
				Location location = new Location(new Namespace(stringBucketType, stringBucket), key);
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
	 * 
	 * @param key
	 * @param increment
	 * @return
	 */
	public boolean incrementCounter(String key, Long increment) {
		if (key != null) {
			connect();
			try {
				Location location = new Location(new Namespace(counterBucketType, counterBucket), key);
				CounterUpdate cu = new CounterUpdate(increment);
				UpdateCounter update = new UpdateCounter
						.Builder(location, cu)
		        	.build();
				riakClient.execute(update);
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
	 * 
	 * @param key
	 * @return
	 */
	public Long getCounter(String key) {
		if (key != null) {
			connect();
			try {
				Location location = new Location(new Namespace(counterBucketType, counterBucket), key);
				FetchCounter fetch = new FetchCounter
					.Builder(location)
		        	.build();
				FetchCounter.Response response = riakClient.execute(fetch);
				RiakCounter counter = response.getDatatype();
				return counter.view();
			}
			catch (Exception e) {
				e.printStackTrace();
				throw null;
			}
			finally {
				closeConnection();
			}
		}
		else {
			throw null;
		}
	}
	
	
	/**
	 * 
	 * @param key
	 * @param values
	 * @return
	 */
	public boolean addToSet(String key, ArrayList<String> values) {
		if (key != null && values.size() > 0) {
			
		}
		return false;
	}
	
	
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public ArrayList<String> getSet(String key) {
		if (key != null) {
			connect();
			try {
				Location location = new Location(new Namespace(setBucketType, setBucket), key);
				FetchSet fetch = new FetchSet
					.Builder(location)
		        	.build();
				FetchSet.Response response = riakClient.execute(fetch);
				Set<BinaryValue> set = response.getDatatype().view();
				if (set.isEmpty()) return null;
				ArrayList<String> returnSet = new ArrayList<String>();
				for (BinaryValue member : set) {
					returnSet.add(member.toString());
				}
				return returnSet;
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
	
	
	
	public boolean deleteString(String key) {
		return delete(stringBucketType, stringBucket, key);
	}
	
	public boolean deleteCounter(String key) {
		return delete(counterBucketType, counterBucket, key);
	}
	
	public boolean deleteSet(String key) {
		return delete(setBucketType, setBucket, key);
	}
	
	public boolean deleteMap(String key) {
		return delete(mapBucketType, mapBucket, key);
	}
	
	private boolean delete(String type, String bucket, String key) {
		if (key != null) {
			connect();
			try {
				final Location location = new Location(new Namespace(type, bucket), key);
				final DeleteValue dv = new DeleteValue.Builder(location).build();
				riakClient.execute(dv);
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			} finally {
				closeConnection();
			}
			return true;
		} else {
			return false;
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
			nodes = RiakNode.Builder.buildNodes(builder, Arrays.asList(nodesArray));
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
