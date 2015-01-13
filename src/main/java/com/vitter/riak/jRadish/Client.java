package com.vitter.riak.jRadish;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.datatypes.CounterUpdate;
import com.basho.riak.client.api.commands.datatypes.FetchCounter;
import com.basho.riak.client.api.commands.datatypes.FetchMap;
import com.basho.riak.client.api.commands.datatypes.FetchSet;
import com.basho.riak.client.api.commands.datatypes.FlagUpdate;
import com.basho.riak.client.api.commands.datatypes.MapUpdate;
import com.basho.riak.client.api.commands.datatypes.RegisterUpdate;
import com.basho.riak.client.api.commands.datatypes.SetUpdate;
import com.basho.riak.client.api.commands.datatypes.UpdateCounter;
import com.basho.riak.client.api.commands.datatypes.UpdateMap;
import com.basho.riak.client.api.commands.datatypes.UpdateSet;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.StoreValue.Option;
import com.basho.riak.client.api.commands.datatypes.Context;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.crdt.types.RiakCounter;
import com.basho.riak.client.core.query.crdt.types.RiakMap;
import com.basho.riak.client.core.query.crdt.types.RiakRegister;
import com.basho.riak.client.core.util.BinaryValue;


public class Client {

	private Connection conn;
	
	public Client() {
		conn = new Connection();
	}


	/**
	 * set - stores a string under the specified key
	 * @param key
	 * @param value
	 * @return true if item is successfully saved
	 */
	public boolean set(String key, String value) {
		if (key != null && value != null) {
			try {
				Location location = new Location(new Namespace(conn.getStringBucketType(), conn.getStringBucket()), key);
				RiakObject object = new RiakObject()
					.setContentType("text/plain")
					.setValue(BinaryValue.create(value));
				StoreValue store = new StoreValue
					.Builder(object)
					.withLocation(location)
            		.withOption(Option.W, new Quorum(conn.getWValue()))
            		.build();
				conn.getRiakClient().execute(store);
			}
			catch (Exception e) {
				e.printStackTrace();
				return false;
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
	 * @return string
	 */
	public String get(String key) {
		if (key != null) {
			try {
				Location location = new Location(new Namespace(conn.getStringBucketType(), conn.getStringBucket()), key);
				final FetchValue fv = new FetchValue.Builder(location)
            		.withOption(FetchValue.Option.R, new Quorum(conn.getRValue()))
            		.build();
				final FetchValue.Response response = fetch(fv);
	            final RiakObject obj = response.getValue(RiakObject.class);
	            return obj.getValue().toString();
			}
			catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}
		else {
			return null;
		}
	}
	
	
	/**
	 * incrementCounter - increments specified counter
	 * @param key
	 * @param increment
	 * @return true if successful
	 */
	public boolean incrementCounter(String key, Long increment) {
		if (key != null) {
			try {
				Location location = new Location(new Namespace(conn.getCounterBucketType(), conn.getCounterBucket()), key);
				CounterUpdate cu = new CounterUpdate(increment);
				UpdateCounter update = new UpdateCounter
						.Builder(location, cu)
		        	.build();
				conn.getRiakClient().execute(update);
			}
			catch (Exception e) {
				e.printStackTrace();
				return false;
			}
			return true;
		}
		else {
			return false;
		}
	}
	
	
	/**
	 * getCounter - retrieves value of a counter at a specified key
	 * @param key
	 * @return value of counter as a long if the key exists or null
	 */
	public Long getCounter(String key) {
		if (key != null) {
			try {
				Location location = new Location(new Namespace(conn.getCounterBucketType(), conn.getCounterBucket()), key);
				
				// Check if counter exists first, throw null if not since FetchCounter
				// will return 0 if the key doesn't exist
				FetchValue fv = new FetchValue.Builder(location).build();
				FetchValue.Response keyResponse = fetch(fv);
	            if (keyResponse.isNotFound()) throw null;
				
				FetchCounter fetch = new FetchCounter
					.Builder(location)
		        	.build();
				FetchCounter.Response response = conn.getRiakClient().execute(fetch);
				RiakCounter counter = response.getDatatype();
				return counter.view();
			}
			catch (Exception e) {
				e.printStackTrace();
				throw null;
			}
		}
		else {
			throw null;
		}
	}
	
	
	/**
	 * addToSet - Add the contents of an ArrayList to the specified set
	 * @param key
	 * @param values
	 * @return true if successful
	 */
	public boolean addToSet(String key, ArrayList<String> values) {
		return setOperations( key, values, true );
	}

	/**
	 * removeFromSet - Remove the contents of an ArrayList from the specified set
	 * @param key
	 * @param values
	 * @return true if successful
	 */
	public boolean removeFromSet(String key, ArrayList<String> values) {
		return setOperations( key, values, false );
	}
	
	private boolean setOperations(String key, ArrayList<String> values, boolean add) {
		if (key != null && values.size() > 0) {
			try {
				Location location = new Location(new Namespace(conn.getSetBucketType(), conn.getSetBucket()), key);
				SetUpdate su = new SetUpdate();
				for (String value : values) {
					if (add) {
						su.add(value);
					}
					else {
						su.remove(value);
					}
				}
				
				Context ctx = null;
				if (!add) {
					FetchSet fetch = new FetchSet
						.Builder(location)
			        	.build();
					FetchSet.Response response = conn.getRiakClient().execute(fetch);
					ctx = response.getContext();
					if (response.getDatatype().view().isEmpty()) return false;
				}
				
				UpdateSet update = null;
				if (ctx != null) {
					update = new UpdateSet
						.Builder(location, su)
						.withContext(ctx)
						.build();
				}
				else
				{
					update = new UpdateSet
						.Builder(location, su)
						.build();
				}
				
				conn.getRiakClient().execute(update);
				return true;
			}
			catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		return false;
	}	
	
	
	/**
	 * getSet - Returns the contents of a set as an ArrayList
	 * @param key
	 * @return ArrayList<String>
	 */
	public ArrayList<String> getSet(String key) {
		if (key != null) {
			try {
				Location location = new Location(new Namespace(conn.getSetBucketType(), conn.getStringBucket()), key);
				FetchSet fetch = new FetchSet
					.Builder(location)
		        	.build();
				FetchSet.Response response = conn.getRiakClient().execute(fetch);
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
		}
		else {
			return null;
		}
	}
	
	
	
	/**
	 * mapAddUpdateRegisters - Adds or updates registers in the specified map
	 * @param key
	 * @param values
	 * @return true if successful
	 */
	public boolean mapAddUpdateRegisters(String key, Map<String,String> values) {
		if (key != null && values.size() > 0) {
			try {
				Location location = new Location(new Namespace(conn.getMapBucketType(), conn.getMapBucket()), key);
				MapUpdate mu = new MapUpdate();
				for (Map.Entry<String, String> entry : values.entrySet()) {
					mu.update(entry.getKey(), new RegisterUpdate(entry.getValue()));
				}
		        UpdateMap update = new UpdateMap
		        	.Builder(location, mu)
		        	.build();
		        conn.getRiakClient().execute(update);
				return true;
			}
			catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		return false;
	}
	
	
	/**
	 * getRegisters - Get one or more register from a map
	 * @param key
	 * @param registers
	 * @return Map<String, String> containing registers and their values
	 */
	public Map<String, String> mapGetRegisters(String key, ArrayList<String> registers) {
		if (key != null && registers.size() > 0) {
			Map<String, String> returnVal = new HashMap<String, String>();

			Location location = new Location(new Namespace(conn.getMapBucketType(), conn.getMapBucket()), key);
			FetchMap fetch = new FetchMap.Builder(location).build();
			FetchMap.Response response;
			try {
				response = conn.getRiakClient().execute(fetch);
				RiakMap map = response.getDatatype();
				for (String register : registers)
				{
					returnVal.put(register, map.getRegister(register).toString());
				}
				return returnVal;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		} else {
			return null;
		}
	}
	
	
	/**
	 * mapRemoveRegisters - Remove one or more register from a map
	 * @param key
	 * @param registers
	 * @return true if successful
	 */
	public boolean mapRemoveRegisters(String key, ArrayList<String> registers) {
		if (key != null && registers.size() > 0) {
			Location location = new Location(new Namespace(conn.getMapBucketType(), conn.getMapBucket()), key);
			FetchMap fetch = new FetchMap
				.Builder(location)
	        	.build();
			try {
				FetchMap.Response response = conn.getRiakClient().execute(fetch);
				Context ctx = response.getContext();
				MapUpdate mapUpdate = new MapUpdate();
				for (String register : registers) {
					mapUpdate.removeRegister(register);
				}
				UpdateMap update = new UpdateMap
					.Builder(location, mapUpdate)
		        	.withContext(ctx)
		        	.build();
				conn.getRiakClient().execute(update);
				return true;
			}
			catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		else {		
			return false;
		}
	}
	
	
	/**
	 * mapAddUpdateFlags - 
	 * @param key
	 * @param values
	 * @return
	 */
	public boolean mapAddUpdateFlags(String key, Map<String,Boolean> values) {
		if (key != null && values.size() > 0) {
			try {
				Location location = new Location(new Namespace(conn.getMapBucketType(), conn.getMapBucket()), key);
				MapUpdate mu = new MapUpdate();
				for (Map.Entry<String, Boolean> entry : values.entrySet()) {
					mu.update(entry.getKey(), new FlagUpdate(entry.getValue()));
				}
		        UpdateMap update = new UpdateMap
		        	.Builder(location, mu)
		        	.build();
		        conn.getRiakClient().execute(update);
				return true;
			}
			catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		return false;
	}
	
	
	/**
	 * mapGetFlags - 
	 * @param key
	 * @param flags
	 * @return
	 */
	public Map<String, Boolean> mapGetFlags(String key, ArrayList<String> flags) {
		if (key != null && flags.size() > 0) {
			Map<String, Boolean> returnVal = new HashMap<String, Boolean>();

			Location location = new Location(new Namespace(conn.getMapBucketType(), conn.getMapBucket()), key);
			FetchMap fetch = new FetchMap.Builder(location).build();
			FetchMap.Response response;
			try {
				response = conn.getRiakClient().execute(fetch);
				RiakMap map = response.getDatatype();
				for (String flag : flags)
				{
					if (map.getFlag(flag) != null) {
						boolean val = map.getFlag(flag) != null;
						returnVal.put(flag, val);
					}
				}
				return returnVal;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		} else {
			return null;
		}
	}
	
	
	
	
	/**
	 * deleteString - Deletes the value found at the specified key
	 * @param key
	 * @return true if successful
	 */
	public boolean deleteString(String key) {
		return delete(conn.getSetBucketType(), conn.getStringBucket(), key);
	}
	
	/**
	 * deleteCounter - Deletes the value found at the specified key
	 * @param key
	 * @return true if successful
	 */
	public boolean deleteCounter(String key) {
		return delete(conn.getCounterBucketType(), conn.getCounterBucket(), key);
	}
	
	/**
	 * deleteSet - Deletes the value found at the specified key
	 * @param key
	 * @return true if successful
	 */
	public boolean deleteSet(String key) {
		return delete(conn.getSetBucketType(), conn.getSetBucket(), key);
	}
	
	/**
	 * deleteMap - Deletes the value found at the specified key
	 * @param key
	 * @return true if successful
	 */
	public boolean deleteMap(String key) {
		return delete(conn.getMapBucketType(), conn.getMapBucket(), key);
	}
	
	private boolean delete(String type, String bucket, String key) {
		if (key != null) {
			try {
				final Location location = new Location(new Namespace(type, bucket), key);
				final DeleteValue dv = new DeleteValue.Builder(location).build();
				conn.getRiakClient().execute(dv);
			} 
			catch (Exception e) {
				e.printStackTrace();
				return false;
			}
			return true;
		} else {
			return false;
		}
	}
	
	
	

	private FetchValue.Response fetch(FetchValue fv) {
		try {
			FetchValue.Response response = null;
			for (int i = 0; i < conn.getReadRetry(); i++) {
				response = conn.getRiakClient().execute(fv);
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

	
	public void closeConnection()
	{
		conn.cleanup();
	}

}
