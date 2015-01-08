package com.vitter.riak.jRadish;

import static org.junit.Assert.*;
import java.util.ArrayList;
import org.junit.Test;

public class ClientTest {

	@Test
	public void testSet() {
		Client client = new Client();
		assertEquals(true, client.set("user:craigvitter:id", "craigvitter") );
	}

	@Test
	public void testGet() {
		Client client = new Client();
		assertEquals("craigvitter", client.get("user:craigvitter:id") );
	}
	
	@Test
	public void testDeleteString() {
		Client client = new Client();
		client.set("user:todelete", "deleteme");
		assertEquals(true, client.deleteString("user:todelete") );
	}
	
	@Test
	public void testIncrementCounter() {
		Client client = new Client();
		assertEquals(true, client.incrementCounter("craigscounter", 2L) );
	}
	
	@Test
	public void testGetCounter() {
		Client client = new Client();
		try
		{
			Long result = client.getCounter("craigscounter");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	
	@Test
	public void testAddToSet() {
		Client client = new Client();
		ArrayList<String> test = new ArrayList<String>();
		test.add("Chicago");
		test.add("New York");
		assertEquals(true, client.addToSet("cities", test) );
	}
	
	@Test
	public void testRemoveFromSet() {
		Client client = new Client();
		ArrayList<String> addTo = new ArrayList<String>();
		addTo.add("val1");
		addTo.add("val2"); 
		client.addToSet("removefrom", addTo);
		assertEquals(true, client.removeFromSet("removefrom", addTo) );
	}
	
	
	@Test
	public void testGetEmptySet() {
		Client client = new Client();
		assertEquals(null, client.getSet("emptyset") );
	}
	

	@Test
	public void testDeleteCounter() {
		Client client = new Client();
		client.incrementCounter("countertodelete", 2L);
		assertEquals(true, client.deleteCounter("countertodelete") );
	}

}
