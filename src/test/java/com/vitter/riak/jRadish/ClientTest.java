package com.vitter.riak.jRadish;

import static org.junit.Assert.*;
import java.util.ArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClientTest {
	
	Client client;
	
	@Before
	public void setUp() throws Exception {
		client = new Client();
	}
	
	@After
	public void tearDown() throws Exception {
		client.closeConnection();
	}

	@Test
	public void testSetString() {
		assertEquals(true, client.set("user:craigvitter:id", "craigvitter") );
	}

	@Test
	public void testGetString() {
		assertEquals("craigvitter", client.get("user:craigvitter:id") );
	}
	
	@Test
	public void testDeleteString() {
		client.set("user:todelete", "deleteme");
		assertEquals(true, client.deleteString("user:todelete") );
	}
	
	@Test
	public void testIncrementCounter() {
		assertEquals(true, client.incrementCounter("craigscounter", 2L) );
	}
	
	@Test
	public void testGetCounter() {
		try
		{
			client.getCounter("craigscounter");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	
	@Test
	public void testDeleteCounter() {
		client.incrementCounter("countertodelete", 2L);
		assertEquals(true, client.deleteCounter("countertodelete") );
	}

	
	@Test
	public void testAddToSet() {
		ArrayList<String> test = new ArrayList<String>();
		test.add("Chicago");
		test.add("New York");
		assertEquals(true, client.addToSet("cities", test) );
	}
	
	@Test
	public void testRemoveFromSet() {
		ArrayList<String> addTo = new ArrayList<String>();
		addTo.add("val1");
		addTo.add("val2"); 
		client.addToSet("removefrom", addTo);
		assertEquals(true, client.removeFromSet("removefrom", addTo) );
	}
	
	
	@Test
	public void testGetEmptySet() {
		assertEquals(null, client.getSet("emptyset") );
	}
	
	
	@Test
	public void testDeleteSet() {
		ArrayList<String> test = new ArrayList<String>();
		test.add("Chicago");
		test.add("New York");
		client.addToSet("settodelete", test);
		assertEquals(true, client.deleteSet("settodelete") );
	}
	



}
