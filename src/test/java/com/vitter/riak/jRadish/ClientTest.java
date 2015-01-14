package com.vitter.riak.jRadish;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
	

	@Test
	public void testMapAddUpdateRegisters() {
		Map<String, Object> map = new HashMap<String,Object>();
		map.put("username", "testuser");
		map.put("email", "test@user.email");
		assertEquals(true, client.mapModifyRegisters("myawesomemap", map) );
	}

	@Test
	public void testGetRegisters() {
		ArrayList<String> test = new ArrayList<String>();
		test.add("username");
		test.add("email");
		assertEquals(2, client.mapGetRegisters("myawesomemap", test).size());
	}
	
	@Test
	public void testRemoveRegisters() {
		Map<String, Object> add = new HashMap<String,Object>();
		add.put("reg1", "1");
		add.put("reg2", "2");
		client.mapModifyRegisters("removeregisters", add);
		
		ArrayList<String> remove = new ArrayList<String>();
		remove.add("reg1");
		remove.add("reg2");
		assertEquals(true, client.mapRemoveRegisters("removeregisters", remove));
	}
	
	
	@Test
	public void testMapAddUpdateFlags() {
		Map<String, Object> map = new HashMap<String,Object>();
		map.put("true", true);
		map.put("false", false);
		assertEquals(true, client.mapModifyFlags("mapwithflags", map) );
	}
	
	
	@Test
	public void testGetFlags() {
		ArrayList<String> test = new ArrayList<String>();
		test.add("true");
		test.add("false");
		assertEquals(2, client.mapGetFlags("mapwithflags", test).size());
	}
	
	@Test
	public void testRemovFlags() {
		Map<String, Object> add = new HashMap<String,Object>();
		add.put("flag1", true);
		add.put("flag2", false);
		client.mapModifyFlags("removeflags", add);
		
		ArrayList<String> remove = new ArrayList<String>();
		remove.add("flag1");
		remove.add("flag2");
		assertEquals(true, client.mapRemoveFlags("removeflags", remove));
	}

}
