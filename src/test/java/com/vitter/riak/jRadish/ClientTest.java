package com.vitter.riak.jRadish;

import static org.junit.Assert.*;

import org.junit.Assert;
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
	public void testSetCounter() {
		Client client = new Client();
		assertEquals(true, client.setCounter("craigscounter", 2L) );
	}
	
	@Test
	public void testGetCounter() {
		Client client = new Client();
		Long result = client.getCounter("craigscounter");
		
	}

	@Test
	public void testDeleteCounter() {
		Client client = new Client();
		client.setCounter("countertodelete", 2L);
		assertEquals(true, client.deleteCounter("countertodelete") );
	}

}
