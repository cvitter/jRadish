package com.vitter.riak.jRadish;

import static org.junit.Assert.*;
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

}
