package org.terifan.raccoon.util;

import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class CacheNGTest
{
	@Test
	public void testPut()
	{
		LRUMap<String,Integer> cache = new LRUMap<>(4);
		
		cache.put("a", 1);
		cache.put("b", 2);
		cache.put("c", 3);
		cache.put("d", 4);
		cache.put("e", 5);
		cache.put("f", 6);
		
		assertEquals(cache.size(), 4);
		assertTrue(cache.containsKey("f"));
		assertTrue(cache.containsKey("e"));
		assertTrue(cache.containsKey("d"));
		assertTrue(cache.containsKey("c"));
		assertFalse(cache.containsKey("b"));
		assertFalse(cache.containsKey("a"));
	}


	@Test
	public void testGet()
	{
		LRUMap<String,Integer> cache = new LRUMap<>(4);
		
		cache.put("a", 1);
		cache.put("b", 2);
		cache.put("c", 3);
		cache.put("d", 4);
		cache.get("a");
		cache.get("b");
		cache.put("e", 5);
		cache.put("f", 6);
		
		assertEquals(cache.size(), 4);
		assertTrue(cache.containsKey("f"));
		assertTrue(cache.containsKey("e"));
		assertTrue(cache.containsKey("b"));
		assertTrue(cache.containsKey("a"));
		assertFalse(cache.containsKey("c"));
		assertFalse(cache.containsKey("d"));
	}
}
