package org.terifan.raccoon;

import org.terifan.raccoon.LeafNode.PutResult;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;
import static org.testng.Assert.*;


public class LeafNodeTest
{
	public LeafNodeTest()
	{
	}


	@Test
	public void testSomeMethod()
	{
		LeafNode leafNode = LeafNode.alloc(4096);
		PutResult result = new PutResult();
		leafNode.put(0, "key".getBytes(), "value".getBytes(), result);
		assertTrue(result.inserted);
	}
}
