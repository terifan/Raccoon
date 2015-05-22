package org.terifan.raccoon;

import org.junit.Test;
import static org.junit.Assert.*;
import org.terifan.raccoon.LeafNode.PutResult;


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
