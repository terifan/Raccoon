package org.terifan.raccoon.btree;

import org.terifan.raccoon.btree.ArrayMapKey;
import java.util.UUID;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class ArrayMapKeyNGTest
{
	@Test
	public void testToString()
	{
		ArrayMapKey k1 = new ArrayMapKey(1L);
		ArrayMapKey k2 = new ArrayMapKey("key");
		ArrayMapKey k3 = new ArrayMapKey(UUID.fromString("31e18b3e-0f90-4f24-80a0-fff857fbdbf8"));
		assertEquals(k1.get(), 1L);
		assertEquals(k2.get(), "key");
		assertEquals(k3.get().toString(), "31e18b3e-0f90-4f24-80a0-fff857fbdbf8");
	}


	@Test
	public void testCompareLong()
	{
		ArrayMapKey a1 = new ArrayMapKey(1L);
		ArrayMapKey a2 = new ArrayMapKey(2L);
		ArrayMapKey a3 = new ArrayMapKey(3L);
		assertTrue(a1.compareTo(a2) < 0);
		assertTrue(a2.compareTo(a2) == 0);
		assertTrue(a3.compareTo(a2) > 0);
	}


	@Test
	public void testCompareString()
	{
		ArrayMapKey a1 = new ArrayMapKey("alexander");
		ArrayMapKey a2 = new ArrayMapKey("bob");
		ArrayMapKey a3 = new ArrayMapKey("steve");
		assertTrue(a1.compareTo(a2) < 0);
		assertTrue(a2.compareTo(a2) == 0);
		assertTrue(a3.compareTo(a2) > 0);
	}


	@Test
	public void testCompareUUID()
	{
		ArrayMapKey a1 = new ArrayMapKey(UUID.fromString("31e18b3e-0f90-4f24-80a0-fff857fbdbf8"));
		ArrayMapKey a2 = new ArrayMapKey(UUID.fromString("661f0d28-01a5-4f22-9dfa-8a24d869b8ed"));
		ArrayMapKey a3 = new ArrayMapKey(UUID.fromString("9695d82c-4035-43a7-98c0-4e4a048167da"));
		assertTrue(a1.compareTo(a2) < 0);
		assertTrue(a2.compareTo(a2) == 0);
		assertTrue(a3.compareTo(a2) > 0);
	}
}
