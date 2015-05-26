package org.terifan.raccoon.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import org.testng.annotations.Test;
import static org.testng.Assert.*;


public class RangeMapTest
{
	@Test
	public void testSerialization1() throws IOException
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		RangeMap inMap = new RangeMap();
		inMap.add(0, Integer.MAX_VALUE);
		inMap.remove(0, 10);
		inMap.write(baos);

		RangeMap outMap = new RangeMap();
		outMap.read(new ByteArrayInputStream(baos.toByteArray()));

		assertEquals(inMap.toString(), outMap.toString());
	}


	@Test
	public void testSerialization2() throws IOException
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		int limit = Integer.MAX_VALUE;

		RangeMap inMap = new RangeMap();
		inMap.add(0, limit);

		Random rnd = new Random(1);
		for (int i = 0; i >= 0 && i < limit; )
		{
			int j = 1 + Math.max(0, Math.min(rnd.nextInt(100000), limit - i));
			inMap.remove(i, j);
			i += j * 2;
		}

		inMap.write(baos);

		RangeMap outMap = new RangeMap();
		outMap.read(new ByteArrayInputStream(baos.toByteArray()));

		assertEquals(inMap.toString(), outMap.toString());
	}
}
