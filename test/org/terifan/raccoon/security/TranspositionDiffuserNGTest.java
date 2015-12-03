package org.terifan.raccoon.security;

import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TranspositionDiffuserNGTest
{
	@Test(dataProvider = "sizes")
	public void testSomeMethod(int aSize)
	{
		byte[] in = new byte[aSize];

		for (int i = 0; i < aSize; i++)
		{
			in[i] = (byte)i;
		}

		TranspositionDiffuser diffuser = new TranspositionDiffuser(new byte[32], aSize);

		byte[] encoded = in.clone();
		diffuser.encode(encoded, 0, aSize, 0);

		assertNotEquals(encoded, in);

		byte[] decoded = encoded.clone();
		diffuser.decode(decoded, 0, aSize, 0);

		assertEquals(decoded, in);
	}


	@DataProvider
	Object[][] sizes()
	{
		return new Object[][]{{16},{512},{4096},{65536}};
	}
}
