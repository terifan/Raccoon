package org.terifan.raccoon.security;

import java.util.Arrays;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import java.util.Random;
import org.terifan.raccoon.util.Log;


public class CBCNGTest
{
	@Test
	public void testSomeMethod()
	{
		int unitSize = 512;
		int offset = 100;
		int length = 8 * unitSize;

		byte[] input = new byte[length + offset + 100];
		byte[] key1 = new byte[16];
		byte[] key2 = new byte[16];
		byte[] iv = new byte[16];

		Random rnd = new Random(1);
		rnd.nextBytes(input);
		rnd.nextBytes(key1);
		rnd.nextBytes(key2);
		rnd.nextBytes(iv);
		long blockKey = rnd.nextLong();
		long unitNo = rnd.nextLong();

		byte[] output = input.clone();
		byte[] encrypted = new byte[input.length];
		AES cipher1 = new AES(new SecretKey(key1));
		AES cipher2 = new AES(new SecretKey(key2));

		CBC cbc = new CBC();
		cbc.encrypt(unitSize, input, encrypted, offset, length, unitNo, iv, cipher1, cipher2, blockKey);

		cbc.decrypt(unitSize, encrypted, output, offset, length, unitNo, iv, cipher1, cipher2, blockKey);

		assertEquals(output, input);
	}
}
