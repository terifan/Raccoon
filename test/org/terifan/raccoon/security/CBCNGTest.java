package org.terifan.raccoon.security;

import org.terifan.security.cryptography.CBCCipherMode;
import org.terifan.security.cryptography.AES;
import org.terifan.security.cryptography.SecretKey;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import java.util.Random;
import org.testng.annotations.DataProvider;


public class CBCNGTest
{
	@Test(dataProvider = "blockSizes")
	public void testSomeMethod(int unitSize, int length)
	{
		int offset = 100;

		byte[] input = new byte[length + offset + 100];
		byte[] key1 = new byte[16];
		byte[] key2 = new byte[16];
		byte[] iv = new byte[16];
		byte[] tweakKey = new byte[64];

		Random rnd = new Random(1);
		rnd.nextBytes(input);
		rnd.nextBytes(key1);
		rnd.nextBytes(key2);
		rnd.nextBytes(iv);
		rnd.nextBytes(tweakKey);
		long blockKey = rnd.nextLong();
		long unitNo = rnd.nextLong();

		AES cipher1 = new AES(new SecretKey(key1));
		AES cipher2 = new AES(new SecretKey(key2));

		byte[] output = input.clone();

		CBCCipherMode cbc = new CBCCipherMode();
		cbc.encrypt(output, offset, length, cipher1, iv, unitNo, blockKey, cipher2, unitSize);

		assertNotEquals(output, input);

		cbc.decrypt(output, offset, length, cipher1, iv, unitNo, blockKey, cipher2, unitSize);

		assertEquals(output, input);
	}


	@DataProvider
	private Object[][] blockSizes()
	{
		return new Object[][]{
			{512, 512},
			{512, 4096},
			{512, 1024*1024},
			{4096, 4096},
			{4096, 32768},
			{4096, 1024*1024}
		};
	}
}
