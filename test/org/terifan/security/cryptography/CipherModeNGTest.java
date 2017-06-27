package org.terifan.security.cryptography;

import static org.testng.Assert.*;
import org.testng.annotations.Test;
import java.util.Random;
import org.testng.annotations.DataProvider;


public class CipherModeNGTest
{
	@Test(dataProvider = "params")
	public void testSomeMethod(CipherMode aCipherMode, int aUnitSize, int aBlockLength)
	{
		int padd = 100;

		byte[] input = new byte[padd + aBlockLength + padd];
		byte[] key = new byte[32];
		long[] iv = new long[2];

		Random rnd = new Random(1);
		rnd.nextBytes(input);
		rnd.nextBytes(key);
		iv[0] = rnd.nextLong();
		iv[1] = rnd.nextLong();
		long blockKey0 = rnd.nextLong();
		long blockKey1 = rnd.nextLong();
		long unitNo = rnd.nextLong();

		AES cipher = new AES(new SecretKey(key));

		byte[] encrypted = input.clone();

		aCipherMode.encrypt(encrypted, padd, aBlockLength, cipher, unitNo, aUnitSize, iv, blockKey0, blockKey1);

		byte[] decrypted = encrypted.clone();

		aCipherMode.decrypt(decrypted, padd, aBlockLength, cipher, unitNo, aUnitSize, iv, blockKey0, blockKey1);

		assertNotEquals(encrypted, input);
		assertEquals(decrypted, input);
	}


	@DataProvider
	private Object[][] params()
	{
		return new Object[][]{
			{new XTSCipherMode(), 512, 512},
			{new XTSCipherMode(), 512, 4096},
			{new XTSCipherMode(), 512, 1024*1024},
			{new XTSCipherMode(), 4096, 4096},
			{new XTSCipherMode(), 4096, 32768},
			{new XTSCipherMode(), 4096, 1024*1024},
			{new CBCCipherMode(), 512, 512},
			{new CBCCipherMode(), 512, 4096},
			{new CBCCipherMode(), 512, 1024*1024},
			{new CBCCipherMode(), 4096, 4096},
			{new CBCCipherMode(), 4096, 32768},
			{new CBCCipherMode(), 4096, 1024*1024},
			{new PCBCCipherMode(), 512, 512},
			{new PCBCCipherMode(), 512, 4096},
			{new PCBCCipherMode(), 512, 1024*1024},
			{new PCBCCipherMode(), 4096, 4096},
			{new PCBCCipherMode(), 4096, 32768},
			{new PCBCCipherMode(), 4096, 1024*1024}
		};
	}
}
