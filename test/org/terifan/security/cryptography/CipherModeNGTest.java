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
		byte[] tweak = new byte[32];
		byte[] iv = new byte[16];

		Random rnd = new Random(1);
		rnd.nextBytes(input);
		rnd.nextBytes(key);
		rnd.nextBytes(tweak);
		rnd.nextBytes(iv);
		long blockKey = rnd.nextLong();
		long unitNo = rnd.nextLong();

		AES cipher = new AES(new SecretKey(key));
		AES tweakCipher = new AES(new SecretKey(tweak));

		byte[] output = input.clone();

		aCipherMode.encrypt(output, padd, aBlockLength, cipher, tweakCipher, unitNo, aUnitSize, iv, blockKey);
		
		assertNotEquals(output, input);

		aCipherMode.decrypt(output, padd, aBlockLength, cipher, tweakCipher, unitNo, aUnitSize, iv, blockKey);

		assertEquals(output, input);
	}


	@DataProvider
	private Object[][] params()
	{
		return new Object[][]{
			{new CBCCipherMode(), 512, 512},
			{new CBCCipherMode(), 512, 4096},
			{new CBCCipherMode(), 512, 1024*1024},
			{new CBCCipherMode(), 4096, 4096},
			{new CBCCipherMode(), 4096, 32768},
			{new CBCCipherMode(), 4096, 1024*1024},
			{new XTSCipherMode(), 512, 512},
			{new XTSCipherMode(), 512, 4096},
			{new XTSCipherMode(), 512, 1024*1024},
			{new XTSCipherMode(), 4096, 4096},
			{new XTSCipherMode(), 4096, 32768},
			{new XTSCipherMode(), 4096, 1024*1024}
		};
	}
}
