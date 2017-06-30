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
		byte[] tweakKey = new byte[32];

		Random rnd = new Random(1);
		rnd.nextBytes(input);
		rnd.nextBytes(key);
		rnd.nextBytes(tweakKey);
		long[] iv = {rnd.nextLong(),rnd.nextLong()};
		long[] blockIV = {rnd.nextLong(),rnd.nextLong()};
		long unitNo = rnd.nextLong();

		AES cipher = new AES(new SecretKey(key));
		AES tweakCipher = new AES(new SecretKey(tweakKey));

		byte[] encrypted = input.clone();

		aCipherMode.encrypt(encrypted, padd, aBlockLength, cipher, unitNo, aUnitSize, iv, blockIV, tweakCipher);

		byte[] decrypted = encrypted.clone();

		aCipherMode.decrypt(decrypted, padd, aBlockLength, cipher, unitNo, aUnitSize, iv, blockIV, tweakCipher);

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
