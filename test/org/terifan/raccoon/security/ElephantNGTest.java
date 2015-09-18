package org.terifan.raccoon.security;

import java.io.UnsupportedEncodingException;
import java.util.Random;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ElephantNGTest
{
	@Test(dataProvider = "cryptoParams")
	public void testCrypto(int aKeyLength, int aIterationCount, int aSaltLength, int aUnitSize, int aBlockSize, int aEncryptUnitsInFirstChunk, int adecryptUnitsInFirstChunk) throws UnsupportedEncodingException
	{
		Random rnd = new Random();

		byte[] salt = new byte[aSaltLength];
		int[] iv = new int[4];
		int[] tweakKey = new int[8];

		long unit = rnd.nextLong();
		long extraTweak = rnd.nextLong();
		rnd.nextBytes(salt);
		for (int i = 0; i < iv.length; i++)
		{
			iv[i] = rnd.nextInt();
		}
		for (int i = 0; i < tweakKey.length; i++)
		{
			tweakKey[i] = rnd.nextInt();
		}

		String password = "password";
		String tweakPassword = "tweak-password";

		Cipher cipher = new AES(PBKDF2.generateKey(new HMAC(new SHA512(), password.getBytes("utf-8")), salt, aIterationCount, aKeyLength));
		Cipher tweakCipher = new Twofish(PBKDF2.generateKey(new HMAC(new SHA512(), tweakPassword.getBytes("utf-8")), salt, aIterationCount, aKeyLength));

		byte[] input = new byte[aBlockSize];
		rnd.nextBytes(input);


		Elephant elephant = new Elephant(aUnitSize);
		byte[] cipherText = new byte[input.length];
		byte[] output = new byte[input.length];

		if (aEncryptUnitsInFirstChunk > 0)
		{
			elephant.encrypt(input, cipherText, 0, aUnitSize * aEncryptUnitsInFirstChunk, unit, iv, cipher, tweakCipher, tweakKey, extraTweak);
		}
		elephant.encrypt(input, cipherText, aUnitSize * aEncryptUnitsInFirstChunk, input.length - aUnitSize * aEncryptUnitsInFirstChunk, unit + aEncryptUnitsInFirstChunk, iv, cipher, tweakCipher, tweakKey, extraTweak);

		if (adecryptUnitsInFirstChunk > 0)
		{
			elephant.decrypt(cipherText, output, 0, aUnitSize * adecryptUnitsInFirstChunk, unit, iv, cipher, tweakCipher, tweakKey, extraTweak);
		}
		elephant.decrypt(cipherText, output, aUnitSize * adecryptUnitsInFirstChunk, input.length - aUnitSize * adecryptUnitsInFirstChunk, unit + adecryptUnitsInFirstChunk, iv, cipher, tweakCipher, tweakKey, extraTweak);

		assertEquals(input, output);
		assertNotEquals(input, cipherText);
	}

	@DataProvider(name="cryptoParams")
	private Object[][] cryptoParams()
	{
		return new Object[][]{
			{16, 10, 10, 4096, 8192, 0, 0},
			{16, 100, 10, 4096, 8192, 1, 0},
			{16, 100, 100, 4096, 8192, 0, 1},
			{16, 1000, 10, 512, 8192, 2, 7},
			{16, 1000, 100, 512, 8192, 7, 2},
			{32, 10, 10, 4096, 8192, 1, 0},
			{32, 100, 10, 4096, 8192, 1, 1},
			{32, 100, 100, 4096, 8192, 0, 0},
			{32, 1000, 10, 512, 8192, 4, 0},
			{32, 1000, 100, 4096, 65536, 0, 0},
			{32, 1000, 100, 4096, 65536, 0, 0}
		};
	}
}
