package org.terifan.security.cryptography;

import java.util.Random;
import org.terifan.raccoon.util.Log;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;


public class BlockCipherNGTest
{
	@Test(dataProvider = "ciphers")
	public void testCBCBlockEncryption(BlockCipher aCipher, BlockCipher aTweak)
	{
		Random rnd = new Random();

		byte[] cipherKey = new byte[16];
		rnd.nextBytes(cipherKey);

		byte[] tweakKey = new byte[16];
		rnd.nextBytes(tweakKey);

		aCipher.engineInit(new SecretKey(cipherKey));

		aTweak.engineInit(new SecretKey(tweakKey));

		byte[] iv = new byte[16];
		rnd.nextBytes(iv);

		byte[] plain = new byte[1024];
		rnd.nextBytes(plain);

		byte[] encrypted = plain.clone();

		long blockKey = rnd.nextLong();

		CBCCipherMode cbc = new CBCCipherMode();
		cbc.encrypt(encrypted,   0, 256, aCipher, iv, 0, blockKey, aTweak, 128);
		cbc.encrypt(encrypted, 256, 256, aCipher, iv, 2, blockKey, aTweak, 128);
		cbc.encrypt(encrypted, 512, 256, aCipher, iv, 4, blockKey, aTweak, 128);
		cbc.encrypt(encrypted, 768, 256, aCipher, iv, 6, blockKey, aTweak, 128);

		byte[] decrypted = encrypted.clone();

		cbc.decrypt(decrypted,   0, 256, aCipher, iv, 0, blockKey, aTweak, 128);
		cbc.decrypt(decrypted, 256, 256, aCipher, iv, 2, blockKey, aTweak, 128);
		cbc.decrypt(decrypted, 512, 256, aCipher, iv, 4, blockKey, aTweak, 128);
		cbc.decrypt(decrypted, 768, 256, aCipher, iv, 6, blockKey, aTweak, 128);

		assertEquals(decrypted, plain);
	}


	@DataProvider(name = "ciphers")
	private Object[][] getCiphers()
	{
		return new Object[][]
		{
			{new AES(), new AES()},
			{new Twofish(), new Twofish()},
			{new Serpent(), new Serpent()}
		};
	}
}
