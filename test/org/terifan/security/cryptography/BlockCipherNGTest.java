package org.terifan.security.cryptography;

import java.util.Random;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;


public class BlockCipherNGTest
{
	@Test(dataProvider = "ciphers")
	public void testCBCBlockEncryption(BlockCipher aCipher, BlockCipher aTweak, int aKeyLength)
	{
		Random rnd = new Random();

		byte[] cipherKey = new byte[aKeyLength];
		rnd.nextBytes(cipherKey);

		aCipher.engineInit(new SecretKey(cipherKey));

		long[] masterIV = {rnd.nextLong(), rnd.nextLong()};

		byte[] plain = new byte[1024];
		rnd.nextBytes(plain);

		byte[] encrypted = plain.clone();

		long[] blockIV = {rnd.nextLong(), rnd.nextLong()};

		XTSCipherMode crypto = new XTSCipherMode();
		crypto.encrypt(encrypted,   0, 256, aCipher, 0, 128, masterIV, blockIV);
		crypto.encrypt(encrypted, 256, 256, aCipher, 2, 128, masterIV, blockIV);
		crypto.encrypt(encrypted, 512, 256, aCipher, 4, 128, masterIV, blockIV);
		crypto.encrypt(encrypted, 768, 256, aCipher, 6, 128, masterIV, blockIV);

		byte[] decrypted = encrypted.clone();

		crypto.decrypt(decrypted,   0, 256, aCipher, 0, 128, masterIV, blockIV);
		crypto.decrypt(decrypted, 256, 256, aCipher, 2, 128, masterIV, blockIV);
		crypto.decrypt(decrypted, 512, 256, aCipher, 4, 128, masterIV, blockIV);
		crypto.decrypt(decrypted, 768, 256, aCipher, 6, 128, masterIV, blockIV);

		assertEquals(decrypted, plain);
	}


	@DataProvider(name = "ciphers")
	private Object[][] getCiphers()
	{
		return new Object[][]
		{
			{new AES(), new AES(), 16},
			{new AES(), new AES(), 24},
			{new AES(), new AES(), 32},
			{new Twofish(), new Twofish(), 8},
			{new Twofish(), new Twofish(), 16},
			{new Twofish(), new Twofish(), 24},
			{new Twofish(), new Twofish(), 32},
			{new Serpent(), new Serpent(), 16},
			{new Serpent(), new Serpent(), 24},
			{new Serpent(), new Serpent(), 32}
		};
	}
}
