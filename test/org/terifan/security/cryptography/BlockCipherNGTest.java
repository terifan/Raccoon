package org.terifan.security.cryptography;

import java.util.Random;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;


public class BlockCipherNGTest
{
	@Test(dataProvider = "ciphers")
	public void testCBCBlockEncryption(CipherMode aCipherMode, BlockCipher aCipher, BlockCipher aTweak, int aKeyLength)
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

		aCipherMode.encrypt(encrypted,   0, 256, aCipher, 0, 128, masterIV, blockIV);
		aCipherMode.encrypt(encrypted, 256, 256, aCipher, 2, 128, masterIV, blockIV);
		aCipherMode.encrypt(encrypted, 512, 256, aCipher, 4, 128, masterIV, blockIV);
		aCipherMode.encrypt(encrypted, 768, 256, aCipher, 6, 128, masterIV, blockIV);

		byte[] decrypted = encrypted.clone();

		aCipherMode.decrypt(decrypted,   0, 512, aCipher, 0, 128, masterIV, blockIV);
		aCipherMode.decrypt(decrypted, 512, 512, aCipher, 4, 128, masterIV, blockIV);

		assertEquals(decrypted, plain);
	}


	@DataProvider(name = "ciphers")
	private Object[][] getCiphers()
	{
		return new Object[][]
		{
			{new XTSCipherMode(), new AES(), new AES(), 16},
			{new XTSCipherMode(), new AES(), new AES(), 24},
			{new XTSCipherMode(), new AES(), new AES(), 32},
			{new XTSCipherMode(), new Twofish(), new Twofish(), 8},
			{new XTSCipherMode(), new Twofish(), new Twofish(), 16},
			{new XTSCipherMode(), new Twofish(), new Twofish(), 24},
			{new XTSCipherMode(), new Twofish(), new Twofish(), 32},
			{new XTSCipherMode(), new Serpent(), new Serpent(), 16},
			{new XTSCipherMode(), new Serpent(), new Serpent(), 24},
			{new XTSCipherMode(), new Serpent(), new Serpent(), 32},

			{new CBCCipherMode(), new AES(), new AES(), 16},
			{new CBCCipherMode(), new AES(), new AES(), 24},
			{new CBCCipherMode(), new AES(), new AES(), 32},
			{new CBCCipherMode(), new Twofish(), new Twofish(), 8},
			{new CBCCipherMode(), new Twofish(), new Twofish(), 16},
			{new CBCCipherMode(), new Twofish(), new Twofish(), 24},
			{new CBCCipherMode(), new Twofish(), new Twofish(), 32},
			{new CBCCipherMode(), new Serpent(), new Serpent(), 16},
			{new CBCCipherMode(), new Serpent(), new Serpent(), 24},
			{new CBCCipherMode(), new Serpent(), new Serpent(), 32},

			{new PCBCCipherMode(), new AES(), new AES(), 16},
			{new PCBCCipherMode(), new AES(), new AES(), 24},
			{new PCBCCipherMode(), new AES(), new AES(), 32},
			{new PCBCCipherMode(), new Twofish(), new Twofish(), 8},
			{new PCBCCipherMode(), new Twofish(), new Twofish(), 16},
			{new PCBCCipherMode(), new Twofish(), new Twofish(), 24},
			{new PCBCCipherMode(), new Twofish(), new Twofish(), 32},
			{new PCBCCipherMode(), new Serpent(), new Serpent(), 16},
			{new PCBCCipherMode(), new Serpent(), new Serpent(), 24},
			{new PCBCCipherMode(), new Serpent(), new Serpent(), 32}
		};
	}
}
