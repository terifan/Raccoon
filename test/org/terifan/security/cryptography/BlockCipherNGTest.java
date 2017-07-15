package org.terifan.security.cryptography;

import java.util.Random;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;


public class BlockCipherNGTest
{
	@Test(dataProvider = "ciphers")
	public void testBlockEncryption(CipherMode aCipherMode, BlockCipher aCipher, BlockCipher aTweakCipher, int aKeyLength)
	{
		Random rnd = new Random();

		byte[] cipherKey = new byte[aKeyLength];
		rnd.nextBytes(cipherKey);

		byte[] tweakKey = new byte[aKeyLength];
		rnd.nextBytes(tweakKey);

		aCipher.engineInit(new SecretKey(cipherKey));
		aTweakCipher.engineInit(new SecretKey(tweakKey));

		long[] masterIV = {rnd.nextLong(), rnd.nextLong()};
		long[] blockIV = {rnd.nextLong(), rnd.nextLong()};

		byte[] plain = new byte[1024*1024];
		rnd.nextBytes(plain);

		byte[] encrypted = plain.clone();

		aCipherMode.encrypt(encrypted, 1024*  0, 1024*256, aCipher, 1024*  0/4096, 4096, masterIV, blockIV, aTweakCipher);
		aCipherMode.encrypt(encrypted, 1024*256, 1024*512, aCipher, 1024*256/4096, 4096, masterIV, blockIV, aTweakCipher);
		aCipherMode.encrypt(encrypted, 1024*768, 1024*256, aCipher, 1024*768/4096, 4096, masterIV, blockIV, aTweakCipher);

		byte[] decrypted = encrypted.clone();

		aCipherMode.decrypt(decrypted, 1024*  0, 1024*128, aCipher, 1024*  0/4096, 4096, masterIV, blockIV, aTweakCipher);
		aCipherMode.decrypt(decrypted, 1024*128, 1024*896, aCipher, 1024*128/4096, 4096, masterIV, blockIV, aTweakCipher);

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
			{new XTSCipherMode(), new Kuznechik(), new Kuznechik(), 32},
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
			{new CBCCipherMode(), new Kuznechik(), new Kuznechik(), 32},
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
			{new PCBCCipherMode(), new Kuznechik(), new Kuznechik(), 32},
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
