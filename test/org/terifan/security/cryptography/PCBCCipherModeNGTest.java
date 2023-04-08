package org.terifan.security.cryptography;

import org.testng.annotations.Test;


public class PCBCCipherModeNGTest extends CipherModeNGTest
{
	@Test
	public void testEncryption()
	{
		testBlockEncryption(new PCBCCipherMode(), new AES(), new AES(), 16);
		testBlockEncryption(new PCBCCipherMode(), new AES(), new AES(), 24);
		testBlockEncryption(new PCBCCipherMode(), new AES(), new AES(), 32);
		testBlockEncryption(new PCBCCipherMode(), new Kuznechik(), new Kuznechik(), 32);
		testBlockEncryption(new PCBCCipherMode(), new Twofish(), new Twofish(), 8);
		testBlockEncryption(new PCBCCipherMode(), new Twofish(), new Twofish(), 16);
		testBlockEncryption(new PCBCCipherMode(), new Twofish(), new Twofish(), 24);
		testBlockEncryption(new PCBCCipherMode(), new Twofish(), new Twofish(), 32);
		testBlockEncryption(new PCBCCipherMode(), new Serpent(), new Serpent(), 16);
		testBlockEncryption(new PCBCCipherMode(), new Serpent(), new Serpent(), 24);
		testBlockEncryption(new PCBCCipherMode(), new Serpent(), new Serpent(), 32);
	}
}
