package org.terifan.security.cryptography;

import static org.testng.Assert.*;
import org.testng.annotations.Test;
import resources.__TestUtils;


public class KuznechikNGTest
{
	@Test
	public void test1()
	{
		byte[] key = __TestUtils.hexToBytes("8899AABBCCDDEEFF0011223344556677FEDCBA98765432100123456789ABCDEF");
		byte[] plainTex = __TestUtils.hexToBytes("1122334455667700FFEEDDCCBBAA9988");
		byte[] cipherText = __TestUtils.hexToBytes("7F679D90BEBC24305A468D42B9D4EDCD");

		BlockCipher cipher = new Kuznechik(new SecretKey(key));

		byte[] encrypted = new byte[16];
		byte[] decrypted = new byte[16];

		cipher.engineEncryptBlock(plainTex, 0, encrypted, 0);

		cipher.engineDecryptBlock(encrypted, 0, decrypted, 0);

		assertEquals(encrypted, cipherText);
		assertEquals(decrypted, plainTex);
	}
}
