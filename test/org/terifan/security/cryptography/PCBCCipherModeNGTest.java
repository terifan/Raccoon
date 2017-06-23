package org.terifan.security.cryptography;

import java.util.Random;
import org.terifan.raccoon.util.Log;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class PCBCCipherModeNGTest
{
	@Test(enabled = false)
	public void testSomeMethod()
	{
		byte[] input = new byte[256];
		byte[] key = new byte[32];
		byte[] tweak = new byte[32];
		byte[] iv = new byte[16];

		Random rnd = new Random(1);
//		rnd.nextBytes(input);
		rnd.nextBytes(key);
		rnd.nextBytes(tweak);
		rnd.nextBytes(iv);
		long blockKey = rnd.nextLong();
		long unitNo = rnd.nextLong();

		AES cipher = new AES(new SecretKey(key));
		AES tweakCipher = new AES(new SecretKey(tweak));

		byte[] encrypted = input.clone();

		new PCBCCipherMode().encrypt(encrypted, 0, input.length, cipher, tweakCipher, unitNo, input.length, iv, blockKey);

		byte[] decrypted = encrypted.clone();
		
		decrypted[0] ^= 1;

		new PCBCCipherMode().decrypt(decrypted, 0, input.length, cipher, tweakCipher, unitNo, input.length, iv, blockKey);

		Log.hexDump(input);
		System.out.println();
		Log.hexDump(encrypted);
		System.out.println();
		Log.hexDump(decrypted);
		
		assertNotEquals(encrypted, input);
		assertEquals(decrypted, input);
	}
}
