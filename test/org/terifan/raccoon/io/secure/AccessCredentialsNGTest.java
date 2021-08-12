package org.terifan.raccoon.io.secure;

import org.terifan.raccoon.util.Log;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class AccessCredentialsNGTest
{
	@Test
	public void testSomeMethod()
	{
		AccessCredentials ac = new AccessCredentials("password".toCharArray(), EncryptionFunction.AES, KeyGenerationFunction.SHA512, CipherModeFunction.XTS);

		byte[] salt = "salt".getBytes();
		byte[] keypool = ac.generateKeyPool(KeyGenerationFunction.SKEIN512, salt, 16);

		assertEquals(Log.toHex(keypool), "0B875E667E38918FF04F8F1737790836");
	}
}
