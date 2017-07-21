package org.terifan.security.cryptography;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import org.terifan.security.messagedigest.HMAC;
import org.terifan.security.messagedigest.SHA512;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class PBKDF2NGTest
{
	@Test
	public void testSomeMethod() throws NoSuchAlgorithmException, InvalidKeySpecException
	{
		String password = "pw";
		byte[] salt = "salt-salt-salt-salt-salt-salt".getBytes();

		SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
		KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, 65536, 8 * 32);

		byte[] java = new SecretKeySpec(factory.generateSecret(spec).getEncoded(), "AES").getEncoded();

		byte[] soft = PBKDF2.generateKey(new HMAC(new SHA512(), password.getBytes()), salt, 65536, 32).bytes();

		assertEquals(java, soft);
	}
}
