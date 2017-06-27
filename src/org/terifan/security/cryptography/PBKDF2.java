package org.terifan.security.cryptography;

import org.terifan.security.messagedigest.HMAC;
import java.util.Arrays;


/**
 * This class implements the PBKDF2 function (password-based-key-derivation-function-2) from the PKCS#5 v2.0 Password-Based Cryptography
 * Standard.
 */
public final class PBKDF2
{
	private PBKDF2()
	{
	}


	public static SecretKey generateKey(HMAC aPassword, byte[] aSalt, int aIterationCount, int aKeyLengthBytes)
	{
		return new SecretKey(generateKeyBytes(aPassword, aSalt, aIterationCount, aKeyLengthBytes));
	}


	public static byte[] generateKeyBytes(HMAC aHMAC, byte[] aSalt, int aIterationCount, int aKeyLengthBytes)
	{
		assert aIterationCount > 0;
		assert aKeyLengthBytes > 0;

		int hashLen = aHMAC.getMessageDigest().getDigestLength();
		int blockCount = (aKeyLengthBytes + hashLen - 1) / hashLen;

		byte[] buffer = new byte[blockCount * hashLen];
		byte[] salt = Arrays.copyOfRange(aSalt, 0, aSalt.length + 4);

		for (int blockIndex = 1, offset = 0; blockIndex <= blockCount; blockIndex++, offset += hashLen)
		{
			process(aHMAC, salt, aIterationCount, blockIndex, buffer, offset);
		}

		return Arrays.copyOfRange(buffer, 0, aKeyLengthBytes);
	}


	private static void process(HMAC aHMAC, byte[] aSalt, int aIterationCount, int aBlockIndex, byte[] aBuffer, int aOffset)
	{
		aSalt[aSalt.length - 4] = (byte)(aBlockIndex >>> 24);
		aSalt[aSalt.length - 3] = (byte)(aBlockIndex >> 16);
		aSalt[aSalt.length - 2] = (byte)(aBlockIndex >> 8);
		aSalt[aSalt.length - 1] = (byte)(aBlockIndex);

		byte[] u = aHMAC.digest(aSalt);

		System.arraycopy(u, 0, aBuffer, aOffset, u.length);

		for (int j = 1; j < aIterationCount; j++)
		{
			u = aHMAC.digest(u);

			for (int i = 0; i < u.length; i++)
			{
				aBuffer[aOffset + i] ^= u[i];
			}
		}
	}


//	public static void main(String ... args)
//	{
//		try
//		{
//			long t = System.currentTimeMillis();
//			PBKDF2.generateKey(new HMAC(new SHA512(), "password".getBytes()), new byte[256], 100_000, 32+3*32+3*16+8+8);
//			System.out.println(System.currentTimeMillis()-t);
//		}
//		catch (Throwable e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
}
