package org.terifan.raccoon.io.secure;

import java.security.MessageDigest;
import org.terifan.security.messagedigest.SHA512;
import org.terifan.security.messagedigest.Skein512;
import org.terifan.security.messagedigest.Whirlpool;
import org.terifan.security.cryptography.PBKDF2;
import org.terifan.security.messagedigest.HMAC;
import org.terifan.security.messagedigest.SHA3;


public final class AccessCredentials
{
	private final static EncryptionFunction DEFAULT_ENCRYPTION = EncryptionFunction.AES;
	private final static KeyGenerationFunction DEFAULT_KEY_GENERATOR = KeyGenerationFunction.SHA512;

	private final static int PASSWORD_EXTENSION_LENGTH = 16;
	private final static int PASSWORD_EXTENSION_ITERATION_COUNT = 100_000;
	private final static byte[] EXTENSION_PASS_PREFIX = {113, -55, 23, -51, -55, -113, 113, 20, 40, 26, 39, -52, -70, -41, -109, -105}; // MD5("Assume a virtue, if you have it not")
	private final static byte[] EXTENSION_SALT_PREFIX = {-79, -2, 91, 65, 54, 30, 77, -65, 107, 39, 20, 56, -26, -71, -41, 70}; // MD5("Be great in act, as you have been in thought")

	private EncryptionFunction mEncryptionFunction;
	private KeyGenerationFunction mKeyGeneratorFunction;
	private byte [] mUserPassword;
	private byte [] mExtendedPassword;
	private boolean mPasswordExtended;


	public AccessCredentials(String aPassword)
	{
		this(aPassword, DEFAULT_ENCRYPTION, DEFAULT_KEY_GENERATOR);
	}


	public AccessCredentials(char [] aPassword)
	{
		init(aPassword, DEFAULT_ENCRYPTION, DEFAULT_KEY_GENERATOR);
	}


	public AccessCredentials(String aPassword, EncryptionFunction aEncryptionFunction, KeyGenerationFunction aKeyFunction)
	{
		init(aPassword.toCharArray(), aEncryptionFunction, aKeyFunction);
	}


	public AccessCredentials(char [] aPassword, EncryptionFunction aEncryptionFunction, KeyGenerationFunction aKeyFunction)
	{
		init(aPassword, aEncryptionFunction, aKeyFunction);
	}


	private void init(char [] aPassword, EncryptionFunction aEncryptionFunction, KeyGenerationFunction aKeyFunction)
	{
		mEncryptionFunction = aEncryptionFunction;
		mKeyGeneratorFunction = aKeyFunction;

		mUserPassword = new byte[2 * aPassword.length];

		for (int i = 0, j = 0; i < aPassword.length; i++)
		{
			mUserPassword[j++] = (byte)(aPassword[i] >>> 8);
			mUserPassword[j++] = (byte)(aPassword[i]);
		}

		mPasswordExtended = false;
	}


	void setKeyGeneratorFunction(KeyGenerationFunction aKeyGeneratorFunction)
	{
		mKeyGeneratorFunction = aKeyGeneratorFunction;
	}


	EncryptionFunction getEncryptionFunction()
	{
		return mEncryptionFunction;
	}


	byte[] generateKeyPool(byte[] aSalt, int aIterationCount, int aPoolSize)
	{
		ensurePasswordExtended();

		HMAC mac = new HMAC(newMessageDigest(), mExtendedPassword);

		return PBKDF2.generateKeyBytes(mac, aSalt, aIterationCount, aPoolSize);
	}


	/**
	 * Password extension is intended to slow down the creation of key material. This method doesn't depend on the key generator or file being protected.
	 */
	private void ensurePasswordExtended()
	{
		if (!mPasswordExtended)
		{
			byte[] pass = join(EXTENSION_PASS_PREFIX, mUserPassword);
			byte[] salt = join(EXTENSION_SALT_PREFIX, mUserPassword);

			HMAC mac = new HMAC(new SHA512(), pass);

			byte[] extension = PBKDF2.generateKeyBytes(mac, salt, PASSWORD_EXTENSION_ITERATION_COUNT, PASSWORD_EXTENSION_LENGTH);

			mExtendedPassword = join(extension, mUserPassword);

			mPasswordExtended = true;
		}
	}


	private static byte [] join(byte[] aBufferA, byte[] aBufferB)
	{
		byte [] output = new byte[aBufferA.length + aBufferB.length];
		System.arraycopy(aBufferA, 0, output, 0, aBufferA.length);
		System.arraycopy(aBufferB, 0, output, aBufferA.length, aBufferB.length);
		return output;
	}


	private MessageDigest newMessageDigest()
	{
		switch (mKeyGeneratorFunction)
		{
			case SHA3:
				return new SHA3();
			case SHA512:
				return new SHA512();
			case Skein512:
				return new Skein512();
			case Whirlpool:
				return new Whirlpool();
			default:
				throw new IllegalStateException();
		}
	}
}
