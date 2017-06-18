package org.terifan.raccoon.io.secure;

import java.security.MessageDigest;
import org.terifan.security.messagedigest.SHA512;
import org.terifan.security.messagedigest.Skein512;
import org.terifan.security.messagedigest.Whirlpool;
import org.terifan.security.cryptography.PBKDF2;
import org.terifan.security.messagedigest.HMAC;
import org.terifan.security.messagedigest.SHA3;

// CBC/XTS
// 128/256

public final class AccessCredentials
{
	public final static EncryptionFunction DEFAULT_ENCRYPTION = EncryptionFunction.AES;
	public final static KeyGenerationFunction DEFAULT_KEY_GENERATOR = KeyGenerationFunction.SHA512;
	
	/**
	 * Passwords are expanded into cryptographic keys by iterating a hash function this many times, default is 100000 times.
	 */
	public final static int DEFAULT_ITERATION_COUNT = 100_000;

	private EncryptionFunction mEncryptionFunction;
	private KeyGenerationFunction mKeyGeneratorFunction;
	private byte [] mPassword;
	private int mIterationCount;


	public AccessCredentials(String aPassword)
	{
		this(aPassword.toCharArray(), DEFAULT_ENCRYPTION, DEFAULT_KEY_GENERATOR, DEFAULT_ITERATION_COUNT);
	}


	public AccessCredentials(char [] aPassword)
	{
		this(aPassword, DEFAULT_ENCRYPTION, DEFAULT_KEY_GENERATOR, DEFAULT_ITERATION_COUNT);
	}


	/**
	 *
	 * @param aIterationCount
	 *   Passwords are expanded into cryptographic keys by iterating a hash function this many times. A larger number means more security
	 *   but also longer time to open a database. Default is 100000 iterations. WARNING: this value is not recorded in the database file and
	 *   must always be provided!
	 */
	public AccessCredentials(char [] aPassword, EncryptionFunction aEncryptionFunction, KeyGenerationFunction aKeyFunction, int aIterationCount)
	{
		mIterationCount = aIterationCount;
		mEncryptionFunction = aEncryptionFunction;
		mKeyGeneratorFunction = aKeyFunction;
		mPassword = new byte[2 * aPassword.length];

		for (int i = 0, j = 0; i < aPassword.length; i++)
		{
			mPassword[j++] = (byte)(aPassword[i] >>> 8);
			mPassword[j++] = (byte)(aPassword[i]);
		}
	}


	public AccessCredentials setEncryptionFunction(EncryptionFunction aEncryptionFunction)
	{
		mEncryptionFunction = aEncryptionFunction;
		return this;
	}


	public AccessCredentials setKeyGeneratorFunction(KeyGenerationFunction aKeyGeneratorFunction)
	{
		mKeyGeneratorFunction = aKeyGeneratorFunction;
		return this;
	}


	/**
	 * Passwords are expanded into cryptographic keys by iterating a hash function this many times. A larger number means more security but
	 * also longer time to open a database. Default is 100000 iterations.
	 *
	 * WARNING: this value is not recorded in the database file and must be provided when opening a database!
	 *
	 * @param aIterationCount
	 *   the iteration count used.
	 */
	public AccessCredentials setIterationCount(int aIterationCount)
	{
		mIterationCount = aIterationCount;
		return this;
	}


	EncryptionFunction getEncryptionFunction()
	{
		return mEncryptionFunction;
	}


	byte[] generateKeyPool(byte[] aSalt, int aPoolSize)
	{
		HMAC mac = new HMAC(newMessageDigestInstance(), mPassword);

		return PBKDF2.generateKeyBytes(mac, aSalt, mIterationCount, aPoolSize);
	}


	private MessageDigest newMessageDigestInstance()
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
