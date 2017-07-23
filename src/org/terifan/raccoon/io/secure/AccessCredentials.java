package org.terifan.raccoon.io.secure;

import org.terifan.security.cryptography.SCrypt;
import org.terifan.security.messagedigest.HMAC;


public final class AccessCredentials
{
	public final static EncryptionFunction DEFAULT_ENCRYPTION = EncryptionFunction.AES;
	public final static CipherModeFunction DEFAULT_CIPHER_MODE = CipherModeFunction.XTS;
	public final static KeyGenerationFunction DEFAULT_KEY_GENERATOR = KeyGenerationFunction.SHA512;

	public final static int DEFAULT_ITERATION_COUNT = 32768;

	private EncryptionFunction mEncryptionFunction;
	private KeyGenerationFunction mKeyGeneratorFunction;
	private CipherModeFunction mCipherModeFunction;
	private int mIterationCount;
	private byte[] mPassword;


	public AccessCredentials(String aPassword)
	{
		this(aPassword.toCharArray());
	}


	public AccessCredentials(char[] aPassword)
	{
		this(aPassword, DEFAULT_ENCRYPTION, DEFAULT_KEY_GENERATOR, DEFAULT_CIPHER_MODE);
	}


	/**
	 *
	 * @param aIterationCount
	 * Passwords are expanded into cryptographic keys by iterating a hash function this many times.
	 * A larger number means more security but also longer time to open a database. WARNING: this
	 * value is not recorded in the database file and must always be provided if different from the
	 * default value!
	 */
	public AccessCredentials(char[] aPassword, EncryptionFunction aEncryptionFunction, KeyGenerationFunction aKeyFunction, CipherModeFunction aCipherModeFunction)
	{
		mIterationCount = DEFAULT_ITERATION_COUNT;
		mEncryptionFunction = aEncryptionFunction;
		mKeyGeneratorFunction = aKeyFunction;
		mCipherModeFunction = aCipherModeFunction;
		mPassword = new byte[2 * aPassword.length];

		for (int i = 0, j = 0; i < aPassword.length; i++)
		{
			mPassword[j++] = (byte)(aPassword[i] >>> 8);
			mPassword[j++] = (byte)(aPassword[i]);
		}
	}


	public EncryptionFunction getEncryptionFunction()
	{
		return mEncryptionFunction;
	}


	public AccessCredentials setEncryptionFunction(EncryptionFunction aEncryptionFunction)
	{
		mEncryptionFunction = aEncryptionFunction;
		return this;
	}


	public KeyGenerationFunction getKeyGeneratorFunction()
	{
		return mKeyGeneratorFunction;
	}


	public AccessCredentials setKeyGeneratorFunction(KeyGenerationFunction aKeyGeneratorFunction)
	{
		mKeyGeneratorFunction = aKeyGeneratorFunction;
		return this;
	}


	public CipherModeFunction getCipherModeFunction()
	{
		return mCipherModeFunction;
	}


	public AccessCredentials setCipherModeFunction(CipherModeFunction aCipherModeFunction)
	{
		mCipherModeFunction = aCipherModeFunction;
		return this;
	}


	public int getIterationCount()
	{
		return mIterationCount;
	}


	/**
	 * Passwords are expanded into cryptographic keys by iterating a hash function this many times. A larger number means more security but
	 * also longer time to open a database.
	 *
	 * WARNING: this value is not recorded in the database file and must be provided when opening a database if other than the default value!
	 *
	 * @param aIterationCount
	 * the iteration count used.
	 */
	public AccessCredentials setIterationCount(int aIterationCount)
	{
		mIterationCount = aIterationCount;
		return this;
	}


	byte[] generateKeyPool(KeyGenerationFunction aKeyGenerator, byte[] aSalt, int aPoolSize)
	{
		HMAC mac = new HMAC(aKeyGenerator.newInstance(), mPassword);

		int cost = 1 << Math.max(1, (int)(Math.log(mIterationCount) / Math.log(2)));

//		long t = System.currentTimeMillis();

		byte[] pool = SCrypt.generate(mac, aSalt, cost, 32, 1, mIterationCount, aPoolSize);

//		System.out.println(System.currentTimeMillis() - t);

		return pool;
	}
}
