package org.terifan.v1.security.cryptography;

import java.util.Arrays;


public abstract class Cipher
{
	public final static int ENCRYPT = 1;
	public final static int DECRYPT = 2;

	private transient int mTransformation;
	private transient CipherMode mCipherMode;
	private transient byte [] mIV;
	private transient byte [] mInitialIV;
	private transient byte [] mTemp1;
	private transient byte [] mTemp2;
	private transient boolean mKeyInitialized;
	private transient byte [] mWhiteningA;
	private transient byte [] mWhiteningB;
	private transient boolean mUseWhitening;


	Cipher()
	{
	}


	public Cipher(SecretKey aSecretKey)
	{
		init(aSecretKey);
	}


	public void init(SecretKey aSecretKey)
	{
		engineInit(aSecretKey);
		mKeyInitialized = true;
	}


	public void init(int aTransformation, CipherMode aCipherMode)
	{
		doInit(aTransformation, aCipherMode, null, null);
	}


	public void init(int aTransformation, CipherMode aCipherMode, SecretKey aSecretKey)
	{
		doInit(aTransformation, aCipherMode, aSecretKey, null);
	}


	public void init(int aTransformation, CipherMode aCipherMode, SecretKey aSecretKey, byte [] aIV)
	{
		doInit(aTransformation, aCipherMode, aSecretKey, aIV);
	}


	public void init(int aTransformation, CipherMode aCipherMode, byte [] aIV)
	{
		doInit(aTransformation, aCipherMode, null, aIV);
	}


	private void doInit(int aTransformation, CipherMode aCipherMode, SecretKey aSecretKey, byte [] aIV)
	{
		if (aTransformation != ENCRYPT && aTransformation != DECRYPT)
		{
			throw new IllegalArgumentException("aTransformation != ENCRYPT && aTransformation != DECRYPT");
		}

		if (aSecretKey != null)
		{
			engineInit(aSecretKey);
			mKeyInitialized = true;
		}
		else if (!mKeyInitialized)
		{
			throw new RuntimeException("A SecretKey must be provided.");
		}

		mTransformation = aTransformation;
		mCipherMode = aCipherMode;
		mTemp1 = new byte[engineGetBlockSize()];
		mTemp2 = new byte[engineGetBlockSize()];

		setIV(aIV);
	}


	public void setWhitening(byte [] aWhiteningA, byte [] aWhiteningB)
	{
		if (aWhiteningA == null && aWhiteningB == null)
		{
			mUseWhitening = false;
			return;
		}
		if (aWhiteningA == null || aWhiteningB == null)
		{
			throw new IllegalArgumentException("Both whitening values must be provided.");
		}
		if (aWhiteningA.length != engineGetBlockSize() || aWhiteningB.length != engineGetBlockSize())
		{
			throw new IllegalArgumentException("Whitening values must have same length as block");
		}

		mWhiteningA = aWhiteningA.clone();
		mWhiteningB = aWhiteningB.clone();
		mUseWhitening = true;
	}


	public byte [][] getWhitening()
	{
		if (!mUseWhitening)
		{
			return null;
		}

		return new byte[][]{mWhiteningA.clone(), mWhiteningB.clone()};
	}


	public void setIV(byte [] aIV)
	{
		if (mCipherMode == CipherMode.CBC && aIV == null)
		{
			throw new IllegalArgumentException("An IV is required in CBC mode.");
		}
		if (aIV != null && aIV.length < engineGetBlockSize())
		{
			throw new IllegalArgumentException("The aIV length must be equal or greater than the cipher block size: aIV length: " + aIV.length + ", cipher block size: " + engineGetBlockSize());
		}

		if (mCipherMode == CipherMode.CBC)
		{
			if (mInitialIV == null || mIV == null)
			{
				mInitialIV = new byte[engineGetBlockSize()];
				mIV = new byte[engineGetBlockSize()];
			}

			System.arraycopy(aIV, 0, mInitialIV, 0, engineGetBlockSize());
			System.arraycopy(aIV, 0, mIV, 0, engineGetBlockSize());
		}
		else
		{
			mInitialIV = null;
			mIV = null;
		}
	}


	public byte [] getIV()
	{
		switch (mCipherMode)
		{
			case ECB:
				return null;
			case CBC:
				return mInitialIV.clone();
			default:
				throw new IllegalArgumentException("CipherMode not supported: " + mCipherMode);
		}
	}


	public void update(byte [] aBuffer, int aOffset, int aLength)
	{
		update(aBuffer, aOffset, aBuffer, aOffset, aLength);
	}


	public void update(byte [] aInput, int aInputOffset, byte [] aOutput, int aOutputOffset, int aInputLength)
	{
		if (aInputLength % engineGetBlockSize() != 0)
		{
			throw new IllegalArgumentException("aInputLength must be a multiple of the cipher block size: aInputLength: " + aInputLength + ", cipher block size: " + engineGetBlockSize());
		}

		switch (mCipherMode)
		{
			case ECB:
				switch (mTransformation)
				{
					case ENCRYPT: encryptECB(aInput, aInputOffset, aOutput, aOutputOffset, aInputLength); break;
					case DECRYPT: decryptECB(aInput, aInputOffset, aOutput, aOutputOffset, aInputLength); break;
				}
				break;
			case CBC:
				switch (mTransformation)
				{
					case ENCRYPT: encryptCBC(aInput, aInputOffset, aOutput, aOutputOffset, aInputLength); break;
					case DECRYPT: decryptCBC(aInput, aInputOffset, aOutput, aOutputOffset, aInputLength); break;
				}
				break;
			default:
				throw new IllegalArgumentException("CipherMode not supported: " + mCipherMode);
		}
	}


	public int getBlockSize()
	{
		return engineGetBlockSize();
	}


	public int getKeySize()
	{
		return engineGetKeySize();
	}


	protected abstract void engineInit(SecretKey aSecretKey);


	/**
	 * Encrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in
	 *    A buffer containing the plaintext to be encrypted.
	 * @param inOffset
	 *    Index in the in buffer where plaintext should be read.
	 * @param out
	 *    A buffer where ciphertext is written.
	 * @param outOffset
	 *    Index in the out buffer where ciphertext should be written.
	 */
	public abstract void engineEncryptBlock(byte [] in, int inOffset, byte [] out, int outOffset);


	/**
	 * Decrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in
	 *    A buffer containing the ciphertext to be decrypted.
	 * @param inOffset
	 *    Index in the in buffer where ciphertext should be read.
	 * @param out
	 *    A buffer where plaintext is written.
	 * @param outOffset
	 *    Index in the out buffer where plaintext should be written.
	 */
	public abstract void engineDecryptBlock(byte [] in, int inOffset, byte [] out, int outOffset);


	/**
	 * Encrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in
	 *    A buffer containing the plaintext to be encrypted.
	 * @param inOffset
	 *    Index in the in buffer where plaintext should be read.
	 * @param out
	 *    A buffer where ciphertext is written.
	 * @param outOffset
	 *    Index in the out buffer where ciphertext should be written.
	 */
	public abstract void engineEncryptBlock(int [] in, int inOffset, int [] out, int outOffset);


	/**
	 * Decrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in
	 *    A buffer containing the ciphertext to be decrypted.
	 * @param inOffset
	 *    Index in the in buffer where ciphertext should be read.
	 * @param out
	 *    A buffer where plaintext is written.
	 * @param outOffset
	 *    Index in the out buffer where plaintext should be written.
	 */
	public abstract void engineDecryptBlock(int [] in, int inOffset, int [] out, int outOffset);


	/**
	 * Returns the block size in bytes.
	 */
	protected abstract int engineGetBlockSize();


	/**
	 * Returns the key size in bytes.
	 */
	protected abstract int engineGetKeySize();


	/**
	 * Resets all internal state data. This Cipher object needs to be
	 * reinitialized again before it can be used again.
	 */
	protected abstract void engineReset();


	/**
	 * This method returns a new instance of the Cipher.
	 */
	public abstract Cipher newInstance();


	/**
	 * This method returns a new instance of the Cipher.
	 */
	public Cipher newInstance(SecretKey aKey)
	{
		Cipher c = newInstance();
		c.init(aKey);
		return c;
	}


	/**
	 * Resets all internal state data. This Cipher object needs to be
	 * reinitialized again before it can be used again.
	 */
	public void reset()
	{
		engineReset();

		mTransformation = ENCRYPT;
		mCipherMode = CipherMode.ECB;
		burn(mIV);
		burn(mInitialIV);
		burn(mTemp1);
		burn(mTemp2);
		burn(mWhiteningA);
		burn(mWhiteningB);
		mKeyInitialized = false;
		mUseWhitening = false;
	}


	private static void burn(byte [] aBuffer)
	{
		if (aBuffer != null)
		{
			Arrays.fill(aBuffer, (byte)255);
			Arrays.fill(aBuffer, (byte)0);
		}
	}


	private void encryptECB(byte [] aInput, int aInputOffset, byte [] aOutput, int aOutputOffset, int aInputLength)
	{
		if (mUseWhitening)
		{
			for (int blockSize = engineGetBlockSize(); aInputLength > 0; aInputLength -= blockSize, aInputOffset += blockSize, aOutputOffset += blockSize)
			{
				for (int i = blockSize; --i >= 0;)
				{
					mTemp1[i] = (byte)(aInput[aInputOffset + i] ^ mWhiteningA[i]);
				}

				engineEncryptBlock(mTemp1, 0, aOutput, aOutputOffset);

				for (int i = blockSize; --i >= 0;)
				{
					aOutput[aOutputOffset + i] ^= mWhiteningB[i];
				}
			}
		}
		else
		{
			for (int blockSize = engineGetBlockSize(); aInputLength > 0; aInputLength -= blockSize, aInputOffset += blockSize, aOutputOffset += blockSize)
			{
				engineEncryptBlock(aInput, aInputOffset, aOutput, aOutputOffset);
			}
		}
	}


	private void decryptECB(byte [] aInput, int aInputOffset, byte [] aOutput, int aOutputOffset, int aInputLength)
	{
		if (mUseWhitening)
		{
			for (int blockSize = engineGetBlockSize(); aInputLength > 0; aInputLength -= blockSize, aInputOffset += blockSize, aOutputOffset += blockSize)
			{
				for (int i = blockSize; --i >= 0;)
				{
					mTemp1[i] = (byte)(aInput[aInputOffset + i] ^ mWhiteningB[i]);
				}

				engineDecryptBlock(mTemp1, 0, aOutput, aOutputOffset);

				for (int i = blockSize; --i >= 0;)
				{
					aOutput[aOutputOffset + i] ^= mWhiteningA[i];
				}
			}
		}
		else
		{
			for (int blockSize = engineGetBlockSize(); aInputLength > 0; aInputLength -= blockSize, aInputOffset += blockSize, aOutputOffset += blockSize)
			{
				engineDecryptBlock(aInput, aInputOffset, aOutput, aOutputOffset);
			}
		}
	}


	private void encryptCBC(byte [] aInput, int aInputOffset, byte [] aOutput, int aOutputOffset, int aInputLength)
	{
		if (mUseWhitening)
		{
			byte [] iv = mIV;

			for (int blockSize = engineGetBlockSize(); aInputLength > 0; aInputLength -= blockSize, aInputOffset += blockSize, aOutputOffset += blockSize)
			{
				for (int i = blockSize; --i >= 0;)
				{
					iv[i] ^= aInput[aInputOffset+i] ^ mWhiteningA[i];
				}

				engineEncryptBlock(iv, 0, iv, 0);

				for (int i = blockSize; --i >= 0;)
				{
					aOutput[aOutputOffset+i] = (byte)(iv[i] ^ mWhiteningB[i]);
				}
			}
		}
		else
		{
			byte [] iv = mIV;

			for (int blockSize = engineGetBlockSize(); aInputLength > 0; aInputLength -= blockSize, aInputOffset += blockSize, aOutputOffset += blockSize)
			{
				for (int i = blockSize; --i >= 0;)
				{
					iv[i] ^= aInput[aInputOffset+i];
				}

				engineEncryptBlock(iv, 0, iv, 0);

				System.arraycopy(iv, 0, aOutput, aOutputOffset, blockSize);
			}
		}
	}


	private void decryptCBC(byte [] aInput, int aInputOffset, byte [] aOutput, int aOutputOffset, int aInputLength)
	{
		if (mUseWhitening)
		{
			byte [] iv = mIV;
			byte [] nextIV = mTemp2;

			for (int blockSize = engineGetBlockSize(); aInputLength > 0; aInputLength -= blockSize, aInputOffset += blockSize, aOutputOffset += blockSize)
			{
				for (int i = blockSize; --i >= 0;)
				{
					mTemp1[i] = (byte)(aInput[aInputOffset+i] ^ mWhiteningB[i]);
				}

				System.arraycopy(mTemp1, 0, nextIV, 0, blockSize);

				engineDecryptBlock(mTemp1, 0, mTemp1, 0);

				for (int i = blockSize; --i >= 0;)
				{
					aOutput[aOutputOffset+i] = (byte)(mTemp1[i] ^ iv[i] ^ mWhiteningA[i]);
				}

				System.arraycopy(nextIV, 0, iv, 0, blockSize);
			}
		}
		else
		{
			byte [] iv = mIV;
			byte [] nextIV = mTemp1;

			for (int blockSize = engineGetBlockSize(); aInputLength > 0; aInputLength -= blockSize, aInputOffset += blockSize, aOutputOffset += blockSize)
			{
				System.arraycopy(aInput, aInputOffset, nextIV, 0, blockSize);

				engineDecryptBlock(aInput, aInputOffset, aOutput, aOutputOffset);

				for (int i = blockSize; --i >= 0;)
				{
					aOutput[aOutputOffset+i] ^= iv[i];
				}

				System.arraycopy(nextIV, 0, iv, 0, blockSize);
			}
		}
	}
}