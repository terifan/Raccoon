package org.terifan.security.cryptography;

import static org.terifan.raccoon.util.ByteArrayUtil.*;


public abstract class CipherMode
{
	public CipherMode()
	{
	}


	/**
	 * Encrypts a buffer using the cipher mode and the provided ciphers.
	 *
	 * @param aBuffer the buffer to encrypt
	 * @param aOffset the start offset in the buffer
	 * @param aLength number of bytes to encrypt; must be divisible by 16
	 * @param aStartDataUnitNo the sequential number of the data unit with which the buffer starts.
	 * @param aCipher the primary key schedule
	 * @param aTweak the secondary key schedule
	 * @param aIV initialization vector used for diffusing cipher text
	 * @param aBlockKey a value nonce value used in the encryption
	 */
	public abstract void encrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final long aStartDataUnitNo, final int aUnitSize, final long[] aMasterIV, final long[] aIV, BlockCipher aTweakCipher);


	/**
	 * Decrypts a buffer using the cipher mode and the provided ciphers.
	 *
	 * @param aBuffer the buffer to decrypt
	 * @param aOffset the start offset in the buffer
	 * @param aLength number of bytes to decrypt; must be divisible by 16
	 * @param aStartDataUnitNo the sequential number of the data unit with which the buffer starts.
	 * @param aCipher the primary key schedule
	 * @param aTweak the secondary key schedule
	 * @param aIV initialization vector used for diffusing cipher text
	 * @param aBlockKey a nonce value used in the encryption
	 */
	public abstract void decrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final long aStartDataUnitNo, final int aUnitSize, final long[] aMasterIV, final long[] aIV, BlockCipher aTweakCipher);


	protected static void prepareIV(long[] aMasterIV, long[] aBlockIV, long aDataUnitNo, byte[] aOutputIV, BlockCipher aTweakCipher)
	{
		putLongLE(aOutputIV, 0, aBlockIV[0] ^ aMasterIV[0]);
		putLongLE(aOutputIV, 8, aBlockIV[1] ^ aMasterIV[1] ^ aDataUnitNo);
		aTweakCipher.engineEncryptBlock(aOutputIV, 0, aOutputIV, 0);
	}
}
