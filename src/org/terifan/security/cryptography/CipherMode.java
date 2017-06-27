package org.terifan.security.cryptography;


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
	public abstract void encrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final long aStartDataUnitNo, final int aUnitSize, final long[] aMasterIV, final long aIV0, final long aIV1);


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
	public abstract void decrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final long aStartDataUnitNo, final int aUnitSize, final long[] aMasterIV, final long aIV0, final long aIV1);


	protected static void prepareIV(long[] aMasterIV, long aBlockIV0, long aBlockIV1, long aDataUnitNo, byte[] aOutputIV)
	{
		putLong(aOutputIV, 0, aBlockIV0 ^ aMasterIV[0]);
		putLong(aOutputIV, 8, aBlockIV1 ^ aMasterIV[1] ^ aDataUnitNo);
	}


	protected static void xorLong(byte[] aBuffer, int aOffset, long aValue)
	{
		aBuffer[aOffset + 0] ^= (byte)(aValue >>> 0);
		aBuffer[aOffset + 1] ^= (byte)(aValue >>> 8);
		aBuffer[aOffset + 2] ^= (byte)(aValue >>> 16);
		aBuffer[aOffset + 3] ^= (byte)(aValue >>> 24);
		aBuffer[aOffset + 4] ^= (byte)(aValue >>> 32);
		aBuffer[aOffset + 5] ^= (byte)(aValue >>> 40);
		aBuffer[aOffset + 6] ^= (byte)(aValue >>> 48);
		aBuffer[aOffset + 7] ^= (byte)(aValue >>> 56);
	}


	protected static void xor(byte[] aBuffer, int aOffset, int aLength, byte[] aMask, int aMaskOffset)
	{
		for (int i = 0; i < aLength; i++)
		{
			aBuffer[aOffset + i] ^= aMask[aMaskOffset + i];
		}
	}


	// little endian
	protected static void putLong(byte[] aBuffer, int aOffset, long aValue)
	{
		aBuffer[aOffset + 0] = (byte)(aValue >>> 0);
		aBuffer[aOffset + 1] = (byte)(aValue >>> 8);
		aBuffer[aOffset + 2] = (byte)(aValue >>> 16);
		aBuffer[aOffset + 3] = (byte)(aValue >>> 24);
		aBuffer[aOffset + 4] = (byte)(aValue >>> 32);
		aBuffer[aOffset + 5] = (byte)(aValue >>> 40);
		aBuffer[aOffset + 6] = (byte)(aValue >>> 48);
		aBuffer[aOffset + 7] = (byte)(aValue >>> 56);
	}


	// little endian
	protected static long getLong(byte[] aBuffer, int aOffset)
	{
		return ((255 & aBuffer[aOffset]))
			+ ((255 & aBuffer[aOffset + 1]) << 8)
			+ ((255 & aBuffer[aOffset + 2]) << 16)
			+ ((long)(255 & aBuffer[aOffset + 3]) << 24)
			+ ((long)(255 & aBuffer[aOffset + 4]) << 32)
			+ ((long)(255 & aBuffer[aOffset + 5]) << 40)
			+ ((long)(255 & aBuffer[aOffset + 6]) << 48)
			+ ((long)(255 & aBuffer[aOffset + 7]) << 56);
	}
}
