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
	public abstract void encrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final BlockCipher aTweak, final long aStartDataUnitNo, final int aUnitSize, final byte[] aIV, final long aBlockKey);


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
	public abstract void decrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final BlockCipher aTweak, final long aStartDataUnitNo, final int aUnitSize, final byte[] aIV, final long aBlockKey);


	protected static void prepareIV(long aDataUnitNo, byte[] aInputIV, BlockCipher aTweak, long aBlockKey, byte[] aOutputIV, int aLength)
	{
		System.arraycopy(aInputIV, 0, aOutputIV, 0, aLength);
		xorLong(aOutputIV, 0, aBlockKey);
		xorLong(aOutputIV, 8, aDataUnitNo);
		aTweak.engineEncryptBlock(aOutputIV, 0, aOutputIV, 0);
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
}
