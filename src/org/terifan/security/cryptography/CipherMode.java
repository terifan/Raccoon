package org.terifan.security.cryptography;


public interface CipherMode
{
	/**
	 * Encrypts a buffer using the cipher mode and the provided ciphers.
	 *
	 * @param aBuffer
	 *   the buffer to encrypt
	 * @param aOffset
	 *   the start offset in the buffer
	 * @param aLength
	 *   number of bytes to encrypt; must be divisible by 16
	 * @param aStartDataUnitNo
	 *   the sequential number of the data unit with which the buffer starts.
	 *   Each data unit is 512 bytes in length.
	 * @param aCipher
	 *   the primary key schedule
	 * @param aTweak
	 *   the secondary key schedule
	 * @param aIV 
	 *   initialization vector used for diffusing cipher text
	 */
	void encrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final BlockCipher aTweak, final long aStartDataUnitNo, final int aUnitSize, final byte[] aIV, final long aBlockKey);


	/**
	 * Decrypts a buffer using the cipher mode and the provided ciphers.
	 *
	 * @param aBuffer
	 *   the buffer to decrypt
	 * @param aOffset
	 *   the start offset in the buffer
	 * @param aLength
	 *   number of bytes to decrypt; must be divisible by 16
	 * @param aStartDataUnitNo
	 *   the sequential number of the data unit with which the buffer starts.
	 *   Each data unit is 512 bytes in length.
	 * @param aCipher
	 *   the primary key schedule
	 * @param aTweak
	 *   the secondary key schedule
	 * @param aIV 
	 *   initialization vector used for diffusing cipher text
	 */
	void decrypt(final byte[] aBuffer, final int aOffset, final int aLength, final BlockCipher aCipher, final BlockCipher aTweak, final long aStartDataUnitNo, final int aUnitSize, final byte[] aIV, final long aBlockKey);
}
