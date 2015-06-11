package org.terifan.raccoon.security;


public abstract class Cipher
{
	Cipher()
	{
	}


	Cipher(SecretKey aSecretKey)
	{
		engineInit(aSecretKey);
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
//	public abstract void engineEncryptBlock(byte [] in, int inOffset, byte [] out, int outOffset);


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
//	public abstract void engineDecryptBlock(byte [] in, int inOffset, byte [] out, int outOffset);


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
	 * Resets all internal state data.
	 */
	protected abstract void engineReset();


	/**
	 * This method returns a new instance of the Cipher.
	 */
	public abstract Cipher newInstance();


	public void init(SecretKey aSecretKey)
	{
		engineInit(aSecretKey);
	}


	/**
	 * Resets all internal state data.
	 */
	public void reset()
	{
		engineReset();
	}
}