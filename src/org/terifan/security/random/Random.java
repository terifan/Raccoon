package org.terifan.security.random;

import java.util.Arrays;
import org.terifan.security.cryptography.AES;
import org.terifan.security.cryptography.BlockCipher;
import org.terifan.security.cryptography.SecretKey;


/**
 * Deterministic AES-CTR random number generator. This implementation is only safe to produce maximum 2^32 numbers.
 */
public final class Random
{
	private final transient BlockCipher mCipher;
	private final transient int[] mCounter;
	private final transient int[] mOutput;


	/**
	 *
	 * @param aSeed
	 *   32 bytes of seed. The seed value is assumed to be generated using a secure random number generator or some form of encryption.
	 */
	public Random(byte[] aSeed)
	{
		assert aSeed.length == 32;

		mOutput = new int[4];

		mCipher = new AES(new SecretKey(Arrays.copyOfRange(aSeed, 16, 32)));

		mCounter = new int[4];
		mCounter[0] = readInt32(aSeed, 0);
		mCounter[1] = readInt32(aSeed, 4);
		mCounter[2] = readInt32(aSeed, 8);
		mCounter[3] = readInt32(aSeed, 12);
	}


	public int nextInt()
	{
		mCounter[0]++;

		mCipher.engineEncryptBlock(mCounter, 0, mOutput, 0);

		return mOutput[0] ^ mOutput[1] ^ mOutput[2] ^ mOutput[3];
	}


	public void reset()
	{
		mCipher.engineReset();
		Arrays.fill(mCounter, 0);
		Arrays.fill(mOutput, 0);
	}


	private int readInt32(byte[] aBuffer, int aOffset)
	{
		int ch1 = 0xff & aBuffer[aOffset++];
		int ch2 = 0xff & aBuffer[aOffset++];
		int ch3 = 0xff & aBuffer[aOffset++];
		int ch4 = 0xff & aBuffer[aOffset];
		return (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4;
	}
}