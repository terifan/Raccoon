package org.terifan.raccoon.security;

import java.util.Arrays;
import org.terifan.raccoon.util.ByteArrayBuffer;


/**
 * AES-CTR random number generator. This implementation is only safe to produce maximum 2^32 numbers.
 */
public final class Random
{
	private final transient Cipher mCipher;
	private final transient int[] mCounter;
	private final transient int[] mOutput;


	public Random(byte[] aSeed, byte[] aCounter)
	{
		assert aSeed.length == 16;
		assert aCounter.length == 16;

		mOutput = new int[4];

		mCipher = new AES(new SecretKey(aSeed));

		mCounter = new int[4];
		mCounter[0] = ByteArrayBuffer.readInt32(aCounter, 0);
		mCounter[1] = ByteArrayBuffer.readInt32(aCounter, 4);
		mCounter[2] = ByteArrayBuffer.readInt32(aCounter, 8);
		mCounter[3] = ByteArrayBuffer.readInt32(aCounter, 12);
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
}
