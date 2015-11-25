package org.terifan.raccoon.security;

import java.util.Arrays;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public final class Random
{
	private final transient Cipher mCipher;
	private final transient int[] mCounter;
	private final transient int[] mState;
	private transient int mIndex;


	public Random(byte[] aSeed, byte[] aCounter)
	{
		assert aSeed.length == 16;
		assert aCounter.length == 16;

		mState = new int[4];

		mCipher = new AES(new SecretKey(aSeed));

		mCounter = new int[4];
		mCounter[0] = ByteArrayBuffer.readInt32(aCounter, 0);
		mCounter[1] = ByteArrayBuffer.readInt32(aCounter, 4);
		mCounter[2] = ByteArrayBuffer.readInt32(aCounter, 8);
		mCounter[3] = ByteArrayBuffer.readInt32(aCounter, 12);

		mIndex = mCounter[3];
	}


	public int nextInt()
	{
		mCounter[3] = mIndex++;

		mCipher.engineEncryptBlock(mCounter, 0, mState, 0);

		return mState[0] ^ mState[1];
	}


	public void reset()
	{
		mCipher.engineReset();
		Arrays.fill(mCounter, 0);
		Arrays.fill(mState, 0);
	}


	public static void main(String... args)
	{
		try
		{
			Random random = new Random("0123456789abcdef".getBytes(), "0123456789abcdef".getBytes());

			int zero = 0;
			int n = 10000000;
			for (int i = 0; i < n; i++)
			{
				if ((random.nextInt() & 1) == 0) zero++;
			}

			Log.out.println(zero);
			Log.out.println(n-zero);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
