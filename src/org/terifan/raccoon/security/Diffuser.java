package org.terifan.raccoon.security;

import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.util.Log;


public final class Diffuser
{
	private final int mBlockSize;
	private final int[] mTranspose1;
	private final int[] mTranspose2;
	private final byte[] mWork;


	public Diffuser(int aBlockSize)
	{
		mBlockSize = aBlockSize;
		
		mWork = new byte[mBlockSize];
		mTranspose1 = new int[mBlockSize];
		mTranspose2 = new int[mBlockSize];

		for (int i = 0; i < mTranspose1.length; i++)
		{
			mTranspose1[i] = i;
		}

		Random r = new Random(1);
		for (int h = 0; h < 4; h++)
		{
			for (int i = 0; i < mTranspose1.length; i++)
			{
				int j = r.nextInt(mBlockSize);
				int k = mTranspose1[i];
				mTranspose1[i] = mTranspose1[j];
				mTranspose1[j] = k;
			}
		}

		for (int i = 0; i < mTranspose1.length; i++)
		{
			mTranspose2[mTranspose1[i]] = i;
		}
	}


	public int getBlockSize()
	{
		return mBlockSize;
	}


	public void encode(final byte[] aBuffer, final int aShift1)
	{
		int len = mBlockSize;

		for (int i = 0; i < len; i++)
		{
			mWork[i] = aBuffer[mTranspose1[(i + aShift1) & (mBlockSize-1)]];
		}

		System.arraycopy(mWork, 0, aBuffer, 0, len);
	}


	public void decode(final byte[] aBuffer, final int aShift1)
	{
		int len = mBlockSize;

		for (int i = 0; i < len; i++)
		{
			mWork[i] = aBuffer[(mTranspose2[i] - aShift1) & (mBlockSize-1)];
		}

		System.arraycopy(mWork, 0, aBuffer, 0, len);
	}


	public static void main(String ... args)
	{
		try
		{
			int bs = 256;
			byte[] plain = new byte[bs];
			for (int i = 0; i < bs; i++) 
			{
				plain[i] = (byte)i;
			}

			Log.hexDump(plain);

			Diffuser diffuser = new Diffuser(bs);
			byte[] encoded = plain.clone();
			diffuser.encode(encoded, 47);

			Log.out.println("-----------------------------------------------------------------------------");
			Log.hexDump(encoded);

			byte[] decoded = encoded.clone();
			diffuser.decode(decoded, 47);

			Log.out.println("-----------------------------------------------------------------------------");
			Log.hexDump(decoded);

			Log.out.println("-----------------------------------------------------------------------------");
			Log.out.println(Arrays.equals(plain, decoded));
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}