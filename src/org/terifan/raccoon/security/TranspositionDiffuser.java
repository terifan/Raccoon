package org.terifan.raccoon.security;

import java.util.Arrays;


public final class TranspositionDiffuser
{
	private transient final int[] mEncodeOrder;
	private transient final int[] mDecodeOrder;
	private transient final int mUnitSize;
	private transient final byte[] mWork;
	
	
	public TranspositionDiffuser(byte[] aSeed, byte[] aCounter, int aUnitSize)
	{
		mUnitSize = aUnitSize;
		mEncodeOrder = new int[mUnitSize];
		mDecodeOrder = new int[mUnitSize];
		mWork = new byte[mUnitSize];

		for (int i = 0; i < mEncodeOrder.length; i++)
		{
			mEncodeOrder[i] = i;
		}

		Random prng = new Random(aSeed, aCounter);

		for (int i = 0; i < mUnitSize; i++)
		{
			int p = i & (mUnitSize - 1);
			int q = prng.nextInt() & (mUnitSize - 1);
			int t = mEncodeOrder[p];
			mEncodeOrder[p] = mEncodeOrder[q];
			mEncodeOrder[q] = t;
		}

		for (int i = 0; i < mEncodeOrder.length; i++)
		{
			mDecodeOrder[mEncodeOrder[i]] = i;
		}

		prng.reset();
	}
	
	
	public void reset()
	{
		Arrays.fill(mEncodeOrder, 0);
		Arrays.fill(mDecodeOrder, 0);
		Arrays.fill(mWork, (byte)0);
	}

	
	public void encode(final byte[] aBuffer, int aOffset, int aLength)
	{
		for (int unitIndex = 0, offset = aOffset, numDataUnits = aLength / mUnitSize; unitIndex < numDataUnits; unitIndex++, offset += mUnitSize)
		{
			for (int i = 0; i < mUnitSize; i++)
			{
				mWork[i] = aBuffer[offset + mEncodeOrder[i]];
			}

			System.arraycopy(mWork, 0, aBuffer, offset, mUnitSize);
		}
	}


	public void decode(final byte[] aBuffer, int aOffset, int aLength)
	{
		for (int unitIndex = 0, offset = aOffset, numDataUnits = aLength / mUnitSize; unitIndex < numDataUnits; unitIndex++, offset += mUnitSize)
		{
			for (int i = 0; i < mUnitSize; i++)
			{
				mWork[i] = aBuffer[offset + mDecodeOrder[i]];
			}

			System.arraycopy(mWork, 0, aBuffer, offset, mUnitSize);
		}
	}
}
