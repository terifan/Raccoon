package org.terifan.raccoon.security;

import java.util.Arrays;


public final class TranspositionDiffuser
{
	private transient final int[] mEncodeOrder;
	private transient final int[] mDecodeOrder;
	private transient final int mUnitSize;
	private transient final byte[] mWork;


	public TranspositionDiffuser(byte[] aSeed, int aUnitSize)
	{
		mUnitSize = aUnitSize;

		mEncodeOrder = new int[mUnitSize];
		mDecodeOrder = new int[mUnitSize];
		mWork = new byte[mUnitSize];

		for (int i = 0; i < mEncodeOrder.length; i++)
		{
			mEncodeOrder[i] = i;
		}

		Random prng = new Random(aSeed);

		for (int i = 0; i < mUnitSize; i++)
		{
			int j = prng.nextInt() & (mUnitSize - 1);
			int t = mEncodeOrder[i];
			mEncodeOrder[i] = mEncodeOrder[j];
			mEncodeOrder[j] = t;
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


	public void encode(byte[] aBuffer, int aOffset, int aLength, int aBlockKey)
	{
		int limit = mUnitSize - 1;

		for (int unitIndex = 0, offset = aOffset, numDataUnits = aLength / mUnitSize; unitIndex < numDataUnits; unitIndex++, offset += mUnitSize)
		{
			for (int i = 0; i < mUnitSize; i++)
			{
				mWork[i] = aBuffer[offset + mEncodeOrder[(aBlockKey + i) & limit]];
			}

			System.arraycopy(mWork, 0, aBuffer, offset, mUnitSize);
			aBlockKey = Integer.rotateRight(aBlockKey, 1) ^ ((aBlockKey & 1) << 8);
		}
	}


	public void decode(byte[] aBuffer, int aOffset, int aLength, int aBlockKey)
	{
		int limit = mUnitSize - 1;

		for (int unitIndex = 0, offset = aOffset, numDataUnits = aLength / mUnitSize; unitIndex < numDataUnits; unitIndex++, offset += mUnitSize)
		{
			for (int i = 0; i < mUnitSize; i++)
			{
				mWork[i] = aBuffer[offset + ((mDecodeOrder[i] - aBlockKey) & limit)];
			}

			System.arraycopy(mWork, 0, aBuffer, offset, mUnitSize);
			aBlockKey = Integer.rotateRight(aBlockKey, 1) ^ ((aBlockKey & 1) << 8);
		}
	}
}
