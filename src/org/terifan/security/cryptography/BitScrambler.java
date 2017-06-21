package org.terifan.security.cryptography;

import java.util.Random;
import org.terifan.raccoon.util.Log;


public class BitScrambler
{
	public static void main(String... args)
	{
		try
		{
			byte[] buffer = new byte[256];
			new Random(777).nextBytes(buffer);

			long key = 1;

			Log.hexDump(buffer);

			scramble(key, buffer);

			System.out.println();
			Log.hexDump(buffer);
			System.out.println();

			unscramble(key, buffer);

			Log.hexDump(buffer);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	public static void scramble(long aKey, byte[] aBuffer)
	{
		assert (aBuffer.length & -aBuffer.length) == aBuffer.length;

		PRNG rnd = new PRNG(aKey);

		int size = 8 * aBuffer.length;
		int[][] order = new int[2][size];
		
		for (int p = 0, q = size - 1; p < size; p++, q--)
		{
			order[0][q] = p;
			order[1][q] = rnd.nextInt(size);
		}

		mixBits(aBuffer, order, size);
	}


	public static void unscramble(long aKey, byte[] aBuffer)
	{
		assert (aBuffer.length & -aBuffer.length) == aBuffer.length;

		PRNG rnd = new PRNG(aKey);

		int size = 8 * aBuffer.length;
		int[][] order = new int[2][size];

		for (int p = 0; p < size; p++)
		{
			order[0][p] = p;
			order[1][p] = rnd.nextInt(size);
		}

		mixBits(aBuffer, order, size);
	}


	private static void mixBits(byte[] aBuffer, int[][] aOrder, int aRange)
	{
		for (int i = 0; i < aRange; i++)
		{
			int p0 = aOrder[0][i];
			int p1 = aOrder[1][i];

			boolean b0 = isSet(aBuffer, p0);
			boolean b1 = isSet(aBuffer, p1);

			if (b0)
			{
				clearBit(aBuffer, p1);
			}
			else
			{
				setBit(aBuffer, p1);
			}

			if (b1)
			{
				clearBit(aBuffer, p0);
			}
			else
			{
				setBit(aBuffer, p0);
			}
		}
	}


	private static boolean isSet(byte[] aBuffer, int aPosition)
	{
		return (aBuffer[aPosition >>> 3] & (1 << (aPosition & 7))) == 0;
	}


	private static void setBit(byte[] aBuffer, int aPosition)
	{
		aBuffer[aPosition >>> 3] |= (1 << (aPosition & 7));
	}


	private static void clearBit(byte[] aBuffer, int aPosition)
	{
		aBuffer[aPosition >>> 3] &= ~(1 << (aPosition & 7));
	}


	private static class PRNG
	{
		private static final long MULTIPLIER = 0x5DEECE66DL;
		private static final long ADDED = 0xBL;
		private static final long MASK = (1L << 48) - 1;
		private long mSeed;


		private PRNG(long aSeed)
		{
			mSeed = (aSeed ^ MULTIPLIER) & MASK;
		}


		private int nextInt(int aBound)
		{
			mSeed = (mSeed * MULTIPLIER + ADDED) & MASK;

			return (int)((mSeed >>> 17) & (aBound - 1));
		}
	}
}
