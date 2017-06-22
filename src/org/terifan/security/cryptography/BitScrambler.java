package org.terifan.security.cryptography;


/**
 * The BitScrambler is a simple transposition cipher rearranging data randomly on bit level over a wide block.
 */
public class BitScrambler
{
	public static void scramble(long aKey, byte[] aBuffer)
	{
		mixBits(aBuffer, aKey, 8 * aBuffer.length - 1, -1, -1);
	}


	public static void unscramble(long aKey, byte[] aBuffer)
	{
		mixBits(aBuffer, aKey, 0, 8 * aBuffer.length, 1);
	}


	private static void mixBits(byte[] aBuffer, long aKey, int aStart, int aEnd, int aDirection)
	{
		int size = 8 * aBuffer.length;

		assert (size & -size) == size;

		ISAAC rnd = new ISAAC(aKey);

		int[] order = new int[size];
		for (int i = 0; i < size; i++)
		{
			order[i] = rnd.nextInt(size);
		}

		for (int p0 = aStart; p0 != aEnd; p0+=aDirection)
		{
			int p1 = order[p0];

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
}
