package org.terifan.security.cryptography;


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
		assert (aBuffer.length & -aBuffer.length) == aBuffer.length;

		int size = 8 * aBuffer.length;

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

			if (b0 == b1)
			{
				b0 = b1 = !b0;
			}

			if (b0)
			{
				setBit(aBuffer, p1);
			}
			else
			{
				clearBit(aBuffer, p1);
			}

			if (b1)
			{
				setBit(aBuffer, p0);
			}
			else
			{
				clearBit(aBuffer, p0);
			}
		}
	}


	private static boolean isSet(byte[] aBuffer, int aPosition)
	{
		return (aBuffer[aPosition >>> 3] & (1 << (aPosition & 7))) != 0;
	}


	private static void setBit(byte[] aBuffer, int aPosition)
	{
		aBuffer[aPosition >>> 3] |= (1 << (aPosition & 7));
	}


	private static void clearBit(byte[] aBuffer, int aPosition)
	{
		aBuffer[aPosition >>> 3] &= ~(1 << (aPosition & 7));
	}


//	public static void main(String ... args)
//	{
//		try
//		{
//			int len = 4;
//
//			int[] stats = new int[8 * len];
//
//			for (int test = 0; test < 100000; test++)
//			{
//			byte[] buffer = new byte[len];
////			ISAAC.PRNG.nextBytes(buffer);
//
//			int key = ISAAC.PRNG.nextInt();
//
////			Log.hexDump(buffer);
//
//			BitScrambler.scramble(key, buffer);
//
////			Log.hexDump(buffer);
//
//			for (int i = 0; i < stats.length; i++)
//			{
//				if (isSet(buffer, i)) stats[i]++;
//			}
//			}
//
//			for (int i = 0; i < stats.length; i++)
//				System.out.print(stats[i]+" ");
//			System.out.println();
//
////			BitScrambler.unscramble(key, buffer);
////
////			Log.hexDump(buffer);
//		}
//		catch (Throwable e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
}
