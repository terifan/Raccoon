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

			if (b0 != b1)
			{
				if (b0)
				{
					clearBit(aBuffer, p0);
					setBit(aBuffer, p1);
				}
				else
				{
					setBit(aBuffer, p0);
					clearBit(aBuffer, p1);
				}
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
//			byte[] original = new byte[48];
//			ISAAC.PRNG.nextBytes(original);
//
//			long k = ISAAC.PRNG.nextLong();
//
//			byte[] encodedError = original.clone();
//			BitScrambler.scramble(k, encodedError);
//
//			encodedError[0] ^= 1;
//
//			byte[] decodedError = encodedError.clone();
//			BitScrambler.unscramble(k, decodedError);
//
//
//			byte[] encoded = original.clone();
//			BitScrambler.scramble(k, encoded);
//
//			byte[] decoded = encoded.clone();
//			BitScrambler.unscramble(k, decoded);
//
//			Log.hexDump(original);
//			System.out.println();
//			Log.hexDump(encodedError);
//			Log.hexDump(encoded);
//
//			System.out.print("      ");
//			for (int i = 0; i < original.length; )
//			{
//				for (int j = 0; j<8 && i < original.length; i++,j++)
//				{
//					if (encoded[i]!=encodedError[i])
//					{
//						System.out.print(" * ");
//					}
//					else
//						System.out.print("   ");
//				}
//				System.out.print(" ");
//			}
//			System.out.println();
//
//			System.out.println();
//			Log.hexDump(decodedError);
//			Log.hexDump(decoded);
//
//			System.out.print("      ");
//			for (int i = 0; i < original.length; )
//			{
//				for (int j = 0; j<8 && i < original.length; i++,j++)
//				{
//					if (decoded[i]!=decodedError[i])
//					{
//						System.out.print(" * ");
//					}
//					else
//						System.out.print("   ");
//				}
//				System.out.print(" ");
//			}
//			System.out.println();
//		}
//		catch (Throwable e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
}
