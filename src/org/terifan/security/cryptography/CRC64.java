package org.terifan.security.cryptography;


public final class CRC64
{
	private final static long POLY64REV = 0xd800000000000000L;
	private final static long[] LOOKUPTABLE;
	
	static
	{
		LOOKUPTABLE = new long[0x100];
		for (int i = 0; i < 0x100; i++)
		{
			long v = i;
			for (int j = 0; j < 8; j++)
			{
				if ((v & 1) == 1)
				{
					v = (v >>> 1) ^ POLY64REV;
				}
				else
				{
					v = (v >>> 1);
				}
			}
			LOOKUPTABLE[i] = v;
		}
	}


	private CRC64()
	{
	}

	
	public static long checksum(byte[] aData, int aOffset, int aLength)
	{
		long sum = 0;
		while (aLength-- > 0)
		{
			sum = (sum >>> 8) ^ LOOKUPTABLE[((int)sum ^ aData[aOffset++]) & 0xff];
		}
		return sum;
	}
}
