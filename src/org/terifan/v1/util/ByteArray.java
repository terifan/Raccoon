package org.terifan.v1.util;


public class ByteArray
{
	private ByteArray()
	{
	}


	public static long getUnsignedInt(byte [] aBuffer, int aPosition)
	{
		return getInt(aBuffer, aPosition) & 0xFFFFFFFFL;
	}


	public static void put(byte [] aBuffer, int aPosition, byte [] aSrcBuffer)
	{
		System.arraycopy(aSrcBuffer, 0, aBuffer, aPosition, aSrcBuffer.length);
	}


	public static int getInt(byte [] aBuffer, int aPosition)
	{
		return (int)(((aBuffer[aPosition++] & 255) << 24)
				   + ((aBuffer[aPosition++] & 255) << 16)
				   + ((aBuffer[aPosition++] & 255) <<  8)
				   + ((aBuffer[aPosition  ] & 255)      ));
	}


	public static long getLong(byte [] aBuffer, int aPosition)
	{
		return (((long)(aBuffer[aPosition++]      ) << 56)
			  + ((long)(aBuffer[aPosition++] & 255) << 48)
			  + ((long)(aBuffer[aPosition++] & 255) << 40)
			  + ((long)(aBuffer[aPosition++] & 255) << 32)
			  + ((long)(aBuffer[aPosition++] & 255) << 24)
			  + (      (aBuffer[aPosition++] & 255) << 16)
			  + (      (aBuffer[aPosition++] & 255) <<  8)
			  + (      (aBuffer[aPosition  ] & 255)      ));
	}


	public static void putInt(byte [] aBuffer, int aPosition, int aValue)
	{
		aBuffer[aPosition++] = (byte)(aValue >>> 24);
		aBuffer[aPosition++] = (byte)(aValue >>  16);
		aBuffer[aPosition++] = (byte)(aValue >>   8);
		aBuffer[aPosition  ] = (byte)(aValue       );
	}


	public static void putLong(byte [] aBuffer, int aPosition, long aValue)
	{
		aBuffer[aPosition++] = (byte)(aValue >>> 56);
		aBuffer[aPosition++] = (byte)(aValue >>  48);
		aBuffer[aPosition++] = (byte)(aValue >>  40);
		aBuffer[aPosition++] = (byte)(aValue >>  32);
		aBuffer[aPosition++] = (byte)(aValue >>  24);
		aBuffer[aPosition++] = (byte)(aValue >>  16);
		aBuffer[aPosition++] = (byte)(aValue >>   8);
		aBuffer[aPosition  ] = (byte)(aValue       );
	}


	public static boolean equals(byte[] aArrayA, int aOffsetA, byte[] aArrayB, int aOffsetB, int aLength)
	{
		if (aArrayA.length < aOffsetA + aLength)
		{
			return false;
		}
		if (aArrayB.length < aOffsetB + aLength)
		{
			return false;
		}
		for (int i = 0; i < aLength; i++)
		{
			if (aArrayA[aOffsetA++] != aArrayB[aOffsetB++])
			{
				return false;
			}
		}
		return true;
	}
}