package org.terifan.raccoon.util;


public final class ByteArrayUtil
{
	public static void xorLong(byte[] aBuffer, int aOffset, long aValue)
	{
		aBuffer[aOffset] ^= (byte)aValue;
		aBuffer[aOffset + 1] ^= (byte)(aValue >>> 8);
		aBuffer[aOffset + 2] ^= (byte)(aValue >>> 16);
		aBuffer[aOffset + 3] ^= (byte)(aValue >>> 24);
		aBuffer[aOffset + 4] ^= (byte)(aValue >>> 32);
		aBuffer[aOffset + 5] ^= (byte)(aValue >>> 40);
		aBuffer[aOffset + 6] ^= (byte)(aValue >>> 48);
		aBuffer[aOffset + 7] ^= (byte)(aValue >>> 56);
	}


	public static void xor(byte[] aBuffer, int aOffset, int aLength, byte[] aMask, int aMaskOffset)
	{
		for (int i = 0; i < aLength; i++)
		{
			aBuffer[aOffset + i] ^= aMask[aMaskOffset + i];
		}
	}


	public static byte[] getBytes(byte[] aBuffer, int aOffset, int aLength)
	{
		byte[] buf = new byte[aLength];
		System.arraycopy(aBuffer, aOffset, buf, 0, aLength);
		return buf;
	}


	public static int getInt8(byte[] aBuffer, int aOffset)
	{
		return aBuffer[aOffset] & 0xFF;
	}


	public static void putInt8(byte[] aBuffer, int aOffset, int aValue)
	{
		aBuffer[aOffset] = (byte)aValue;
	}


	public static int getInt16(byte[] aBuffer, int aOffset)
	{
		return ((aBuffer[aOffset] & 0xFF) << 8)
			+ (aBuffer[aOffset + 1] & 0xFF);
	}


	public static void putInt16(byte[] aBuffer, int aPosition, int aValue)
	{
		aBuffer[aPosition++] = (byte)(aValue >> 8);
		aBuffer[aPosition] = (byte)(aValue);
	}


	public static int getInt24(byte[] aBuffer, int aPosition)
	{
		return ((aBuffer[aPosition + 0] & 0xFF) << 16)
			+ ((aBuffer[aPosition + 1] & 0xFF) << 8)
			+ ((aBuffer[aPosition + 2] & 0xFF));
	}


	public static void putInt24(byte[] aBuffer, int aPosition, int aValue)
	{
		aBuffer[aPosition++] = (byte)(aValue >> 16);
		aBuffer[aPosition++] = (byte)(aValue >> 8);
		aBuffer[aPosition] = (byte)(aValue);
	}


	public static int getInt32(byte[] aBuffer, int aPosition)
	{
		return ((aBuffer[aPosition] & 0xFF) << 24)
			+ ((aBuffer[aPosition + 1] & 0xFF) << 16)
			+ ((aBuffer[aPosition + 2] & 0xFF) << 8)
			+ ((aBuffer[aPosition + 3] & 0xFF));
	}


	public static void putInt32(byte[] aBuffer, int aPosition, int aValue)
	{
		aBuffer[aPosition++] = (byte)(aValue >>> 24);
		aBuffer[aPosition++] = (byte)(aValue >> 16);
		aBuffer[aPosition++] = (byte)(aValue >> 8);
		aBuffer[aPosition] = (byte)(aValue);
	}


	public static void putInt64(byte[] aBuffer, int aOffset, long aValue)
	{
		aBuffer[aOffset + 7] = (byte)(aValue);
		aBuffer[aOffset + 6] = (byte)(aValue >>> 8);
		aBuffer[aOffset + 5] = (byte)(aValue >>> 16);
		aBuffer[aOffset + 4] = (byte)(aValue >>> 24);
		aBuffer[aOffset + 3] = (byte)(aValue >>> 32);
		aBuffer[aOffset + 2] = (byte)(aValue >>> 40);
		aBuffer[aOffset + 1] = (byte)(aValue >>> 48);
		aBuffer[aOffset] = (byte)(aValue >>> 56);
	}


	public static long getInt64(byte[] aBuffer, int aOffset)
	{
		return ((0xFF & aBuffer[aOffset + 7]))
			+ ((0xFF & aBuffer[aOffset + 6]) << 8)
			+ ((0xFF & aBuffer[aOffset + 5]) << 16)
			+ ((long)(0xFF & aBuffer[aOffset + 4]) << 24)
			+ ((long)(0xFF & aBuffer[aOffset + 3]) << 32)
			+ ((long)(0xFF & aBuffer[aOffset + 2]) << 40)
			+ ((long)(0xFF & aBuffer[aOffset + 1]) << 48)
			+ ((long)(0xFF & aBuffer[aOffset]) << 56);
	}


	// little endian
	public static void putInt64LE(byte[] aBuffer, int aOffset, long aValue)
	{
		aBuffer[aOffset] = (byte)(aValue);
		aBuffer[aOffset + 1] = (byte)(aValue >>> 8);
		aBuffer[aOffset + 2] = (byte)(aValue >>> 16);
		aBuffer[aOffset + 3] = (byte)(aValue >>> 24);
		aBuffer[aOffset + 4] = (byte)(aValue >>> 32);
		aBuffer[aOffset + 5] = (byte)(aValue >>> 40);
		aBuffer[aOffset + 6] = (byte)(aValue >>> 48);
		aBuffer[aOffset + 7] = (byte)(aValue >>> 56);
	}


	// little endian
	public static long getInt64LE(byte[] aBuffer, int aOffset)
	{
		return ((0xFF & aBuffer[aOffset]))
			+ ((0xFF & aBuffer[aOffset + 1]) << 8)
			+ ((0xFF & aBuffer[aOffset + 2]) << 16)
			+ ((long)(0xFF & aBuffer[aOffset + 3]) << 24)
			+ ((long)(0xFF & aBuffer[aOffset + 4]) << 32)
			+ ((long)(0xFF & aBuffer[aOffset + 5]) << 40)
			+ ((long)(0xFF & aBuffer[aOffset + 6]) << 48)
			+ ((long)(0xFF & aBuffer[aOffset + 7]) << 56);
	}


	public static void toLong(byte[] aSource, long[] aDest)
	{
		for (int i = 0; i < aDest.length; i++)
		{
			aDest[i] = getInt64(aSource, 8 * i);
		}
	}
}
