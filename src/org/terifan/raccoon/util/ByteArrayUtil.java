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


	public static int getInt(byte[] aBuffer, int aPosition)
	{
		return ((aBuffer[aPosition] & 255) << 24)
			+ ((aBuffer[aPosition + 1] & 255) << 16)
			+ ((aBuffer[aPosition + 2] & 255) << 8)
			+ ((aBuffer[aPosition + 3] & 255));
	}


	public static void putInt(byte[] aBuffer, int aPosition, int aValue)
	{
		aBuffer[aPosition++] = (byte)(aValue >>> 24);
		aBuffer[aPosition++] = (byte)(aValue >> 16);
		aBuffer[aPosition++] = (byte)(aValue >> 8);
		aBuffer[aPosition] = (byte)(aValue);
	}


	public static void putLong(byte[] aBuffer, int aOffset, long aValue)
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


	public static long getLong(byte[] aBuffer, int aOffset)
	{
		return ((255 & aBuffer[aOffset + 7]))
			+ ((255 & aBuffer[aOffset + 6]) << 8)
			+ ((255 & aBuffer[aOffset + 5]) << 16)
			+ ((long)(255 & aBuffer[aOffset + 4]) << 24)
			+ ((long)(255 & aBuffer[aOffset + 3]) << 32)
			+ ((long)(255 & aBuffer[aOffset + 2]) << 40)
			+ ((long)(255 & aBuffer[aOffset + 1]) << 48)
			+ ((long)(255 & aBuffer[aOffset]) << 56);
	}


	// little endian
	public static void putLongLE(byte[] aBuffer, int aOffset, long aValue)
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
	public static long getLongLE(byte[] aBuffer, int aOffset)
	{
		return ((255 & aBuffer[aOffset]))
			+ ((255 & aBuffer[aOffset + 1]) << 8)
			+ ((255 & aBuffer[aOffset + 2]) << 16)
			+ ((long)(255 & aBuffer[aOffset + 3]) << 24)
			+ ((long)(255 & aBuffer[aOffset + 4]) << 32)
			+ ((long)(255 & aBuffer[aOffset + 5]) << 40)
			+ ((long)(255 & aBuffer[aOffset + 6]) << 48)
			+ ((long)(255 & aBuffer[aOffset + 7]) << 56);
	}
}
